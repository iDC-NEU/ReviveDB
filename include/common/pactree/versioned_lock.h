#pragma once

#include "common/nvm_utils.h"
#include "glog/logging.h"
#include <emmintrin.h>

using version_t = unsigned long;

namespace {
    static constexpr version_t defaultVersion = 0x00000010;
    static constexpr int minBackoff = 1 << 2;
    static constexpr int genIdOffset = 32;
    static constexpr int maxBackoff = 1 << 8;
}

class VersionedLock {
public:
    inline version_t isLocked(uint32_t genId) const {
        auto ver = m_ver.load(std::memory_order_acquire);
        uint32_t m_genId = ver >> genIdOffset;

        DCHECK(m_genId <= genId);   // 数据库启动版本会越来越大
        if (unlikely(m_genId < genId)) {
            // version已经过期, 构造一个新的version并返回
            // 高32位为generation id, 低31位为 version, 最后一位为锁位
            return defaultVersion + (static_cast<version_t>(genId) << genIdOffset);
        }
        // version有效时, 检查是否被上写锁
        if (unlikely((ver & 0x00000001) != 0)) {
            return 0;   // 被上写锁, 重试
        }
        return ver; // 返回读取时的版本
    }

    inline version_t tryLock(uint32_t genId) {
        version_t ver = m_ver.load(std::memory_order_acquire);
        uint32_t m_genId = ver >> genIdOffset;

        DCHECK(m_genId <= genId);   // 数据库启动版本会越来越大
        if (unlikely(m_genId < genId)) {
            version_t newVer = defaultVersion + (static_cast<version_t>(genId) << genIdOffset) + 0x00000001;
            if (!m_ver.compare_exchange_strong(ver, newVer, std::memory_order_acquire, std::memory_order_relaxed)) {
                return 0;   // 存在并发读写冲突
            }
            return newVer;  // 修改成功, 并已经上锁
        }
        // version有效时, 检查是否被上写锁
        if (unlikely((ver & 0x00000001) != 0)) {
            return 0;   // 已经被上锁, 重试
        }
        if (!m_ver.compare_exchange_strong(ver, ver+1, std::memory_order_acquire, std::memory_order_relaxed)) {
            return 0;   // 存在并发读写冲突
        }
        return ver;
    }

public:
    // 等待并发写入完成, 返回当前版本
    // generationId: 每次数据库启动时加一
    inline version_t waitUntilUnlocked(uint32_t genId) const {
        int backoffCount = minBackoff;
        while (true) {
            version_t ver = isLocked(genId);
            if (likely(ver != 0)) { // 不存在并发写入
                return ver; // 返回读取时的版本
            }
            for (int i = 0; i < backoffCount; ++i) {
                _mm_pause();    // Intel x86 intrinsic to add a delay
            }
            backoffCount = std::min(backoffCount << 1, maxBackoff);
        }
    }

    // 上锁, 一定不会返回失败
    inline version_t lock(uint32_t genId) {
        int backoffCount = minBackoff;
        while (true) {
            version_t ver=tryLock(genId);
            if (likely(ver != 0)) {
                return ver;
            }
            for (int i = 0; i < backoffCount; ++i) {
                _mm_pause();    // Intel x86 intrinsic to add a delay
            }
            backoffCount = std::min(backoffCount << 1, maxBackoff);
        }
    }

    [[nodiscard]] bool checkVersionEqual(version_t oldVer) const {
        version_t ver = m_ver.load(std::memory_order_acquire);
        return ver == oldVer;
    }

    void unlock() {
        m_ver.fetch_add(1, std::memory_order_release);
    }

private:
    std::atomic<version_t> m_ver = {defaultVersion};
};