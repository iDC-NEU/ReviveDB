#pragma once

#include "common/nvm_spinlock.h"
#include "common/nvm_cfg.h"
#include "glog/logging.h"
#include <vector>
#include <mutex>
#include <thread>

namespace NVMDB {

class ProcessArray {
public:
    // 系统最多支持多少个线程
    explicit ProcessArray(int maxCount)
        : m_processArray(maxCount) { }

    // return process index
    size_t addProcess() {
        while (true) {
            const auto index = (m_arrayIndexCounter++) % m_processArray.size();
            auto& procStruct = m_processArray[index];
            if (procStruct.m_inUsed.load(std::memory_order_relaxed)) {
                continue;   // trick
            }
            std::lock_guard<PassiveSpinner42> guard(procStruct.mutex);
            if (procStruct.m_inUsed.load(std::memory_order_relaxed)) {
                continue;
            }
            // 事务基于至少这个版本进行读取, 因此不会产生小于 m_snapshotCSN 的 Undo 日志
            auto snapshotCSN = m_globalCSN.load(std::memory_order_relaxed);
            procStruct.m_snapshotCSN.store(snapshotCSN, std::memory_order_relaxed);
            // LOG(INFO) << "Create a txn process, id: " << index;
            // 最后设置 in used
            procStruct.m_inUsed.store(true, std::memory_order_release);
            return index;
        }
    }

    void removeProcess(size_t index) {
        auto& procStruct = m_processArray[index];
        std::lock_guard<PassiveSpinner42> guard(procStruct.mutex);
        DCHECK(index < m_processArray.size());
        DCHECK(m_processArray[index].m_inUsed);
        m_processArray[index].m_inUsed.store(false, std::memory_order_release);
        // LOG(INFO) << "Destroy a txn process, id: " << index;
    }

    // 事务将基于这个返回的CSN进行读取
    uint64 getAndUpdateProcessLocalCSN(uint32 index) {
        // 所有低于 m_globalCSN 的事务均已经完成执行, 可以使用 m_globalCSN 作为读取的版本
        auto globalCSN = m_globalCSN.load(std::memory_order_relaxed);
        auto& procStruct = m_processArray[index];
        std::lock_guard<PassiveSpinner42> guard(procStruct.mutex);
        procStruct.m_snapshotCSN.store(globalCSN, std::memory_order_relaxed);
        return globalCSN;
    }

    [[nodiscard]] uint64 getProcessLocalCSN(uint32 index) const {
        auto& procStruct = m_processArray[index];
        std::lock_guard<PassiveSpinner42> guard(procStruct.mutex);
        DCHECK(procStruct.m_inUsed);
        return procStruct.m_snapshotCSN.load(std::memory_order_relaxed);
    }

    // 更新最小全局 csn 来回收日志
    uint64 getAndUpdateGlobalMinCSN() {
        uint64 globalMinCSN = m_globalCSN.load(std::memory_order_relaxed);
        for (auto & procStruct : m_processArray) {
            if (procStruct.m_inUsed.load(std::memory_order_acquire)) {  // trick
                // 可能读到过期的 m_snapshotCSN 但是因为 m_snapshotCSN 是单调递增的, 不影响正确性
                globalMinCSN = std::min(globalMinCSN, procStruct.m_snapshotCSN.load(std::memory_order_relaxed));
                continue;
            }
            std::lock_guard<PassiveSpinner42> guard(procStruct.mutex);
            if (!procStruct.m_inUsed.load(std::memory_order_relaxed)) {
                continue;   // 如果没有在使用, 一定没有在使用, 可以安全继续
            }
            // 找到所有process中CSN最小的那个
            globalMinCSN = std::min(globalMinCSN, procStruct.m_snapshotCSN.load(std::memory_order_relaxed));
        }
        DCHECK(globalMinCSN >= m_globalMinCSN.load(std::memory_order_relaxed));
        m_globalMinCSN.store(globalMinCSN, std::memory_order_release);
        return globalMinCSN;
    }

    // 提交时间戳, 几个事务的提交时间戳可以相同
    [[nodiscard]] inline uint64 getGlobalCSN() const {
        return m_globalCSN.load(std::memory_order_relaxed);
    }

    // 事物被提交, 更新全局 csn
    inline uint64 advanceGlobalCSN() {
        return m_globalCSN.fetch_add(1, std::memory_order_relaxed);
    }

    // 低于该版本的 Undo 日志可以被安全回收
    [[nodiscard]] uint64 getGlobalMinCSN() const {
        return m_globalMinCSN.load(std::memory_order_relaxed);
    }

    // 小于m_globalCSN的事务都已经被提交了, 启动时设置
    void setRecoveredCSN(uint64 maxUndoCSN) {
        DCHECK(IsCSNValid(maxUndoCSN));
        m_globalCSN = maxUndoCSN + 1;
    }

public:
    inline static void InitGlobalProcArray() {
        DCHECK(g_processArray == nullptr);
        g_processArray = std::make_unique<ProcessArray>(NVMDB_MAX_THREAD_NUM);
    }

    inline static void DestroyGlobalProcArray() {
        g_processArray = nullptr;
    }

    inline static auto* GetGlobalProcArray() {
        return g_processArray.get();
    }

private:
    struct Process {
        mutable PassiveSpinner42 mutex;
        std::atomic<bool> m_inUsed = {false};
        std::atomic<uint64> m_snapshotCSN = {MIN_TX_CSN};
    };

    std::vector<Process> m_processArray;

    // 单调递增的指针, 用于判断哪个 index 被使用
    std::atomic<uint64> m_arrayIndexCounter = {0};

    // 下一个提交交易的CSN
    // 小于m_globalCSN的heap不会再被写, 可以被安全的读取
    std::atomic<uint64> m_globalCSN = {MIN_TX_CSN};

    // 回收水位线
    // 小于m_globalMinCSN的Undo不会再被读或写, 可以被安全回收
    std::atomic<uint64> m_globalMinCSN = {MIN_TX_CSN};

    static std::unique_ptr<ProcessArray> g_processArray;
};

}