#pragma once

#include "common/nvm_cfg.h"
#include "glog/logging.h"
#include <memory>
#include <vector>
#include <x86intrin.h>

namespace NVMDB {
/*
 *  一个逻辑上的大文件，向外展示连续的页号；给定一个页号会翻译成对应的虚拟地址。
 *  内部实现会切成多个segment，每个segment是一个物理文件，mmap到虚拟地址空间中。支持不同长度的segment。
 *  支持 create、mount、unmount、extend、truncate操作。
 *
 *  Heap的tablespace和 Undo的segment都可以继承这个类，使用其文件管理、地址翻译的功能。
 */
class LogicFile {
public:
    // 每个segment的大小
    // tablespace 1 GB for release， 10 MB for debug
    // undo segment 1M for debug, 16M for release
    [[nodiscard]] inline auto getSegmentSize() const { return m_segmentSize; }

    // 存储每个segment用到的pages数量
    // tablespace 1280 in debug
    // undo segment 128 in debug
    [[nodiscard]] inline auto getPagesPerSegment() const { return m_pagesPerSegment; }

    /*  segment_addr 数组初始化的时候就设够足够大，否则当数组扩展的时候，会影响并发的读 */
    LogicFile(std::shared_ptr<DirectoryConfig> dirConfig,
              std::string spaceName,
              size_t segmentSize,
              size_t maxSegmentCount)
        : m_dirConfig(std::move(dirConfig)),
          m_spaceName(std::move(spaceName)),
          m_segmentSize(segmentSize), // 每个segment的大小, 10M in debug
          m_pagesPerSegment(segmentSize / NVM_PAGE_SIZE) {
        m_segmentAddr.reserve(maxSegmentCount);
        CHECK(mMapFile(0, true)) << "Mount failed!";  // create file if not exist
    }

    // 当前已经分配的segment数量
    inline size_t segmentCount() const { return m_segmentAddr.size(); }

    // 最大支持的segment数量 16 * 1024, store in memory
    inline size_t segmentCapacity() const { return m_segmentAddr.capacity(); }

    // input global page number (for table space)
    void extend(uint32 pageId) {
        mMapFile(pageId / m_pagesPerSegment, true);
    }

    void punch(uint32 startSegmentId, uint32 endSegmentId) {
        CHECK(startSegmentId < endSegmentId);
        for (auto i = startSegmentId; i < endSegmentId; i++) {
            reMMapFile(i);
        }
    }

    void truncate() { }

protected:
    template <typename T=void*>
    [[nodiscard]] inline T nvmAddrFromSegmentId(uint32 segmentId) const {
        return reinterpret_cast<T>(m_segmentAddr[segmentId]);
    }

public:
    void mount() {
        CHECK(nvmAddrFromSegmentId(0) != nullptr) << "Init contains error!";
        for (auto i = 1; i < segmentCapacity(); i++) {
            if (!mMapFile(i, false)) {
                break;  // the segment does not exist
            }
        }
    }

    void unmount() {
        for (auto i = 0; i < segmentCount(); i++) {
            unMMapFile(i);
        }
        m_segmentAddr.clear();
    }

    // 输入global page号, 返回Logic file中对应的地址
    // return nvm pointer of the segment (the offset of the page with pageId)
    [[nodiscard]] void* getNvmAddrByPageId(uint32 globalPageId) const {
        auto pagesPerSegment = getPagesPerSegment();
        auto segmentId = globalPageId / pagesPerSegment;
        CHECK(segmentId < segmentCount()) << "PageId overflow!";
        auto* nvmAddr = nvmAddrFromSegmentId<char*>(segmentId);
        CHECK(nvmAddr != nullptr) << "Cannot find nvmAddr by pageId";
        return nvmAddr + static_cast<uint64>(globalPageId % pagesPerSegment) * NVM_PAGE_SIZE;
    }

protected:
    static inline void flush(const void *src, size_t n) {
        const auto *s = (const uint8_t *)src;
        for (size_t i = 0; i < n; i += 64) {
            _mm_clflushopt((void *)(s + i));
        }
    }

    static void WriteToNVM(void *dest, const void *src, size_t n) {
        DCHECK(((uintptr_t)src & 0x3F) == 0);    // 64字节对齐

        auto *d = (uint8_t *)dest;
        const auto *s = (const uint8_t *)src;

        // 对于大块数据，使用AVX-512指令集
        if (n >= 64) {
            size_t blocks = n / 64;
            for (size_t i = 0; i < blocks; i++) {
                // 使用非时序存储加载和写入到NVM，因为之后不会访问这些数据
                __m512i data = _mm512_stream_load_si512((__m512i *)(s + i * 64));
                _mm512_storeu_si512((__m512i *)(d + i * 64), data);
            }

            // 确保所有非时序写入已完成，不清除DRAM缓存行，因为一会要写
            _mm_sfence();
            for (size_t i = 0; i < blocks; i++) {
                // 清除NVM缓存行
                _mm_clflushopt((void *)(d + i * 64));
            }

            // 更新指针位置
            s += blocks * 64;
            d += blocks * 64;
            n -= blocks * 64;

            // 处理剩余字节, 不需要flush，因为之后会追加
            if (n > 0) {
                memcpy(d, s, n);
            }
            return;
        }
        // 对于小数据，直接复制，并按情况逐出缓存
        memcpy(d, s, n);
    }

public:
    // vptr 为当前segment中的虚拟地址, 页号 + offset
    void seekAndWrite(uint64 vptr, const char *src, size_t len) {
        auto segmentSpaceRemain = m_segmentSize - (vptr % m_segmentSize);
        uint32 pageId = vptr / NVM_PAGE_SIZE;
        uint32 offset = vptr % NVM_PAGE_SIZE;
        this->extend(pageId); // make sure the page is mounted
        auto* firstPageAddr = static_cast<char*>(this->getNvmAddrByPageId(pageId));
        if (segmentSpaceRemain >= len) {
            WriteToNVM(firstPageAddr + offset, src, len);
            return;
        }
        this->extend(pageId + 1);
        // CHECK((pageId + 1) % m_pagesPerSegment == 0);
        auto* secondPageAddr = static_cast<char*>(this->getNvmAddrByPageId(pageId + 1));
        if (secondPageAddr == firstPageAddr + NVM_PAGE_SIZE) {
            WriteToNVM(firstPageAddr + offset, src, len);
            return;
        }
        // segment_remain < len
        WriteToNVM(firstPageAddr + offset, src, segmentSpaceRemain);
        // copy the rest to pageId + 1
        auto ret = memcpy_no_flush_nt(secondPageAddr, len - segmentSpaceRemain, src + segmentSpaceRemain, len - segmentSpaceRemain);
        SecureRetCheck(ret);
    }

    // 根据虚拟地址读指定长度的片段
    void seekAndRead(uint64 vptr, char *dst, size_t len) {
        CHECK(len < m_segmentSize) << "length overflow";
        auto segmentSpaceRemain = m_segmentSize - (vptr % m_segmentSize);
        uint32 pageId = vptr / NVM_PAGE_SIZE;
        uint32 offset = vptr % NVM_PAGE_SIZE;
        this->extend(pageId);
        auto* nvmAddr = static_cast<char*>(getNvmAddrByPageId(pageId));
        if (segmentSpaceRemain >= len) {
            errno_t ret = memcpy_no_flush_nt(dst, len, nvmAddr + offset, len);
            SecureRetCheck(ret);
            return;
        }

        // segmentSpaceRemain < len
        errno_t ret = memcpy_no_flush_nt(dst, segmentSpaceRemain, nvmAddr + offset, segmentSpaceRemain);
        SecureRetCheck(ret);
        CHECK((pageId + 1) % m_pagesPerSegment == 0);
        this->extend(pageId + 1);
        ret = memcpy_no_flush_nt(dst + segmentSpaceRemain, len - segmentSpaceRemain,
                                 getNvmAddrByPageId(pageId + 1), len - segmentSpaceRemain);
        SecureRetCheck(ret);
    }

protected:
    // for undo: undo0
    std::string m_spaceName;

    // for undo seg: 16, store nvmAddr of each segment
    // segment_addr 数组初始化的时候就设够足够大，否则当数组扩展的时候，会影响并发的读
    std::vector<void *> m_segmentAddr;

    std::shared_ptr<DirectoryConfig> m_dirConfig;

    const size_t m_segmentSize;

    const size_t m_pagesPerSegment;

public:
    // if init is true, the file will be deleted and recreate
    bool mMapFile(uint32 segmentId, bool init);

    void unMMapFile(uint32 segmentId, bool destroy = false);

    void reMMapFile(uint32 segmentId);

private:
    // for heap: data/heap.0
    inline std::string segmentFilename(uint32 segmentId) const {
        const auto& dirs = m_dirConfig->getDirPaths();
        return dirs[segmentId % dirs.size()] + "/" + m_spaceName + '.' + std::to_string(segmentId);
    }
};

}  // namespace NVMDB