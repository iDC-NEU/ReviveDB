#ifndef NVMDB_ROWID_MAP_H
#define NVMDB_ROWID_MAP_H

#include "table_space/nvm_table_space.h"
#include "heap/nvm_tuple.h"
#include "nvm_vecstore.h"
#include "common/nvm_spinlock.h"
#include <atomic>
#include <functional>

namespace NVMDB {

bool ForceWriteBackCSN();

void SetForceWriteBackCSN(bool flag);

class RowIdMapEntry {
public:
    inline void Lock() { m_mutex.lock(); }

    [[nodiscard]] inline bool TryLock() { return m_mutex.try_lock(); }

    inline void Unlock() { m_mutex.unlock(); }

    inline bool IsValid() const { return m_isTupleValid; }

    template <typename T=NVMTuple>
    T *loadDRAMCache(size_t tupleSize) {
        // prefetch_from_nvm(m_nvmAddr, tupleSize);
        return reinterpret_cast<T *>(m_nvmAddr);
        //        DCHECK(tupleSize <= MAX_TUPLE_LEN);
        //        // 防止段错误, 重新加载tuple
        //        if (m_dramCache.size() < tupleSize) {
        //            clearCache();
        //        }
        //        if (m_dramCache.empty()) {
        //            m_dramCache.resize(tupleSize);
        //            errno_t ret = memcpy_s(m_dramCache.data(), tupleSize, m_nvmAddr, tupleSize);
        //            SecureRetCheck(ret);
        //        }
        //        return reinterpret_cast<T *>(m_dramCache.data());
    }

    void flushToNVM() {
        //        if (unlikely(m_dramCache.empty())) {
        //            LOG(ERROR) << "DRAM cache is empty!";
        //            return;
        //        }
        //        errno_t ret = memcpy_s(m_nvmAddr, m_dramCache.size(), m_dramCache.data(), m_dramCache.size());
        //        SecureRetCheck(ret);
    }

    void flushHeaderToNVM() {
        //        if (unlikely(m_dramCache.size() < NVMTupleHeadSize)) {
        //            LOG(ERROR) << "DRAM cache is empty!";
        //            return;
        //        }
        //        errno_t ret = memcpy_s(m_nvmAddr, NVMTupleHeadSize, m_dramCache.data(), NVMTupleHeadSize);
        //        SecureRetCheck(ret);
    }

    void flushTxnInfoToNVM() {
        //        if (unlikely(m_dramCache.size() < NVMTupleHeadSize)) {
        //            LOG(ERROR) << "DRAM cache is empty!";
        //            return;
        //        }
        //        auto* nvmTuple = reinterpret_cast<NVMTuple*>(m_nvmAddr);
        //        const auto* dramTuple = reinterpret_cast<NVMTuple*>(m_dramCache.data());
        //        nvmTuple->m_txInfo = dramTuple->m_txInfo;
        //        std::atomic_thread_fence(std::memory_order_release);
    }

    // 针对非read modify write设计, 直接写入
    void wrightThroughCache(const std::function<void(char*)>& nvmFunc, size_t syncSize) {
        nvmFunc(m_nvmAddr);
        //        // 如果缓存size不够, 清理缓存
        //        if (syncSize < m_dramCache.size()) {
        //            clearCache();
        //        }
        //        // 对于未读缓存的tuple, 直接写NVM
        //        if (m_dramCache.empty()) {
        //            nvmFunc(m_nvmAddr);
        //            return;
        //        }
        //        // 对于读缓存的tuple, 先写dram之后刷盘
        //        nvmFunc(m_dramCache.data());
        //        errno_t ret = memcpy_s(m_nvmAddr, syncSize, m_dramCache.data(), syncSize);
        //        SecureRetCheck(ret);
    }

    void Init(char* nvmAddr) {
        m_nvmAddr = nvmAddr;
        // dramSurrogateKey在读取或更新时被设置
        //        m_dramSurrogateKey = INVALID_CSN;
        //        clearCache();
        std::atomic_thread_fence(std::memory_order_release);
        m_isTupleValid = true;
    }

    // 写入前需要加锁
    inline void setSurrogateKey(uint64_t surrogateKey) { m_dramSurrogateKey = surrogateKey; }

    // 读取前也需要加锁
    [[nodiscard]] inline uint64_t getSurrogateKey() const { return m_dramSurrogateKey; }

public:
    // 每张表, 每个线程一个LRU, 引用计数相关
    int increaseReference() { return m_referenceCount.fetch_add(1); }

    int decreaseReference() { return m_referenceCount.fetch_sub(1); }

    int getReferenceCount(std::memory_order memoryOrder) const { return m_referenceCount.load(memoryOrder); }

    // 清理缓存 (此操作会释放空间)
    inline void clearAndShrinkCache() { std::vector<char>().swap(m_dramCache); }

    void addReadRef() { m_readCount += 1; }

    void addWriteRef() { m_writeCount += 1; }

    void clearRef() { m_writeCount = 0; m_readCount = 0; }

    bool needCache() const {
        if (ForceWriteBackCSN()) {
            return true;
        }
        // 插入时候不增加写计数，因此需要缓存
        if (m_readCount > m_writeCount * 4) {
            return true;
        }
        return false;
    }

protected:
    // 清理缓存 (此操作不释放空间)
    inline void clearCache() { m_dramCache.clear(); }

private:
    SpinLock m_mutex;
    bool m_isTupleValid = false;

    uint32 m_readCount = 0;
    uint32 m_writeCount = 0;

private:
    // DRAM中缓存的SurrogateKey, 避免访问NVM造成的读放大
    uint64_t m_dramSurrogateKey = INVALID_CSN;
    // 其余部分为真实tuple
    char *m_nvmAddr = nullptr;
    // 引用计数, 当变为0时销毁对应缓存
    std::atomic<int> m_referenceCount = {0};
    std::vector<char> m_dramCache = {};
};

namespace {
constexpr int RowIdMapSegmentLen = 256 * 1024;
constexpr int SegmentEntryLen = UINT32_MAX / RowIdMapSegmentLen;
}

class RowIdMap {
public:
    // rowLen: 定长存储, 每一行数据的最大长度
    RowIdMap(TableSpace *tableSpace, uint32 segHead, uint32 rowLen) : m_rowLen(rowLen) {
        m_rowidMgr = new RowIDMgr(tableSpace, segHead, rowLen);
        auto tuplesPerExtent = m_rowidMgr->getTuplesPerExtent();
        m_vecStore = new VecStore((int)tableSpace->getDirConfig()->size(), segHead, tuplesPerExtent);
    }

    std::pair<RowId, char*> getNextEmptyRow(uint64 txInfo) {
        while (true) {
            // 尝试在Table对应的 segments 中找到一个空的 rowId
            RowId rowId = m_vecStore->tryNextRowid();
            // 对应的页面是否存在。以及是否有人占，重启之后需要这种方法来确认。
            char *tuple = m_rowidMgr->getNVMTupleByRowId(rowId, true);
            auto *tupleHead = reinterpret_cast<NVMTuple *>(tuple);
            DCHECK(tupleHead != nullptr);
            if (tupleHead->m_isUsed) {
                if (!m_isInsertInit) {
                    m_vecStore->tryNextSegment();
                }
                continue;   // 重试, row被占用
            }
            if (!m_isInsertInit) {
                m_isInsertInit = true;
            }
            tupleHead->m_txInfo = txInfo;
            std::atomic_thread_fence(std::memory_order_release);
            // 确定这个RowId对应的 heap 为空
            return std::make_pair(rowId, tuple);
        }
    }

    inline uint32 GetRowLen() const { return m_rowLen; }

    RowId getUpperRowId() const { return m_rowidMgr->getUpperRowId(); }

    RowIdMapEntry *GetEntry(RowId rowId, bool isRead);

protected:
    RowIdMapEntry *GetSegment(int segId);

private:
    const uint32 m_rowLen;

    static thread_local bool m_isInsertInit;

    std::mutex m_segmentsMutex;
    std::unique_ptr<std::array<RowIdMapEntry, RowIdMapSegmentLen>> m_segments[SegmentEntryLen];

    VecStore *m_vecStore;

    // 为 Table 提供进一步抽象 rowId 为一个 Table 中的行 ID
    RowIDMgr *m_rowidMgr = nullptr;
};

RowIdMap *GetRowIdMap(uint32 segHead, uint32 row_len);

void InitGlobalRowIdMapCache();

void InitLocalRowIdMapCache();

void DestroyGlobalRowIdMapCache();

void DestroyLocalRowIdMapCache();

}  // namespace NVMDB

#endif  // NVMDB_ROWID_MAP_H
