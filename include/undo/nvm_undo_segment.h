#ifndef NVMDB_UNDO_SEGMENT_H
#define NVMDB_UNDO_SEGMENT_H

#include "undo/nvm_undo_page.h"
#include "undo/nvm_undo_record.h"
#include "table_space/nvm_logic_file.h"
#include <atomic>

namespace NVMDB {

/*
 * 前16位， segment id,  后48位，segment 内 tx slot id
 * 但是实际不会有 1<<48 这么多个 tx slot，实际在文件头部存有 UNDO_TX_SLOTS 个 slot， slot id通过求模映射到对应的位置。
 * segment head 中的 m_minSlotId 表示最小的 tx slot id，更小的都被回收掉了。
 * */
using TxSlotPtr = uint64 ;  // 2048 undo segment, with 8k slots in debug mode

static constexpr int UNDO_TX_SLOTS = CompileValue(256 * 1024, 8 * 1024);
static constexpr size_t UNDO_SEGMENT_SIZE = CompileValue(256 * 1024 * 1024, 1024 * 1024);
static constexpr size_t UNDO_MAX_SEGMENT_NUM = 64;

static constexpr uint32 TSP_SLOT_ID_BIT = 48;
static constexpr uint32 TSP_SEGMENT_ID_BIT = 16;
static constexpr uint64 TSP_SLOT_ID_MASK = (1llu << TSP_SLOT_ID_BIT) - 1;

struct UndoSegmentHead {
    uint64 m_minSnapshot; /* next available csn to boostrap if all slots recycled. */
    uint64 m_freeBegin;  /* next free space for undo record */
    uint64 m_recycledBegin; /* next undo record to be recycled */
    // recovery the Tx slot between recovery_start and recovery_end
    // recovery_start and recovery_end may be bigger than UNDO_TX_SLOTS
    uint64 m_recoveryStart;
    uint64 m_recoveryEnd;
    std::atomic<uint64> m_nextFreeSlot;  /* 下一个可用的 tx slot id; 分配的时候从这里开始 */
    std::atomic<uint64> m_nextRecycleSlot;   /* 下一个需要回收的 slot id;  分配的时候不得超过这个限制，回收的时候会往前推这个下标 */
    std::atomic<uint64> m_minSlotId; /* min transaction slot id; any smaller transactions slot id is recycled */
    TxSlot m_txSlots[UNDO_TX_SLOTS]; /* transaction slots, 2KB, 8192 count */
};

/* ensure the undo segment head can be located in the first segment */
static_assert(UNDO_SEGMENT_SIZE >= sizeof(UndoSegmentHead), "");

static_assert(NVMDB_UNDO_SEGMENT_NUM <= (1 << TSP_SEGMENT_ID_BIT), "");

void UndoRecycle(); // start a thread with this function

class UndoSegment {
public:
    // UNDO_SEGMENT_SIZE 1M for debug 64M for release, UNDO_MAX_SEGMENT_NUM 16
    // 为了NUMA优化, 一条Undo segment只能存在一个path中, 同时应在启动时完成初始化
    // segment 和 segment 同义
    UndoSegment(const std::string& directory, uint32 undoSegmentId)
        : segId(undoSegmentId), // undo segment id
          filename("undo" + std::to_string(undoSegmentId)), // undo0, undo1
          m_logicFile(std::make_shared<DirectoryConfig>(directory), filename, UNDO_SEGMENT_SIZE, UNDO_MAX_SEGMENT_NUM) {
        m_logicFile.mMapFile(0, true);
        segHead = (UndoSegmentHead *)m_logicFile.getNvmAddrByPageId(0);
        CHECK(segHead != nullptr) << "mount segment head failed";
    }

    [[nodiscard]] inline auto getSegmentId() const {return segId; }

    // 获取当前 segment 最大的 undo CSN, 小于该CSN的事务都已经被提交了
    uint64 getMaxCSNForRollback();

    // recover this undo segment in background
    // undoRecordCache没有什么用, 用来避免频繁内存分配
    void backgroundRecovery(UndoRecord* undoRecordCache);

    // create a new undo segment
    inline void create() {
        m_logicFile.mMapFile(1, true);
        m_logicFile.mMapFile(2, true);
        // init the segment head
        // first, clear the data of the UndoSegmentHead
        errno_t ret = memset_s(segHead, sizeof(UndoSegmentHead), 0, sizeof(UndoSegmentHead));
        SecureRetCheck(ret);
        // set free_begin to the offset of segment head
        segHead->m_freeBegin = sizeof(UndoSegmentHead);
        // set recycled_begin to the offset of segment head
        segHead->m_recycledBegin = sizeof(UndoSegmentHead);
        segHead->m_nextFreeSlot = 0;
        segHead->m_nextRecycleSlot = 0;
        segHead->m_minSlotId = 0;
    }

    inline void mount() { m_logicFile.mount(); }

    inline void unmount() { m_logicFile.unmount(); }

    /* take undo record heap into consideration if you loop use it. */
    [[nodiscard]] inline bool isFull() const {
        // transaction slot is full
        return segHead->m_nextFreeSlot.load(std::memory_order_relaxed) ==
               segHead->m_nextRecycleSlot.load(std::memory_order_relaxed) + UNDO_TX_SLOTS;
    }

    [[nodiscard]] inline bool isEmpty() const {
        return segHead->m_nextFreeSlot.load(std::memory_order_relaxed) ==
               segHead->m_nextRecycleSlot.load(std::memory_order_relaxed);
    }

    uint64 getNextTxSlot() {
        DCHECK(!isFull());
        auto nextSlotId = segHead->m_nextFreeSlot.load(std::memory_order_relaxed);
        DCHECK(nextSlotId <= segHead->m_nextRecycleSlot.load(std::memory_order_relaxed) + UNDO_TX_SLOTS);
        return nextSlotId;
    }

    inline void advanceTxSlot() {
        segHead->m_nextFreeSlot.fetch_add(1, std::memory_order_relaxed);
    }

    // Return false means the transaction slot is recycled
    // copy to read tx slot content AS undo recycle runs background.
    // copy txSlot from nvm and return if found
    bool getTransactionInfo(uint64 slotId, TransactionInfo *txInfo) const {
        do {
            const TxSlot* nvmTxSlot = getTxSlot(slotId);
            txInfo->csn = nvmTxSlot->csn;
            txInfo->status = (TxSlotStatus)nvmTxSlot->status;

            if (slotId < segHead->m_minSlotId.load(std::memory_order_acquire)) {
                // already recycled.
                return false;
            }
            /* never load a recycled slot. */
            if (txInfo->status == TxSlotStatus::EMPTY) {
                LOG(WARNING) << "Tx slot is empty, slotId: " << slotId;
                return false;
            }

            if (txInfo->status != TxSlotStatus::COMMITTED) {
                return true;
            }
            // csn is slot or invalid, but tx is committed
            // retry until get the txn csn
            std::atomic_thread_fence(std::memory_order_acquire);
        } while (!TxInfoIsCSN(txInfo->csn));
        return true;
    }

protected:
    bool isTxSlotRecyclable(uint64 slotId, uint64 minSnapshot) const {
        TransactionInfo info = {};
        bool ret = getTransactionInfo(slotId, &info);
        CHECK(ret) << "Cannot get tx info from slot!";
        // minSnapshot: min_csn
        if (info.status == TxSlotStatus::ROLL_BACKED) {
            return true;
        }
        // 只回收提交的, 并且一定不会被再次访问的slot
        if (info.status == TxSlotStatus::COMMITTED && info.csn < minSnapshot) {
            return true;
        }
        return false;
    }

public:
    // return pointer to tx slot when allocated.
    // called when constructing UndoTxContext
    TxSlot *getTxSlot(uint64 slotId, bool prefetch=false) const {
        if (prefetch) {
            __builtin_prefetch(&segHead->m_txSlots[(slotId+1) % UNDO_TX_SLOTS], 1, 3);
            __builtin_prefetch(segHead, 1, 3);
        }
        return &segHead->m_txSlots[slotId % UNDO_TX_SLOTS];
    }

    [[nodiscard]] uint64 getNextFreeSlot() const {
        return segHead->m_nextFreeSlot.load(std::memory_order_relaxed);
    }

    void recycleUndoPages(const uint64&beginSlot, const uint64&endSlot);

    void recycleTxSlot(uint64 minSnapshot);

    UndoRecPtr insertUndoRecord(const UndoRecord *undoRecordCache) {
        auto undoSize = undoRecordCache->m_payload + sizeof(UndoRecord);
        DCHECK(undoSize <= MAX_UNDO_RECORD_CACHE_SIZE);
        // The pointer of the undo record in undo segment
        UndoRecPtr ptr = AssembleUndoRecPtr(segId, segHead->m_freeBegin);
        // write and increase undo segment
        m_logicFile.seekAndWrite(segHead->m_freeBegin, (const char *)undoRecordCache, undoSize);
        segHead->m_freeBegin += undoSize;
        return ptr;
    }

    void getUndoRecord(UndoRecPtr undoRecPtr, UndoRecord* undoRecordCache) {
        DCHECK(UndoRecPtrGetSegment(undoRecPtr) == segId);
        DCHECK(undoRecordCache != nullptr);
        auto vptr = UndoRecPtrGetOffset(undoRecPtr);
        // 先知道 m_payload 有多长
        UndoRecord undo_head {};
        m_logicFile.seekAndRead(vptr, (char *)&undo_head, sizeof(UndoRecord));
        // 读取真正的 m_payload, 并保存在 undoRecordCache 中
        m_logicFile.seekAndRead(vptr, (char *)undoRecordCache, undo_head.m_payload + sizeof(UndoRecord));
    }

private:
    uint32 segId;
    UndoSegmentHead *segHead{}; /* pointer to segment head, note that it's non-volatile */
    std::string filename;
    LogicFile m_logicFile;
};

void UndoSegmentCreate();

void UndoSegmentMount();

void UndoSegmentUnmount();

// return thread local undo segment
UndoSegment *GetThreadLocalUndoSegment();

void SwitchUndoSegmentIfFull();

UndoSegment *GetUndoSegment(int segId);

// txInfo is the returned value, must be allocated in ram
// 如果成功, 更新txInfo的CSN和提交状态
inline bool GetTransactionInfo(TxSlotPtr txSlotPtr, TransactionInfo *txInfo) {
    // 1. 通过segId和slotId定位对应的槽
    auto segId = static_cast<int>(txSlotPtr >> TSP_SLOT_ID_BIT);
    DCHECK(segId < NVMDB_UNDO_SEGMENT_NUM);
    // find the undo segment
    UndoSegment *undo_segment = GetUndoSegment(segId);
    auto slotId = static_cast<uint32>(txSlotPtr & TSP_SLOT_ID_MASK);
    // 2. 在指定segment中找Tx对应的slot, 判断是否还有效
    return undo_segment->getTransactionInfo(slotId, txInfo);
}

// 根据 undo ptr 获得具体的 undo record
inline void GetUndoRecord(UndoRecPtr undoRecPtr, UndoRecord* undoRecordCache) {
    int segId = (int)UndoRecPtrGetSegment(undoRecPtr);
    // 得到对应的 segment
    UndoSegment *segment = GetUndoSegment(segId);
    // 从对应 segment 中读 record, 并保存在 cache 中
    segment->getUndoRecord(undoRecPtr, undoRecordCache);
}

void InitLocalUndoSegment();

void DestroyLocalUndoSegment();

}

#endif  // NVMDB_UNDO_SEGMENT_H