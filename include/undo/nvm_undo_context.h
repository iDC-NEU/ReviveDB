#ifndef NVMDB_UNDO_CONTEXT_H
#define NVMDB_UNDO_CONTEXT_H

#include "undo/nvm_undo_segment.h"
#include "undo/nvm_undo_rollback.h"

namespace NVMDB {

/* undo context，跟一个事务绑定 */
class UndoTxContext {
public:
    UndoTxContext(UndoSegment *undoSegment, uint32 slotId)
        : m_slotId(slotId), m_undoSegment(undoSegment) {
        DCHECK(m_slotId == m_undoSegment->getNextFreeSlot());
        m_slot = m_undoSegment->getTxSlot(m_slotId, true);
        m_slot->status = TxSlotStatus::IN_PROGRESS;
    }

    void UpdateTxSlotCSN(uint64 csn) {
        m_slot->csn = csn;
        std::atomic_thread_fence(std::memory_order_release);
    }

    void UpdateTxSlotStatus(TxSlotStatus status) {
        m_slot->status = status;
    }

    // 根据segment id和segment 中的slot id生成全局slot id
    inline TxSlotPtr GetTxSlotLocation() const {
        auto segmentId = m_undoSegment->getSegmentId();
        DCHECK(segmentId < NVMDB_UNDO_SEGMENT_NUM);
        return ((uint64)segmentId << TSP_SLOT_ID_BIT) | m_slotId;
    }

    // 将 cachedUndoRecPtr 插入到 nvm 中, 并做链表链接
    UndoRecPtr insertUndoRecord(UndoRecord *cachedUndoRecPtr) {
        cachedUndoRecPtr->m_pre = m_slot->end;
        UndoRecPtr nvmUndoRecPtr = m_undoSegment->insertUndoRecord(cachedUndoRecPtr);
        if (UndoRecPtrIsInValid(m_slot->start)) {
            m_slot->start = nvmUndoRecPtr;
        }
        DCHECK(m_slot->end < nvmUndoRecPtr);
        m_slot->end = nvmUndoRecPtr;
        DCHECK(m_slot->end >= m_slot->start);
        return nvmUndoRecPtr;
    }

    inline void RollBack(UndoRecord* undoRecordCache) {
        UndoRecordRollBack(m_undoSegment, m_slot, undoRecordCache);
    }

private:
    uint64 m_slotId;
    TxSlot *m_slot;
    UndoSegment *m_undoSegment;
};

}

#endif // NVMDB_UNDO_CONTEXT_H