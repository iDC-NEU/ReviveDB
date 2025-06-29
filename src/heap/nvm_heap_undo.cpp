#include "heap/nvm_heap_undo.h"
#include "heap/nvm_tuple.h"
#include "heap/nvm_rowid_map.h"
#include "transaction/nvm_transaction.h"

namespace NVMDB {
static constexpr size_t UNDO_DATA_MAX_SIZE = MAX_UNDO_RECORD_CACHE_SIZE - NVMTupleHeadSize;

UndoRecPtr PrepareInsertUndo(Transaction *tx, uint32 segHead, RowId rowId, uint16 rowLen) {
    auto *undo = reinterpret_cast<UndoRecord *>(tx->undoRecordCache);
    undo->m_undoType = HeapInsertUndo;
    undo->m_rowLen = rowLen;
    undo->m_segHead = segHead;
    undo->m_rowId = rowId;
    undo->m_payload = 0;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_txSlot = tx->GetTxSlotLocation();
#endif
    UndoRecPtr undoPtr = tx->insertUndoRecord(undo);
    return undoPtr;
}

#define DELTA_UNDO_HEAD (sizeof(UndoColumnDesc))

/* offset | length | data */
static inline uint64 DeltaUndoSize(uint32 updateCnt, uint64 updateLen) {
    return DELTA_UNDO_HEAD * updateCnt + updateLen;
}

inline void PackDeltaUndo(const char *rowData, UndoColumnDesc *updatedCols, uint32 updateCnt, char *packData) {
    char *packDataEnd = packData + UNDO_DATA_MAX_SIZE;
    DCHECK(packData != nullptr);
    for (uint32 i = 0; i < updateCnt; i++) {
        int ret = memcpy_no_flush_nt(packData, packDataEnd - packData, &updatedCols[i], DELTA_UNDO_HEAD);
        SecureRetCheck(ret);
        packData += DELTA_UNDO_HEAD;
        ret = memcpy_no_flush_nt(packData, packDataEnd - packData, rowData + updatedCols[i].m_colOffset, updatedCols[i].m_colLen);
        SecureRetCheck(ret);
        packData += updatedCols[i].m_colLen;
    }
}

inline void UnpackDeltaUndo(char *rowData, const char *packData, uint64 deltaLen) {
    UndoColumnDesc updatedCol{};
    while (deltaLen > 0) {
        int ret = memcpy_no_flush_nt(&updatedCol, DELTA_UNDO_HEAD, packData, DELTA_UNDO_HEAD);
        SecureRetCheck(ret);
        packData += DELTA_UNDO_HEAD;
        ret = memcpy_no_flush_nt(rowData + updatedCol.m_colOffset, updatedCol.m_colLen, packData, updatedCol.m_colLen);
        SecureRetCheck(ret);
        packData += updatedCol.m_colLen;
        deltaLen -= (DELTA_UNDO_HEAD + updatedCol.m_colLen);
    }
    DCHECK(deltaLen == 0);
}

UndoRecPtr PrepareUpdateUndo(Transaction *tx, uint32 segHead, RowId rowid, const NVMTuple& oldTuple, const UndoUpdatePara &para) {
    uint64 deltaLen = DeltaUndoSize(para.m_updateCnt, para.m_updateLen);
    auto *undo = reinterpret_cast<UndoRecord *>(tx->undoRecordCache);
    undo->m_undoType = HeapUpdateUndo;
    undo->m_rowLen = oldTuple.m_dataSize;
    undo->m_deltaLen = deltaLen;
    undo->m_segHead = segHead;
    undo->m_rowId = rowid;
    undo->m_payload = NVMTupleHeadSize + deltaLen;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_txSlot = tx->GetTxSlotLocation();
#endif
    int ret = memcpy_no_flush_nt(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &oldTuple, NVMTupleHeadSize);
    SecureRetCheck(ret);
    PackDeltaUndo(oldTuple.m_data, para.m_updatedCols, para.m_updateCnt, undo->data + NVMTupleHeadSize);
    UndoRecPtr undoPtr = tx->insertUndoRecord(undo);

    return undoPtr;
}

UndoRecPtr PrepareDeleteUndo(Transaction *tx, uint32 segHead, RowId rowid, const NVMTuple& oldTuple) {
    auto *undo = reinterpret_cast<UndoRecord *>(tx->undoRecordCache);
    undo->m_undoType = HeapDeleteUndo;
    undo->m_rowLen = oldTuple.m_dataSize;
    undo->m_segHead = segHead;
    undo->m_rowId = rowid;
    undo->m_payload = oldTuple.m_dataSize + NVMTupleHeadSize;
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_txSlot = tx->GetTxSlotLocation();
#endif
    int ret = memcpy_no_flush_nt(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &oldTuple, undo->m_payload);
    SecureRetCheck(ret);
    UndoRecPtr undoPtr = tx->insertUndoRecord(undo);
    return undoPtr;
}

void UndoInsert(const UndoRecord *undo) {
    RowIdMap *rowidMap = GetRowIdMap(undo->m_segHead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId, false);
    row->Lock();
    const auto setUsedFunc = [](char* addr) {
        auto* tuple = reinterpret_cast<NVMTuple *>(addr);
        tuple->m_isUsed = true;
    };
    row->wrightThroughCache(setUsedFunc, NVMTupleHeadSize);
    row->Unlock();
}

void UndoUpdate(const UndoRecord *undo) {
    RowIdMap *rowidMap = GetRowIdMap(undo->m_segHead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId, false);
    row->Lock();
    const auto nvmFunc = [&](char* addr) {
        auto ret = memcpy_no_flush_nt(addr, RealTupleSize(undo->m_rowLen), undo->data, NVMTupleHeadSize);
        SecureRetCheck(ret);
        UnpackDeltaUndo(addr + NVMTupleHeadSize, undo->data + NVMTupleHeadSize, undo->m_deltaLen);
    };
    row->wrightThroughCache(nvmFunc, RealTupleSize(undo->m_rowLen));
    row->Unlock();
}

void UndoUpdate(const UndoRecord *undo, NVMTuple *tuple, char* rowData) {
    int ret = memcpy_no_flush_nt(tuple, sizeof(NVMTuple), undo->data, NVMTupleHeadSize);
    SecureRetCheck(ret);
    UnpackDeltaUndo(rowData, undo->data + NVMTupleHeadSize, undo->m_deltaLen);
}

void UndoDelete(const UndoRecord *undo) {
    RowIdMap *rowidMap = GetRowIdMap(undo->m_segHead, undo->m_rowLen);
    RowIdMapEntry *row = rowidMap->GetEntry(undo->m_rowId, false);
    row->Lock();
    const auto nvmFunc = [&](char* addr) {
        int ret = memcpy_no_flush_nt(addr, RealTupleSize(undo->m_rowLen), undo->data, undo->m_payload);
        SecureRetCheck(ret);
    };
    row->wrightThroughCache(nvmFunc, undo->m_payload);
    row->Unlock();
}

}  // namespace NVMDB