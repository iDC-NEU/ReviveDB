#include "undo/nvm_undo_rollback.h"
#include "heap/nvm_heap_undo.h"
#include "index/nvm_index_undo.h"
#include <string>

namespace NVMDB {

using NVMUndoFunc = void (*)(const UndoRecord*);

struct NVMUndoProcedure {
   UndoRecordType type;
   std::string name;
   NVMUndoFunc undoFunc;
};

static NVMUndoProcedure g_nvmUndoFuncs[] = {
   {InvalidUndoRecordType, "", nullptr},
   {HeapInsertUndo, "HeapInsertUndo", UndoInsert},
   {HeapUpdateUndo, "HeapUpdateUndo", UndoUpdate},
   {HeapDeleteUndo, "HeapDeleteUndo", UndoDelete},
   {IndexInsertUndo, "IndexInsertUndo", UndoIndexInsert},
   {IndexDeleteUndo, "IndexDeleteUndo", UndoIndexDelete},
};

void UndoRecordRollBack(UndoSegment* segment, TxSlot* txSlot, UndoRecord* undoRecordCache) {
    // RollBack is called during bootstrap
    // the undo log has not been written yet,
    // so no data need to be rollback
    if (UndoRecPtrIsInValid(txSlot->start)) {
        DCHECK(UndoRecPtrIsInValid(txSlot->end));
        return;
    }
    // rollback the data
    UndoRecPtr undoRecPtr = txSlot->end;
    while (!UndoRecPtrIsInValid(undoRecPtr)) {    // iterator from bottom to top
        DCHECK(undoRecPtr >= txSlot->start && undoRecPtr <= txSlot->end);   // prevent overflow
        // 在nvm中读取对应 record, 并保存在cache中
        segment->getUndoRecord(undoRecPtr, undoRecordCache);
        // 回滚对应undo记录
        if (UndoRecordTypeIsValid((UndoRecordType)undoRecordCache->m_undoType)) {
            NVMUndoProcedure *procedure = &g_nvmUndoFuncs[undoRecordCache->m_undoType];
            DCHECK(procedure->type == undoRecordCache->m_undoType);
            procedure->undoFunc(undoRecordCache);
        }
        undoRecPtr = undoRecordCache->m_pre;
    }
}

}