#ifndef NVMDB_HEAP_UNDO_H
#define NVMDB_HEAP_UNDO_H

#include "undo/nvm_undo_record.h"
#include "transaction/nvm_transaction.h"

namespace NVMDB {

UndoRecPtr PrepareInsertUndo(Transaction *tx, uint32 segHead, RowId rowid, uint16 row_len);

UndoRecPtr PrepareUpdateUndo(Transaction *tx, uint32 segHead, RowId rowid, const NVMTuple& old_tuple, const UndoUpdatePara &para);

UndoRecPtr PrepareDeleteUndo(Transaction *tx, uint32 segHead, RowId rowid, const NVMTuple& old_tuple);

void UndoInsert(const UndoRecord *undo);

void UndoUpdate(const UndoRecord *undo);

void UndoDelete(const UndoRecord *undo);

void UndoUpdate(const UndoRecord *undo, NVMTuple *tuple, char* rowData);

}  // namespace NVMDB

#endif  // NVMDB_HEAP_UNDO_H