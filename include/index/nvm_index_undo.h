#ifndef NVMDB_INDEX_UNDO_H
#define NVMDB_INDEX_UNDO_H

#include "index/nvm_index.h"
#include "transaction/nvm_transaction.h"

namespace NVMDB {
void UndoIndexInsert(const UndoRecord *undo);

void UndoIndexDelete(const UndoRecord *undo);

}  // namespace NVMDB

#endif  // NVMDB_INDEX_UNDO_H
