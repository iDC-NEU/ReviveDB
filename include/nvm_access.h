#ifndef NVMDB_HEAP_ACCESS_H
#define NVMDB_HEAP_ACCESS_H

#include "transaction/nvm_transaction.h"
#include "nvm_table.h"

namespace NVMDB {

/* heap access method status */
enum class HamStatus {
    OK,
    READ_ROW_NOT_USED,       // the rowid is not used
    NO_VISIBLE_VERSION,      // there is no visible version
    UPDATE_CONFLICT,         // another transaction are updating this version
    ROW_DELETED,             // the row is deleted
    WAIT_ABORT,  // an error happens so the transaction has to be aborted
};

RowId HeapUpperRowId(const Table *table);

RowId HeapInsert(Transaction *tx, Table *table, RAMTuple *tuple);

std::pair<std::unique_ptr<RAMTuple>, RowId> HeapInsertEmptyTuple(Transaction *tx, Table *table);

HamStatus HeapRead(const Transaction *tx, const Table *table, RowId rowid, RAMTuple *tuple);

HamStatus HeapUpdate(Transaction *tx, Table *table, RowId rowid, RAMTuple *new_tuple);

HamStatus HeapUpdate2(Transaction *tx, Table *table, RowId rowid, RAMTuple *new_tuple);

HamStatus HeapDelete(Transaction *tx, Table *table, RowId rowid);

}  // namespace NVMDB

#endif  // NVMDB_HEAP_ACCESS_H