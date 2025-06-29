#pragma once

#include "transaction/nvm_transaction.h"
#include "nvm_access.h"

namespace NVMDB {
/*
 * input: tx, index, tbl, index_tuple,
 * output: RowID, res
 */
inline RowId UniqueSearch(Transaction *tx,
                          NVMIndex *index,
                          Table *tbl,
                          DRAMIndexTuple *index_tuple,
                          RAMTuple *res,
                          bool assert = false) {
    auto ss = tx->GetIndexLookupSnapshot();
    auto iter = index->GenerateIter(index_tuple, index_tuple, ss, 1, false);
    while (iter->Valid()) {
        RowId row_id = iter->Curr();
        HamStatus status = HeapRead(tx, tbl, row_id, res);
        if (status == HamStatus::OK) {
            delete iter;
            return row_id;
        }
        iter->Next();
    }
    CHECK(!assert);
    delete iter;
    return InvalidRowId;
}

inline RowId RangeSearchMin(Transaction *tx,
                            NVMIndex *index,
                            Table *tbl,
                            DRAMIndexTuple *idx_begin,
                            DRAMIndexTuple *idx_end,
                            RAMTuple *res) {
    auto ss = tx->GetIndexLookupSnapshot();
    auto iter = index->GenerateIter(idx_begin, idx_end, ss, 0, false);
    while (iter->Valid()) {
        RowId row_id = iter->Curr();
        HamStatus status = HeapRead(tx, tbl, row_id, res);
        if (status == HamStatus::OK) {
            delete iter;
            return row_id;
        }
        iter->Next();
    }

    delete iter;
    return InvalidRowId;
}

inline RowId RangeSearchMax(Transaction *tx,
                            NVMIndex *index,
                            Table *tbl,
                            DRAMIndexTuple *idx_begin,
                            DRAMIndexTuple *idx_end,
                            RAMTuple *res) {
    auto ss = tx->GetIndexLookupSnapshot();
    auto iter = index->GenerateIter(idx_begin, idx_end, ss, 0, false);
    RowId max_rid = InvalidRowId;
    while (iter->Valid()) {
        RowId row_id = iter->Curr();
        HamStatus status = HeapRead(tx, tbl, row_id, res);
        if (status == HamStatus::OK) {
            max_rid = row_id;
        }
        iter->Next();
    }

    if (RowIdIsValid(max_rid)) {
        HamStatus status = HeapRead(tx, tbl, max_rid, res);
        DCHECK(status == HamStatus::OK);
    }

    return max_rid;
}

inline void RangeSearch(Transaction *tx,
                        NVMIndex *index,
                        Table *tbl,
                        DRAMIndexTuple *idx_begin,
                        DRAMIndexTuple *idx_end,
                        int max_range,
                        int *res_size,
                        RowId *row_ids,
                        RAMTuple **tuples) {
    auto ss = tx->GetIndexLookupSnapshot();
    auto iter = index->GenerateIter(idx_begin, idx_end, ss, 0, false);
    int i = 0;
    while (iter->Valid() && i < max_range) {
        row_ids[i] = iter->Curr();
        HamStatus status = HeapRead(tx, tbl, row_ids[i], tuples[i]);
        if (status == HamStatus::OK) {
            i++;
        }
        iter->Next();
    }

    delete iter;
    *res_size = i;
}

}
