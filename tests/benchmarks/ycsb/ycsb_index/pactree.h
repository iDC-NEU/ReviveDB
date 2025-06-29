#include "common/nvm_types.h"
#include "common/pdl_art/string_key.h"
#include "common/serializer.h"
#include "transaction/nvm_transaction.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cmath>
namespace NVMDB {
namespace YCSB {
// pac-tree index相关hack
class PacTreeIndex {
private:
    static void encode(Key_t &pacKey, RowId pKey, RowId rowId)
    {
        BinaryWriter writer(pacKey.getData());
        writer.write_uint32(pKey);
        writer.write_uint32(rowId);
        pacKey.keyLength = writer.get_size();
        CHECK(pacKey.keyLength == sizeof(RowId) * 2);
    }
    static RowId decode(Key_t &pacKey, RowId pKey)
    {
        CHECK(pacKey.keyLength == sizeof(RowId) * 2);
        BinaryReader reader(pacKey.getData());
        CHECK(pKey == reader.read_uint32());
        return reader.read_uint32();
    }

public:
    static void Insert(RowId pKey, RowId rowId, Transaction *txn)
    {
        /** see void Transaction::IndexInsert(NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId) {
            Key_t key;
            this->PrepareUndo();
            index->Encode(indexTuple, &key, rowId);
            PrepareIndexInsertUndo(key);
            index->Insert(indexTuple, rowId);
         }*/
        Key_t pacKey;
        txn->PrepareUndo();
        encode(pacKey, pKey, rowId);
        txn->PrepareIndexInsertUndo(pacKey);
        GetGlobalPACTree()->Insert(pacKey, INVALID_CSN);  // see NVMIndex::Insert()
    }
    static void Delete(RowId pKey, RowId rowId, Transaction *txn)
    {
        /** see void Transaction::IndexDelete(NVMIndex *index, DRAMIndexTuple *indexTuple, RowId rowId) {
            Key_t key;
            this->PrepareUndo();
            index->Encode(indexTuple, &key, rowId);
            PrepareIndexDeleteUndo(key);
            index->Delete(indexTuple, rowId, this->GetTxSlotLocation());
         }*/
        Key_t pacKey;
        txn->PrepareUndo();
        encode(pacKey, pKey, rowId);
        txn->PrepareIndexDeleteUndo(pacKey);
        GetGlobalPACTree()->Insert(pacKey, txn->GetTxSlotLocation());  // see NVMIndex::Delete()
    }
    static void Get(RowId pKey, Transaction *txn, std::vector<RowId> &rids)
    {
        // see NVMIndex::GenerateIter()
        Key_t kb;
        Key_t ke;
        encode(kb, pKey, 0);
        encode(ke, pKey, 0xffffffff);
        // see NVMIndexIter::search()
        std::vector<std::pair<Key_t, Val_t>> result;
        size_t currentResultSize;
        do {
            currentResultSize = result.size();
            GetGlobalPACTree()->scan(kb, ke, 6, txn->GetIndexLookupSnapshot(), false, result);
        } while (result.size() - currentResultSize == 6);

        for (auto &iter : result) {
            rids.push_back(decode(iter.first, pKey));
        }
    }
};
}  // namespace YCSB
}  // namespace NVMDB