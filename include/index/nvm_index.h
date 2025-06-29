#ifndef NVMDB_INDEX_H
#define NVMDB_INDEX_H

#include "index/nvm_index_iter.h"
#include "index/nvm_index_tuple.h"

namespace NVMDB {

/* idx id + tag + row id */
// only use for FDW
static constexpr uint32 KEY_EXTRA_LENGTH = sizeof(uint32)  + 1  + sizeof(uint32) ;
static constexpr uint32 KEY_DATA_LENGTH = KEYLENGTH - KEY_EXTRA_LENGTH;

// allocate pactree
void IndexBootstrap();

void IndexExitProcess();

// regist thread to pactree
void InitLocalIndex();

void DestroyLocalIndex();

inline IndexColumnDesc *IndexDescCreate(uint32 colCount) {
    DCHECK(colCount != 0);
    return new IndexColumnDesc[colCount];
}

inline void IndexDescDelete(IndexColumnDesc *desc) {
    delete[] desc;
}

// 类型为oid, 唯一确定一个index
using IndexId = uint32;

class NVMIndex {
    IndexId m_idxId;
    uint32 m_colCnt = 0;
    uint64 m_rowLen = 0;
    IndexColumnDesc *m_indexDes = nullptr;
    uint8 *m_colBitmap = nullptr;

public:
    explicit NVMIndex(IndexId id)
        : m_idxId(id) { }

    ~NVMIndex() {
        delete[] m_colBitmap;
        IndexDescDelete(m_indexDes);
    }

    void Encode(DRAMIndexTuple *tuple, Key_t *key, RowId rowId) const {
        BinaryWriter writer(key->getData());
        // For tpcc testing
        // writer.write_uint8(m_idxId);
        writer.write_uint32(m_idxId);
        tuple->Encode(writer);
        writer.write_uint8(CODE_ROWID);
        writer.write_uint32(rowId);
        key->keyLength = writer.get_size();
        CHECK(key->keyLength <= KEYLENGTH) << (int)key->keyLength;
    }

    /* find begin <= key <= end */
    NVMIndexIter *GenerateIter(DRAMIndexTuple *begin,
                               DRAMIndexTuple *end,
                               LookupSnapshot snapshot,
                               int max_range,
                               bool reverse) const {
        Key_t kb;
        Key_t ke;
        Encode(begin, &kb, 0);
        Encode(end, &ke, 0xffffffff);

        auto iter = new NVMIndexIter(kb, ke, snapshot, max_range, reverse);
        return iter;
    }

    void Insert(DRAMIndexTuple *tuple, RowId rowId) const {
        PACTree *pt = GetGlobalPACTree();
        Key_t key;
        Encode(tuple, &key, rowId);
        pt->Insert(key, INVALID_CSN);
    }

    void Delete(DRAMIndexTuple *tuple, RowId rowId, TxSlotPtr tx) const {
        PACTree *pt = GetGlobalPACTree();
        Key_t key;
        Encode(tuple, &key, rowId);
        pt->Insert(key, tx);
    }

    // 表总共有几列
    bool SetNumTableFields(uint32 num) {
        DCHECK(num <= NVMDB_TUPLE_MAX_COL_COUNT);
        m_colBitmap = new uint8[BITMAP_GETLEN(num)]{0};
        DCHECK(m_colBitmap != nullptr);
        return m_colBitmap != nullptr;
    }

    // 指定列被索引了
    void FillBitmap(const uint32 colid) noexcept {
        if (m_colBitmap != nullptr) {
            BITMAP_SET(m_colBitmap, colid);
        } else {
            CHECK(false);
        }
    }

    // 查找指定列是否被索引
    bool IsFieldPresent(uint32 colid) const noexcept {
        DCHECK(m_colBitmap != nullptr);
        return BITMAP_GET(m_colBitmap, colid);
    }

    uint32 GetColCount() const noexcept {
        return m_colCnt;
    }

    uint32 GetRowLen() const noexcept {
        return m_rowLen;
    }

    void SetIndexDesc(IndexColumnDesc *desc, uint32 colCount, uint64 rowLen) noexcept {
        m_indexDes = desc;
        m_colCnt = colCount;
        m_rowLen = rowLen;
    }

    const IndexColumnDesc *GetIndexDesc() const noexcept {
        return m_indexDes;
    }

    IndexId Id() const {
        return m_idxId;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_H
