#ifndef NVMDB_INDEX_TUPLE_H
#define NVMDB_INDEX_TUPLE_H

#include "heap/nvm_tuple.h"
#include "common/serializer.h"

namespace NVMDB {

struct IndexColumnDesc {
    uint32 m_colId;
    uint64 m_colLen;
    uint64 m_colOffset;
};

inline void InitIndexDesc(IndexColumnDesc *indexDes, const ColumnDesc *rowDes, const uint32 &colCnt, uint64 &indexLen) {
    DCHECK(indexDes != nullptr && rowDes != nullptr && colCnt != 0);
    uint64 offset = 0;
    for (uint32 i = 0; i < colCnt; i++) {
        uint32 colId = indexDes[i].m_colId;
        uint64 colLen = rowDes[colId].m_colLen;
        indexDes[i].m_colLen = colLen;
        indexDes[i].m_colOffset = offset;
        offset += colLen;
    }
    indexLen = offset;
}

inline bool IsIndexTypeSupported(ColumnType indexType) {
    if (indexType == COL_TYPE_INT || indexType == COL_TYPE_UNSIGNED_LONG || indexType == COL_TYPE_VARCHAR) {
        return true;
    }
    return false;
}

/* 内存中的 IndexTuple */
class DRAMIndexTuple {
public:
    const ColumnDesc *const m_rowDes;
    const IndexColumnDesc *const m_indexDes;
    const uint32 m_colCnt;
    char *m_indexData;
    uint64 m_indexLen;

    DRAMIndexTuple(const ColumnDesc *const rowDes, const IndexColumnDesc *const indexDes, uint32 colCnt, uint64 indexLen)
        : m_rowDes(rowDes), m_indexDes(indexDes), m_colCnt(colCnt), m_indexLen(indexLen) {
        DCHECK(indexLen <= KEYLENGTH);
        m_indexData = new char[indexLen];
        int ret = memset_s(m_indexData, indexLen, 0, indexLen);
        SecureRetCheck(ret);
    }

    ~DRAMIndexTuple() {
        delete[] m_indexData;
    }

    void ExtractFromTuple(RAMTuple *tuple) const {
        DCHECK(m_rowDes == tuple->m_rowDes);    // 检测定义相同
        const char *const rowData = tuple->m_rowDataPtr;
        uint64 offset = 0;
        for (uint32 i = 0; i < m_colCnt; i++) {
            uint32 colId = m_indexDes[i].m_colId;
            const char *const colData = &rowData[m_rowDes[colId].m_colOffset];
            uint64 colLen = m_rowDes[colId].m_colLen;
            int ret = memcpy_s(m_indexData + offset, m_indexLen - offset, colData, colLen);
            SecureRetCheck(ret);
            offset += colLen;
        }
        DCHECK(offset == m_indexLen);
    }

    inline void SetCol(const uint32 indexColId, const char *const colData, bool isVarChar = false) const {
        DCHECK(indexColId < m_colCnt);
        if (isVarChar) {
            uint32 varLen = *((uint32 *)colData) + sizeof(uint32);
            // For tpcc testing
            // uint32 varLen = strlen(colData);
            int ret = memcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset,
                               m_indexDes[indexColId].m_colLen,
                               colData,
                               varLen);
            SecureRetCheck(ret);
        } else {
            int ret = memcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset,
                               m_indexDes[indexColId].m_colLen,
                               colData,
                               m_indexDes[indexColId].m_colLen);
            SecureRetCheck(ret);
        }
    }

    inline void SetCol(const uint32 indexColId, const char *const colData, uint64 len) const {
        DCHECK(indexColId < m_colCnt);
        DCHECK(m_indexDes[indexColId].m_colLen >= len);
        int ret = memcpy_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen, colData, len);
        SecureRetCheck(ret);
    }

    inline void FillColWith(const uint32 indexColId, char data, uint64 len) const {
        DCHECK(indexColId < m_colCnt);
        DCHECK(m_indexDes[indexColId].m_colLen >= len);
        int ret = memset_s(m_indexData + m_indexDes[indexColId].m_colOffset, m_indexDes[indexColId].m_colLen, data, len);
        SecureRetCheck(ret);
    }

    inline char *GetCol(const uint32 indexColId) const {
        return m_indexData + m_indexDes[indexColId].m_colOffset;
    }

    inline void Encode(BinaryWriter& writer) const {
        uint64 offset = 0;
        for (uint32 i = 0; i < m_colCnt; i++) {
            uint32 colId = m_indexDes[i].m_colId;
            uint64 colLen = m_rowDes[colId].m_colLen;
            char *colData = m_indexData + offset;
            switch (m_rowDes[colId].m_colType) {
                case COL_TYPE_INT: {
                    // writer.write_uint8(CODE_INT32);
                    writer.write_int32(*(int32 *)colData);
                    break;
                }
                case COL_TYPE_UNSIGNED_LONG: {
                    // writer.write_uint8(CODE_UINT64);
                    writer.write_uint64(*(uint64 *)colData);
                    break;
                }
                case COL_TYPE_VARCHAR: {
                    // writer.write_uint8(CODE_VARCHAR);
                    uint32 varLen = *((uint32 *)colData);
                    writer.write_string(colData + sizeof(uint32), varLen);
                    // For tpcc testing
                    // writer.write_string(colData);
                    break;
                }
                default: {
                    CHECK(false);
                }
            }
            offset += colLen;
        }
        DCHECK(offset == m_indexLen);
    }

    void Copy(const DRAMIndexTuple *tuple) const {
        DCHECK(m_rowDes == tuple->m_rowDes);
        DCHECK(m_indexDes == tuple->m_indexDes);

        int ret = memcpy_s(m_indexData, m_indexLen, tuple->m_indexData, m_indexLen);
        SecureRetCheck(ret);
    }

    void Copy(const DRAMIndexTuple *tuple, uint32 colIndex) const {
        DCHECK(m_rowDes == tuple->m_rowDes);
        DCHECK(m_indexDes == tuple->m_indexDes);
        DCHECK(colIndex < tuple->m_colCnt);

        int ret = memcpy_s(m_indexData + m_indexDes[colIndex].m_colOffset, m_indexDes[colIndex].m_colLen,
                           tuple->m_indexData + m_indexDes[colIndex].m_colOffset, m_indexDes[colIndex].m_colLen);
        SecureRetCheck(ret);
    }
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_TUPLE_H
