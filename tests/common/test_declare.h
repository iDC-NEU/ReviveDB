#ifndef NVMDB_TEST_DECLARE_H
#define NVMDB_TEST_DECLARE_H

#include "heap/nvm_rowid_mgr.h"

namespace NVMDB {

const uint32 ColumnSize[] = {sizeof(int32),
                             sizeof(int64),
                             sizeof(float),
                             sizeof(char), /* multiple it. */
                             sizeof(uint64),
                             sizeof(double),
                             sizeof(short),
                             sizeof(char), /* tiny int */
                             sizeof(uint64),
                             sizeof(char), /* multiple it. */
                             18,
                             0};

template <typename ColType>
inline ColumnDesc InitColDesc(ColType colType) {
    ColumnDesc desc{};
    desc.m_colType = static_cast<decltype(desc.m_colType)>(colType);
    desc.m_colLen = ColumnSize[static_cast<int>(colType)];
    return desc;
}

template <typename ColType>
inline ColumnDesc InitVarColDesc(ColType colType, int elemCount) {
    ColumnDesc desc{};
    desc.m_colType = static_cast<decltype(desc.m_colType)>(colType);
    desc.m_colLen = ColumnSize[static_cast<int>(colType)] * elemCount;
    return desc;
}

// 供测试使用, 给出一组rowDes 初始化它们的m_colOffset
inline void InitColumnDesc(ColumnDesc *const rowDes, const uint32 &colCnt, uint64 &rowLen) {
    DCHECK(rowDes != nullptr && colCnt != 0);
    uint64 offset = 0;
    for (uint32 i = 0; i < colCnt; i++) {
        rowDes[i].m_colOffset = offset;
        offset += rowDes[i].m_colLen;
    }
    rowLen = BestTupleLenCalculator::g_btc.getBestAlignment(offset + NVMTupleHeadSize) - NVMTupleHeadSize;
}

}

#endif  // NVMDB_TEST_DECLARE_H
