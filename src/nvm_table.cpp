#include "nvm_table.h"
#include "heap/nvm_heap.h"
#include "heap/nvm_rowid_map.h"

namespace NVMDB {

uint32 Table::CreateSegment() {
    // 在Table space中分配跨多个连续 pages 的一段空间, 将首部 page id保存在 m_segHead 中
    m_segHead = g_heapSpace->allocNewExtent(EXT_SIZE_2M);
    // m_rowLen: 每一行的长度
    m_rowIdMap = GetRowIdMap(m_segHead, m_rowLen);
    return m_segHead;
}

void Table::Mount(uint32 segHead) {
    m_segHead = segHead;
    m_rowIdMap = GetRowIdMap(segHead, m_rowLen);
}

uint32 Table::GetColIdByName(const char *name) const {
    uint32 i;
    uint32 max = m_desc.col_cnt;
    if (name == nullptr) {
        return InvalidColId;
    }

    for (i = 0; i < max; i++) {
        if (strcmp(name, m_desc.col_desc[i].m_colName) == 0) {
            break;
        }
    }
    return (i < max ? i : InvalidColId);
}

}  // namespace NVMDB
