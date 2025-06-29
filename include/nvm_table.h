#ifndef NVMDB_TABLE_H
#define NVMDB_TABLE_H

#include "heap/nvm_rowid_map.h"
#include "index/nvm_index.h"
#include <vector>

namespace NVMDB {

struct TableDesc {  // 表的定义
    ColumnDesc *col_desc = nullptr; // 每个字段的定义
    uint32 col_cnt = 0; // 列数, 总共有几个字段
    uint64 row_len = 0; // 每一行记录的长度(NVMDB每行记录长度固定)
};

// 创建表时候调用, colCount 表有几列
// 返回初始化好的desc NVM中表的定义
inline bool TableDescInit(TableDesc *desc, uint32 colCount) {
    desc->col_cnt = colCount;
    desc->row_len = 0;
    desc->col_desc = new (std::nothrow) ColumnDesc[colCount];
    return desc->col_desc != nullptr;
}

inline void TableDescDestroy(TableDesc *desc) {
    desc->col_cnt = 0;
    desc->row_len = 0;
    delete[] desc->col_desc;
}

class Table {
public:
    // m_rowIdMap: 一张 Table 一个, 可以通过 row id 找到对应的 nvm tuple (table 内 row id 唯一)
    RowIdMap *m_rowIdMap = {nullptr};
    // 每一行的长度
    uint64 m_rowLen = {0};

    Table(TableId tid, uint64 rowLen) noexcept
        : m_tableId(tid), m_rowLen(rowLen), m_rowIdMap(nullptr) { }

    Table(TableId tid, TableDesc desc) noexcept
        : m_tableId(tid), m_rowLen(desc.row_len), m_desc(desc), m_rowIdMap(nullptr) { }

    bool Ready() const {
        return m_rowIdMap != nullptr;
    }

    /* 新建的表必须先申请一个 segment, 返回 segment 页号 */
    uint32 CreateSegment();

    /* 已经建好的表，重启之后需要 mount segment，传参的是 segment 页号 */
    void Mount(uint32 segHead);

    uint32 SegmentHead() const {
        return m_segHead;
    }

    uint32 GetColIdByName(const char *name) const;

    uint32 GetColCount() const noexcept {
        return m_desc.col_cnt;
    }

    const ColumnDesc *GetColDesc(uint32 colIndex) const noexcept {
        DCHECK(colIndex < m_desc.col_cnt && m_desc.col_desc != nullptr);
        return &(m_desc.col_desc[colIndex]);
    }

    const ColumnDesc *GetColDesc() const noexcept {
        DCHECK(m_desc.col_desc != nullptr);
        return m_desc.col_desc;
    }

    uint64 GetRowLen() const noexcept {
        return m_rowLen;
    }

    uint32 GetIndexCount() const {
        return index.size();
    }

    NVMIndex *GetIndex(uint16 num) const {
        DCHECK(num < index.size());
        if (num < index.size()) {
            return index[num];
        } else {
            return nullptr;
        }
    }

    void AddIndex(NVMIndex *i) {
        index.push_back(i);
    }

    NVMIndex *DelIndex(IndexId id) {
        NVMIndex *ret = nullptr;
        for (auto iter = index.begin(); iter != index.end(); ++iter) {
            if ((*iter)->Id() == id) {
                ret = *iter;
                index.erase(iter);
                break;
            }
        }
        return ret;
    }

    void RefCountInc() {
        ++refCount;
    }

    void RefCountDec() {
        DCHECK(refCount > 0);
        if (--refCount == 0) {
            DCHECK(isDropped);
            delete this;
        }
    }

    uint32 RefCount() const {
        return refCount;
    }

    bool IsDropped() const {
        return isDropped;
    }

    void Dropped() {
        isDropped = true;
    }

    TableId Id() const {
        return m_tableId;
    }

    bool ColIsNotNull(uint32 colIndex) const {
        return m_desc.col_desc[colIndex].m_isNotNull;
    }

    ~Table() { TableDescDestroy(&m_desc); }

private:
    TableId m_tableId{0};
    uint32 m_segHead{0};
    TableDesc m_desc;
    std::vector<NVMDB::NVMIndex *> index;
    std::atomic<uint32> refCount{0};
    std::atomic<bool> isDropped{false};
};

}  // namespace NVMDB

#endif  // NVMDB_TABLE_H