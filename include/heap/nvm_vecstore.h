#ifndef NVMDB_VECSTORE_H
#define NVMDB_VECSTORE_H

#include "heap/nvm_rowid_mgr.h"
#include "table_space/nvm_table_space.h"
#include "common/nvm_global_bitmap.h"
#include <mutex>

namespace NVMDB {
/*
 * All tuples in a table are logically in a vector indexed by row id. The vector is implemented as a two-level page
 * table. The segment head of the table is the first level page (root page), storing page number of all second level
 * pages (leaf pages). Leaf pages store tuples.
 *
 * The procedure of allocated a new RowID:
 *     1. Find a unique RowID according to local cache and global bitmap.
 *     2. If corresponding physic page does not exist, allocating a new one.
 *     3. If corresponding physic page exists, and corresponding tuple is used, then return to step 1 and find a new
 *        RowId. This scenario happens after recovery, as global bitmap is reset. In future, if we make FSM info
 *        persistent, this step can be eliminated.
 */
class VecStore {
public:
    VecStore(int spaceCount, uint32 segHead, uint32 row_len);

    ~VecStore() = default;

    RowId tryNextRowid() const;

    void tryNextSegment() const;

private:
    // Table 入口地址, 为 page id
    uint32 m_segHead = 0;
    // 每次扩展出的table segment可以存储的page数量
    uint32 m_tuplesPerExtent = 0;
    // 每个目录对应一个GlobalBitMap
    std::vector<std::unique_ptr<GlobalBitMap>> m_gbm;
};

}  // namespace NVMDB

#endif  // NVMDB_VECSTORE_H