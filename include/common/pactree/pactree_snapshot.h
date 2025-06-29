#pragma once

#include "heap/nvm_tuple.h"
#include "undo/nvm_undo_segment.h"

namespace NVMDB {

struct LookupSnapshot {
    uint64 snapshot;  // 当前事务的csn
    uint64 min_csn;   // 回收
};

}  // namespace NVMDB
