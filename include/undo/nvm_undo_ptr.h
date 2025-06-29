#ifndef NVMDB_UNDO_PTR_H
#define NVMDB_UNDO_PTR_H

#include "common/nvm_types.h"
#include "glog/logging.h"

namespace NVMDB {

static int UNDO_REC_PTR_OFFSET_BITS = 48;
static const UndoRecPtr InvalidUndoRecPtr = 0;

static inline UndoRecPtr AssembleUndoRecPtr(uint64 segId, uint64 offset) {
    DCHECK((offset >> UNDO_REC_PTR_OFFSET_BITS) == 0);
    return (segId << UNDO_REC_PTR_OFFSET_BITS) | offset;
}

static inline bool UndoRecPtrIsInValid(UndoRecPtr undo) {
    return undo == 0;
}

static inline uint32 UndoRecPtrGetSegment(UndoRecPtr undo) {
    return undo >> UNDO_REC_PTR_OFFSET_BITS;
}

// 获取一个segment文件中的偏移量
static inline uint64 UndoRecPtrGetOffset(UndoRecPtr undo) {
    return undo & ((1LLU << UNDO_REC_PTR_OFFSET_BITS) - 1);
}

}

#endif