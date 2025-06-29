#ifndef NVMDB_UNDO_RECORD_H
#define NVMDB_UNDO_RECORD_H

#include "undo/nvm_undo_ptr.h"

namespace NVMDB {

static const int MAX_UNDO_RECORD_CACHE_SIZE = 4096;

struct UndoRecord {
    uint16 m_undoType; // undo record 的大类
    uint16 m_rowLen; // row length
    uint16 m_deltaLen; // total delta data length
    uint32 m_segHead; // tuple 对应的 segment head
    RowId m_rowId;
    uint32 m_payload; // undo 数据长度
    UndoRecPtr m_pre;
#ifndef NDEBUG
    uint32 m_txSlot;
    UndoRecPtr undo_ptr;
#endif
    char data[0]; // Undo 数据
};

enum UndoRecordType {
    InvalidUndoRecordType = 0,
    HeapInsertUndo,
    HeapUpdateUndo,
    HeapDeleteUndo,

    IndexInsertUndo,
    IndexDeleteUndo,

    MaxUndoRecordType,
};

static inline bool UndoRecordTypeIsValid(UndoRecordType type) {
    return type > InvalidUndoRecordType && type < MaxUndoRecordType;
}

}

#endif // NVMDB_UNDO_RECORD_H