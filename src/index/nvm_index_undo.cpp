#include "index/nvm_index_undo.h"

namespace NVMDB {
void UndoIndexInsert(const UndoRecord *undo) {
    uint64 csn = undo->m_segHead;
    csn = (csn << BIS_PER_U32) | undo->m_rowId;
    Key_t key;
    DCHECK(undo->m_payload == sizeof(Key_t));
    int ret = memcpy_s(&key, sizeof(key), undo->data, undo->m_payload);
    SecureRetCheck(ret);
    auto pt = GetGlobalPACTree();
    pt->Insert(key, csn);
}

void UndoIndexDelete(const UndoRecord *undo) {
    Key_t key;
    DCHECK(undo->m_payload == sizeof(Key_t));
    int ret = memcpy_s(&key, sizeof(key), undo->data, undo->m_payload);
    SecureRetCheck(ret);
    auto pt = GetGlobalPACTree();
    pt->Insert(key, INVALID_CSN);
}

}  // namespace NVMDB