#ifndef NVMDB_UNDOAPI_H
#define NVMDB_UNDOAPI_H

#include "undo/nvm_undo_context.h"

namespace NVMDB {

// initdb 时调用, Undo 单独占一个tablespace。(is this true? 第一个数据页面里存所有segment的位置)。
inline void UndoCreate() {
    UndoSegmentCreate();
}

/* 数据库启动时调用，初始化基本信息，启动清理线程。 */
inline void UndoBootStrap() {
    UndoSegmentMount();
}

/* 事务启动时调用，绑定事务的 undo context；事务执行过程中通过UndoLocalContext插入undo日志 */
inline std::unique_ptr<UndoTxContext> AllocUndoContext() {
    SwitchUndoSegmentIfFull();
    UndoSegment *undoSegment = GetThreadLocalUndoSegment();
    uint64 slotId = undoSegment->getNextTxSlot();
    auto ret = std::make_unique<UndoTxContext>(undoSegment, slotId);
    /*
     * As the undo recycle runs background,
     * Only after modified the allocated txSlot, it's safe to advance.
     */
    undoSegment->advanceTxSlot();
    return ret;
}

/* 正常退出时清理undo信息 */
inline void UndoExitProcess() {
    UndoSegmentUnmount();
}

}

#endif // NVMDB_UNDOAPI_H