#ifndef NVMDB_UNDO_PAGE_H
#define NVMDB_UNDO_PAGE_H

#include "heap/nvm_tuple.h"

namespace NVMDB {

enum class TxSlotStatus {
    EMPTY = 0,   /* slot 还没有被分配 */
    IN_PROGRESS = 1, /* 事务正在进行中 */
    COMMITTED, /* 事务已提交，等过一段时间undo就可以失效了 */
    ABORTED,   /* 事务已经回滚，但是没有完成undo */
    ROLL_BACKED   /* undo完成，transaction slot可复用 */
};

/* 持久化的事务信息 */
struct TxSlot {
    volatile uint64 csn;
    UndoRecPtr start;
    UndoRecPtr end;
    volatile TxSlotStatus status;
};

struct TransactionInfo {
    TxSlotStatus status;
    uint64 csn;
};

}

#endif // NVMDB_UNDO_PAGE_H