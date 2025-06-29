#ifndef NVMDB_UNDO_ROLLBACK_H
#define NVMDB_UNDO_ROLLBACK_H

#include "undo/nvm_undo_segment.h"

namespace NVMDB {

void UndoRecordRollBack(UndoSegment* segment, TxSlot* txSlot, UndoRecord* undoRecordCache);

}

#endif