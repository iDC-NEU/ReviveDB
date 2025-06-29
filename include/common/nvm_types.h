#ifndef NVMDB_TYPES_H
#define NVMDB_TYPES_H

#include "common/nvm_utils.h"

namespace NVMDB {

using uint64 = unsigned long long;
using uint32 = unsigned int;
using uint16 = unsigned short;
using uint8 = unsigned char;
using int32 = signed int;
using int64 = signed long long;

static constexpr size_t MAX_TUPLE_LEN = 8192;

using PointerOffset = uint64;

using RowId = uint32;
static constexpr RowId InvalidRowId = 0xFFFFFFFF;
static constexpr RowId MaxRowId = InvalidRowId - 1;
static inline bool RowIdIsValid(RowId rowId) {
    return ((rowId) != InvalidRowId);
}

using TableId = uint32;

static constexpr uint32 InvalidColId = 0xFFFFFFFF;

static constexpr uint32 BIS_PER_BYTE = 8;
static constexpr uint32 BIS_PER_U32 = BIS_PER_BYTE * sizeof(uint32);

using UndoRecPtr = uint64;

// 一般情况下，TxSlot 和 CSN 都会共有一个变量，如 tuple 里的txInfo，index 里的 value，
// 所以需要在范围上予以区分。TxSlot 的首bit肯定是0，因此以 1<<63 为分界线。
static constexpr uint64 MIN_TX_CSN = ((1LLU << 63) + 1);
static constexpr uint64 INVALID_CSN = (1LLU << 63);

inline bool IsCSNValid(uint64 csn) {
    return csn != 0 && csn >= MIN_TX_CSN;
}

inline bool TxInfoIsCSN(uint64 txInfo) {
    return txInfo >= MIN_TX_CSN;
}

inline bool TxInfoIsTxSlot(uint64 txInfo) {
    return txInfo < INVALID_CSN;
}

static constexpr uint32 NVM_PAGE_SIZE = 8192;
static constexpr uint32 NVMInvalidPageId = 0;

static inline bool NVMPageIdIsValid(uint32 pageId) {
    return pageId != NVMInvalidPageId;
}

}  // namespace NVMDB

#endif // NVMDB_TYPES_H