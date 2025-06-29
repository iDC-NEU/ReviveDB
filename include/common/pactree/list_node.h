#ifndef NVMDB_PACTREE_LNODE_H
#define NVMDB_PACTREE_LNODE_H

#include "common/pactree/pactree_snapshot.h"
#include "common/pactree/versioned_lock.h"
#include "common/pdl_art/string_key.h"
#include "common/pdl_art/nvm_ptr.h"

namespace NVMDB {

constexpr int PERM_MOD = 2;
constexpr int KEY_LENGTH_FIRST = 21;
constexpr int KEY_LENGTH_SECOND = 43;

enum class PACTreeWhiteBoxType {
    NO_BREAKPOINT,
    UPDATE_PERMUTATION,
    SPLIT_STEP1,
    SPLIT_STEP2,
};

void SetPACTreeWhiteBoxBP(PACTreeWhiteBoxType type);

constexpr int MAX_ENTRIES = 95;
constexpr int KV_ALIGN_BYTES = 8;
constexpr int MAX_KV_SIZE_PER_NODE = 192 * KV_ALIGN_BYTES;

#define ALIGN_ANY(size, align) (((size) + (align) - 1) / (align) * (align))

struct LinePointItem {
    uint8_t offset;        // 以8字节为粒度，所以最多表示 8 * 255 = 2KB的数据
    uint8_t fingerPrint;  // 保留 finger print，加速 update 操作
};

struct LinePointArray {
    uint8_t count;
    uint8_t recyclable;
    LinePointItem linePoint[MAX_ENTRIES]{};

    LinePointArray() : count(0), recyclable(0) {}

    uint8_t GetOffset(int index) const {
        return linePoint[index].offset;
    }

    uint8_t GetFingerPrint(int index) const {
        return linePoint[index].fingerPrint;
    }
};  // 总共 128 字节
static_assert(sizeof(LinePointArray) % 64 == 0);

struct KVItem {
    Val_t value;
    VarLenString key;  // 变长key，放到后面，方便直接读
};

class ListNode {
public:
    void MakePrefix();

    ListNode() : nextKv(0), currPerm(0) {}

    bool Insert(Key_t &key, Val_t value, int duringSplit);

    bool Lookup(Key_t &key, Val_t &value);

    /* 调用之前，确保  startKey 在 [min, max) 之间, endKey 没有要求 */
    bool ScanInOrder(Key_t &startKey, Key_t &endKey, int maxRange, LookupSnapshot snapshot,
                     std::vector<std::pair<Key_t, Val_t>> &result, bool continueScan, bool *needPrune);

    void SetCur(NVMPtr<ListNode> ptr) {
        this->curPtr = ptr;
    }

    void SetNext(NVMPtr<ListNode> ptr) {
        this->nextPtr = ptr;
    }

    void SetPrev(NVMPtr<ListNode> ptr) {
        this->prevPtr = ptr;
    }

    void SetMin(const Key_t& key) {
        this->min = key;
    }

    void SetMax(const Key_t& key) {
        this->max = key;
    }

    ListNode *GetNext() const {
        ListNode *next = nextPtr.getVaddr();
        return next;
    }

    NVMPtr<ListNode> GetNextPtr() const {
        return nextPtr;
    }

    ListNode *GetPrev() const {
        ListNode *prev = prevPtr.getVaddr();
        return prev;
    }

    NVMPtr<ListNode> GetPrevPtr() const {
        return prevPtr;
    }

    bool GetDeleted() const {
        return deleted;
    }

    void SetDeleted() {
        deleted = true;
    }

    void Prune(LookupSnapshot snapshot, uint64_t genId);

    const Key_t &GetMin() const {
        return min;
    }

    const Key_t &GetMax() const {
        return max;
    }

    bool CheckRange(Key_t &key) const {
        return min <= key && key < max;
    }

    void RecoverSplit(OpStruct *oplog);

    void Print() const {}

    VersionedLock& getVersionedLock() { return verLock; }

private:
    Key_t min;  // 必须放到第一个， 因为 art 拿到的是 ListNode 指针，需要根据第一个 Key 读取 key
    Key_t max;
    Key_t prefix;
    uint8_t nextKv;  // 下一个 KV Item 的offset
    uint8_t currPerm;
    bool deleted;
    uint8_t unusedVariable[5]{};  // 占位，补齐到8字节对齐

    LinePointArray permutation[2];

    NVMPtr<ListNode> curPtr;
    NVMPtr<ListNode> nextPtr;
    NVMPtr<ListNode> prevPtr;

    VersionedLock verLock;  // 8B
    KVItem kvItems[0];      // 变长数组

    // private function
    inline LinePointArray *GetCurrPerm() {
        return &permutation[currPerm];
    }

    inline LinePointArray *GetNextPerm() {
        return &permutation[(currPerm + 1) % PERM_MOD];
    }

    inline void SwitchPerm() {
        std::atomic_thread_fence(std::memory_order_acq_rel);
        currPerm = (currPerm + 1) % PERM_MOD;
    }

    VarLenString *GetRemainKey(Key_t *origin_key) const;

    void GetOriginKey(VarLenString *remain_key, Key_t *origin_key);

    KVItem *GetKVItem(uint8_t offset);

    static auto GetKVItemSize(KVItem *kv) {
        return sizeof(Val_t) + sizeof(kv->key.keyLength) + kv->key.keyLength;
    }

    static auto GetKVItemSize(VarLenString *key)  {
        return sizeof(Val_t) + sizeof(key->keyLength) + key->keyLength;
    }

    static uint8_t GetKeyFingerPrint(VarLenString *key);

    int GetKeyIndex(VarLenString *key, uint8_t keyHash);

    void UpdateAtIndex(Val_t value, int index);

    bool StorageSpaceFull(int size) const;

    bool PermutationFull();

    int InsertKVItem(VarLenString *key, Val_t value, bool duringSplit = false);

    uint8_t PermuteLowerBound(VarLenString *key);

    void UpdatePermutation(KVItem *kvItem, uint8_t offset, uint8_t keyHash);

    void Shrink();

    NVMPtr<ListNode> Split(Key_t &key, Val_t value);

    void MergeEmptyNodeWithPrev(uint64_t genId);

    enum class MVCCVisibility {
        VISIBLE,
        REMOVABLE,
        INVISIBLE,
    };

    static MVCCVisibility CheckMVCCVisibility(KVItem *kv, LookupSnapshot snapshot);

    void RemoveFromPermutation(std::vector<std::pair<uint8_t, int>> &items);

    void CheckPrefix() {
        auto lpa = GetCurrPerm();
        for (int i = 0; i < lpa->count; i++) {
            auto kv = GetKVItem(lpa->GetOffset(i));
            int len = kv->key.keyLength + prefix.keyLength;
            CHECK(len == KEY_LENGTH_FIRST || len == 0 || len == KEY_LENGTH_SECOND);
        }
    }
};

static_assert(sizeof(ListNode) % 8 == 0, "");
constexpr const int LIST_NODE_SIZE = sizeof(ListNode) + MAX_KV_SIZE_PER_NODE;
// op log should have enough space to store a node, but should not waste too much.
static_assert(LIST_NODE_SIZE <= OpListNodeSize && LIST_NODE_SIZE >= OpListNodeSize - 128, "");

void LNodeReport();
}
#endif  // NVMDB_LNODE_H
