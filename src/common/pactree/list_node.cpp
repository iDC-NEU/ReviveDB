#include "common/pactree/list_node.h"

namespace NVMDB {

volatile static PACTreeWhiteBoxType g_whiteBoxType;
constexpr int MAX_OFFSET = 255;
constexpr int HASH_LEFT_OFFSET = 5;
constexpr int HASH_RIGHT_OFFSET = 27;
constexpr int SCAN_ITEM_DIV = 2;
constexpr int MAX_RECYCLE = 128;

void SetPACTreeWhiteBoxBP(PACTreeWhiteBoxType type) {
    g_whiteBoxType = type;
}

static void destroy_remain_key(VarLenString *remain_key) {
    auto key = (Key_t *)remain_key;
    delete key;
}

VarLenString *ListNode::GetRemainKey(Key_t *origin_key) const {
    auto *remain_key = new Key_t();
    remain_key->set(origin_key->getData() + prefix.keyLength, origin_key->keyLength - prefix.keyLength);
    return (VarLenString *)remain_key;
}

void ListNode::GetOriginKey(VarLenString *remain_key, Key_t *origin_key) {
    origin_key->set(prefix.getData(), prefix.keyLength);
    memcpy_s(origin_key->getData() + prefix.keyLength, remain_key->keyLength, remain_key->getData(),
        remain_key->keyLength);
    origin_key->keyLength += remain_key->keyLength;
}

void ListNode::MakePrefix() {
    int k = 0;
    int maxLen = std::min(min.keyLength, max.keyLength);
    for (; k < maxLen; k++) {
        if (min.data[k] != max.data[k]) {
            break;
        }
    }
    prefix.set(min.getData(), k);
}

KVItem *ListNode::GetKVItem(uint8_t offset) {
    return (KVItem *)((char *)&kvItems + KV_ALIGN_BYTES * offset);
}

uint8_t ListNode::GetKeyFingerPrint(VarLenString *key) {
    uint32_t length = key->size();
    uint32_t hash = length;
    const char *str = key->getData();

    for (uint32_t i = 0; i < length; ++str, ++i) {
        hash = ((hash << HASH_LEFT_OFFSET) ^ (hash >> HASH_RIGHT_OFFSET)) ^ (*str);
    }
    return static_cast<uint8_t>(hash & 0xFF);
}

int ListNode::GetKeyIndex(VarLenString *key, uint8_t keyHash) {
    const LinePointArray *lpa = GetCurrPerm();
    for (int i = 0; i < lpa->count; i++) {
        if (lpa->GetFingerPrint(i) == keyHash) {
            auto kv = GetKVItem(lpa->GetOffset(i));
            if (kv->key == *key) {
                return i;
            }
        }
    }
    return -1;
}

uint8_t ListNode::PermuteLowerBound(VarLenString *key) {
    auto lpa = GetCurrPerm();
    uint8_t lower = 0;
    uint8_t upper = lpa->count;
    do {
        int mid = ((upper - lower) / 2) + lower;
        auto offset = lpa->linePoint[mid].offset;
        auto kvItem = GetKVItem(offset);
        if (*key > kvItem->key) {
            lower = mid + 1;
        } else {
            upper = mid;
        }
    } while (lower < upper);
    return static_cast<uint8_t>(lower);
}

void ListNode::UpdateAtIndex(Val_t value, int index) {
    LinePointArray *lpa = GetCurrPerm();
    KVItem *kvItem = GetKVItem(lpa->linePoint[index].offset);
    kvItem->value = value;
}

static std::atomic<int> g_linepointFull;
static std::atomic<int> g_storageFull;
static bool g_reported = false;
void LNodeReport() {
    if (g_reported) {
        return;
    }
    g_reported = true;
}

bool ListNode::StorageSpaceFull(int size) const {
    bool full = (nextKv == 255) | ((nextKv * KV_ALIGN_BYTES + size) > MAX_KV_SIZE_PER_NODE);
    if (full) {
        g_storageFull.fetch_add(1);
    }
    return full;
}

bool ListNode::PermutationFull() {
    LinePointArray *lpa = &permutation[currPerm];
    bool full = lpa->count == MAX_ENTRIES;
    if (full) {
        g_linepointFull.fetch_add(1);
    }
    return full;
}

int ListNode::InsertKVItem(VarLenString *key, Val_t value, bool duringSplit) {
    int size = GetKVItemSize(key);
    if (PermutationFull() || StorageSpaceFull(size)) {
        CHECK(!duringSplit);
        return -1;
    }

    int offset = nextKv;
    auto kvItem = GetKVItem(offset);
    kvItem->value = value;
    std::atomic_thread_fence(std::memory_order_release);
    kvItem->key.set(key->getData(), key->keyLength);
    int nextOffset = offset + (size + KV_ALIGN_BYTES - 1) / KV_ALIGN_BYTES;
    if (nextOffset >= MAX_OFFSET) {
        nextKv = MAX_OFFSET;
    } else {
        nextKv = nextOffset;
    }
    return offset;
}

void ListNode::UpdatePermutation(KVItem *kvItem, uint8_t offset, uint8_t keyHash) {
    uint8_t startIndex = PermuteLowerBound(&kvItem->key);

    LinePointArray *currLpa = GetCurrPerm();
    LinePointArray *nextLpa = GetNextPerm();
    memcpy_s(nextLpa, sizeof(LinePointArray), currLpa, sizeof(LinePointArray));

    CHECK(nextLpa->count < MAX_ENTRIES);
    for (uint8_t i = nextLpa->count; i > startIndex; i--) {
        nextLpa->linePoint[i] = nextLpa->linePoint[i - 1];
    }
    if (g_whiteBoxType == PACTreeWhiteBoxType::UPDATE_PERMUTATION) {
        throw std::invalid_argument("update permutation white box exception");
    }
    nextLpa->linePoint[startIndex] = {.offset = offset, .fingerPrint = keyHash};
    nextLpa->count++;
    SwitchPerm();
}

ListNode::MVCCVisibility ListNode::CheckMVCCVisibility(KVItem *kv, LookupSnapshot snapshot) {
    if (TxInfoIsTxSlot(kv->value)) {
        /* value 存的是 TxSlotPtr */
        TransactionInfo txInfo{};
        bool recycled = !GetTransactionInfo(kv->value, &txInfo);
        if (recycled) {
            /* 事务已提交，且slot被回收了 */
            return MVCCVisibility::REMOVABLE;
        }
        DCHECK(txInfo.status != TxSlotStatus::EMPTY);
        if (txInfo.status == TxSlotStatus::COMMITTED) {
            /* 事务已提交 */
            if (txInfo.csn < snapshot.min_csn) {
                return MVCCVisibility::REMOVABLE;
            } else {
                kv->value = txInfo.csn;
            }
        }
        /* 其它情况，说明删除事务正在执行，或者已经被回滚，那么 对自己是可见的，可以往下走 */
    }

    if (TxInfoIsCSN(kv->value)) {
        /* 删除的事务的CSN已经回填 */
        if (kv->value < snapshot.min_csn) {
            return MVCCVisibility::REMOVABLE;
        }
        if (kv->value < snapshot.snapshot) {
            /* 删除的事务对自己是可见的，所以该 IndexTuple 是不可见的 */
            return MVCCVisibility::INVISIBLE;
        }
    }
    return MVCCVisibility::VISIBLE;
}

void ListNode::RemoveFromPermutation(std::vector<std::pair<uint8_t, int>> &items) {
    auto currLpa = GetCurrPerm();
    auto nextLpa = GetNextPerm();

    int j = 0;
    int k = 0;
    for (int i = 0; i < currLpa->count; i++) {
        if (k >= items.size() || i != items[k].first) {
            nextLpa->linePoint[j] = currLpa->linePoint[i];
            j++;
        } else {
            nextLpa->recyclable += CM_ALIGN_ANY(items[k].second, KV_ALIGN_BYTES) / KV_ALIGN_BYTES;
            k++;
        }
    }
    nextLpa->count = j;
    SwitchPerm();
}

void ListNode::Shrink() {
    // todo: 用日志保证 crash consistency
    int oldPrefixLen = prefix.keyLength;
    MakePrefix();
    int prefixDelta = prefix.keyLength - oldPrefixLen;
    CHECK(prefixDelta >= 0);

    auto lpa = GetCurrPerm();
    char buf[LIST_NODE_SIZE];
    LinePointArray newLpa;
    uint8_t newOffset = 0;

    for (int i = 0; i < lpa->count; i++) {
        auto kv = GetKVItem(lpa->linePoint[i].offset);
        auto newKv = reinterpret_cast<KVItem *>(buf + KV_ALIGN_BYTES * newOffset);
        newKv->value = kv->value;
        newKv->key.set(kv->key.getData() + prefixDelta, kv->key.keyLength - prefixDelta);

        newLpa.linePoint[i] = {.offset = newOffset, .fingerPrint = GetKeyFingerPrint(&newKv->key)};
        newOffset += ALIGN_ANY(GetKVItemSize(newKv), KV_ALIGN_BYTES) / KV_ALIGN_BYTES;
    }
    newLpa.recyclable = 0;
    newLpa.count = lpa->count;

    if (g_whiteBoxType == PACTreeWhiteBoxType::SPLIT_STEP2) {
        throw std::invalid_argument("split 2 white box exception");
    }

    errno_t ret = memcpy_s(static_cast<void *>(GetKVItem(0)), KV_ALIGN_BYTES * newOffset, buf,
        KV_ALIGN_BYTES * newOffset);
    SecureRetCheck(ret);
    this->nextKv = newOffset;
    ret = memcpy_s(static_cast<void *>(lpa), sizeof(LinePointArray), static_cast<void *>(&newLpa),
        sizeof(LinePointArray));
    SecureRetCheck(ret);
}

NVMPtr<ListNode> ListNode::Split(Key_t &key, Val_t value) {
    Key_t new_min;
    auto lpa = GetCurrPerm();
    int midIndex = lpa->count / 2;
    GetOriginKey(&GetKVItem(lpa->GetOffset(midIndex))->key, &new_min);

    OpStruct *oplog = Oplog::allocOpLog();
    errno_t ret = memcpy_s((void *)oplog->oldNodeData, LIST_NODE_SIZE, (void *)this, LIST_NODE_SIZE);
    SecureRetCheck(ret);

    // 1) Add Oplog and set the information for current(overflown) node.
    Oplog::WriteOpLog(oplog, OpStruct::insert, new_min, (void *)curPtr.getRawPtr(), 0, key, value);
    oplog->step = OpStruct::during_split;

    if (lpa->recyclable > MAX_RECYCLE) {
        // if the node is recyclable, shrink directly.
        Shrink();
        Insert(key, value, 1);
        oplog->op = OpStruct::done;  // no need to update search layer
        return curPtr;
    }

    // 2) Allocate new data node and store persistent pointer to the oplog.
    NVMPtr<ListNode> newNodePtr;
    PMem::alloc(LIST_NODE_SIZE, (void **)&(newNodePtr), &(oplog->newNodeOid));
    oplog->poolId = newNodePtr.getPoolId();

    memset_s((void *)newNodePtr.getVaddr(), sizeof(ListNode), 0, sizeof(ListNode));
    auto newNode = (ListNode *)new (newNodePtr.getVaddr()) ListNode();

    // 3) Perform Split Operation
    //    3-1) Update New node
    newNode->SetMin(new_min);
    newNode->SetMax(GetMax());
    newNode->MakePrefix();
    int prefixDelta = newNode->prefix.keyLength - prefix.keyLength;
    CHECK(prefixDelta >= 0);

    if (g_whiteBoxType == PACTreeWhiteBoxType::SPLIT_STEP1) {
        throw std::invalid_argument("split 1 white box exception");
    }

    auto* newNode_lpa = newNode->GetCurrPerm();
    CHECK(newNode_lpa->count == 0);
    for (int i = midIndex; i < lpa->count; i++) {
        auto kv = GetKVItem(lpa->GetOffset(i));
        auto offset = newNode->nextKv;
        auto new_kv = newNode->GetKVItem(offset);

        new_kv->value = kv->value;
        new_kv->key.set(kv->key.getData() + prefixDelta, kv->key.keyLength - prefixDelta);

        newNode_lpa->linePoint[newNode_lpa->count] = {
            .offset = (uint8_t)offset,
            .fingerPrint = GetKeyFingerPrint(&new_kv->key)
        };
        newNode_lpa->count++;

        // not worry about overflow, as total node size must be smaller than the old node.
        newNode->nextKv += ALIGN_ANY(GetKVItemSize(new_kv), KV_ALIGN_BYTES) / KV_ALIGN_BYTES;
    }

    //    3-2) Update current node
    lpa->count = midIndex;
    SetMax(new_min);
    Shrink();

    // 4) Insert the new KV
    if (key < new_min) {
        Insert(key, value, 1);
    } else {
        newNode->Insert(key, value, 1);
    }

    // 5) connect the new node into the linked list.
    newNode->SetNext(GetNextPtr());
    newNode->SetCur(newNodePtr);
    newNode->SetPrev(curPtr);

    /*
     * The next node is not protected by lock so it can not be accessed by readers.
     * Thus, we change the list link only after all modification has finished.
     */
    ListNode *nextNode = GetNext();
    SetNext(newNodePtr);
    oplog->step = OpStruct::finish_split;

    if (nextNode != nullptr) {
        nextNode->SetPrev(newNodePtr);
    }

    Oplog::EnqPerThreadLog(oplog);

    return newNodePtr;
}

bool ListNode::Insert(Key_t &key, Val_t value, int duringSplit) {
    auto remain_key = GetRemainKey(&key);
    uint8_t keyHash = GetKeyFingerPrint(remain_key);
    int index = GetKeyIndex(remain_key, keyHash);
    if (index >= 0) {
        // key exists
        UpdateAtIndex(value, index);
        destroy_remain_key(remain_key);
        return true;
    }

    // key not exists, insert into current node if free space is enough
    int offset = InsertKVItem(remain_key, value, duringSplit);
    destroy_remain_key(remain_key);
    if (offset < 0) {  // need split
        CHECK(!duringSplit);
        Split(key, value);
        return false;
    }
    CHECK(offset <= MAX_OFFSET);

    // 用去除掉 prefix 的变长key，去做二分查找，速度更快
    auto kvItem = GetKVItem(offset);
    UpdatePermutation(kvItem, offset, keyHash);
    return false;
}

bool ListNode::Lookup(Key_t &key, Val_t &value) {
    auto remain_key = GetRemainKey(&key);
    uint8_t keyHash = GetKeyFingerPrint(remain_key);
    int index = GetKeyIndex(remain_key, keyHash);
    destroy_remain_key(remain_key);

    if (index >= 0) {
        auto lpa = GetCurrPerm();
        auto kv = GetKVItem(lpa->linePoint[index].offset);
        value = kv->value;
        return true;
    }
    return false;
}

bool ListNode::ScanInOrder(Key_t &startKey, Key_t &endKey, int maxRange, LookupSnapshot snapshot,
                           std::vector<std::pair<Key_t, Val_t>> &result, bool continueScan, bool *needPrune) {
    VarLenString *ed_remain;
    if (endKey >= max) {
        ed_remain = GetRemainKey(&max);
    } else {
        ed_remain = GetRemainKey(&endKey);
    }
    uint8_t startIndex = 0;
    if (!continueScan) {
        DCHECK(startKey >= GetMin() && startKey < GetMax());
        auto st_remain = GetRemainKey(&startKey);
        startIndex = PermuteLowerBound(st_remain);
        destroy_remain_key(st_remain);
    }

    int todo = maxRange - (int)result.size();
    bool end = false;
    int removeItems = 0;
    int scanItems = 0;
    auto lpa = GetCurrPerm();
    for (uint8_t i = startIndex; i < lpa->count && todo > 0; i++) {
        scanItems++;
        auto kv = GetKVItem(lpa->linePoint[i].offset);
        auto status = CheckMVCCVisibility(kv, snapshot);
        if (kv->key >= *ed_remain) {
            end = true;
            break;
        }

        if (status == MVCCVisibility::VISIBLE) {
            Key_t match;
            CHECK(kv->key.keyLength != 0);
            GetOriginKey(&kv->key, &match);
            result.emplace_back(match, kv->value);
            todo--;
        } else if (status == MVCCVisibility::REMOVABLE) {
            removeItems++;
        }
    }

    *needPrune = (scanItems > 0 && removeItems >= scanItems / SCAN_ITEM_DIV);

    destroy_remain_key(ed_remain);
    if (end) {
        return true;
    }
    return result.size() == maxRange;
}

void ListNode::RecoverSplit(OpStruct *oplog) {
    int ret = memcpy_s((void *)this, LIST_NODE_SIZE, (void *)oplog->oldNodeData, LIST_NODE_SIZE);
    SecureRetCheck(ret);
}

void ListNode::Prune(LookupSnapshot snapshot, uint64_t genId) {
    auto lpa = GetCurrPerm();
    std::vector<std::pair<uint8, int>> remove_items;
    for (int i = 0; i < lpa->count; i++) {
        auto kv = GetKVItem(lpa->linePoint[i].offset);
        auto status = CheckMVCCVisibility(kv, snapshot);
        if (status == MVCCVisibility::REMOVABLE) {
            remove_items.emplace_back(i, GetKVItemSize(kv));
        }
    }
    if (!remove_items.empty()) {
        RemoveFromPermutation(remove_items);
        lpa = GetCurrPerm();
        if (lpa->count == 0) {
            MergeEmptyNodeWithPrev(genId);
        }
    }
}

void ListNode::MergeEmptyNodeWithPrev(uint64_t genId) {
    OpStruct *oplog = Oplog::allocOpLog();
    ListNode *prevNode = GetPrev();
    while (true) {
        auto& mutex = prevNode->getVersionedLock();
        mutex.lock(genId);
        auto* tmpNode = GetPrev();
        if (prevNode == tmpNode) {
            break;
        }
        // 上错锁了, 重试prevNode
        mutex.unlock();
        prevNode = tmpNode;
    }
    // 此刻上了锁，且 cur/prev 都是正常节点 没有被删除
    CHECK(prevNode->nextPtr.getRawPtr() == curPtr.getRawPtr());

    // 本来落在curNode范围内的key，会发现  < min, 需要找 prev
    min = max;
    prevNode->max = max;
    prevNode->nextPtr = nextPtr;

    // 在此期间， cur, prev 都不会被修改，所以没有人会修改 nextNode 的 prev指针，可以无锁
    ListNode *nextNode = GetNext();
    CHECK(!nextNode->GetDeleted());
    nextNode->prevPtr = prevPtr;
    SetDeleted();

    prevNode->getVersionedLock().unlock();

    Oplog::WriteOpLog(oplog, OpStruct::remove, GetMin(), (void *)curPtr.getRawPtr(), curPtr.getPoolId(), Key_t(-1), -1);
    Oplog::EnqPerThreadLog(oplog);
}

}  // namespace NVMDB
