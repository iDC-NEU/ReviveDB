#include "common/pactree/linked_list.h"
#include "common/pactree/search_layer.h"

namespace NVMDB {

ListNode *LinkedList::Initialize() {
    genId = 0;
    auto *oplog = (OpStruct *)PMem::getOpLog(0);

    PMem::alloc(LIST_NODE_SIZE, (void **)&headPtr, &(oplog->newNodeOid));
    auto *head = (ListNode *)new (headPtr.getVaddr()) ListNode();
    flushToNVM((char *)&headPtr, sizeof(NVMPtr<ListNode>));
    smp_wmb();

    auto *oplog2 = (OpStruct *)PMem::getOpLog(1);

    PMem::alloc(LIST_NODE_SIZE, (void **)&tailPtr, &(oplog->newNodeOid));
    auto *tail = (ListNode *)new (tailPtr.getVaddr()) ListNode();

    oplog->op = OpStruct::done;
    oplog2->op = OpStruct::done;

    NVMPtr<ListNode> nullPtr(0, 0);

    head->SetNext(tailPtr);
    head->SetPrev(nullPtr);
    head->SetCur(headPtr);
    static const std::string minString = "\0";
    static const std::string maxString = "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
    Key_t max;
    max.setFromString(maxString);
    Key_t min;
    min.setFromString(minString);
    head->SetMin(min);
    head->SetMax(max);
    head->Insert(min, INVALID_CSN, 0);
    tail->SetMin(max);
    tail->SetMax(max);
    tail->Insert(max, INVALID_CSN, 0);

    tail->SetNext(nullPtr);
    tail->SetPrev(headPtr);
    tail->SetCur(tailPtr);

    flushToNVM(reinterpret_cast<char *>(head), sizeof(ListNode));
    flushToNVM(reinterpret_cast<char *>(tail), sizeof(ListNode));
    smp_wmb();
    return head;
}

static NVMPtr<ListNode> getPrevActiveNode(ListNode *prev) {
    NVMPtr<ListNode> res;
    while (prev->GetDeleted()) {
        res = prev->GetPrevPtr();
        prev = res.getVaddr();
    }
    return res;
}

static NVMPtr<ListNode> getNextActiveNode(ListNode *next) {
    NVMPtr<ListNode> res;
    while (next->GetDeleted()) {
        res = next->GetNextPtr();
        next = res.getVaddr();
    }
    return res;
}

static ListNode *searchAndLockNode(ListNode *cur, uint64_t genId, Key_t &key) {
    while (true) {
        auto& mutex = cur->getVersionedLock();
        mutex.lock(genId);
        // key小于当前节点最小的key, 向左面查找
        if (key < cur->GetMin()) {
            auto* prev = cur->GetPrev();
            if (cur->GetDeleted() && prev->GetDeleted()) {
                auto prevPtr = getPrevActiveNode(prev);
                cur->SetPrev(prevPtr);
                prev = prevPtr.getVaddr();
            }
            mutex.unlock();
            cur = prev;
            continue;
        }
        // key大于当前节点最小的key, 向右面查找
        if (key >= cur->GetMax()) {
            ListNode *next = cur->GetNext();
            if (cur->GetDeleted() && next->GetDeleted()) {
                auto nextPtr = getNextActiveNode(next);
                cur->SetNext(nextPtr);
                next = nextPtr.getVaddr();
            }
            mutex.unlock();
            cur = next;
            continue;
        }
        break;
    }
    return cur;
}

/*
 * return: true(更新成功）， false(传入的head有问题，需要重新遍历 art）
 */
bool LinkedList::Insert(Key_t &key, Val_t value, ListNode *head) {
    ListNode* cur = searchAndLockNode(head, genId, key);
    /* current node is locked and the range is matched */
    bool res = cur->Insert(key, value, 0);
    cur->getVersionedLock().unlock();
    return res;
}

bool LinkedList::Lookup(Key_t &key, Val_t &value, ListNode *head) const {
    ListNode* cur = head;
    while (true) {
        auto& mutex = cur->getVersionedLock();
        auto version = mutex.isLocked(genId);
        // 在双向链表中搜索
        if (key < cur->GetMin()) {
            cur = cur->GetPrev();
            continue;
        }
        if (key >= cur->GetMax()) {
            cur = cur->GetNext();
            continue;
        }
        // 在范围内, 但是正在被并发修改
        // 因为修改后的范围不确定, 所以从头开始查找
        if (version == 0) {
            cur = head; // 重新遍历 art
            continue;
        }
        // 一定在指定范围内
        DCHECK(cur->CheckRange(key));
        // 乐观读取值
        bool ret = cur->Lookup(key, value);
        // 值在读取时候被修改了
        if (!mutex.checkVersionEqual(version)) {
            cur = head; // 重新遍历 art
            continue;
        }
        // 可能值不存在
        return ret;
    }
}

void LinkedList::Print(ListNode *head) {
    ListNode *cur = head;
    while (cur->GetNext() != nullptr) {
        cur->Print();
        cur = cur->GetNext();
    }
}

uint32_t LinkedList::Size(ListNode *head) {
    ListNode *cur = head;
    int count = 0;
    while (cur->GetNext() != nullptr) {
        count++;
        cur = cur->GetNext();
    }
    return count;
}

ListNode *LinkedList::GetHead() {
    auto *head = (ListNode *)headPtr.getVaddr();
    return head;
}

bool LinkedList::ScanInOrder(Key_t &startKey, Key_t &endKey, ListNode *head, int maxRange, LookupSnapshot snapshot,
                             std::vector<std::pair<Key_t, Val_t>> &result) const {
    ListNode *cur = head;
    cur = searchAndLockNode(cur, genId, startKey);

    // The current node is locked and the range is matched
    //  i.e.,    min <= startKey < max
    DCHECK(!cur->GetDeleted() && cur->GetMin() <= startKey && startKey < cur->GetMax());
    result.clear();
    bool continueScan = false;
    while (true) {
        bool needPrune;
        bool end = cur->ScanInOrder(startKey, endKey, maxRange, snapshot, result, continueScan, &needPrune);
        if (needPrune) {
            cur->Prune(snapshot, genId);
        }
        end |= endKey < cur->GetMax();
        cur->getVersionedLock().unlock();
        if (end) {
            break;
        }
        ListNode *next = cur->GetNext();
        next->getVersionedLock().lock(genId);
        cur = next;
        continueScan = true;
    }

    return true;
}

void LinkedList::Recover(void *sl) {
    genId++;
    auto art = (SearchLayer *)sl;

    for (int i = 0; i < NVMDB_NUM_LOGS_PER_THREAD * NVMDB_MAX_THREAD_NUM; i++) {
        auto oplog = (OpStruct *)PMem::getOpLog(i);
        if (oplog->op == OpStruct::done || oplog->op == OpStruct::dummy) {
            continue;
        }
        if (!(oplog->searchLayers.load() & art->grpMask)) {
            if (oplog->searchLayers.load() == 0) {
                oplog->op = OpStruct::done;
            }
            continue;
        }
        auto remain = oplog->searchLayers.fetch_sub(art->grpMask);

        if (oplog->op == OpStruct::insert) {
            NVMPtr<ListNode> node;
            NVMPtr<ListNode> next(oplog->poolId, oplog->newNodeOid.off);
            node.setRawPtr(oplog->oldNodePtr);
            if (oplog->step == OpStruct::initial) {
                // nothing to do
            } else if (oplog->step == OpStruct::during_split) {
                if (remain == art->grpMask) {
                    /* the split is not finished, just recover the old node; the insert operation considered as failed
                     * not that only one thread need do the recovery
                     * */
                    node.getVaddr()->RecoverSplit(oplog);
                }
            } else {
                // todo: update next's prev point if necessary
                CHECK(oplog->step == OpStruct::finish_split);
                if (!next.getVaddr()->GetDeleted() &&
                    (art->IsEmpty() || art->lookup(oplog->key) != (void *)next.getRawPtr())) {
                    art->Insert(oplog->key, (void *)next.getRawPtr());
                }
            }
        } else if (oplog->op == OpStruct::remove) {
            if (art->lookup(oplog->key) != nullptr) {
                art->remove(oplog->key, oplog->oldNodePtr);
            }
        }
        if (remain == art->grpMask) {
            oplog->op = OpStruct::done;
        }
    }
}

}  // namespace NVMDB
