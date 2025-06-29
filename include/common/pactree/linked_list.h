#ifndef LINKEDLIST_H
#define LINKEDLIST_H

#include "common/pactree/list_node.h"

namespace NVMDB {

class LinkedList {
public:
    ListNode *Initialize();

    bool Insert(Key_t &key, Val_t value, ListNode *head);

    bool Lookup(Key_t &key, Val_t &value, ListNode *head) const;

    bool ScanInOrder(Key_t &startKey, Key_t &endKey,
                     ListNode *head, int maxRange, LookupSnapshot snapshot,
                     std::vector<std::pair<Key_t, Val_t>> &result) const;

    static void Print(ListNode *head);

    static uint32_t Size(ListNode *head);

    ListNode *GetHead();

    void Recover(void *sl);

private:
    // 每次启动版本+1
    uint64_t genId;

    NVMPtr<ListNode> headPtr;

    NVMPtr<ListNode> tailPtr;

    // 500 *200
    OpStruct oplogs[100000];

    int nunOpLogs[112];
};

}  // namespace NVMDB

#endif  // _LINKEDLIST_H
