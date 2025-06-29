#ifndef PACTREE_IMPL_H
#define PACTREE_IMPL_H

#include "common/pactree/linked_list.h"
#include "common/pactree/search_layer.h"
#include "common/pactree/thread_data.h"

namespace NVMDB {

extern std::vector<SearchLayer *> g_perGrpSlPtr;
extern std::set<ThreadData *> g_threadDataSet;

class PACTreeImpl {
public:
    explicit PACTreeImpl(int numGrp, root_obj *root);

    ~PACTreeImpl();

    bool Insert(Key_t &key, Val_t val);

    void RegisterThread();

    void UnregisterThread();

    Val_t Lookup(Key_t &key, bool *found);

    void Recover();

    void Scan(Key_t &startKey, Key_t &endKey, int maxRange, LookupSnapshot snapshot, bool reverse,
              std::vector<std::pair<Key_t, Val_t>> &result);

    static SearchLayer *CreateSearchLayer(root_obj *root, int threadId);

    void Init(int numGrp, root_obj *root);

#ifdef PACTREE_ENABLE_STATS
    std::atomic<uint64_t> total_sl_time;
    std::atomic<uint64_t> total_dl_time;
#endif

protected:
    void CreateWorkerThread(int numGrp, root_obj *root);

    void CreateCombinerThread();

    ListNode *getJumpNode(Key_t &key);

private:
    LinkedList dl;

    // CurrentOne but there should be group number of threads
    std::vector<std::thread *> *wtArray;

    std::thread *combinerThead{};

    static volatile int totalGroupActive;

    std::atomic<uint32_t> numThreads{};
};

PACTreeImpl *InitPT();

}  // namespace NVMDB
#endif  // PACTREE_IMPL_H
