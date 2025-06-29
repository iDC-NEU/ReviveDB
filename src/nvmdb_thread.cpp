#include "nvmdb_thread.h"
#include "heap/nvm_heap_cache.h"
#include "index/nvm_index.h"
#include "transaction/nvm_transaction.h"
#include "common/numa.h"
#include <unordered_map>


namespace NVMDB {

struct ThreadLocalStorage {
    int threadId;  // 逻辑 id, unique for each thread
    int groupId;    // nvm storage group id (for load balance)
};

struct GlobalThreadStorageMgr {
    int groupNum = 0;   // 2 when running
    int threadNum = 0;  // total thread count
    std::vector<int> groupSize; // groupSize.size() is the size of g_dirPaths (thread count per group)
    std::mutex mtx;

    void Init(int num) { // num: the size of nvm dirs (g_dirPaths)
        groupNum = num;
        groupSize.clear();
        for (int i = 0; i < groupNum; i++) {
            groupSize.push_back(0);
        }
    }

    void register_thrd(ThreadLocalStorage *thrd) {   // register thread in global reister
        std::lock_guard<std::mutex> lockGuard(mtx); // prevent reenter
        int minId = 0;
        int minNum = groupSize[0];
        for (int i = 1; i < groupNum; i++) {
            if (minNum > groupSize[i]) {
                minNum = groupSize[i];
                minId = i;
            }
        }

        thrd->groupId = minId;
        groupSize[minId]++; // load balance for each group
        thrd->threadId = threadNum++;
    }

    void unregister_thrd(ThreadLocalStorage *thrd) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        groupSize[thrd->groupId]--;
    }
};

struct GlobalThreadStorageMgr g_thrdMgr;

void InitGlobalThreadStorageMgr() {
    g_thrdMgr.Init(g_dir_config->size());   // the number of dirs
}

void InitGlobalVariables() {
    InitGlobalThreadStorageMgr();
    InitGlobalRowIdMapCache();  // clear g_globalRowidMaps
    ProcessArray::InitGlobalProcArray();
}

void DestroyGlobalVariables() {
    DestroyGlobalRowIdMapCache();
    ProcessArray::DestroyGlobalProcArray();
}

static thread_local ThreadLocalStorage *t_storage = nullptr;

void InitThreadLocalStorage() { // regist current thread
    t_storage = new ThreadLocalStorage;
    g_thrdMgr.register_thrd(t_storage);
}

void DestroyThreadLocalStorage() {
    g_thrdMgr.unregister_thrd(t_storage);
    delete t_storage;
    t_storage = nullptr;
}

void InitThreadLocalVariables() {
    InitThreadLocalStorage();   // regist current thread
    InitLocalRowIdMapCache();
    InitLocalUndoSegment(); // grab an empty undo segment and continue.
    InitLocalIndex();    // pac tree
#ifndef NVMDB_ADAPTER
    InitTransactionContext();
#endif
}

void DestroyThreadLocalVariables() {
    DestroyThreadLocalStorage();
    TLTupleCache::Clear();
    TLTableCache::Clear();
    DestroyLocalRowIdMapCache();
    DestroyLocalIndex();
#ifndef NVMDB_ADAPTER
    DestroyTransactionContext();
#endif
    DestroyLocalUndoSegment();
}

}  // namespace NVMDB
