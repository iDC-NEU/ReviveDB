#include "common/pactree/pactree_impl.h"
#include "common/pactree/worker_thread.h"
#include "common/pactree/combiner.h"
#include "common/numa.h"
#include "nvmdb_thread.h"

namespace NVMDB {

constexpr int RUN_CNT_MOD = 2;
constexpr int SLEEP_TIME = 200;

#ifdef PACTREE_ENABLE_STATS
#define acc_sl_time(x) (curThreadData->sltime += x)
#define acc_dl_time(x) (curThreadData->dltime += x)
#define hydralist_reset_timers() \
    do {                         \
        total_sl_time = 0;       \
        total_dl_time = 0;       \
    } while (0)
#define hydralist_start_timer()      \
    do {                             \
        unsigned long __b_e_g_i_n__; \
    __b_e_g_i_n__ = read_tsc()
#define hydralist_stop_timer(tick)        \
    (tick) += read_tsc() - __b_e_g_i_n__; \
    }                                     \
    while (0)
#else
#define ACC_SL_TIME(x)
#define ACC_DL_TIME(x)
#define HYDRALIST_START_TIMER()
#define HYDRALIST_STOP_TIMER(tick)
#define HYDRALIST_RESET_TIMERS()
#endif

std::vector<WorkerThread *> g_WorkerThreadInst(NVMDB_MAX_GROUP *NVMDB_OPLOG_WORKER_THREAD_PER_GROUP);

std::vector<SearchLayer *> g_perGrpSlPtr(NVMDB_MAX_GROUP);

std::set<ThreadData *> g_threadDataSet;

std::atomic<bool> g_globalStop;

std::atomic<bool> g_combinerStop;

thread_local ThreadData *g_curThreadData = nullptr;

volatile int PACTreeImpl::totalGroupActive = 0;

volatile bool threadInitialized[NVMDB_MAX_GROUP];

volatile bool slReady[NVMDB_MAX_GROUP];

std::mutex g_threadDataLock;

uint64_t g_removeCount;

volatile std::atomic<bool> g_removeDetected;

void workerThreadExec(int threadId, int activeGrp, root_obj *root) {
    CHECK(activeGrp > 0);
    auto groupId = threadId / NVMDB_OPLOG_WORKER_THREAD_PER_GROUP;
    auto thread_name = std::string("PACTreeW_") + std::to_string(threadId);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    NumaBinding::bindThreadToNode(groupId);
    WorkerThread wt(threadId, activeGrp);
    g_WorkerThreadInst[threadId] = &wt;
    if (threadId < activeGrp) {
        while (threadInitialized[threadId] == 0) { }
        slReady[threadId] = false;
        g_perGrpSlPtr[threadId] = PACTreeImpl::CreateSearchLayer(root, threadId);
        g_perGrpSlPtr[threadId]->SetGroupId(threadId);
        slReady[threadId] = true;
    }
    int count = 0;
    uint64_t lastRemoveCount = 0;
    while (!g_combinerStop) {
        usleep(1);
        while (!wt.IsWorkQueueEmpty()) {
            count++;
            wt.ApplyOperation();
        }
        if (threadId == 0 && lastRemoveCount != g_removeCount) {
        }
    }
    while (!wt.IsWorkQueueEmpty()) {
        count++;
        if (wt.ApplyOperation() && !g_removeDetected) {
            g_removeDetected.store(true);
        }
    }
    // If worker threads are stopping that means there are no more user threads
    if (threadId == 0) {
        wt.FreeListNodes(ULLONG_MAX);
    }
    g_WorkerThreadInst[threadId] = nullptr;
}

uint64_t gracePeriodInit(std::vector<ThreadData *> &threadsToWait) {
    uint64_t curTime = ordo_get_clock();
    for (auto td : g_threadDataSet) {
        if (td->GetFinish()) {
            g_threadDataLock.lock();
            g_threadDataSet.erase(td);
            g_threadDataLock.unlock();
            free(td);
            continue;
        }
        if (td->GetRunCnt() % RUN_CNT_MOD) {
            if (ordo_gt_clock(td->GetLocalClock(), curTime))
                continue;
            else
                threadsToWait.push_back(td);
        }
    }
    return curTime;
}

void waitForThreads(std::vector<ThreadData *> &threadsToWait, uint64_t gpStartTime) {
    for (auto & td : threadsToWait) {
        if (td == nullptr)
            continue;
        if (td->GetRunCnt() % RUN_CNT_MOD == 0)
            continue;
        if (ordo_gt_clock(td->GetLocalClock(), gpStartTime))
            continue;
        while (td->GetRunCnt() % RUN_CNT_MOD) {
            usleep(1);
            std::atomic_thread_fence(std::memory_order::memory_order_acq_rel);
        }
    }
}

void BroadcastDoneCount(uint64_t removeCount) {
    g_removeCount = removeCount;
}

void CombinerThreadExec(int activeGrp) {
    pthread_setname_np(pthread_self(), "PACTreeCombiner");
    CombinerThread ct;
    int count = 0;
    while (!g_globalStop) {
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeGrp);
        }
        uint64_t doneCountWt = ct.FreeMergedLogs(activeGrp, false);
        std::vector<ThreadData *> threadsToWait;
        if (g_removeDetected && doneCountWt != 0) {
            uint64_t gpStartTime = gracePeriodInit(threadsToWait);
            waitForThreads(threadsToWait, gpStartTime);
            BroadcastDoneCount(doneCountWt);
            g_removeDetected.store(false);
        } else {
            usleep(1);
        }
    }
    int i = 20;
    while (i--) {
        usleep(SLEEP_TIME);
        std::vector<OpStruct *> *mergedLog = ct.combineLogs();
        if (mergedLog != nullptr) {
            count++;
            ct.broadcastMergedLog(mergedLog, activeGrp);
        }
    }
    while (!ct.MergedLogsToBeFreed()) {
        ct.FreeMergedLogs(activeGrp, true);
    }
    g_combinerStop = true;
}

void PACTreeImpl::CreateWorkerThread(int numGrp, root_obj *root) {
    for (int i = 0; i < numGrp * NVMDB_OPLOG_WORKER_THREAD_PER_GROUP; i++) {
        threadInitialized[i % numGrp] = false;
        auto *wt = new std::thread(workerThreadExec, i, numGrp, root);
        usleep(1);
        wtArray->push_back(wt);
        threadInitialized[i % numGrp] = true;
    }
}

void PACTreeImpl::CreateCombinerThread() {
    combinerThead = new std::thread(CombinerThreadExec, totalGroupActive);
}

PACTreeImpl *InitPT() {
    LOG(INFO) << "Init PACTree.";
    root_obj *root = nullptr;
    root_obj *sl_root = nullptr;
    int isCreate;
    PMem::CreatePMemPool(&isCreate, &root, &sl_root);
    int poolNum = PMem::GetPoolNum();
    if (isCreate == 0) {
        auto *pt = (PACTreeImpl *)pmemobj_direct(root->ptr[0]);
        pt->Init(poolNum, sl_root);
        return pt;
    }

    auto *pop = (PMEMobjpool *)PMem::getBaseOf(1);
    pmemobj_alloc(pop, &(root->ptr[0]), sizeof(PACTreeImpl), 0, nullptr, nullptr);
    void *rootVirtAddr = pmemobj_direct(root->ptr[0]);
    auto *pt = (PACTreeImpl *)new (rootVirtAddr) PACTreeImpl(poolNum, sl_root);
    flushToNVM((char *)root, sizeof(root_obj));
    smp_wmb();
    return pt;
}

void PACTreeImpl::Init(int numGrp, root_obj *root) {
    InitGlobalLogMgr();
    totalGroupActive = numGrp;
    wtArray = new std::vector<std::thread *>;
    g_WorkerThreadInst.clear();
    g_perGrpSlPtr.clear();
    g_threadDataSet.clear();

    g_perGrpSlPtr.resize(totalGroupActive);
    g_globalStop = false;
    g_combinerStop = false;
    CreateWorkerThread(numGrp, root);
    CreateCombinerThread();
    for (int i = 0; i < totalGroupActive; i++) {
        while (!slReady[i]) { }
    }
    RegisterThread();
    Recover();
    UnregisterThread();
    HYDRALIST_RESET_TIMERS();
}

PACTreeImpl::PACTreeImpl(int numGrp, root_obj *root) {
    totalGroupActive = numGrp;
    InitGlobalLogMgr();
    wtArray = new std::vector<std::thread *>;
    g_perGrpSlPtr.resize(totalGroupActive);
    dl.Initialize();
    // need to read from PM

    g_globalStop = false;
    g_combinerStop = false;

    CreateWorkerThread(numGrp, root);
    CreateCombinerThread();
    for (int i = 0; i < totalGroupActive; i++) {
        while (!slReady[i]) { }
    }
    HYDRALIST_RESET_TIMERS();
}

PACTreeImpl::~PACTreeImpl() {
    g_globalStop = true;
    for (auto &t : *wtArray)
        t->join();
    combinerThead->join();
}

ListNode *PACTreeImpl::getJumpNode(Key_t &key) {
    int grpId = NVMDB::NumaBinding::getThreadLocalGroupId();
    SearchLayer &sl = *g_perGrpSlPtr[grpId];
    if (sl.IsEmpty()) {
        return dl.GetHead();
    }
    auto *jumpNode = reinterpret_cast<ListNode *>(sl.lookup(key));
    if (jumpNode == nullptr)
        jumpNode = dl.GetHead();
    return jumpNode;
}

bool PACTreeImpl::Insert(Key_t &key, Val_t val) {
    uint64_t clock = ordo_get_clock();
    g_curThreadData->ReadLock(clock);

    bool ret;
    HYDRALIST_START_TIMER();
    ListNode *jumpNode = getJumpNode(key);

    HYDRALIST_STOP_TIMER(ticks);
    HYDRALIST_START_TIMER();
    ACC_SL_TIME(ticks);
    ret = dl.Insert(key, val, jumpNode);

    HYDRALIST_STOP_TIMER(ticks);
    ACC_DL_TIME(ticks);
    g_curThreadData->ReadUnlock();

    return ret;
}

Val_t PACTreeImpl::Lookup(Key_t &key, bool *found) {
    uint64_t clock = ordo_get_clock();
    g_curThreadData->ReadLock(clock);
    Val_t val;

    ListNode *jumpNode = getJumpNode(key);
    *found = dl.Lookup(key, val, jumpNode);
    g_curThreadData->ReadUnlock();
    return val;
}

SearchLayer *PACTreeImpl::CreateSearchLayer(root_obj *root, int threadId) {
    // Read from Root Object or Allocate new one.
    if (pmemobj_direct(root->ptr[threadId]) == nullptr) {
        NVMPtr<SearchLayer> sPtr;
        PMem::alloc(sizeof(SearchLayer), (void **)&sPtr, &(root->ptr[threadId]));
        auto *s = new (sPtr.getVaddr()) SearchLayer();
        return s;
    }
    auto *s = (SearchLayer *)pmemobj_direct(root->ptr[threadId]);
    s->Init();
    return s;
}

void PACTreeImpl::Scan(Key_t &startKey,
                       Key_t &endKey,
                       int maxRange,
                       LookupSnapshot snapshot,
                       bool reverse,
                       std::vector<std::pair<Key_t, Val_t>> &result) {
    while (true) {
        if (reverse) {
            CHECK(false);
        }
        ListNode *jumpNode = getJumpNode(startKey);
        if (dl.ScanInOrder(startKey, endKey, jumpNode, maxRange, snapshot, result)) {
            return;
        }
    }
}

void PACTreeImpl::RegisterThread() {
    int threadId = (int)numThreads.fetch_add(1);
    auto td = new ThreadData(threadId);
    g_threadDataLock.lock();
    g_threadDataSet.insert(td);
    g_threadDataLock.unlock();
    g_curThreadData = td;
    std::atomic_thread_fence(std::memory_order_acq_rel);
}

void PACTreeImpl::UnregisterThread() {
    if (g_curThreadData == nullptr) {
        return;
    }
    ReleaseThreadLogMgr();
    g_curThreadData->SetFinish();
}

void PACTreeImpl::Recover() {
    for (int i = 0; i < totalGroupActive; i++) {
        dl.Recover(g_perGrpSlPtr[i]);
    }
}

}  // namespace NVMDB