#ifndef OPLOG_H
#define OPLOG_H

#include "common/pdl_art/string_key.h"
#include "common/nvm_cfg.h"
#include <boost/lockfree/spsc_queue.hpp>
#include <vector>
#include <algorithm>
#include <queue>
#include <set>
#include <mutex>

constexpr int QNUM_SIZE = 2;
constexpr int LOCK_CAPACITY = 10000;

class Oplog {
public:
    std::atomic<unsigned long> curQ{};
    Oplog();
    void ResetQ(int qnum);
    std::vector<OpStruct *> *getQ(int qnum);
    static Oplog *GetPerThreadInstance() {
        return perThreadLog;
    }
    static void SetPerThreadInstance(Oplog *ptr) {
        perThreadLog = ptr;
    }
    static Oplog *GetOpLog();
    static void EnqPerThreadLog(OpStruct *ops);
    void Enq(OpStruct *ops);
    void Lock(int qnum) {
        qLock[qnum].lock();
    }
    void Unlock(int qnum) {
        qLock[qnum].unlock();
    }
    bool Empty() {
        for (int i = 0; i < QNUM_SIZE; i++) {
            Lock(i);
            if (!op_[i].empty()) {
                Unlock(i);
                return false;
            }
            Unlock(i);
        }
        return true;
    }
    static void WriteOpLog(OpStruct *oplog, OpStruct::Operation op, const Key_t& key, void *oldNodeRawPtr, uint16_t poolId,
                           const Key_t& newKey, Val_t newVal);
    static OpStruct *allocOpLog();
private:
    std::mutex qLock[2];
    std::vector<OpStruct *> oplog1;
    std::vector<OpStruct *> oplog2;
    std::vector<std::vector<OpStruct *>> op_{oplog1, oplog2};
    static thread_local Oplog *perThreadLog;
};

struct ThreadLogMgr {
public:
    int logCnt;
    int core;
    Oplog *perThreadLog;
    ThreadLogMgr() : logCnt(0), core(0), perThreadLog(nullptr) {}
    OpStruct *allocNextLog();
};

extern void InitGlobalLogMgr();
extern void RegisterThreadLogMgr();
extern void ReleaseThreadLogMgr();

extern std::set<Oplog *> g_perThreadLog;
extern boost::lockfree::spsc_queue<std::vector<OpStruct *> *, boost::lockfree::capacity<LOCK_CAPACITY>>
    g_workQueue[NVMDB::NVMDB_MAX_GROUP * NVMDB::NVMDB_OPLOG_WORKER_THREAD_PER_GROUP];
extern std::atomic<int> g_numSplits;
extern int g_combinerSplits;
extern std::set<Oplog *> *CopyGlobalThreadLogs();

#endif
