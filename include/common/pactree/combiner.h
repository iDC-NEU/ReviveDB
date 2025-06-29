#ifndef PACTREE_COMBINER_H
#define PACTREE_COMBINER_H

#include "common/pdl_art/string_key.h"

namespace NVMDB {

constexpr int MAX_LOG_QUEUE_SIZE = 100;

class CombinerThread {
public:
    CombinerThread() = default;

    std::vector<OpStruct *> *combineLogs() {
        auto mergedLog = new std::vector<OpStruct *>;
        auto gThreadLogs = CopyGlobalThreadLogs();
        for (auto &i : *gThreadLogs) {
            Oplog &log = *i;
            int qnum = log.curQ % 2;
            log.Lock(qnum);
            log.curQ.fetch_add(1);
            auto op_ = log.getQ(qnum);
            if (!op_->empty()) {
                mergedLog->insert(std::end(*mergedLog), std::begin(*op_), std::end(*op_));
            }
            log.ResetQ(qnum);
            log.Unlock(qnum);
        }
        delete gThreadLogs;
        std::sort(mergedLog->begin(), mergedLog->end());
        g_combinerSplits += mergedLog->size();
        if (!mergedLog->empty()) {
            logQueue.push(make_pair(doneCountCombiner, mergedLog));
            doneCountCombiner++;
            return mergedLog;
        } else {
            delete mergedLog;
            return nullptr;
        }
    }

    void broadcastMergedLog(std::vector<OpStruct *> *mergedLog, int activeGrp) {
        for (auto i = 0; i < activeGrp * NVMDB_OPLOG_WORKER_THREAD_PER_GROUP; i++) {
            while (!g_workQueue[i].push(mergedLog)) {
            }
        }
    }

    uint64_t FreeMergedLogs(int activeGrp, bool force) {
        if (!force && logQueue.size() < MAX_LOG_QUEUE_SIZE) {
            return 0;
        }

        unsigned long minDoneCountWt = ULONG_MAX;
        for (auto i = 0; i < activeGrp * NVMDB_OPLOG_WORKER_THREAD_PER_GROUP; i++) {
            unsigned long logDoneCount = g_WorkerThreadInst[i]->GetLogDoneCount();
            if (logDoneCount < minDoneCountWt)
                minDoneCountWt = logDoneCount;
        }

        while (!logQueue.empty() && logQueue.front().first < minDoneCountWt) {
            auto mergedLog = logQueue.front().second;
            delete mergedLog;
            logQueue.pop();
        }
        return minDoneCountWt;
    }

    bool MergedLogsToBeFreed() {
        return logQueue.empty();
    }
private:
    std::queue<std::pair<unsigned long, std::vector<OpStruct *> *>> logQueue;
    unsigned long doneCountCombiner{0};
};

}  // namespace NVMDB

#endif
