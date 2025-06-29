#include "common/pactree/worker_thread.h"
#include "common/pactree/search_layer.h"

namespace NVMDB {

extern std::vector<SearchLayer *> g_perGrpSlPtr;

WorkerThread::WorkerThread(int id, int activeGrp) {
    this->workerThreadId = id;
    this->activeGrp = activeGrp;
    this->workQueue = &g_workQueue[workerThreadId];
    this->logDoneCount = 0;
    this->opcount = 0;
    if (id == 0) {
        freeQueue = new std::queue<std::pair<uint64_t, void *>>();
    }
}

bool WorkerThread::ApplyOperation() {
    std::vector<OpStruct *> *oplog = workQueue->front();
    int grpId = workerThreadId % activeGrp;
    SearchLayer *sl = g_perGrpSlPtr[grpId];
    auto hash = static_cast<uint8_t>(workerThreadId / activeGrp);
    for (auto opsPtr : *oplog) {
        OpStruct &ops = *opsPtr;
        if (ops.hash != hash) {
            continue;
        }
        opcount++;
        if (ops.op == OpStruct::insert) {
            void *newNodePtr = reinterpret_cast<void *>((static_cast<unsigned long>(ops.poolId) << 48) | ops.newNodeOid.off);
            sl->Insert(ops.key, newNodePtr);
            uint8_t remain_task = opsPtr->searchLayers.fetch_sub(sl->grpMask);
            if (remain_task == sl->grpMask) {
                opsPtr->op = OpStruct::done;
            }
        } else if (ops.op == OpStruct::remove) {
            sl->remove(ops.key, ops.oldNodePtr);
            uint8_t remain_task = opsPtr->searchLayers.fetch_sub(sl->grpMask);
            if (remain_task == sl->grpMask) {
                opsPtr->op = OpStruct::done;
            }
        } else if (ops.op == OpStruct::done) {
            LOG(WARNING) << "Operation is done, " << opsPtr;
        } else {
            CHECK(false) << "unknown op";
        }
        flushToNVM((char *)opsPtr, sizeof(OpStruct));
        smp_wmb();
    }
    workQueue->pop();
    logDoneCount++;
    return true;
}

void WorkerThread::FreeListNodes(uint64_t removeCount) {
    DCHECK(workerThreadId == 0 && freeQueue != nullptr);
    if (freeQueue->empty()) {
        return;
    }
    while (!freeQueue->empty()) {
        std::pair<uint64_t, void *> removePair = freeQueue->front();
        if (removePair.first < removeCount) {
            PMEMoid ptr = pmemobj_oid(removePair.second);
            pmemobj_free(&ptr);
            freeQueue->pop();
        } else {
            break;
        }
    }
}

}  // namespace NVMDB
