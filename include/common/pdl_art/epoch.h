#pragma once

#include "tbb/enumerable_thread_specific.h"
#include "tbb/combinable.h"
#include "glog/logging.h"
#include <atomic>
#include <array>

namespace ART {

struct LabelDelete {
    static constexpr size_t defaultArraySize = 32;
    std::array<void *, defaultArraySize> nodes;
    uint64_t epoch;
    std::size_t nodesCount;
    LabelDelete *next;
};

class DeletionList {
    LabelDelete *headDeletionList = nullptr;
    LabelDelete *freeLabelDeletes = nullptr;
    std::size_t deletitionListCount = 0;

public:
    std::atomic<uint64_t> localEpoch;
    size_t thresholdCounter{0};

    ~DeletionList();

    LabelDelete *Head();

    void Add(void *n, uint64_t globalEpoch);

    void Remove(LabelDelete *label, LabelDelete *prev);

    std::size_t Size();

    std::uint64_t deleted = 0;
    std::uint64_t added = 0;
};

class Epoch;
class EpochGuard;

class ThreadInfo {
    friend class Epoch;
    friend class EpochGuard;
    Epoch &epoch;
    DeletionList &deletionList;

    DeletionList &GetDeletionList() const;

public:
    explicit ThreadInfo(Epoch &epoch);

    ThreadInfo(const ThreadInfo &ti) : epoch(ti.epoch), deletionList(ti.deletionList) {}

    ~ThreadInfo();

    Epoch &GetEpoch() const;
};

class Epoch {
    friend class ThreadInfo;
    std::atomic<uint64_t> currentEpoch{0};

    tbb::enumerable_thread_specific<DeletionList> deletionLists;

    size_t startGCThreshhold;

public:
    static constexpr size_t thresholdCounterMask = 64 - 1;
    explicit Epoch(size_t startGCThreshhold) : startGCThreshhold(startGCThreshhold) {}

    ~Epoch();

    void EnterEpoch(ThreadInfo &epochInfo);

    void MarkNodeForDeletion(void *n, ThreadInfo &epochInfo);

    void ExitEpochAndCleanup(ThreadInfo &info);

    void ShowDeleteRatio();
};

class EpochGuard {
    ThreadInfo &threadEpochInfo;

public:
    explicit EpochGuard(ThreadInfo &threadEpochInfo) : threadEpochInfo(threadEpochInfo) {
        threadEpochInfo.GetEpoch().EnterEpoch(threadEpochInfo);
    }

    ~EpochGuard() {
        threadEpochInfo.GetEpoch().ExitEpochAndCleanup(threadEpochInfo);
    }
};

class EpochGuardReadonly {
public:
    explicit EpochGuardReadonly(ThreadInfo &threadEpochInfo) {
        threadEpochInfo.GetEpoch().EnterEpoch(threadEpochInfo);
    }

    ~EpochGuardReadonly() {}
};

inline ThreadInfo::~ThreadInfo() {
    deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());
}


inline DeletionList::~DeletionList() {
    DCHECK(deletitionListCount == 0 && headDeletionList == nullptr);
    LabelDelete *cur = nullptr;
    LabelDelete *next = freeLabelDeletes;
    while (next != nullptr) {
        cur = next;
        next = cur->next;
        delete cur;
    }
    freeLabelDeletes = nullptr;
}

inline std::size_t DeletionList::Size() {
    return deletitionListCount;
}

inline void DeletionList::Remove(LabelDelete *label, LabelDelete *prev) {
    if (prev == nullptr) {
        headDeletionList = label->next;
    } else {
        prev->next = label->next;
    }
    deletitionListCount -= label->nodesCount;

    label->next = freeLabelDeletes;
    freeLabelDeletes = label;
    deleted += label->nodesCount;
}

inline void DeletionList::Add(void *n, uint64_t globalEpoch) {
    deletitionListCount++;
    LabelDelete *label;
    if (headDeletionList != nullptr && headDeletionList->nodesCount < headDeletionList->nodes.size()) {
        label = headDeletionList;
    } else {
        if (freeLabelDeletes != nullptr) {
            label = freeLabelDeletes;
            freeLabelDeletes = freeLabelDeletes->next;
        } else {
            label = new LabelDelete();
        }
        label->nodesCount = 0;
        label->next = headDeletionList;
        headDeletionList = label;
    }
    label->nodes[label->nodesCount] = n;
    label->nodesCount++;
    label->epoch = globalEpoch;

    added++;
}

inline LabelDelete *DeletionList::Head() {
    return headDeletionList;
}

inline void Epoch::EnterEpoch(ThreadInfo &epochInfo) {
    unsigned long curEpoch = currentEpoch.load(std::memory_order_relaxed);
    epochInfo.GetDeletionList().localEpoch.store(curEpoch, std::memory_order_release);
}

inline void Epoch::MarkNodeForDeletion(void *n, ThreadInfo &epochInfo) {
    epochInfo.GetDeletionList().Add(n, currentEpoch.load());
    epochInfo.GetDeletionList().thresholdCounter++;
}

inline void Epoch::ExitEpochAndCleanup(ThreadInfo &epochInfo) {
    DeletionList &deletionList = epochInfo.GetDeletionList();
    if ((deletionList.thresholdCounter & thresholdCounterMask) == 1) {
        currentEpoch++;
    }
    if (deletionList.thresholdCounter > startGCThreshhold) {
        if (deletionList.Size() == 0) {
            deletionList.thresholdCounter = 0;
            return;
        }
        deletionList.localEpoch.store(std::numeric_limits<uint64_t>::max());

        uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
        for (auto &epoch : deletionLists) {
            auto e = epoch.localEpoch.load();
            if (e < oldestEpoch) {
                oldestEpoch = e;
            }
        }

        LabelDelete *cur = deletionList.Head();
        LabelDelete *next = nullptr;
        LabelDelete *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            if (cur->epoch < oldestEpoch) {
                for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                    PMEMoid ptr = pmemobj_oid(cur->nodes[i]);
                    pmemobj_free(&ptr);
                }
                deletionList.Remove(cur, prev);
            } else {
                prev = cur;
            }
            cur = next;
        }
        deletionList.thresholdCounter = 0;
    }
}

inline Epoch::~Epoch() {
    uint64_t oldestEpoch = std::numeric_limits<uint64_t>::max();
    for (auto &epoch : deletionLists) {
        auto e = epoch.localEpoch.load();
        if (e < oldestEpoch) {
            oldestEpoch = e;
        }
    }
    for (auto &d : deletionLists) {
        LabelDelete *cur = d.Head(), *next, *prev = nullptr;
        while (cur != nullptr) {
            next = cur->next;

            DCHECK(cur->epoch < oldestEpoch);
            for (std::size_t i = 0; i < cur->nodesCount; ++i) {
                PMEMoid ptr = pmemobj_oid(cur->nodes[i]);
                pmemobj_free(&ptr);
            }
            d.Remove(cur, prev);
            cur = next;
        }
    }
}

inline void Epoch::ShowDeleteRatio() {
    for (auto &d : deletionLists) {
        LOG(INFO) << "deleted " << d.deleted << " of " << d.added << std::endl;
    }
}

inline ThreadInfo::ThreadInfo(Epoch &epoch) : epoch(epoch), deletionList(epoch.deletionLists.local()) {}

inline DeletionList &ThreadInfo::GetDeletionList() const {
    return deletionList;
}

inline Epoch &ThreadInfo::GetEpoch() const
{
    return epoch;
}
}  // namespace ART

