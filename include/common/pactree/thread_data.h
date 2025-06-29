#ifndef PACTREE_THREADS_H
#define PACTREE_THREADS_H

#include <atomic>

class ThreadData {
public:
    explicit ThreadData(int threadId) {
        this->threadId = threadId;
#ifdef PACTREE_ENABLE_STATS
        sltime = 0;
        dltime = 0;
#endif
    }
#ifdef PACTREE_ENABLE_STATS
    uint64_t sltime;
    uint64_t dltime;
#endif

    void SetThreadId(int thrdId) {
        this->threadId = thrdId;
    }

    int GetThreadId() const {
        return this->threadId;
    }

    void SetFinish() {
        this->finish = true;
    }

    bool GetFinish() const {
        return this->finish;
    }

    void SetLocalClock(uint64_t clock) {
        this->localClock = clock;
    }

    uint64_t GetLocalClock() const {
        return this->localClock;
    }

    void IncrementRunCntAtomic() {
        runCnt.fetch_add(1);
    }

    void IncrementRunCnt() {
        runCnt++;
    };

    uint64_t GetRunCnt() {
        return this->runCnt;
    }

    void ReadLock(uint64_t clock) {
        this->SetLocalClock(clock);
        this->IncrementRunCntAtomic();
    }

    void ReadUnlock() {
        this->IncrementRunCnt();
    }

private:
    int threadId{0};
    uint64_t localClock{0};
    bool finish{false};
    volatile std::atomic<uint64_t> runCnt{0};
};

#endif
