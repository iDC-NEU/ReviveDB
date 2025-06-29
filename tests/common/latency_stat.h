#pragma once
#include <chrono>
#include <cstdint>
#include <cstring>
#include <glog/logging.h>
#include <immintrin.h>
#define TEST_LATENCY
namespace NVMDB {
class TestTimer {
public:
    using TimePoint = std::chrono::_V2::steady_clock::time_point;
    TestTimer()
    {
        start();
    }
    void start()
    {
#ifdef TEST_LATENCY
        start_point = std::chrono::steady_clock::now();
#endif
    }
    int64_t getDurationUs() const
    {
#ifdef TEST_LATENCY
        auto duration = std::chrono::steady_clock::now() - start_point;
        return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
#else
        return 0;
#endif
    }

private:
#ifdef TEST_LATENCY
    TimePoint start_point{};
#endif
};
template <size_t BucketNum = 248, int64_t Precision = 1>
class LatencyStat {
public:
    static constexpr int64_t m_maxLatencyUs = BucketNum * Precision;
    static constexpr int64_t m_precision = Precision;
    LatencyStat()
    {
        reset();
    }
    void reset()
    {
        std::memset(m_data, 0, sizeof(int64_t) * BucketNum);
    }
    void insert(int64_t latencyUs)
    {
        if (__glibc_likely(latencyUs < m_maxLatencyUs)) {
            ++m_data[latencyUs / Precision];
        } else {
            DLOG(INFO) << "Latency" << latencyUs << ">=" << m_maxLatencyUs;
            ++m_overflowCnt;
            m_overflowSum += latencyUs;
        }
    }
    int64_t avg() const
    {
        __m512i totalTimeVec = _mm512_set1_epi64(0);
        __m512i totalTxnsVec = _mm512_set1_epi64(0);
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            __m512i latencyUsVec = _mm512_setr_epi64(latencyUs, latencyUs + 1, latencyUs + 2, latencyUs + 3,
                                                     latencyUs + 4, latencyUs + 5, latencyUs + 6, latencyUs + 7);
            __m512i latencyCntVec = _mm512_load_epi64(m_data + latencyUs);
            totalTimeVec = _mm512_add_epi64(totalTimeVec, _mm512_mullo_epi64(latencyUsVec, latencyCntVec));
            totalTxnsVec = _mm512_add_epi64(totalTxnsVec, latencyCntVec);
        }
        if (_mm512_sumup_epi64(totalTxnsVec) + m_overflowCnt == 0) {
            return 0;
        }
        return (_mm512_sumup_epi64(totalTimeVec) * Precision + m_overflowSum) /
               (_mm512_sumup_epi64(totalTxnsVec) + m_overflowCnt);
    }
    int64_t percentile(size_t p = 95) const
    {
        __m512i totalTxnsVec = _mm512_set1_epi64(0);
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            __m512i latencyCntVec = _mm512_load_epi64(m_data + latencyUs);
            totalTxnsVec = _mm512_add_epi64(totalTxnsVec, latencyCntVec);
        }
        int64_t totalTxns = _mm512_sumup_epi64(totalTxnsVec) + m_overflowCnt;
        if (totalTxns == 0) {
            return 0;
        }
        int64_t ith = totalTxns * p / 100;
        int64_t preffixTotalTxns = 0;
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs++) {
            if (preffixTotalTxns <= ith && ith < preffixTotalTxns + m_data[latencyUs]) {
                return latencyUs * Precision;
            }
            preffixTotalTxns += m_data[latencyUs];
        }
        LOG(INFO) << "percentile > " << m_maxLatencyUs << ", result isn't accurate";
        return m_overflowSum / m_overflowCnt;
    }
    LatencyStat &operator+=(const LatencyStat &other)
    {
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            _mm512_store_epi64(m_data + latencyUs, _mm512_add_epi64(_mm512_load_epi64(m_data + latencyUs),
                                                                    _mm512_load_epi64(other.m_data + latencyUs)));
        }
        m_overflowCnt += other.m_overflowCnt;
        m_overflowSum += other.m_overflowSum;
        return *this;
    }
    LatencyStat &operator-=(const LatencyStat &other)
    {
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            _mm512_store_epi64(m_data + latencyUs, _mm512_sub_epi64(_mm512_load_epi64(m_data + latencyUs),
                                                                    _mm512_load_epi64(other.m_data + latencyUs)));
        }
        m_overflowCnt -= other.m_overflowCnt;
        m_overflowSum -= other.m_overflowSum;
        return *this;
    }
    bool operator==(const LatencyStat &other) const
    {
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs++) {
            if (m_data[latencyUs] != other.m_data[latencyUs]) {
                return false;
            }
        }
        return m_overflowCnt == other.m_overflowCnt && m_overflowSum == other.m_overflowSum;
    }
    static void sumUp(LatencyStat &out, const LatencyStat *stats, size_t len)
    {
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            __m512i vec = _mm512_set1_epi64(0);
            for (size_t i = 0; i < len; i++) {
                vec = _mm512_add_epi64(vec, _mm512_load_epi64(stats[i].m_data + latencyUs));
            }
            _mm512_store_epi64(out.m_data + latencyUs, vec);
        }
        out.m_overflowCnt = 0;
        out.m_overflowSum = 0;
        for (size_t i = 0; i < len; i++) {
            out.m_overflowCnt += stats[i].m_overflowCnt;
            out.m_overflowSum += stats[i].m_overflowSum;
        }
    }
    template <typename T, typename F>
    static inline void sumUp(LatencyStat &out, const T *boxes, size_t len, const F &&getStatFromBox)
    {
        for (int64_t latencyUs = 0; latencyUs < BucketNum; latencyUs += BatchSize) {
            __m512i vec = _mm512_set1_epi64(0);
            for (size_t i = 0; i < len; i++) {
                vec = _mm512_add_epi64(vec, _mm512_load_epi64(getStatFromBox(boxes[i]).m_data + latencyUs));
            }
            _mm512_store_epi64(out.m_data + latencyUs, vec);
        }
        out.m_overflowCnt = 0;
        out.m_overflowSum = 0;
        for (size_t i = 0; i < len; i++) {
            out.m_overflowCnt += getStatFromBox(boxes[i]).m_overflowCnt;
            out.m_overflowSum += getStatFromBox(boxes[i]).m_overflowSum;
        }
    }

private:
    // Sequence
    static int64_t _mm512_sumup_epi64(__m512i vec)
    {
        int64_t res = 0;
        int64_t *arr = reinterpret_cast<int64_t *>(&vec);
        for (size_t i = 0; i < BatchSize; i++) {
            res += arr[i];
        }
        return res;
    }
    static constexpr int64_t BatchSize = sizeof(__m512i) / sizeof(int64_t);
    static_assert(BucketNum % BatchSize == 0, "MaxLatency should be divisible by 8.");
    int64_t m_data[BucketNum];
    int64_t m_overflowCnt{0};
    int64_t m_overflowSum{0};
} __attribute__((aligned(64)));

/* if class T is aligned m, in c++14, new T or new T[] may NOT be aligned. */

template <typename T, size_t Align = 64>
T *makeAlignedArray(size_t length)
{
    T *arr = static_cast<T *>(_mm_malloc(sizeof(T) * length, Align));
    for (size_t i = 0; i < length; i++) {
        new (&arr[i]) T();
    }
    return arr;
}
template <typename T, size_t Align = 64>
void deleteAlignedArray(T *arr, size_t length)
{
    for (size_t i = 0; i < length; i++) {
        arr[i].~T();
    }
    _mm_free(arr);
}
}  // namespace NVMDB