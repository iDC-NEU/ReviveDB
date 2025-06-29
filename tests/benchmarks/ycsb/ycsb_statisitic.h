#pragma once
#include "common/latency_stat.h"
#include <cstdint>
#include <immintrin.h>
#include <vector>
#include <chrono>
#include <cstdio>
#include <glog/logging.h>
namespace NVMDB {
namespace YCSB {
/**
 * @brief 统计YCSB读、写、提交、回退
 */
class YcsbStat : boost::noncopyable {
public:
    class alignas(64) Stat {
    public:
        Stat()
        {
            CHECK((uint64_t)(this) % 64 == 0);
            CHECK(((uint64_t)(&m_latencyStat) - (uint64_t)(&m_readCount)) % 64 == 0);
        }
        void commit(const int64_t read, const int64_t write, const int64_t latencyUs)
        {
            m_commitCount++;
            m_readCount += read;
            m_writeCount += write;
            m_latencyStat.insert(latencyUs);
        }
        void abort()
        {
            m_abortCount++;
        }
        static void sumUp(Stat &out, const Stat *stats, size_t len)
        {
            LatencyStat<>::sumUp(out.m_latencyStat, stats, len,
                                 [](const Stat &stat) -> const LatencyStat<> & { return stat.m_latencyStat; });
            __m512i vec = _mm512_set1_epi64(0);
            for (size_t i = 0; i < len; i++) {
                vec = _mm512_add_epi64(vec, stats[i].loadVec());
            }
            out.storeVec(vec);
        }
        Stat &operator-=(const Stat &stat)
        {
            storeVec(_mm512_sub_epi64(loadVec(), stat.loadVec()));
            m_latencyStat -= stat.m_latencyStat;
            return *this;
        }
        void clear()
        {
            storeVec(_mm512_set1_epi64(0));
            m_latencyStat.reset();
        }
        int64_t getReadCount() const
        {
            return m_readCount;
        }
        int64_t getWriteCount() const
        {
            return m_writeCount;
        }
        int64_t getCommitCount() const
        {
            return m_commitCount;
        }
        int64_t getAbortCount() const
        {
            return m_abortCount;
        }
        double getReadRate() const
        {
            return static_cast<double>(m_readCount) / (m_readCount + m_writeCount) * 100;
        }
        double getWriteRate() const
        {
            return static_cast<double>(m_writeCount) / (m_readCount + m_writeCount) * 100;
        }
        double getCommitRate() const
        {
            return static_cast<double>(m_commitCount) / (m_commitCount + m_abortCount) * 100;
        }
        double getAbortRate() const
        {
            return static_cast<double>(m_abortCount) / (m_commitCount + m_abortCount) * 100;
        }
        int64_t getAvgLatencyUs() const
        {
            return m_latencyStat.avg();
        }
        int64_t getPercentileLatencyUs(size_t p = 95) const
        {
            return m_latencyStat.percentile(p);
        }

    private:
        __m512i loadVec() const
        {
            return _mm512_load_epi64(&m_readCount);
        }
        void storeVec(__m512i vec)
        {
            return _mm512_store_epi64(&m_readCount, vec);
        }
        int64_t m_readCount{0};
        int64_t m_writeCount{0};
        int64_t m_commitCount{0};
        int64_t m_abortCount{0};
        LatencyStat<> m_latencyStat;
    };

    class Snapshot {
    public:
        Snapshot(const Stat &stat, const double runSec, const size_t terminal)
            : m_stat(stat), m_runSec(runSec), m_terminal(terminal)
        {}
        Snapshot()
        {}
        void print() const
        {
            std::printf("Latency: Avg=%6ldus, P95=%6ldus\n", m_stat.getAvgLatencyUs(),
                        m_stat.getPercentileLatencyUs(95));
            std::printf("+=====+================+================+================+================+\n");
            std::printf("|     |     commit     |      abort     |      read      |      write     |\n");
            std::printf("+-----+----------------+----------------+----------------+----------------+\n");
            std::printf("|   # |%16ld %16ld %16ld %16ld|\n", m_stat.getCommitCount(), m_stat.getAbortCount(),
                        m_stat.getReadCount(), m_stat.getWriteCount());
            std::printf("|   %% |%15.2f%% %15.2f%% %15.2f%% %15.2f%%|\n", m_stat.getCommitRate(), m_stat.getAbortRate(),
                        m_stat.getReadRate(), m_stat.getWriteRate());
            std::printf("| TPS |%16.0f %16s %16.0f %16.0f|\n", m_stat.getCommitCount() / m_runSec, "#",
                        m_stat.getReadCount() / m_runSec, m_stat.getWriteCount() / m_runSec);
            std::printf("+=====+================+================+================+================+\n\n");
            std::fflush(stdout);
        }
        Snapshot &operator-=(const Snapshot &snapshot)
        {
            CHECK(m_terminal == snapshot.m_terminal);
            m_runSec -= snapshot.m_runSec;
            m_stat -= snapshot.m_stat;
            return *this;
        }
        const Stat &getStat() const
        {
            return m_stat;
        }
        double getRunSec() const
        {
            return m_runSec;
        }
        int64_t getTps() const
        {
            return static_cast<int64_t>(m_stat.getCommitCount() / m_runSec);
        }

    private:
        Stat m_stat{};
        double m_runSec{0};
        size_t m_terminal{0};
    };

    explicit YcsbStat(const size_t terminal = 1)
        : m_terminals(terminal), m_stats(makeAlignedArray<Stat>(terminal)), m_startTs(std::chrono::steady_clock::now())
    {}
    ~YcsbStat()
    {
        deleteAlignedArray(m_stats, m_terminals);
    }

    /**
     * @brief 重置时钟和统计状态
     */
    void resetStat()
    {
        m_startTs = std::chrono::steady_clock::now();
        for (size_t i = 0; i < m_terminals; i++) {
            m_stats[i].clear();
        }
    }
    /**
     * @brief 重新设置运行线程数并重置时钟和统计状态
     * @param terminal 运行的线程数
     */
    void resize(const size_t terminal)
    {
        deleteAlignedArray(m_stats, m_terminals);
        m_terminals = terminal;
        m_stats = makeAlignedArray<Stat>(terminal);
        resetStat();
    }

    Stat &getStat(const size_t threadId)
    {
        CHECK(threadId < m_terminals);
        return m_stats[threadId];
    }

    /**
     * @brief 返回调用的时间戳和全局统计状态
     * 单线程调用
     */
    Snapshot getGlobalSnapshot() const
    {
        const double runSec =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - m_startTs)
                .count() /
            1000.0;
        static Stat gstat;
        Stat::sumUp(gstat, m_stats, m_terminals);
        return Snapshot(gstat, runSec, m_terminals);
    }

private:
    size_t m_terminals;
    Stat *m_stats{nullptr};
    std::chrono::_V2::steady_clock::time_point m_startTs;
};
}  // namespace YCSB
}  // namespace NVMDB