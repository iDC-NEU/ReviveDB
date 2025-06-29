#include "latency_stat.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <random>
#include <vector>
TEST(LatencyStatTest, BenchTimer)
{
    NVMDB::TestTimer g_timer;
    constexpr int64_t n = 1000000;
    int64_t unused_int = 0;
    for (size_t i = 0; i < n; i++) {
        NVMDB::TestTimer timer;
        unused_int += timer.getDurationUs();
    }
    int64_t total = g_timer.getDurationUs();
    double latency = static_cast<double>(total) / n;
    GTEST_LOG_(INFO) << "latency " << latency << "us" << std::endl;
    GTEST_LOG_(INFO) << "unused_int" << unused_int << " total " << total << std::endl;
}
static int64_t avg(int64_t *arr, size_t len)
{
    int64_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum += arr[i];
    }
    return sum / len;
}
static int64_t percentile(size_t p, int64_t *arr, size_t len)
{
    std::sort(arr, arr + len);
    return arr[len * p / 100];
}
TEST(LatencyStatTest, Basic)
{
    constexpr size_t workers = 100;
    NVMDB::LatencyStat<> stat;
    NVMDB::LatencyStat<> stats[workers];
    std::vector<int64_t> vals;

    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int64_t> dist(0, NVMDB::LatencyStat<>::m_maxLatencyUs);
    for (size_t i = 0; i < 10000000; i++) {
        auto x = dist(gen);
        vals.push_back(x);
        stat.insert(x);
        stats[i % workers].insert(x);
    }
    NVMDB::LatencyStat<> stat1;
    NVMDB::LatencyStat<> stat2;
    for (size_t i = 0; i < workers; i++) {
        stat1 += stats[i];
    }
    CHECK(stat == stat1);
    for (size_t i = 0; i < 10; i++) {
        NVMDB::TestTimer timer;
        NVMDB::LatencyStat<>::sumUp(stat2, stats, workers);
        LOG(INFO) << timer.getDurationUs();
        CHECK(stat == stat2);
    }
    {
        NVMDB::TestTimer timer;
        auto a = stat.avg();
        LOG(INFO) << timer.getDurationUs();
        CHECK(std::abs(avg(vals.data(), vals.size()) - a) <= NVMDB::LatencyStat<>::m_precision);
    }
    {
        NVMDB::TestTimer timer;
        auto p = stat.percentile(95);
        LOG(INFO) << timer.getDurationUs();
        CHECK(std::abs(percentile(95, vals.data(), vals.size()) - p) <= NVMDB::LatencyStat<>::m_precision);
    }
}