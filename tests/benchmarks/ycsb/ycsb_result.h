#pragma once
#include "ycsb_def.h"
#include "ycsb_statisitic.h"
namespace NVMDB {
namespace YCSB {
struct BenchResult {
    YcsbTableParam m_tableParam;
    YcsbRunParam m_runParam;
    YcsbStat::Snapshot m_statistic;
};

// 用于在最后输出结果
class BenchResultList : public std::vector<BenchResult*> {
public:
    void print(const int columnWidth, const int decimalPlace) const
    {
        printf("Results:\n");
        printSpliter(columnWidth, '=');
        printLine("#OpPerTxn", columnWidth, decimalPlace, [](const BenchResult &result) { return OpPerTxn; });
        printLine("BytePerColumn", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_tableParam.BytePerColumn; });
        printLine("Items", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_tableParam.Items; });
        printSpliter(columnWidth, '-');
        printLine("Theta", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.Theta; });
        printLine("SkewOpPerTxn", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.SkewOpPerTxn; });
        printLine("ReadPercent", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.ReadPercent; });
        printLine("#Terminal", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.Terminal; });
        printLine("WarmUp(sec)", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.WarmUpSec; });
        printLine("Run(sec)", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_runParam.RunSec; });
        printSpliter(columnWidth, '-');
        printLine("Run(sec)", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getRunSec(); });
        printLine("#Commit", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getCommitCount(); });
        printLine("#Abort", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getAbortCount(); });
        printLine("#Read", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getReadCount(); });
        printLine("#Write", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getWriteCount(); });
        printSpliter(columnWidth, '-');
        printLine("%Commit", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getCommitRate(); });
        printLine("%Abort", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getAbortRate(); });
        printLine("%Read", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getReadRate(); });
        printLine("%Write", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getWriteRate(); });
        printSpliter(columnWidth, '-');
        printLine("#Commit/sec", columnWidth, decimalPlace, [](const BenchResult &result) {
            return result.m_statistic.getStat().getCommitCount() / result.m_statistic.getRunSec();
        });
        printLine("#Read/sec", columnWidth, decimalPlace, [](const BenchResult &result) {
            return result.m_statistic.getStat().getReadCount() / result.m_statistic.getRunSec();
        });
        printLine("#Write/sec", columnWidth, decimalPlace, [](const BenchResult &result) {
            return result.m_statistic.getStat().getWriteCount() / result.m_statistic.getRunSec();
        });
        printSpliter(columnWidth, '-');
        printLine("Avg Latency/us", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getAvgLatencyUs(); });
        printLine("P95 Latency/us", columnWidth, decimalPlace,
                  [](const BenchResult &result) { return result.m_statistic.getStat().getPercentileLatencyUs(95); });
        printSpliter(columnWidth, '=');
    }

private:
    void printSpliter(const int columnWidth, char spliter) const
    {
        printf("+");
        for (int i = 0; i < 16; i++) {
            printf("%c", spliter);
        }
        printf("+");
        for ([[maybe_unused]] const BenchResult *item : *this) {
            for (int i = 0; i < columnWidth; i++) {
                printf("%c", spliter);
            }
            printf("+");
        }
        printf("\n");
    }
    template <typename GetResultF>
    void printLine(const char *Title, const int columnWidth, const int decimalPlace, const GetResultF &getValueF) const
    {
        printf("|%-16s|", Title);
        for (const BenchResult *item : *this) {
            printValue(columnWidth, decimalPlace, getValueF(*item));
        }
        printf("\n");
    }
    static void printValue(const int columnWidth, const int decimalPlace, const double value)
    {
        printf("%*.*f|", columnWidth, decimalPlace, value);
    }
    static void printValue(const int columnWidth, [[maybe_unused]] const int decimalPlace, const int64_t value)
    {
        printf("%*ld|", columnWidth, value);
    }
    static void printValue(const int columnWidth, [[maybe_unused]] const int decimalPlace, const size_t value)
    {
        printf("%*lu|", columnWidth, value);
    }
};

}  // namespace YCSB
}  // namespace NVMDB