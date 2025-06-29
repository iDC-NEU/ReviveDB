#pragma once
#include <common/nvm_types.h>
#include <glog/logging.h>
#include <random>
namespace NVMDB {
namespace YCSB {
constexpr size_t ColumnCount = 10;      // 一张表的列数
constexpr size_t MaxColumnSize = 1024;  // 每列最长为MaxColumnSize
static_assert(ColumnCount * MaxColumnSize % sizeof(decltype(std::default_random_engine()())) == 0);
constexpr size_t OpPerTxn = 10;           // 一个事务的operation数量
constexpr TableId YcsbTableId = 1;        // 测试表的ID
constexpr uint32 YcsbTableSegHead = 2;    // 测试表的SegHead
constexpr size_t IngestBatchSize = 1024;  // 插入每批IngestBatchSize行
constexpr size_t IngestThreadNum = 48;    // 插入线程数
using ColumnIdx = uint32;

/**
 * 1. 行长度：10列 * ${ColumnSize, default=100}
 * 2. 偏斜 zipfian(\theta) No-Skew (\theta = 0), Low-Skew (\theta = 0.6), and
 * High-Skew(\theta = 0.95)
 * 3. YCSB-T，每个事务10个op：A(50%读50%写),B(95%,5%),C(全读)，
 * ${SkewOpPerTxn}个op是热点数据,写只更新一列
 * 4. 表大小(行数) ${Items}
 * 5. 线程数${terminal}
 * 6. rowid为主键，各线程插入直到rowid>Items
 */

struct YcsbTableParam {
    /**
     * @brief 每列大小
     * @details 测试表有10列，每列大小${BytePerColumn}
     */
    size_t BytePerColumn{100};
    /**
     * @brief 表中(用于测试的)行数
     */
    size_t Items{static_cast<size_t>(1e6)};

    YcsbTableParam(const size_t BytePerColumn, const size_t Items) : BytePerColumn(BytePerColumn), Items(Items)
    {
        CHECK(0 < BytePerColumn && BytePerColumn < MaxColumnSize);
    }
};

struct YcsbRunParam {
    /**
     * @brief zipfian 分布的参数
     */
    double Theta{0.0};
    /**
     * @brief 每个事务中有多少操作使用zipfian分布，其他操作使用均匀分布
     * @details 每个事务有10各操作
     */
    size_t SkewOpPerTxn{10};
    /**
     * @brief 事务中读操作的概率
     */
    size_t ReadPercent{50};
    /**
     * @brief 测试时并行线程数
     */
    size_t Terminal{16};
    /**
     * @brief 预热时间(秒)
     */
    size_t WarmUpSec{120};
    /**
     * @brief 运行时间(秒)
     */
    size_t RunSec{300};

    YcsbRunParam(const double Theta, const size_t SkewOpPerTxn, const size_t ReadPercent, const size_t Terminal,
                 const size_t WarmUpSec, const size_t RunSec)
        : Theta(Theta),
          SkewOpPerTxn(SkewOpPerTxn),
          ReadPercent(ReadPercent),
          Terminal(Terminal),
          WarmUpSec(WarmUpSec),
          RunSec(RunSec)
    {
        CHECK(0 <= Theta && Theta != 1);
        CHECK(SkewOpPerTxn <= OpPerTxn);
        CHECK(ReadPercent <= 100);
    }
};
}  // namespace YCSB
}  // namespace NVMDB