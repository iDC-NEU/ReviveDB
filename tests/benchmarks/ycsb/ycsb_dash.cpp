#include "ycsb.h"
#include "nvm_table.h"
#include "nvm_init.h"
#include "nvm_access.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cmath>
#include "ycsb_index/dash.h"
namespace NVMDB {
namespace YCSB {
namespace DASH {
void *dash{nullptr};
class DashYcsbDB {
public:
    void read(Transaction *txn, Table *table, RowId key, RAMTuple *tuple) const
    {
        RowId rowId = dash_find(dash, key);
        CHECK(HeapRead(txn, table, rowId, tuple) == HamStatus::OK) << key << ":" << rowId;
    }
    bool write(Transaction *txn, Table *table, RowId key, RAMTuple *tuple, ColumnIdx colIdx, char *colValue) const
    {
        RowId rowId = dash_find(dash, key);
        tuple->UpdateCol(colIdx, colValue);
        return HeapUpdate2(txn, table, rowId, tuple) == HamStatus::OK;
    }
    RowId insert(Transaction *txn, Table *table, RAMTuple *tuple) const
    {
        RowId rowId = HeapInsert(txn, table, tuple);
        dash_insert(dash, rowId, rowId);
        return rowId;
    }
};
using DashYcsbTable = YcsbTable<DashYcsbDB>;
class YCSBTest : public testing::Test {
public:
    static void RunBench(const YcsbRunParam &runParam)
    {
        results.emplace_back(ycsbTable->runBench(runParam));
    }

protected:
    static void PrintResults()
    {
        LOG(INFO) << "All Tests Finished.";
        results.print(12, 2);
    }
    static std::unique_ptr<DashYcsbTable> ycsbTable;
    static std::string dir_config;
    static std::string dash_config;
    static const YcsbTableParam tableParam;
    static BenchResultList results;
};
std::unique_ptr<DashYcsbTable> YCSBTest::ycsbTable{nullptr};
std::string YCSBTest::dir_config = genTestDir<1>("ycsb");
std::string YCSBTest::dash_config = genTestDir<1>("ycsb") + "/dash";
BenchResultList YCSBTest::results{};

class YCSBTestWithInit : public YCSBTest {
protected:
    static void SetUpTestSuite()
    {
        InitDB(dir_config);
        std::remove(dash_config.c_str());
        init(dash_config.c_str());
        dash = dash_create();
        ycsbTable = std::make_unique<DashYcsbTable>(tableParam, true);
    }
    static void TearDownTestSuite()
    {
        ExitDBProcess();
        PrintResults();
    }
};
class YCSBTestWithBootStrap : public YCSBTest {
protected:
    void SetUp() override
    {
        init(dash_config.c_str());
        LOG(INFO) << "BootStrap Start." << std::endl;
        BootStrap(dir_config);
        LOG(INFO) << "MountIndex Start." << std::endl;
        dash = dash_create();
        LOG(INFO) << "BootStrap End." << std::endl;
        ycsbTable = std::make_unique<DashYcsbTable>(tableParam, false);
    }
    void TearDown() override
    {
        ExitDBProcess();
        ycsbTable = nullptr;
    }
    static void SetUpTestSuite()
    {}
    static void TearDownTestSuite()
    {
        PrintResults();
    }
};
}  // namespace DASH
}  // namespace YCSB
}  // namespace NVMDB
using NVMDB::YCSB::YcsbRunParam;
using NVMDB::YCSB::YcsbTableParam;
using YCSBDASHTestWithBootStrap = NVMDB::YCSB::DASH::YCSBTestWithBootStrap;
using YCSBDASHTestWithInit = NVMDB::YCSB::DASH::YCSBTestWithInit;

const YcsbTableParam NVMDB::YCSB::DASH::YCSBTest::tableParam = YcsbTableParam(100, 256000000);
constexpr size_t Terminal = 48;
constexpr size_t WarmUpSec = 5;
constexpr size_t RunSec = 5;

struct ReadPercent {
    static constexpr size_t YcsbA = 50;
    static constexpr size_t YcsbB = 95;
    static constexpr size_t YcsbC = 100;
};

struct SkewTheta {
    static constexpr double NoSkew = 0.0;
    static constexpr double Skew = 0.99;
};

TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_12A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_12B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_12C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 12, WarmUpSec, RunSec));
}

TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_24A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_24B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_24C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 24, WarmUpSec, RunSec));
}

TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_36A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_36B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_36C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 36, WarmUpSec, RunSec));
}


TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_48A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_48B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_48C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 48, WarmUpSec, RunSec));
}

// TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_A)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_B)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBDASHTestWithInit, YCSB_NOSKEW_C)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
// }

TEST_F(YCSBDASHTestWithInit, YCSB_SKEW_A)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
}

TEST_F(YCSBDASHTestWithInit, YCSB_SKEW_B)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
}

TEST_F(YCSBDASHTestWithInit, YCSB_SKEW_C)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
}