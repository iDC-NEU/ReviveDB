#include "ycsb.h"
#include "ycsb_def.h"
#include "common/nvm_types.h"
#include "heap/nvm_tuple.h"
#include "nvm_access.h"
#include "nvm_init.h"
#include "nvm_table.h"
#include "transaction/nvm_transaction.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cmath>
namespace NVMDB {
namespace YCSB {
class DramYcsbDB {
public:
    void read(Transaction *txn, Table *table, RowId key, RAMTuple *tuple) const
    {
        CHECK(HeapRead(txn, table, key, tuple) == HamStatus::OK) << key;
    }
    bool write(Transaction *txn, Table *table, RowId key, RAMTuple *tuple, ColumnIdx colIdx, char *colValue) const
    {
        tuple->UpdateCol(colIdx, colValue);
        return HeapUpdate2(txn, table, key, tuple) == HamStatus::OK;
    }
    RowId insert(Transaction *txn, Table *table, RAMTuple *tuple) const
    {
        return HeapInsert(txn, table, tuple);
    }
};
using DramYcsbTable = YcsbTable<DramYcsbDB>;
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
    static std::unique_ptr<DramYcsbTable> ycsbTable;
    static std::string dir_config;
    static const YcsbTableParam tableParam;
    static BenchResultList results;
};
std::unique_ptr<DramYcsbTable> YCSBTest::ycsbTable{nullptr};
std::string YCSBTest::dir_config = genTestDir<1>("ycsb");
BenchResultList YCSBTest::results{};

class YCSBTestWithInit : public YCSBTest {
protected:
    static void SetUpTestSuite()
    {
        InitDB(dir_config);
        ycsbTable = std::make_unique<DramYcsbTable>(tableParam, true);
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
        LOG(INFO) << "BootStrap Start." << std::endl;
        BootStrap(dir_config);
        LOG(INFO) << "BootStrap End." << std::endl;
        ycsbTable = std::make_unique<DramYcsbTable>(tableParam, false);
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
}  // namespace YCSB
}  // namespace NVMDB
using NVMDB::YCSB::YcsbRunParam;
using NVMDB::YCSB::YcsbTableParam;
using NVMDB::YCSB::YCSBTestWithBootStrap;
using NVMDB::YCSB::YCSBTestWithInit;

const YcsbTableParam NVMDB::YCSB::YCSBTest::tableParam = YcsbTableParam(100, 256000000);
constexpr size_t Terminal = 48;
constexpr size_t WarmUpSec = 10;
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

TEST_F(YCSBTestWithInit, YCSB_NOSKEW_12A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_12B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_12C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 12, WarmUpSec, RunSec));
}

TEST_F(YCSBTestWithInit, YCSB_NOSKEW_24A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_24B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_24C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 24, WarmUpSec, RunSec));
}

TEST_F(YCSBTestWithInit, YCSB_NOSKEW_36A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_36B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_36C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 36, WarmUpSec, RunSec));
}

TEST_F(YCSBTestWithInit, YCSB_NOSKEW_48A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_48B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBTestWithInit, YCSB_NOSKEW_48C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 48, WarmUpSec, RunSec));
}



// TEST_F(YCSBTestWithInit, YCSB_NOSKEW_A)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBTestWithInit, YCSB_NOSKEW_B)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBTestWithInit, YCSB_NOSKEW_C)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
// }

TEST_F(YCSBTestWithInit, YCSB_SKEW_A)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
}

TEST_F(YCSBTestWithInit, YCSB_SKEW_B)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
}

TEST_F(YCSBTestWithInit, YCSB_SKEW_C)
{
    RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
}