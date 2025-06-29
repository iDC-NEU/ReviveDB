#ifdef TEST_PACTREE_INDEX
#include "ycsb.h"
#include "common/nvm_types.h"
#include "heap/nvm_tuple.h"
#include "nvm_access.h"
#include "nvm_init.h"
#include "nvm_table.h"
#include "transaction/nvm_transaction.h"
#include "ycsb_index/pactree.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cmath>
// not used
namespace NVMDB {
namespace YCSB {
namespace PacTree {
class PacTreeYcsbDB {
    // TODO: 加上threadlocal index init
public:
    void read(Transaction *txn, Table *table, RowId key, RAMTuple *tuple) const
    {
        CHECK(HeapRead(txn, table, getRowId(txn, key), tuple) == HamStatus::OK);
    }
    bool write(Transaction *txn, Table *table, RowId key, RAMTuple *tuple, ColumnIdx colIdx, char *colValue) const
    {
        tuple->UpdateCol(colIdx, colValue);
        return HeapUpdate2(txn, table, getRowId(txn, key), tuple) == HamStatus::OK;
    }
    RowId insert(Transaction *txn, Table *table, RAMTuple *tuple) const
    {
        RowId rowId = HeapInsert(txn, table, tuple);
        PacTreeIndex::Insert(rowId, rowId, txn);
        return rowId;
    }

private:
    RowId getRowId(Transaction *txn, RowId key) const
    {
        std::vector<RowId> rids;
        PacTreeIndex::Get(key, txn, rids);
        CHECK(rids.size() == 1);
        return rids.front();
    }
};
using PacTreeYcsbTable = YcsbTable<PacTreeYcsbDB>;
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
    static std::unique_ptr<PacTreeYcsbTable> ycsbTable;
    static std::string dir_config;
    static const YcsbTableParam tableParam;
    static BenchResultList results;
};
std::unique_ptr<PacTreeYcsbTable> YCSBTest::ycsbTable{nullptr};
std::string YCSBTest::dir_config = genTestDir<1>("ycsb");
BenchResultList YCSBTest::results{};

class YCSBTestWithInit : public YCSBTest {
protected:
    static void SetUpTestSuite()
    {
        InitDB(dir_config);
        IndexBootstrap();
        ycsbTable = std::make_unique<PacTreeYcsbTable>(tableParam, true);
    }
    static void TearDownTestSuite()
    {
        IndexExitProcess();
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
        IndexBootstrap();
        LOG(INFO) << "BootStrap End." << std::endl;
        ycsbTable = std::make_unique<PacTreeYcsbTable>(tableParam, false);
    }
    void TearDown() override
    {
        IndexExitProcess();
        ExitDBProcess();
        ycsbTable = std::make_unique<PacTreeYcsbTable>(tableParam, false);
    }
    static void SetUpTestSuite()
    {}
    static void TearDownTestSuite()
    {
        PrintResults();
    }
};
}  // namespace PacTree
}  // namespace YCSB
}  // namespace NVMDB
using NVMDB::YCSB::YcsbRunParam;
using NVMDB::YCSB::YcsbTableParam;
using YCSBPacTreeTestWithBootStrap = NVMDB::YCSB::PacTree::YCSBTestWithBootStrap;
using YCSBPacTreeTestWithInit = NVMDB::YCSB::PacTree::YCSBTestWithInit;

const YcsbTableParam NVMDB::YCSB::PacTree::YCSBTest::tableParam = YcsbTableParam(100, 256000000);
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

TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_12A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_12B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 12, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_12C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 12, WarmUpSec, RunSec));
}

TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_24A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_24B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 24, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_24C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 24, WarmUpSec, RunSec));
}

TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_36A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_36B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 36, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_36C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 36, WarmUpSec, RunSec));
}

TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_48A)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_48B)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, 48, WarmUpSec, RunSec));
}
TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_48C)
{
    RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, 48, WarmUpSec, RunSec));
}

// TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_A)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_B)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBPacTreeTestWithInit, YCSB_NOSKEW_C)
// {
//     RunBench(YcsbRunParam(SkewTheta::NoSkew, 0, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBPacTreeTestWithInit, YCSB_SKEW_A)
// {
//     RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbA, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBPacTreeTestWithInit, YCSB_SKEW_B)
// {
//     RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbB, Terminal, WarmUpSec, RunSec));
// }

// TEST_F(YCSBPacTreeTestWithInit, YCSB_SKEW_C)
// {
//     RunBench(YcsbRunParam(SkewTheta::Skew, NVMDB::YCSB::OpPerTxn, ReadPercent::YcsbC, Terminal, WarmUpSec, RunSec));
// }
#endif