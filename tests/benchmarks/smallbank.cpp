#include "nvm_init.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "common/test_declare.h"
#include "random_generator.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <getopt.h>
#include <thread>

using namespace NVMDB;

constexpr int MILL_SECOND = 1000;
constexpr int SPINLOCK_TYPE = 2;

ColumnDesc SmallBankAccountColDesc[] = {InitColDesc(COL_TYPE_INT)};

TableDesc SmallBankAccountDesc = {&SmallBankAccountColDesc[0], sizeof(SmallBankAccountColDesc) / sizeof(ColumnDesc)};

class SmallBankBench {
    std::string dataDir;
    int accounts;
    int workers;
    Table *table;
    int runTime;
    int type;

    volatile bool onWorking;

    struct WorkerStatistics {
        union {
            struct {
                uint64 commit;
                uint64 abort;
            };
            char padding[64];
        };
    } __attribute__((aligned(64)));

    WorkerStatistics *statistics;
    volatile RowIdMapEntry *locks;

public:
    SmallBankBench(const char *dir, int accounts, int workers, int duration, int type)
        : dataDir(dir), accounts(accounts), workers(workers), onWorking(true), runTime(duration), type(type) {
        statistics = new WorkerStatistics[workers];
        memset(statistics, 0, sizeof(WorkerStatistics) * workers);
        locks = nullptr;
    }

    ~SmallBankBench() {
        delete[] statistics;
        if (type == SPINLOCK_TYPE) {
            delete[] locks;
        }
    }

    void InitBench() {
        InitColumnDesc(SmallBankAccountDesc.col_desc, SmallBankAccountDesc.col_cnt, SmallBankAccountDesc.row_len);
        if (type == 0) {
            InitDB(dataDir.c_str());
            InitThreadLocalVariables();
            table = new Table(0, SmallBankAccountDesc.row_len);
            table->CreateSegment();

            RAMTuple tuple(SmallBankAccountDesc.col_desc, SmallBankAccountDesc.row_len);
            int balance = 0;
            tuple.SetCol(0, (char *)&balance);

            Transaction *tx = GetCurrentTxContext();
            tx->Begin();
            for (int i = 0; i < accounts; i++) {
                RowId rowId = HeapInsert(tx, table, &tuple);
            }
            tx->Commit();
        }
    }

    void EndBench() {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }

    void UpdateFunc(WorkerStatistics *stats) {
        InitThreadLocalVariables();
        stats->commit = stats->abort = 0;

        auto rnd = new RandomGenerator();
        RAMTuple tuple(SmallBankAccountDesc.col_desc, SmallBankAccountDesc.row_len);
        while (onWorking) {
            RowId rowId1 = (RowId)(rnd->Next() % accounts);
            RowId rowId2 = (RowId)(rnd->Next() % accounts);
            while (rowId1 == rowId2) {
                rowId2 = (RowId)(rnd->Next() % accounts);
            }
            int transfer = rnd->Next() % 100;

            auto tx = GetCurrentTxContext();
            tx->Begin();
            HamStatus status = HeapRead(tx, table, rowId1, &tuple);
            DCHECK(status == HamStatus::OK);
            int balance = 0;
            tuple.GetCol(0, (char *)&balance);
            balance -= transfer;
            tuple.UpdateCol(0, (char *)&balance);
            status = HeapUpdate(tx, table, rowId1, &tuple);
            if (status != HamStatus::OK) {
                tx->Abort();
                stats->abort++;
                continue;
            }

            status = HeapRead(tx, table, rowId2, &tuple);
            DCHECK(status == HamStatus::OK);
            tuple.GetCol(0, (char *)&balance);
            balance += transfer;
            tuple.UpdateCol(0, (char *)&balance);
            status = HeapUpdate(tx, table, rowId2, &tuple);
            if (status != HamStatus::OK) {
                tx->Abort();
                stats->abort++;
                continue;
            }

            tx->Commit();
            stats->commit++;
        }

        DestroyThreadLocalVariables();
    }

    void ScanFunc() {
        InitThreadLocalVariables();
        RAMTuple tuple(SmallBankAccountDesc.col_desc, SmallBankAccountDesc.row_len);
        int scanTime = 0;
        auto rnd = new RandomGenerator();
        static int scanLen = 10;

        while (onWorking) {
            RowId rid = rnd->Next() % (accounts - scanLen);
            auto tx = GetCurrentTxContext();
            tx->Begin();
            for (int i = 0; i < scanLen; i++) {
                HamStatus status = HeapRead(tx, table, i + rid, &tuple);
                DCHECK(status == HamStatus::OK);
            }
            tx->Commit();
            scanTime++;
        }
        LOG(INFO) << "scan " << scanTime << " times, (" << scanTime * 1.0 / runTime / MILL_SECOND << " KQPS)";

        DestroyThreadLocalVariables();
    }

    void SimulateCsnFunc(WorkerStatistics *stats) {
        static volatile uint64 csn = 0;
        while (onWorking) {
            __sync_fetch_and_add(&csn, 1);
            stats->commit++;
        }
    }

    void SimulateSpinlockFunc(WorkerStatistics *stats) {
        auto rnd = new RandomGenerator();
        while (onWorking) {
            int id = rnd->Next() % accounts;
            RowIdMapEntry *entry = (RowIdMapEntry *)&locks[id];
            entry->Lock();
            entry->Unlock();
            stats->commit++;
        }
    }

    void Run() {
        if (type == SPINLOCK_TYPE) {
            locks = new RowIdMapEntry[accounts];
            memset((char *)locks, 0, sizeof(RowIdMapEntry) * accounts);
        }

        std::thread updateTid[workers];
        for (int i = 0; i < workers; i++) {
            if (type == 0) {
                updateTid[i] = std::thread(&SmallBankBench::UpdateFunc, this, statistics + i);
            } else if (type == 1) {
                updateTid[i] = std::thread(&SmallBankBench::SimulateCsnFunc, this, statistics + i);
            } else if (type == SPINLOCK_TYPE) {
                updateTid[i] = std::thread(&SmallBankBench::SimulateSpinlockFunc, this, statistics + i);
            }
        }
        std::thread scanTid;
        if (type == 0) {
            scanTid = std::thread(&SmallBankBench::ScanFunc, this);
        } else {
            scanTid = std::thread([]() {});
        }

        sleep(runTime);
        onWorking = false;

        for (int i = 0; i < workers; i++) {
            updateTid[i].join();
        }
        scanTid.join();
    }

    void Report() {
        uint64 total_commit = 0;
        uint64 total_abort = 0;
        for (int i = 0; i < workers; i++) {
            total_abort += statistics[i].abort;
            total_commit += statistics[i].commit;
        }
        LOG(INFO) << "Finish test, total commit " << total_commit << " (" <<
            total_commit * 1.0 / runTime / 1000000 << " MQPS) total abort " <<
            total_abort << " (" << total_abort * 1.0 / runTime / 1000000 << " MQPS)";
    }
};

static struct option g_opts[] = {
    {"threads", required_argument, nullptr, 't'},
    {"duration", required_argument, nullptr, 'd'},
    {"accounts", required_argument, nullptr, 'a'},
    {"type", required_argument, nullptr, 'T'},
};

struct SmallBankOpts {
    int threads;
    int duration;
    int accounts;
    int type;
};

static void UsageExit() {
    LOG(INFO) << "Command line options : smallbank <options> \n"
              << "   -h --help              : Print help message \n"
              << "   -t --threads           : Thread num\n"
              << "   -d --duration          : Duration time: (second)\n"
              << "   -t --type              : Type (0: transfer, 1: simulate csn, 2. simulate spinlock)\n"
              << "   -a --accounts          : Account Number(>0)\n";
    exit(EXIT_FAILURE);
}

//SmallBankOpts ParseOpt(int argc, char **argv)
//{
//    SmallBankOpts opt = {.threads = 16, .duration = 10, .accounts = 1000000, .type = 0};
//
//    while (true) {
//        int idx = 0;
//        int c = getopt_long(argc, argv, "ht:d:T:a:", g_opts, &idx);
//        if (c == -1) {
//            break;
//        }
//
//        switch (c) {
//            case 'h':
//                UsageExit();
//                break;
//            case 't':
//                opt.threads = atoi(optarg);
//                break;
//            case 'd':
//                opt.duration = atoi(optarg);
//                break;
//            case 'T':
//                opt.type = atoi(optarg);
//                break;
//            case 'a':
//                opt.accounts = atoi(optarg);
//                break;
//            default:
//                LOG(ERROR) << "\nUnknown option";
//                UsageExit();
//                break;
//        }
//    }
//    return opt;
//}

class SmallBankTest : public ::testing::Test {
protected:
    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(SmallBankTest, SmallBankTestMain) {
    SmallBankOpts opt = {.threads = 16, .duration = 10, .accounts = 1000000, .type = 0};

    SmallBankBench bench("/mnt/pmem0/bench", opt.accounts, opt.threads, opt.duration, opt.type);
    bench.InitBench();
    bench.Run();
    bench.Report();
    bench.EndBench();
}