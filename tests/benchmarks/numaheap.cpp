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

ColumnDesc HeapAccountColDesc[] = {InitColDesc(COL_TYPE_INT), InitVarColDesc(COL_TYPE_VARCHAR, 128)};

TableDesc HeapAccountDesc = {&HeapAccountColDesc[0], sizeof(HeapAccountColDesc) / sizeof(ColumnDesc)};

void GetSplitRange(int dop, uint32 size, int seq, uint32 *start, uint32 *end) {
    int range;
    DCHECK(seq >= 0 && seq <= dop - 1);
    range = size / dop;
    *start = range * seq;
    *end = *start + range - 1;
    if (seq == dop - 1)
        *end = size;
    if (*end < *start) {
        LOG(ERROR) << "GetSplitRange Failed!" << std::endl;
    }
}

class HeapBench {
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

public:
    HeapBench(const char *dir, int accounts, int workers, int duration, int type)
        : dataDir(dir), accounts(accounts), workers(workers), onWorking(true), runTime(duration), type(type) {
        statistics = new WorkerStatistics[workers];
        memset(statistics, 0, sizeof(WorkerStatistics) * workers);
    }

    ~HeapBench() {
        delete[] statistics;
    }

    void InsertNewAccountInner(uint32 begin, uint32 end) {
        InitThreadLocalVariables();
        RAMTuple tuple(HeapAccountDesc.col_desc, HeapAccountDesc.row_len);
        int balance = 0;
        tuple.SetCol(0, (char *)&balance);
        Transaction *tx = GetCurrentTxContext();
        for (uint32 i = begin; i <= end; i++) {
            tx->Begin();
            RowId rowId = HeapInsert(tx, table, &tuple);
            tx->Commit();
        }
        DestroyThreadLocalVariables();
    }

    void InsertNewAccount() {
        std::thread workerTids[workers];
        for (int i = 0; i < workers; i++) {
            uint32 start;
            uint32 end;
            GetSplitRange(workers, accounts, i, &start, &end);
            workerTids[i] = std::thread(&HeapBench::InsertNewAccountInner, this, start, end);
        }
        for (int i = 0; i < workers; i++) {
            workerTids[i].join();
        }
    }

    void InitBench() {
        InitColumnDesc(HeapAccountDesc.col_desc, HeapAccountDesc.col_cnt, HeapAccountDesc.row_len);
        InitDB(dataDir.c_str());
        InitThreadLocalVariables();
        table = new Table(0, HeapAccountDesc.row_len);
        table->CreateSegment();
        InsertNewAccount();
    }

    void EndBench() {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }

    void UpdateFunc(WorkerStatistics *stats) {
        InitThreadLocalVariables();
        stats->commit = stats->abort = 0;

        auto rnd = new RandomGenerator();
        RAMTuple tuple(HeapAccountDesc.col_desc, HeapAccountDesc.row_len);
        while (onWorking) {
            auto rowId1 = (RowId)(rnd->Next() % accounts);
            auto rowId2 = (RowId)(rnd->Next() % accounts);
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

    void Run() {
        std::thread updateTid[workers];
        for (int i = 0; i < workers; i++) {
            if (type == 0) {
                updateTid[i] = std::thread(&HeapBench::UpdateFunc, this, statistics + i);
            } else if (type == 1) {
                updateTid[i] = std::thread([]() {});
            }
        }

        sleep(runTime);
        onWorking = false;

        for (int i = 0; i < workers; i++) {
            updateTid[i].join();
        }
    }

    void Report() {
        uint64 total_commit = 0;
        uint64 total_abort = 0;
        for (int i = 0; i < workers; i++) {
            total_abort += statistics[i].abort;
            total_commit += statistics[i].commit;
        }

        LOG(INFO) << "Finish test, total commit " << total_commit << " (" << total_commit * 1.0 / runTime / 1000000
                  << " MQPS) total abort " << total_abort << " (" << total_abort * 1.0 / runTime / 1000000 << " MQPS)";
    }
};

static struct option g_opts[] = {
    {"threads", required_argument, nullptr, 't'},
    {"duration", required_argument, nullptr, 'd'},
    {"accounts", required_argument, nullptr, 'a'},
    {"type", required_argument, nullptr, 'T'},
};

struct NumaHeapOpts {
    int threads;
    int duration;
    int accounts;
    int type;
};

static void UsageExit() {
    LOG(INFO) << "Command line options : numaheap <options> \n"
              << "   -h --help              : Print help message \n"
              << "   -t --threads           : Thread num\n"
              << "   -d --duration          : Duration time: (second)\n"
              << "   -T --type              : Type (0: transfer, 1: insert)\n"
              << "   -a --accounts          : Account Number(>0)\n";
    exit(EXIT_FAILURE);
}

//NumaHeapOpts ParseOpt(int argc, char **argv) {
//    NumaHeapOpts opt = {.threads = 16, .duration = 10, .accounts = 1000000, .type = 0};
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

class NumaHeapBenchTest : public ::testing::Test {
protected:
    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(NumaHeapBenchTest, NumaHeapBenchTestMain) {
    NumaHeapOpts opt = {.threads = 16, .duration = 10, .accounts = 1000000, .type = 0};

    HeapBench bench("/mnt/pmem0/bench;/mnt/pmem1/bench", opt.accounts, opt.threads, opt.duration, opt.type);
    bench.InitBench();
    bench.Run();
    bench.Report();
    bench.EndBench();
}