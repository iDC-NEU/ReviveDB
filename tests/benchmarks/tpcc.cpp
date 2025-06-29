#include "tpcc.h"
#include "nvm_init.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "index/index_test.h"
#include "random_generator.h"
#include <gtest/gtest.h>
#include <x86intrin.h>
#include <getopt.h>
#include <thread>
#include <unordered_set>

#define  TPCC_OP_ONLY

namespace NVMDB {
namespace TPCC {

/* hardwired. */
#ifdef NDEBUG
static const uint32_t Heap_SegHead[] = {2, 258, 514, 770, 1026, 1282, 1538, 1794, 2050};
#else
static const uint32_t Heap_SegHead[] = {2, 258, 514, 770, 2560, 2816, 3072, 3328, 3584};
#endif

static struct option opts[] = {
    {"help", no_argument, nullptr, 'h'},           {"threads", required_argument, nullptr, 't'},
    {"duration", required_argument, nullptr, 'd'}, {"warehouse", required_argument, nullptr, 'a'},
    {"type", required_argument, nullptr, 'T'},     {"bind", no_argument, nullptr, 'b'},
};

struct IndexBenchOpts {
    int threads;
    int duration;
    int warehouse;
    int type;
    /* bind warehouses to threads with no overlap */
    bool bind;
};

const char *test_name[3] = {
    "insert test",
    "insert/remove test",
    "lookup test",
};

enum BENCH_TYPE {
    INSERT,
    INSERT_REMOVE,
    LOOKUP,
};

static void usage_exit() {
    std::cout << "Command line options : [binary] <options> \n"
              << "   -h --help              : Print help message \n"
              << "   -t --threads           : Thread num\n"
              << "   -d --duration          : Duration time: (second)\n"
              << "   -T --type              : Type (0: runDatabaseBuild, 1: runBenchmark, 2: runConsistencyCheck, 3: "
                 "all, default 3)\n"
              << "   -w --warehouse            : WareHouse number(>0)\n"
              << "   -b --bind              : Bind WareHouses to threads\n";
    exit(EXIT_FAILURE);
}

//IndexBenchOpts ParseOpt(int argc, char **argv)
//{
//    IndexBenchOpts opt = {.threads = 16, .duration = 10, .warehouse = 10, .type = 3, .bind = false};
//
//    while (true) {
//        int idx = 0;
//        int c = getopt_long(argc, argv, "bht:d:T:w:", opts, &idx);
//        if (c == -1) {
//            break;
//        }
//
//        switch (c) {
//            case 'h':
//                usage_exit();
//                break;
//            case 't':
//                opt.threads = atoi(optarg);
//                break;
//            case 'd':
//                opt.duration = atoi(optarg);
//                if (opt.duration > 100) {
//                    LOG(WARNING) << "duration is too long";
//                }
//                break;
//            case 'T':
//                opt.type = atoi(optarg);
//                if (opt.type < 0 || opt.type > 3) {
//                    LOG(ERROR) << "test type is illegal; please use number between 0 and 3";
//                }
//                break;
//            case 'w':
//                opt.warehouse = atoi(optarg);
//                break;
//            case 'b':
//                opt.bind = true;
//                break;
//            default:
//                LOG(ERROR) << "\nUnknown option";
//                usage_exit();
//                break;
//        }
//    }
//    return opt;
//}

class TPCCBench {
    std::string dir_config;
    int workers;
    int run_time;
    int wh_count;
    /* # of WareHouse */
    int wh_start;
    int wh_end;
    /* if bind warehouse to threads */
    bool bind;
    int type;

    volatile bool on_working;

    TpccRunStat *g_stats;
    /* different tableIds' index tuple funcs */
    NVMIndex **idxs = nullptr;
    /* table heaps */
    Table **tables;
    /* secondary index of customer table */
    NVMIndex *cus_sec_idx = nullptr;
    /*
     * secondary index of order table, real tpcc doesn't have this.
     * It's used to find max o_id of a customer, this avoid implement scan filter.
     */
    NVMIndex *ord_sec_idx = nullptr;

public:
    TPCCBench(const char *_dir, int _workers, int _duration, int _wh, bool _bind, int _type)
        : dir_config(_dir),
          workers(_workers),
          on_working(true),
          run_time(_duration),
          wh_count(_wh),
          wh_start(1),
          wh_end(_wh),
          bind(_bind),
          type(_type) {}

    void InitBench() {
        InitTableDesc();
        InitIndexDesc();
        g_stats = new TpccRunStat[workers];
        bool is_init = type == 0 || type == 3;
        if (is_init) {
            InitDB(dir_config.c_str());
        } else {
            DCHECK(type == 1 || type == 2);
            LOG(INFO) << "BootStrap Start." << std::endl;
            BootStrap(dir_config.c_str());
            LOG(INFO) << "BootStrap End." << std::endl;
        }
        idxs = new NVMIndex *[TABLE_NUM];
        tables = new Table *[TABLE_NUM];
        for (uint32_t i = 0; i < TABLE_NUM; i++) {
            uint32_t table_type = TABLE_FIRST + i;
            idxs[i] = new NVMIndex(table_type);
            tables[i] = new Table(table_type, TABLE_ROW_LEN(table_type));
            if (is_init) {
                tables[i]->CreateSegment();
            } else {
                tables[i]->Mount(Heap_SegHead[i]);
            }
        }
        cus_sec_idx = new NVMIndex(SECOND_INDEX(TABLE_CUSTOMER));
        ord_sec_idx = new NVMIndex(SECOND_INDEX(TABLE_ORDER));
    }

    void EndBench() {
        delete ord_sec_idx;
        delete cus_sec_idx;
        for (uint32_t i = 0; i < TABLE_NUM; i++) {
            delete tables[i];
            delete idxs[i];
        }
        delete[] tables;
        delete[] idxs;
        delete[] g_stats;
        ExitDBProcess();
    }

    /* select other warehouse, this is the cause of Tx abort even thread bind. */
    int other_ware(int ware_id) {
        if (wh_start == wh_end)
            return ware_id;
        int tmp;
        do {
            tmp = RandomNumber(wh_start, wh_end);
        } while (tmp == ware_id);
        return tmp;
    }

    void load_warehouse(int wh_start, int wh_end) {
        STACK_WAREHOUSE(wh);
        WAREHOUSE_INDEX(whit);
        float w_tax;
        int64 w_ytd = 300000;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            SET_COL(wh, w_id, i);
            MakeAlphaString(6, 10, GET_COL(wh, w_name));
            MakeAddress(GET_COL(wh, w_street_1), GET_COL(wh, w_street_2), GET_COL(wh, w_city), GET_COL(wh, w_state),
                        GET_COL(wh, w_zip));
            w_tax = RandomNumber(10L, 20L) / 100.0;
            SET_COL(wh, w_tax, w_tax);
            SET_COL(wh, w_ytd, w_ytd);
            InsertTupleWithIndex(tx, TABLE_WAREHOUSE, &whit, &wh);
        }
        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void load_district(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        const uint64 ytd = 300000.0 / DIST_PER_WARE;
        const int next_o_id = 3001L;
        float d_tax;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_COL(dis, d_id, j);
                SET_COL(dis, d_w_id, i);
                SET_COL(dis, d_ytd, ytd);
                SET_COL(dis, d_next_o_id, next_o_id);
                MakeAlphaString(6L, 10L, GET_COL(dis, d_name));
                MakeAddress(GET_COL(dis, d_street_1), GET_COL(dis, d_street_2), GET_COL(dis, d_city),
                            GET_COL(dis, d_state), GET_COL(dis, d_zip));
                d_tax = RandomNumber(10L, 20L) / 100.0;
                SET_COL(dis, d_tax, d_tax);
                InsertTupleWithIndex(tx, TABLE_DISTRICT, &disit, &dis);
            }
        }
        tx->Commit();

        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                CHECK(SelectTuple(tx, TABLE_DISTRICT, &disit, &dis));
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void load_item(int item_start, int item_end) {
        int *orig = new int[MAXITEMS];
        memset(orig, 0, sizeof(int) * MAXITEMS);
        int pos = 0;
        for (int i = 0; i < MAXITEMS / 10; i++) {
            do {
                pos = RandomNumber(0L, MAXITEMS - 1);
            } while (orig[pos]);
            orig[pos] = 1;
        }
        STACK_ITEM(item);
        ITEM_INDEX(itemit);
        int i_im_id;
        float i_price;
        char *i_data;
        int idatasiz;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = item_start; i <= item_end; i++) {
            SET_COL(item, i_id, i);
            i_im_id = RandomNumber(1L, 10000L);
            SET_COL(item, i_im_id, i_im_id);
            MakeAlphaString(14, 24, GET_COL(item, i_name));
            i_price = RandomNumber(100L, 10000L) / 100.0;
            SET_COL(item, i_price, i_price);
            i_data = GET_COL(item, i_data);
            MakeAlphaString(26, 50, i_data);
            if (orig[GET_COL_INT(item, i_id)]) {
                idatasiz = strlen(i_data);
                pos = RandomNumber(0L, idatasiz - 8);
                i_data[pos] = 'o';
                i_data[pos + 1] = 'r';
                i_data[pos + 2] = 'i';
                i_data[pos + 3] = 'g';
                i_data[pos + 4] = 'i';
                i_data[pos + 5] = 'n';
                i_data[pos + 6] = 'a';
                i_data[pos + 7] = 'l';
            }
            InsertTupleWithIndex(tx, TABLE_ITEM, &itemit, &item);
        }
        tx->Commit();
        DestroyThreadLocalVariables();
        delete[] orig;
    }

    void load_customer(int wh_start, int wh_end) {
        STACK_CUSTOMER(cus);
        CUSTOMER_INDEX(cusit);
        CUSTOMER_SEC_INDEX(cus_secit);
        char *c_middle;
        char *c_last;
        char *c_credit;
        const int c_credit_lim = 50000;
        float c_discount;
        float c_balance = -10.0;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                for (int k = 1; k <= CUST_PER_DIST; k++) {
                    SET_COL(cus, c_id, k);
                    SET_COL(cus, c_w_id, i);
                    SET_COL(cus, c_d_id, j);
                    MakeAlphaString(8, 16, GET_COL(cus, c_first));
                    c_middle = GET_COL(cus, c_middle);
                    c_middle[0] = 'O';
                    c_middle[1] = 'E';
                    c_middle[2] = 0;
                    c_last = GET_COL(cus, c_last);
                    memset(c_last, 0, strlen(c_last) + 1);
                    /* k == c_id */
                    if (k <= 1000) {
                        Lastname(k - 1, c_last);
                    } else {
                        Lastname(NURand(255, 0, 999), c_last);
                    }
                    MakeAddress(GET_COL(cus, c_street_1), GET_COL(cus, c_street_2), GET_COL(cus, c_city),
                                GET_COL(cus, c_state), GET_COL(cus, c_zip));
                    MakeNumberString(16, 16, GET_COL(cus, c_phone));
                    c_credit = GET_COL(cus, c_credit);
                    if (RandomNumber(0L, 1L))
                        c_credit[0] = 'G';
                    else
                        c_credit[0] = 'B';
                    c_credit[1] = 'C';
                    c_credit[2] = 0;
                    SET_COL(cus, c_credit_lim, c_credit_lim);
                    c_discount = RandomNumber(0L, 50L) / 100.0;
                    SET_COL(cus, c_discount, c_discount);
                    SET_COL(cus, c_balance, c_balance);
                    MakeAlphaString(300, 500, GET_COL(cus, c_data));
                    InsertTupleWithIndex(tx, TABLE_CUSTOMER, &cusit, &cus, &cus_secit);
                }
            }
        }
        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void load_stock(int wh_start, int wh_end) {
        int *orig = new int[MAXITEMS];
        memset(orig, 0, sizeof(int) * MAXITEMS);
        int pos = 0;
        for (int i = 0; i < MAXITEMS / 10; i++) {
            do {
                pos = RandomNumber(0L, MAXITEMS - 1);
            } while (orig[pos]);
            orig[pos] = 1;
        }
        int sdatasiz;
        STACK_STOCK(stock);
        STOCK_INDEX(stockit);
        int s_quantity;
        char *s_data;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= MAXITEMS; j++) {
                SET_COL(stock, s_i_id, j);
                SET_COL(stock, s_w_id, i);

                s_quantity = RandomNumber(10L, 100L);
                SET_COL(stock, s_quantity, s_quantity);
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_01));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_02));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_03));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_04));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_05));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_06));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_07));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_08));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_09));
                MakeAlphaString(24, 24, GET_COL(stock, s_dist_10));
                MakeAlphaString(26, 50, GET_COL(stock, s_data));
                s_data = GET_COL(stock, s_data);
                /* j == s_i_id */
                if (orig[j]) {
                    sdatasiz = strlen(s_data);
                    pos = RandomNumber(0L, sdatasiz - 8);
                    s_data[pos] = 'o';
                    s_data[pos + 1] = 'r';
                    s_data[pos + 2] = 'i';
                    s_data[pos + 3] = 'g';
                    s_data[pos + 4] = 'i';
                    s_data[pos + 5] = 'n';
                    s_data[pos + 6] = 'a';
                    s_data[pos + 7] = 'l';
                }
                InsertTupleWithIndex(tx, TABLE_STOCK, &stockit, &stock);
            }
        }
        tx->Commit();
        DestroyThreadLocalVariables();
        delete[] orig;
    }

    void load_order(int wh_start, int wh_end) {
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        ORDER_SEC_INDEX(ord_secit);

        STACK_NEWORDER(neworder);
        NEWORDER_INDEX(neworderit);

        STACK_ORDERLINE(orderline);
        ORDERLINE_INDEX(orderlineit);

        uint64 o_entry_d;
        int o_carrier_id;
        int o_ol_cnt;
        const bool o_all_local = true;
        int ol_number;
        int ol_i_id;
        const int ol_quantity = 5;
        float ol_amount;
        uint64 ol_delivery_d;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {

                /* initialize permutation of customer numbers */
                InitPermutation();
                for (int k = 1; k <= ORD_PER_DIST; k++) {
                    SET_COL(order, o_id, k);
                    SET_COL(order, o_w_id, i);
                    SET_COL(order, o_d_id, j);
                    SET_COL(order, o_c_id, GetPermutation());
                    o_entry_d = __rdtsc();
                    SET_COL(order, o_entry_d, o_entry_d);
                    o_carrier_id = 0;
                    SET_COL(order, o_carrier_id, o_carrier_id);
                    o_ol_cnt = RandomNumber(5L, 15L);
                    SET_COL(order, o_ol_cnt, o_ol_cnt);
                    SET_COL(order, o_all_local, o_all_local);

                    /* the last 900 orders have not been delivered) */
                    /* o_id == k */
                    if (k > 2100) {
                        SET_COL(neworder, no_o_id, k);
                        SET_COL(neworder, no_w_id, i);
                        SET_COL(neworder, no_d_id, j);
                        InsertTupleWithIndex(tx, TABLE_NEWORDER, &neworderit, &neworder);
                    } else {
                        o_carrier_id = RandomNumber(1L, DIST_PER_WARE);
                        SET_COL(order, o_carrier_id, o_carrier_id);
                    }
                    InsertTupleWithIndex(tx, TABLE_ORDER, &orderit, &order, &ord_secit);
                    for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                        SET_COL(orderline, ol_o_id, k);
                        SET_COL(orderline, ol_w_id, i);
                        SET_COL(orderline, ol_d_id, j);
                        SET_COL(orderline, ol_number, ol_number);

                        /* Generate Order Line Data */
                        ol_i_id = RandomNumber(1L, MAXITEMS);
                        SET_COL(orderline, ol_i_id, ol_i_id);
                        SET_COL(orderline, ol_supply_w_id, i);
                        SET_COL(orderline, ol_quantity, ol_quantity);
                        MakeAlphaString(24, 24, GET_COL(orderline, ol_dist_info));
                        ol_amount = RandomNumber(10L, 10000L) / 100.0;
                        SET_COL(orderline, ol_amount, ol_amount);

                        if (k > 2100)
                            ol_delivery_d = 0;
                        else
                            ol_delivery_d = __rdtsc();
                        SET_COL(orderline, ol_delivery_d, ol_delivery_d);
                        InsertTupleWithIndex(tx, TABLE_ORDERLINE, &orderlineit, &orderline);
                    }
                }
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void __load_db(TableType tabletype) {
        std::thread worker_tids[workers];
        for (int i = 0; i < workers; i++) {
            uint32_t start, end;
            if (tabletype == TABLE_ITEM) {
                GetSplitRange(workers, MAXITEMS, i, &start, &end);
            } else {
                GetSplitRange(workers, wh_end, i, &start, &end);
            }
            switch (tabletype) {
                case TABLE_WAREHOUSE: {
                    worker_tids[i] = std::thread(&TPCCBench::load_warehouse, this, start, end);
                    break;
                }
                case TABLE_DISTRICT: {
                    worker_tids[i] = std::thread(&TPCCBench::load_district, this, start, end);
                    break;
                }
                case TABLE_ITEM: {
                    worker_tids[i] = std::thread(&TPCCBench::load_item, this, start, end);
                    break;
                }
                case TABLE_CUSTOMER: {
                    worker_tids[i] = std::thread(&TPCCBench::load_customer, this, start, end);
                    break;
                }
                case TABLE_STOCK: {
                    worker_tids[i] = std::thread(&TPCCBench::load_stock, this, start, end);
                    break;
                }
                case TABLE_ORDER: {
                    worker_tids[i] = std::thread(&TPCCBench::load_order, this, start, end);
                    break;
                }
            }
        }
        for (int i = 0; i < workers; ++i) {
            worker_tids[i].join();
        }
    }

    void load_db() {
        int seed = time(0);
        srand(seed);
        fast_rand_srand(seed);
        for (int i = 0; i < TABLE_LOAD_NUM; i++) {
            LOG(INFO) << "Loading " << i << "-th table.";
            __load_db(tableNeedLoad[i]);
        }
    }

    void LoadDB() {
        if (type == 1 || type == 2) {
            return;
        }
        load_db();
        LOG(INFO) << "Warm up finished, loaded " << wh_count << " Warehouses.";
    }

    TpccRunStat getRunStat() {
        TpccRunStat summary;
        for (int i = 0; i < 5; i++) {
            for (int k = 0; k < workers; k++) {
                TpccRunStat &wid = g_stats[k];
                RunStat &stat = wid.runstat_[i];
                summary.runstat_[i].nCommitted_ += stat.nCommitted_;
                summary.runstat_[i].nAborted_ += stat.nAborted_;

                summary.nTotalCommitted_ += stat.nCommitted_;
                summary.nTotalAborted_ += stat.nAborted_;
            }
        }
        return summary;
    }

    void clearRunStat() {
        for (int i = 0; i < 5; i++) {
            for (int k = 0; k < workers; k++) {
                TpccRunStat &wid = g_stats[k];
                RunStat &stat = wid.runstat_[i];
                stat.nCommitted_ = 0;
                stat.nAborted_ = 0;
            }
        }
    }

    void printTpccStat() {
        TpccRunStat summary = getRunStat();
        uint64_t total = summary.nTotalCommitted_ + summary.nTotalAborted_;

        printf("==> Committed TPS: %lu, per worker: %lu\n\n", summary.nTotalCommitted_ / run_time,
               summary.nTotalCommitted_ / run_time / workers);

        printf("trans         #totaltran       %%ratio     #committed       #aborted       %%abort\n");
        printf("-----         ----------       ------      ----------       --------       ------\n");
        for (int i = 0; i < 5; i++) {
            const RunStat &stat = summary.runstat_[i];
            uint64_t totalpert = stat.nCommitted_ + stat.nAborted_;
            printf("%-8s     %11lu      %6.1f%%     %11lu      %9lu      %6.1f%%\n", tname[i], totalpert,
                   (totalpert * 100.0) / total, stat.nCommitted_, stat.nAborted_, (stat.nAborted_ * 100.0) / totalpert);
        }
        printf("\n");
        printf("%s        %11lu      %6.1f%%      %10lu      %9lu      %6.1f%%\n", "Total", total, 100.0,
               summary.nTotalCommitted_, summary.nTotalAborted_, (summary.nTotalAborted_ * 100.0) / total);
        printf("-----         ----------       ------      ----------       --------       ------\n");
    }

    RAMTuple **InitCustomerArray() {
        auto rowLen = TableDescArr[(TABLE_CUSTOMER - TABLE_WAREHOUSE)].row_len;
        auto colCount = TableDescArr[(TABLE_CUSTOMER - TABLE_WAREHOUSE)].col_cnt;
        auto* rowTupleData = static_cast<RAMTuple*>(malloc(CUST_PER_DIST * sizeof(RAMTuple)));
        auto* undoBuffer = new UndoColumnDesc[colCount * CUST_PER_DIST];
        auto tupleRealSize = ((RealTupleSize(rowLen) + 63) / 64) * 64;
        auto* buffer = (char*)_mm_malloc(tupleRealSize* CUST_PER_DIST, 64);
        auto **tuples = new RAMTuple *[CUST_PER_DIST];
        for (int i = 0; i < CUST_PER_DIST; i++) {
            tuples[i] = &rowTupleData[i];
            new (&rowTupleData[i]) RAMTuple(TableDescArr[(TABLE_CUSTOMER - TABLE_WAREHOUSE)].col_desc,
                                            rowLen,
                                            buffer+i*tupleRealSize,
                                            undoBuffer+i*colCount);
        }
        return tuples;
    }

    bool SelectCustomerByName(Transaction *tx, int c_w_id, int c_d_id, char *c_last, RowId &cusid, RAMTuple &cus) {
        CUSTOMER_SEC_INDEX(keyb);
        CUSTOMER_SEC_INDEX(keye);
        customer_to_sk_range(keyb, keye, c_w_id, c_d_id, c_last);
        int res_size = 0;
        thread_local static RAMTuple **tuples = nullptr;
        if (tuples == nullptr) {
            tuples = InitCustomerArray();
        }
        RowId *row_ids = new RowId[CUST_PER_DIST];
        RangeSearch(tx, cus_sec_idx, tables[TABLE_OFFSET(TABLE_CUSTOMER)], &keyb, &keye, CUST_PER_DIST, &res_size,
                    row_ids, tuples);
        if (res_size != 0) {
            cusid = row_ids[res_size / 2];
            cus.CopyRow(*tuples[res_size / 2]);
        } else {
#ifndef NDEBUG
            LOG(ERROR) << "Select customer by name failed!" << std::endl;
#endif
        }
        delete[] row_ids;
        return (res_size != 0);
    }

    /* Most recent order of specified customer. */
    inline bool SelectMostRecentOrder(Transaction *tx, int c_w_id, int c_d_id, int c_id, RAMTuple *ord) {
        ORDER_SEC_INDEX(keyb);
        ORDER_SEC_INDEX(keye);
        order_to_sk_range(keyb, keye, c_w_id, c_d_id, c_id);
        RowId row_id = RangeSearchMax(tx, ord_sec_idx, tables[TABLE_OFFSET(TABLE_ORDER)], &keyb, &keye, ord);
#ifndef NDEBUG
        if (row_id == InvalidRowId)
            LOG(ERROR) << "Select most recent order failed!" << std::endl;
#endif
        return (row_id != InvalidRowId);
    }

    inline bool SelectOldestNewOrder(Transaction *tx, int w_id, int d_id, RowId &newordid, RAMTuple *neword) {
        NEWORDER_INDEX(keyb);
        NEWORDER_INDEX(keye);
        neworder_to_oid_range(keyb, keye, w_id, d_id);
        newordid = RangeSearchMin(tx, idxs[TABLE_OFFSET(TABLE_NEWORDER)], tables[TABLE_OFFSET(TABLE_NEWORDER)], &keyb,
                                  &keye, neword);
        return (newordid != InvalidRowId);
    }

    inline bool SelectNewestNewOrder(Transaction *tx, int w_id, int d_id, RAMTuple *neword) {
        NEWORDER_INDEX(keyb);
        NEWORDER_INDEX(keye);
        neworder_to_oid_range(keyb, keye, w_id, d_id);
        RowId newordid = RangeSearchMax(tx, idxs[TABLE_OFFSET(TABLE_NEWORDER)], tables[TABLE_OFFSET(TABLE_NEWORDER)],
                                        &keyb, &keye, neword);
        return (newordid != InvalidRowId);
    }

    inline bool SelectNewestOrder(Transaction *tx, int w_id, int d_id, RAMTuple *order) {
        ORDER_INDEX(keyb);
        ORDER_INDEX(keye);
        order_to_oid_range(keyb, keye, w_id, d_id);
        RowId orderid = RangeSearchMax(tx, idxs[TABLE_OFFSET(TABLE_ORDER)], tables[TABLE_OFFSET(TABLE_ORDER)], &keyb,
                                       &keye, order);
        return (orderid != InvalidRowId);
    }

    /* Assume you have inited and prepared already. */
    inline bool SelectTuple(Transaction *tx, TableType table_type, DRAMIndexTuple *index_tuple, RAMTuple *tuple,
                            RowId *row_id = nullptr, bool report = true, bool assert = false) {
        RowId rowId = UniqueSearch(tx, idxs[TABLE_OFFSET(table_type)], tables[TABLE_OFFSET(table_type)], index_tuple,
                                   tuple, assert);
        if (rowId == InvalidRowId) {
#ifndef NDEBUG
            if (report) {
                LOG(ERROR) << "Table Id " << table_type << " UniqueSearch Failed!" << std::endl;
            }
#endif
            return false;
        }
        if (row_id != nullptr)
            *row_id = rowId;
        return true;
    }

    void ExtractIndexKey(TableType table_type, DRAMIndexTuple *index_tuple, RAMTuple *tuple,
                         DRAMIndexTuple *sec_index_tuple = nullptr) {
        DCHECK(tuple != nullptr);
        switch (table_type) {
            case TABLE_WAREHOUSE: {
                SanityCheckWarehouse(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_DISTRICT: {
                SanityCheckDistrict(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_STOCK: {
                SanityCheckStock(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_ITEM: {
                SanityCheckItem(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_CUSTOMER: {
                SanityCheckCustomer(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                /* secondary index of table customer */
                DCHECK(sec_index_tuple != nullptr);
                sec_index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_ORDER: {
                SanityCheckOrder(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                /* secondary index of table order */
                DCHECK(sec_index_tuple != nullptr);
                sec_index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_NEWORDER: {
                SanityCheckNewOrder(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_ORDERLINE: {
                SanityCheckOrderLine(*tuple);
                index_tuple->ExtractFromTuple(tuple);
                break;
            }
            case TABLE_HISTORY:
                /* index_tuple is nullptr */
                break;
        }
    }

    /* Assume you have inited and prepared already. */
    inline void InsertTupleWithIndex(Transaction *tx, TableType table_type, DRAMIndexTuple *index_tuple,
                                     RAMTuple *tuple, DRAMIndexTuple *sec_index_tuple = nullptr) {
        ExtractIndexKey(table_type, index_tuple, tuple, sec_index_tuple);
        /* insert table segment */
        RowId rowId = HeapInsert(tx, tables[TABLE_OFFSET(table_type)], tuple);
#ifdef TPCC_OP_ONLY
        if (index_tuple == nullptr || table_type == TABLE_ORDERLINE || table_type == TABLE_HISTORY) {
#else
        if (index_tuple == nullptr || table_type == TABLE_HISTORY) {
#endif
            return;
        }
        /* insert table index */
        tx->IndexInsert(idxs[TABLE_OFFSET(table_type)], index_tuple, rowId);
        /* insert secondary index if need */
        if (table_type == TABLE_CUSTOMER) {
            tx->IndexInsert(cus_sec_idx, sec_index_tuple, rowId);
        } else if (table_type == TABLE_ORDER) {
            tx->IndexInsert(ord_sec_idx, sec_index_tuple, rowId);
        }
    }

    /* currently only used in table new order */
    inline bool DeleteTupleWithIndex(Transaction *tx, TableType table_type, DRAMIndexTuple *index_tuple,
                                     RAMTuple *tuple, RowId row_id) {
        DCHECK(table_type != TABLE_CUSTOMER && table_type != TABLE_ORDER);
        ExtractIndexKey(table_type, index_tuple, tuple);
        /* delete table segment */
        auto ret = HeapDelete(tx, tables[TABLE_OFFSET(table_type)], row_id);
        if (ret == HamStatus::OK)
            /* delete table index only HeapDelete success */
            tx->IndexDelete(idxs[TABLE_OFFSET(table_type)], index_tuple, row_id);
        return (ret == HamStatus::OK);
    }

    int neword(int w_id_arg,        /* warehouse id */
               int d_id_arg,        /* district id */
               int c_id_arg,        /* customer id */
               int o_ol_cnt_arg,    /* number of items */
               int o_all_local_arg, /* are all order lines local */
               int itemid[],        /* ids of items to be ordered */
               int supware[],       /* warehouses supplying items */
               int qty[]            /* quantity of each item */
    ) {
        int w_id = w_id_arg;
        int d_id = d_id_arg;
        int c_id = c_id_arg;
        int o_ol_cnt = o_ol_cnt_arg;
        int o_all_local = o_all_local_arg;
        /* next available order id of this district */
        int d_next_o_id;
        /* update value */
        int u_d_next_o_id;
        uint64 o_entry_d = __rdtsc();
        const int o_carrier_id = 0;
        const uint64 ol_delivery_d = 0;

        STACK_WAREHOUSE(wh);
        WAREHOUSE_INDEX(whit);
        STACK_CUSTOMER(cus);
        CUSTOMER_INDEX(cusit);
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        ORDER_SEC_INDEX(ord_secit);
        STACK_NEWORDER(neworder);
        NEWORDER_INDEX(neworderit);
        STACK_ITEM(item);
        ITEM_INDEX(itemit);
        STACK_STOCK(stock);
        STOCK_INDEX(stockit);
        STACK_ORDERLINE(orderline);
        ORDERLINE_INDEX(orderlineit);

        auto tx = GetCurrentTxContext();
        tx->Begin();

        /* select from warehouse */
        SET_INDEX_COL(whit, pk, w_id, w_id);
        SelectTuple(tx, TABLE_WAREHOUSE, &whit, &wh);

        /* select from customer */
        SET_INDEX_COL(cusit, pk, c_id, c_id);
        SET_INDEX_COL(cusit, pk, c_d_id, d_id);
        SET_INDEX_COL(cusit, pk, c_w_id, w_id);
        SelectTuple(tx, TABLE_CUSTOMER, &cusit, &cus);

        /* update district set d_next_o_id += 1 */
        RowId disid;
        SET_INDEX_COL(disit, pk, d_id, d_id);
        SET_INDEX_COL(disit, pk, d_w_id, w_id);
        SelectTuple(tx, TABLE_DISTRICT, &disit, &dis, &disid);
        FETCH_COL(dis, d_next_o_id, d_next_o_id);
        u_d_next_o_id = d_next_o_id + 1;
        UPDATE_COL(dis, d_next_o_id, u_d_next_o_id);
        if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_DISTRICT)], disid, &dis) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        /* insert into orders (8 columns, others NULL) */
        SET_COL(order, o_id, d_next_o_id);
        SET_COL(order, o_w_id, w_id);
        SET_COL(order, o_d_id, d_id);
        SET_COL(order, o_c_id, c_id);
        SET_COL(order, o_entry_d, o_entry_d);
        SET_COL(order, o_carrier_id, o_carrier_id);
        SET_COL(order, o_ol_cnt, o_ol_cnt);
        SET_COL(order, o_all_local, o_all_local);

        InsertTupleWithIndex(tx, TABLE_ORDER, &orderit, &order, &ord_secit);

        /* insert into neworder (3 columns, others NULL) */
        SET_COL(neworder, no_o_id, d_next_o_id);
        SET_COL(neworder, no_w_id, w_id);
        SET_COL(neworder, no_d_id, d_id);
        InsertTupleWithIndex(tx, TABLE_NEWORDER, &neworderit, &neworder);

        // loop
        char iname[MAX_NUM_ITEMS][MAX_ITEM_LEN];
        char bg[MAX_NUM_ITEMS];
        float amt[MAX_NUM_ITEMS];
        float price[MAX_NUM_ITEMS];

        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            int ol_number_idx = ol_number - 1;
            int ol_supply_w_id = supware[ol_number_idx];
            if (ol_supply_w_id != w_id)
                DCHECK(o_all_local == 0);
            int ol_i_id = itemid[ol_number_idx];
            int ol_quantity = qty[ol_number_idx];
            /* select from item  */
            SET_INDEX_COL(itemit, pk, i_id, ol_i_id);
            if (!SelectTuple(tx, TABLE_ITEM, &itemit, &item, nullptr, false)) {
                DCHECK(ol_i_id == notfound);
                tx->Abort();
                return -1;
            }

            float total = 0.0;
            int s_quantity;
            price[ol_number_idx] = GET_COL_FLOAT(item, i_price);
            strncpy(iname[ol_number_idx], GET_COL(item, i_name), 25);

            RowId stockid;
            SET_INDEX_COL(stockit, pk, s_w_id, ol_supply_w_id);
            SET_INDEX_COL(stockit, pk, s_i_id, ol_i_id);
            /* select from stock */
            SelectTuple(tx, TABLE_STOCK, &stockit, &stock, &stockid);

            FETCH_COL(stock, s_quantity, s_quantity);
            if (strstr(GET_COL(item, i_data), "original") != nullptr &&
                strstr(GET_COL(stock, s_data), "original") != nullptr)
                bg[ol_number_idx] = 'B';
            else
                bg[ol_number_idx] = 'G';
            if (s_quantity > ol_quantity)
                s_quantity = s_quantity - ol_quantity;
            else
                s_quantity = s_quantity - ol_quantity + 91;

            int ol_amount;
            ol_amount = ol_quantity * GET_COL_FLOAT(item, i_price) *
                        (1 + GET_COL_FLOAT(wh, w_tax) + GET_COL_FLOAT(dis, d_tax)) *
                        (1 - GET_COL_FLOAT(cus, c_discount));
            amt[ol_number_idx] = ol_amount;
            total += ol_amount;

            /* update stock */
            UPDATE_COL(stock, s_quantity, s_quantity);
            if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_STOCK)], stockid, &stock) != HamStatus::OK) {
                tx->Abort();
                return -1;
            }

            /* insert order_line (9 columns, others NULL) */
            SET_COL(orderline, ol_o_id, d_next_o_id);
            SET_COL(orderline, ol_w_id, w_id);
            SET_COL(orderline, ol_d_id, d_id);
            SET_COL(orderline, ol_number, ol_number);
            SET_COL(orderline, ol_i_id, ol_i_id);
            SET_COL(orderline, ol_supply_w_id, ol_supply_w_id);
            SET_COL(orderline, ol_delivery_d, ol_delivery_d);
            SET_COL(orderline, ol_quantity, ol_quantity);
            SET_COL(orderline, ol_amount, ol_amount);
            /* pick correct s_dist_xx */
            pick_dist_info(stock, GET_COL(orderline, ol_dist_info), d_id);  // pick correct s_dist_xx
            InsertTupleWithIndex(tx, TABLE_ORDERLINE, &orderlineit, &orderline);
        }

        tx->Commit();
        return 0;
    }

    int do_neword(int wh_start, int wh_end) {
        int i, ret;
        int w_id, d_id, c_id, ol_cnt;
        int all_local = 1;
        int rbk;
        int itemid[MAX_NUM_ITEMS];
        int supware[MAX_NUM_ITEMS];
        int qty[MAX_NUM_ITEMS];

        /* params */
        w_id = RandomNumber(wh_start, wh_end);
        d_id = RandomNumber(1, DIST_PER_WARE);
        c_id = NURand(1023, 1, CUST_PER_DIST);
        ol_cnt = RandomNumber(5, MAX_NUM_ITEMS);
        rbk = RandomNumber(1, 100);
        for (i = 0; i < ol_cnt; i++) {
            itemid[i] = NURand(8191, 1, MAXITEMS);
            if ((i == ol_cnt - 1) && (rbk == 1))
                itemid[i] = notfound;
            if (RandomNumber(1, 100) != 1)
                supware[i] = w_id;
            else {
                supware[i] = other_ware(w_id);
                all_local = 0;
            }
            qty[i] = RandomNumber(1, 10);
        }

        /* transaction */
        ret = neword(w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
        return ret;
    }

    int payment(int w_id_arg,                                 /* warehouse id */
                int d_id_arg,                                 /* district id */
                bool byname,                                  /* select by c_id or c_last? */
                int c_w_id_arg, int c_d_id_arg, int c_id_arg, /* customer id */
                char c_last_arg[],                            /* customer last name */
                float h_amount_arg                            /* payment amount */
    ) {
        const int w_id = w_id_arg;
        const int d_id = d_id_arg;
        const int c_id = c_id_arg;
        const int c_d_id = c_d_id_arg;
        const int c_w_id = c_w_id_arg;
        const float h_amount = h_amount_arg;

        STACK_WAREHOUSE(wh);
        WAREHOUSE_INDEX(whit);
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_CUSTOMER(cus);
        CUSTOMER_INDEX(cusit);
        STACK_HISTORY(hist);

        int64 w_ytd;
        int64 d_ytd;
        uint64 h_date = __rdtsc();
        int64 i_h_amount = h_amount;

        auto tx = GetCurrentTxContext();
        tx->Begin();

        /* select/update warehouse w_ytd += h_amount */
        RowId whid;
        SET_INDEX_COL(whit, pk, w_id, w_id);
        SelectTuple(tx, TABLE_WAREHOUSE, &whit, &wh, &whid);
        FETCH_COL(wh, w_ytd, w_ytd);
        w_ytd += (int)h_amount;
        UPDATE_COL(wh, w_ytd, w_ytd);
        if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_WAREHOUSE)], whid, &wh) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        /* select/update district d_ytd += h_amount */
        RowId disid;
        SET_INDEX_COL(disit, pk, d_id, d_id);
        SET_INDEX_COL(disit, pk, d_w_id, w_id);
        SelectTuple(tx, TABLE_DISTRICT, &disit, &dis, &disid);
        FETCH_COL(dis, d_ytd, d_ytd);
        d_ytd += (int)h_amount;
        UPDATE_COL(dis, d_ytd, d_ytd);
        if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_DISTRICT)], disid, &dis) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        RowId cusid;
        if (byname) {
            /* select customer by last name */
            if (!SelectCustomerByName(tx, c_w_id, d_id, c_last_arg, cusid, cus)) {
                /* should never happen */
                tx->Abort();
                return -1;
            }
        } else {
            /* select customer by id */
            SET_INDEX_COL(cusit, pk, c_id, c_id);
            SET_INDEX_COL(cusit, pk, c_d_id, d_id);
            SET_INDEX_COL(cusit, pk, c_w_id, c_w_id);
            SelectTuple(tx, TABLE_CUSTOMER, &cusit, &cus, &cusid);
        }

        float c_balance = GET_COL_FLOAT(cus, c_balance) - h_amount;
        // if (strstr(GET_COL(cus, c_credit), "BC")) {
        //     char datetime[TIMESTAMP_LEN + 1];
        //     char *c_new_data = GET_COL(cus, c_data);
        //
        //     gettimestamp(datetime, TIMESTAMP_LEN);
        //     snprintf(c_new_data, 501, "| %4d %2d %4d %2d %4d $%7.2f %.12s %.24s", c_id, c_d_id, c_w_id, d_id, w_id,
        //              h_amount, datetime, GET_COL(cus, c_data));
        //     strncat(c_new_data, GET_COL(cus, c_data), 500 - strlen(c_new_data));
        // }

        /* update customer */
        UPDATE_COL(cus, c_balance, c_balance);
        if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_CUSTOMER)], cusid, &cus) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        /* insert into history (8 columns) */
        char *h_data = GET_COL(hist, h_data);
        // strncpy(h_data, GET_COL(wh, w_name), 10);
        // h_data[10] = '\0';
        // strncat(h_data, GET_COL(dis, d_name), 10);
        // h_data[20] = ' ';
        // h_data[21] = ' ';
        // h_data[22] = ' ';
        // h_data[23] = ' ';
        // h_data[24] = '\0';
        SET_COL(hist, h_c_id, c_id);
        SET_COL(hist, h_d_id, d_id);
        SET_COL(hist, h_w_id, w_id);
        SET_COL(hist, h_amount, i_h_amount);
        SET_COL(hist, h_c_d_id, c_d_id);
        SET_COL(hist, h_c_w_id, c_w_id);
        SET_COL(hist, h_date, h_date);
        InsertTupleWithIndex(tx, TABLE_HISTORY, nullptr, &hist);
        tx->Commit();
        return 0;
    }

    int do_payment(int wh_start, int wh_end) {
        bool byname;
        int w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
        char c_last[4];
        memset(c_last, 0, sizeof(c_last));

        w_id = RandomNumber(wh_start, wh_end);
        d_id = RandomNumber(1, DIST_PER_WARE);
        c_id = NURand(1023, 1, CUST_PER_DIST);
        Lastname(NURand(255, 0, 999), c_last);
        h_amount = RandomNumber(1, 5000);
        /* 60% select by last name, 40% select by customer id */
        byname = (RandomNumber(1, 100) <= 60);
        if (RandomNumber(1, 100) <= 85) {
            c_w_id = w_id;
            c_d_id = d_id;
        } else {
            c_w_id = other_ware(w_id);
            c_d_id = RandomNumber(1, DIST_PER_WARE);
        }

        return payment(w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
    }

    int ordstat(int w_id_arg,     /* warehouse id */
                int d_id_arg,     /* district id */
                int byname,       /* select by c_id or c_last? */
                int c_id_arg,     /* customer id */
                char c_last_arg[] /* customer last name, format? */
    ) {
        int w_id = w_id_arg;
        int d_id = d_id_arg;
        int c_id = c_id_arg;
        int c_d_id = d_id;
        int c_w_id = w_id;

        STACK_CUSTOMER(cus);
        CUSTOMER_INDEX(cusit);
        STACK_ORDER(order);

        auto tx = GetCurrentTxContext();
        tx->Begin();

        if (byname) {
            RowId cusid;
            /* select from customer by last_name */
            if (!SelectCustomerByName(tx, c_w_id, d_id, c_last_arg, cusid, cus)) {
                /* should never happen */
                tx->Abort();
                return -1;
            }
        } else {
            /* select from customer by id */
            SET_INDEX_COL(cusit, pk, c_id, c_id);
            SET_INDEX_COL(cusit, pk, c_d_id, d_id);
            SET_INDEX_COL(cusit, pk, c_w_id, c_w_id);
            SelectTuple(tx, TABLE_CUSTOMER, &cusit, &cus);
        }

        /* select most recent order of this customer */
        FETCH_COL(cus, c_id, c_id);
        if (!SelectMostRecentOrder(tx, c_w_id, c_d_id, c_id, &order)) {
            /* should never happen */
            tx->Abort();
            return -1;
        }

        /* range select order_line */
        ORDERLINE_INDEX(keyb);
        ORDERLINE_INDEX(keye);
        orderline_to_number_range(keyb, keye, w_id, d_id, GET_COL_INT(order, o_id));
        int res_size = 0;
        RowId *row_ids = new RowId[MAX_NUM_ITEMS];
        RAMTuple **tuples = new RAMTuple *[MAX_NUM_ITEMS];
        for (int i = 0; i < MAX_NUM_ITEMS; i++) {
            tuples[i] = HEAP_ORDERLINE();
        }
        RangeSearch(tx, idxs[TABLE_OFFSET(TABLE_ORDERLINE)], tables[TABLE_OFFSET(TABLE_ORDERLINE)], &keyb, &keye,
                    MAX_NUM_ITEMS, &res_size, row_ids, tuples);
        /* Maybe MAX_NUM_ITEMS + 1 to debug if res_size legal. */
        DCHECK(res_size > 0 && res_size <= MAX_NUM_ITEMS);
        for (int i = 0; i < MAX_NUM_ITEMS; i++) {
            delete tuples[i];
        }
        delete[] tuples;
        delete[] row_ids;

        if (res_size == 0) {
            tx->Abort();
            return -1;
        } else {
            tx->Commit();
            return 0;
        }
    }

    int do_ordstat(int wh_start, int wh_end) {
        bool byname;
        int w_id, d_id, c_id;
        char c_last[4];
        memset(c_last, 0, sizeof(c_last));

        w_id = RandomNumber(wh_start, wh_end);
        d_id = RandomNumber(1, DIST_PER_WARE);
        c_id = NURand(1023, 1, CUST_PER_DIST);
        Lastname(NURand(255, 0, 999), c_last);
        /* 60% select by last name, 40% select by customer id */
        byname = (RandomNumber(1, 100) <= 60);
        return ordstat(w_id, d_id, byname, c_id, c_last);
    }

    int delivery(int w_id_arg, int o_carrier_id_arg) {
        int w_id = w_id_arg;
        int o_carrier_id = o_carrier_id_arg;
        int c_id;
        int no_o_id;
        char datetime[81];

        STACK_NEWORDER(neword);
        NEWORDER_INDEX(newordit);
        RowId newordid;

        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        RowId orderid;

        STACK_CUSTOMER(cus);
        CUSTOMER_INDEX(cusit);
        RowId cusid;

        uint64 ol_delivery_d;
        float c_balance;

        auto tx = GetCurrentTxContext();
        tx->Begin();

        for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
            /* reset it every new order */
            float ol_total = 0;
            /* select oldest new order */
            if (!SelectOldestNewOrder(tx, w_id, d_id, newordid, &neword)) {
                /* no new order to deliver */
                continue;
            }
            /* delete the oldest new order */
            if (!DeleteTupleWithIndex(tx, TABLE_NEWORDER, &newordit, &neword, newordid)) {
                tx->Abort();
                return -1;
            }
            FETCH_COL(neword, no_o_id, no_o_id);
            /* update the corresponding order */
            SET_INDEX_COL(orderit, pk, o_w_id, w_id);
            SET_INDEX_COL(orderit, pk, o_d_id, d_id);
            SET_INDEX_COL(orderit, pk, o_id, no_o_id);
            SelectTuple(tx, TABLE_ORDER, &orderit, &order, &orderid);
            UPDATE_COL(order, o_carrier_id, o_carrier_id);
            if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_ORDER)], orderid, &order) != HamStatus::OK) {
                tx->Abort();
                return -1;
            }
            /* update order lines */
            ORDERLINE_INDEX(keyb);
            ORDERLINE_INDEX(keye);
            orderline_to_number_range(keyb, keye, w_id, d_id, no_o_id);
            int res_size = 0;
            RowId *row_ids = new RowId[MAX_NUM_ITEMS];
            RAMTuple **tuples = new RAMTuple *[MAX_NUM_ITEMS];
            for (int i = 0; i < MAX_NUM_ITEMS; i++) {
                tuples[i] = HEAP_ORDERLINE();
            }
            RangeSearch(tx, idxs[TABLE_OFFSET(TABLE_ORDERLINE)], tables[TABLE_OFFSET(TABLE_ORDERLINE)], &keyb, &keye,
                        MAX_NUM_ITEMS, &res_size, row_ids, tuples);
            DCHECK(res_size > 0 && res_size <= MAX_NUM_ITEMS && res_size == GET_COL_INT(order, o_ol_cnt));
            /* Abort if update order line failed. */
            bool aborted = false;
            for (int i = 0; i < res_size; i++) {
                RowId row_id = row_ids[i];
                RAMTuple *tuple = tuples[i];
                ol_delivery_d = __rdtsc();
                RAMTuple &alias_tuple = *tuples[i];
                UPDATE_COL(alias_tuple, ol_delivery_d, ol_delivery_d);
                if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_ORDERLINE)], row_id, tuple) != HamStatus::OK) {
                    aborted = true;
                    break;
                }
                /* select sum(ol_amount) order line */
                ol_total += GET_COL_FLOAT(alias_tuple, ol_amount);
            }
            for (int i = 0; i < MAX_NUM_ITEMS; i++) {
                delete tuples[i];
            }
            delete[] tuples;
            delete[] row_ids;

            if (res_size == 0 || aborted) {
                tx->Abort();
                return -1;
            }

            /* update customer */
            SET_INDEX_COL(cusit, pk, c_w_id, w_id);
            SET_INDEX_COL(cusit, pk, c_d_id, d_id);
            SET_INDEX_COL(cusit, pk, c_id, GET_COL_INT(order, o_c_id));
            SelectTuple(tx, TABLE_CUSTOMER, &cusit, &cus, &cusid);
            FETCH_COL(cus, c_balance, c_balance);
            c_balance += ol_total;
            UPDATE_COL(cus, c_balance, c_balance);
            if (HeapUpdate(tx, tables[TABLE_OFFSET(TABLE_CUSTOMER)], cusid, &cus) != HamStatus::OK) {
                tx->Abort();
                return -1;
            }
        }

        tx->Commit();
        return 0;
    }

    int do_delivery(int wh_start, int wh_end) {
        int w_id, o_carrier_id;

        w_id = RandomNumber(wh_start, wh_end);
        o_carrier_id = RandomNumber(1, 10);

        return delivery(w_id, o_carrier_id);
    }

    static RAMTuple **InitOrderLineArray(int maxres) {
        RAMTuple **tuples = new RAMTuple *[maxres];
        for (int i = 0; i < maxres; i++) {
            tuples[i] = HEAP_ORDERLINE();
        }
        return tuples;
    }

    int stocklevel(int w_id_arg, /* warehouse id */
                   int d_id_arg, /* district id */
                   int level_arg /* stock level */
    ) {
        int w_id = w_id_arg;
        int d_id = d_id_arg;
        int level = level_arg;
        int d_next_o_id;
        int32_t distinctc = 0;
        std::unordered_set<int> distset;

        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);

        STACK_STOCK(stock);
        STOCK_INDEX(stockit);

        auto tx = GetCurrentTxContext();
        tx->Begin();

        /* select district */
        SET_INDEX_COL(disit, pk, d_id, d_id);
        SET_INDEX_COL(disit, pk, d_w_id, w_id);
        SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
        FETCH_COL(dis, d_next_o_id, d_next_o_id);

        /* select orderline join stock */
        const int maxres = 20 * MAX_NUM_ITEMS;
        ORDERLINE_INDEX(keyb);
        ORDERLINE_INDEX(keye);
        /* [d_next_o_id - 20, d_next_o_id - 1] */
        orderline_to_number_range(keyb, keye, w_id, d_id, d_next_o_id - 20, d_next_o_id - 1);
        int res_size = 0;
        RowId *row_ids = new RowId[maxres];
        thread_local static RAMTuple **tuples = nullptr;
        if (tuples == nullptr) {
            tuples = InitOrderLineArray(maxres);
        }

        RangeSearch(tx, idxs[TABLE_OFFSET(TABLE_ORDERLINE)], tables[TABLE_OFFSET(TABLE_ORDERLINE)], &keyb, &keye,
                    maxres, &res_size, row_ids, tuples);
        DCHECK(res_size > 0 && res_size <= maxres);

        /* if item under stock level */
        for (int i = 0; i < res_size; i++) {
            RAMTuple &tuple = *tuples[i];
            SET_INDEX_COL(stockit, pk, s_w_id, w_id);
            SET_INDEX_COL(stockit, pk, s_i_id, GET_COL_INT(tuple, ol_i_id));
            SelectTuple(tx, TABLE_STOCK, &stockit, &stock);
            DCHECK(GET_COL_INT(stock, s_i_id) == GET_COL_INT(tuple, ol_i_id));
            if (GET_COL_INT(stock, s_quantity) < level) {
                distset.insert(GET_COL_INT(tuple, ol_i_id));
            }
        }

        delete[] row_ids;

        distinctc = distset.size();

        tx->Commit();
        return 0;
    }

    int do_stocklevel(int wh_start, int wh_end) {
        int w_id, d_id, level;

        w_id = RandomNumber(wh_start, wh_end);
        d_id = RandomNumber(1, DIST_PER_WARE);
        level = RandomNumber(10, 20);

        return stocklevel(w_id, d_id, level);
    }

    void tpcc_q(uint32_t wid) {
        int tranid;
        int r;
        int ret;
        uint32_t start = wh_start;
        uint32_t end = wh_end;
        if (bind) {
            GetSplitRange(workers, wh_end, wid, &start, &end);
        }
        InitThreadLocalVariables();
        /* fast_rand() needs per thread initialization */
        fast_rand_srand(__rdtsc() & UINT32_MAX);
        while (on_working) {
            r = RandomNumber(1, 1000);
#ifdef TPCC_OP_ONLY
            if (r <= 511) {
                tranid = 0;
                ret = do_neword(start, end);
            }
            else {
                tranid = 1;
                ret = do_payment(start, end);
            }
#else
            if (r <= 450)
                tranid = 0;
            else if (r > 450 && r <= 880)
                tranid = 1;
            else if (r > 880 && r <= 920)
                tranid = 2;
            else if (r > 920 && r <= 960)
                tranid = 3;
            else if (r > 960)
                tranid = 4;

            switch (tranid) {
                /*
                 * update district, insert order and new order,
                 * update stock, insert order line. 1% abort on item.
                 */
                case 0:
                    ret = do_neword(start, end);
                    break;
                    /*
                     * update warehouse and district,
                     * update customer, insert into history.
                     */
                case 1:
                    ret = do_payment(start, end);
                    break;
                case 2:
                    ret = do_ordstat(start, end);
                    break;
                    /*
                     * delete oldest new order of every district,
                     * update order, update order line, update customer.
                     */
                case 3:
                    ret = do_delivery(start, end);
                    break;
                case 4:
                    ret = do_stocklevel(start, end);
                    break;
            }
#endif

            /* -1: Aborted */
            if (ret == 0)
                __sync_fetch_and_add(&g_stats[wid].runstat_[tranid].nCommitted_, 1);
            else
                __sync_fetch_and_add(&g_stats[wid].runstat_[tranid].nAborted_, 1);
        }
        DestroyThreadLocalVariables();
    }

    // 3.3.2.1 Consistency Condition 1
    //  Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship: W_YTD = sum(D_YTD)
    //  for each warehouse defined by (W_ID = D_W_ID).
    void check_step1(int wh_start, int wh_end) {
        STACK_WAREHOUSE(wh);
        WAREHOUSE_INDEX(whit);
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            SET_INDEX_COL(whit, pk, w_id, i);
            SelectTuple(tx, TABLE_WAREHOUSE, &whit, &wh);

            int64 sum_ytd = 0;
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_id, j);
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
                sum_ytd += GET_COL_LONG(dis, d_ytd);
            }
            if (GET_COL_LONG(wh, w_ytd) != sum_ytd) {
                tx->Abort();
                LOG(ERROR) << "Consistency Condition 1 Failed!" << std::endl;
                DestroyThreadLocalVariables();
                return;
            }
        }
        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.2 Consistency Condition 2
    // Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
    // D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
    // TPC Ben chm ark. C - Standard Specification, Revision 5.11 - Page 49 of 130
    // for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID).
    // This condition does not apply to the NEW-ORDER table for any districts
    // which have no outstanding new orders (i.e., the number of rows is zero).
    void check_step2(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        STACK_NEWORDER(neworder);
        NEWORDER_INDEX(neworderit);

        int d_next_o_id = 0;
        int valid_o_id = 0;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                CHECK(SelectTuple(tx, TABLE_DISTRICT, &disit, &dis));
                d_next_o_id = GET_COL_INT(dis, d_next_o_id);
                valid_o_id = d_next_o_id - 1;
                SET_INDEX_COL(orderit, pk, o_w_id, i);
                SET_INDEX_COL(orderit, pk, o_d_id, j);
                SET_INDEX_COL(orderit, pk, o_id, valid_o_id);
                if (!SelectTuple(tx, TABLE_ORDER, &orderit, &order)) {
                    tx->Abort();
                    LOG(ERROR) << "Consistency Condition 2 Failed!" << std::endl;
                    DestroyThreadLocalVariables();
                    return;
                }
                SET_INDEX_COL(orderit, pk, o_id, d_next_o_id);
                if (SelectTuple(tx, TABLE_ORDER, &orderit, &order, nullptr, false)) {
                    tx->Abort();
                    LOG(ERROR) << "Consistency Condition 2 Failed!" << std::endl;
                    DestroyThreadLocalVariables();
                    return;
                }
                SelectNewestOrder(tx, i, j, &order);
                if (GET_COL_INT(order, o_id) != valid_o_id) {
                    tx->Abort();
                    LOG(ERROR) << "Consistency Condition 2 Failed!" << std::endl;
                    DestroyThreadLocalVariables();
                    return;
                }
                SelectNewestNewOrder(tx, i, j, &neworder);
                if (GET_COL_INT(neworder, no_o_id) != valid_o_id) {
                    tx->Abort();
                    LOG(ERROR) << "Consistency Condition 2 Failed!" << std::endl;
                    DestroyThreadLocalVariables();
                    return;
                }
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.4 Consistency Condition 4
    // Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
    // sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
    // for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).
    //
    // 3.3.2.6 Consistency Condition 6
    // For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table for the
    // corresponding order defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).
    void check_step3(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        STACK_ORDERLINE(orderline);
        ORDERLINE_INDEX(orderlineit);

        int o_ol_cnt = 0;

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
                for (int o_id = 1; o_id < GET_COL_INT(dis, d_next_o_id); o_id++) {
                    SET_INDEX_COL(orderit, pk, o_w_id, i);
                    SET_INDEX_COL(orderit, pk, o_d_id, j);
                    SET_INDEX_COL(orderit, pk, o_id, o_id);
                    SelectTuple(tx, TABLE_ORDER, &orderit, &order);
                    for (int k = 1; k <= GET_COL_INT(order, o_ol_cnt); k++) {
                        SET_INDEX_COL(orderlineit, pk, ol_w_id, i);
                        SET_INDEX_COL(orderlineit, pk, ol_d_id, j);
                        SET_INDEX_COL(orderlineit, pk, ol_o_id, GET_COL_INT(order, o_id));
                        SET_INDEX_COL(orderlineit, pk, ol_number, k);
                        if (!SelectTuple(tx, TABLE_ORDERLINE, &orderlineit, &orderline, nullptr, false)) {
                            tx->Abort();
                            LOG(ERROR) << "Consistency Condition 6 Failed!" << std::endl;
                            DestroyThreadLocalVariables();
                            return;
                        }
                    }
                    /* must not found */
                    o_ol_cnt = GET_COL_INT(order, o_ol_cnt) + 1;
                    SET_INDEX_COL(orderlineit, pk, ol_number, o_ol_cnt);
                    if (SelectTuple(tx, TABLE_ORDERLINE, &orderlineit, &orderline, nullptr, false)) {
                        tx->Abort();
                        LOG(ERROR) << "Consistency Condition 4 Failed!" << std::endl;
                        DestroyThreadLocalVariables();
                        return;
                    }
                }
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.5 Consistency Condition 5
    // For any row in the ORDER table, O_CARRIER_ID is set to a null value
    // if and only if there is a corresponding row in
    // the NEW-ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).
    void check_step4(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        STACK_NEWORDER(neworder);
        NEWORDER_INDEX(neworderit);

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
                for (int o_id = 1; o_id < GET_COL_INT(dis, d_next_o_id); o_id++) {
                    SET_INDEX_COL(orderit, pk, o_w_id, i);
                    SET_INDEX_COL(orderit, pk, o_d_id, j);
                    SET_INDEX_COL(orderit, pk, o_id, o_id);
                    SelectTuple(tx, TABLE_ORDER, &orderit, &order);
                    SET_INDEX_COL(neworderit, pk, no_w_id, i);
                    SET_INDEX_COL(neworderit, pk, no_d_id, j);
                    SET_INDEX_COL(neworderit, pk, no_o_id, o_id);
                    bool null_o_carrier_id = (GET_COL_INT(order, o_carrier_id) == 0);
                    bool exist_new_order = SelectTuple(tx, TABLE_NEWORDER, &neworderit, &neworder, nullptr, false);
                    /* XNOR */
                    if (null_o_carrier_id != exist_new_order) {
                        tx->Abort();
                        LOG(ERROR) << "Consistency Condition 5 Failed!" << std::endl;
                        DestroyThreadLocalVariables();
                        return;
                    }
                }
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.7 Consistency Condition 7
    // For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/ time if and only if the corresponding
    // row in the ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has
    // O_CARRIER_ID set to a null value.
    void check_step5(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        STACK_ORDER(order);
        ORDER_INDEX(orderit);
        STACK_ORDERLINE(orderline);
        ORDERLINE_INDEX(orderlineit);

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();
        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
                for (int o_id = 1; o_id < GET_COL_INT(dis, d_next_o_id); o_id++) {
                    SET_INDEX_COL(orderit, pk, o_w_id, i);
                    SET_INDEX_COL(orderit, pk, o_d_id, j);
                    SET_INDEX_COL(orderit, pk, o_id, o_id);
                    SelectTuple(tx, TABLE_ORDER, &orderit, &order);

                    bool null_o_carrier_id = (GET_COL_INT(order, o_carrier_id) == 0);
                    bool zero_delivery_d = true;

                    /* loop order lines */
                    ORDERLINE_INDEX(keyb);
                    ORDERLINE_INDEX(keye);
                    orderline_to_number_range(keyb, keye, i, j, o_id);
                    int res_size = 0;
                    RowId *row_ids = new RowId[MAX_NUM_ITEMS];
                    RAMTuple **tuples = new RAMTuple *[MAX_NUM_ITEMS];
                    for (int k = 0; k < MAX_NUM_ITEMS; k++) {
                        tuples[k] = HEAP_ORDERLINE();
                    }
                    RangeSearch(tx, idxs[TABLE_OFFSET(TABLE_ORDERLINE)], tables[TABLE_OFFSET(TABLE_ORDERLINE)], &keyb,
                                &keye, MAX_NUM_ITEMS, &res_size, row_ids, tuples);
                    DCHECK(res_size > 0 && res_size <= MAX_NUM_ITEMS && res_size == GET_COL_INT(order, o_ol_cnt));
                    for (int k = 0; k < res_size; k++) {
                        RAMTuple &tuple = *tuples[k];
                        zero_delivery_d &= (GET_COL_UNSIGNED_LONG(tuple, ol_delivery_d) == 0);
                    }
                    for (int k = 0; k < MAX_NUM_ITEMS; k++) {
                        delete tuples[k];
                    }
                    delete[] tuples;
                    delete[] row_ids;

                    /* XNOR */
                    if (null_o_carrier_id != zero_delivery_d) {
                        tx->Abort();
                        LOG(ERROR) << "Consistency Condition 7 Failed!" << std::endl;
                        DestroyThreadLocalVariables();
                        return;
                    }
                }
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.8 Consistency Condition 8
    // Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship:
    // W_YTD = sum(H_AMOUNT)
    // for each warehouse defined by (W_ID = H_W_ID).
    void check_step6(uint32_t rowid_start, uint32_t rowid_end, int64 *ytd_arr) {
        STACK_HISTORY(his);

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();

        for (uint32_t i = rowid_start; i <= rowid_end; i++) {
            HamStatus status = HeapRead(tx, tables[TABLE_OFFSET(TABLE_HISTORY)], i, &his);
            if (status != HamStatus::OK) {
                continue;
            }
            __sync_fetch_and_sub(&ytd_arr[GET_COL_INT(his, h_w_id) - 1], GET_COL_LONG(his, h_amount));
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    // 3.3.2.9 Consistency Condition 9
    // Entries in the DISTRICT and HISTORY tables must satisfy the relationship:
    // D_YTD = sum(H_AMOUNT)TPC Benchmark? C - Standard Specification, Revision 5.11 - Page 50 of 130
    // for each district defined by (D_W_ID, D_ID) = (H_W_ID, H_D_ID).
    void check_step7(uint32_t rowid_start, uint32_t rowid_end, int64 *ytd_arr) {
        STACK_HISTORY(his);

        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();

        for (uint32_t i = rowid_start; i <= rowid_end; i++) {
            HamStatus status = HeapRead(tx, tables[TABLE_OFFSET(TABLE_HISTORY)], i, &his);
            if (status != HamStatus::OK) {
                continue;
            }
            __sync_fetch_and_sub(
                &ytd_arr[(GET_COL_INT(his, h_w_id) - 1) * DIST_PER_WARE + (GET_COL_INT(his, h_d_id) - 1)],
                GET_COL_LONG(his, h_amount));
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void __set_warehouse_ytd(int wh_start, int wh_end, int64 *ytd_arr) {
        STACK_WAREHOUSE(wh);
        WAREHOUSE_INDEX(whit);
        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();

        for (int i = wh_start; i <= wh_end; i++) {
            SET_INDEX_COL(whit, pk, w_id, i);
            SelectTuple(tx, TABLE_WAREHOUSE, &whit, &wh);
            ytd_arr[i - 1] = GET_COL_LONG(wh, w_ytd);
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void __set_district_ytd(int wh_start, int wh_end, int64 *ytd_arr) {
        STACK_DISTRICT(dis);
        DISTRICT_INDEX(disit);
        InitThreadLocalVariables();
        auto tx = GetCurrentTxContext();
        tx->Begin();

        for (int i = wh_start; i <= wh_end; i++) {
            for (int j = 1; j <= DIST_PER_WARE; j++) {
                SET_INDEX_COL(disit, pk, d_w_id, i);
                SET_INDEX_COL(disit, pk, d_id, j);
                SelectTuple(tx, TABLE_DISTRICT, &disit, &dis);
                ytd_arr[(i - 1) * DIST_PER_WARE + (j - 1)] = GET_COL_LONG(dis, d_ytd);
            }
        }

        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void __set_ytd_parallel(bool step6, int64 *ytd_arr) {
        std::thread worker_tids[workers];
        for (int i = 0; i < workers; i++) {
            uint32_t start, end;
            GetSplitRange(workers, wh_end, i, &start, &end);
            if (step6) {
                worker_tids[i] = std::thread(&TPCCBench::__set_warehouse_ytd, this, start, end, ytd_arr);
            } else {
                worker_tids[i] = std::thread(&TPCCBench::__set_district_ytd, this, start, end, ytd_arr);
            }
        }
        for (int i = 0; i < workers; ++i) {
            worker_tids[i].join();
        }
    }

    void __check_consistency(uint32_t step) {
        /* prepared for history table check */
        int64 *ytd_arr = nullptr;
        int ytd_arr_len = 0;
        bool step_6_or_7 = (step == 6 || step == 7);
        RowId history_upper_rowid = 0;
        if (step_6_or_7) {
            ytd_arr_len = wh_end * ((step == 6) ? 1 : DIST_PER_WARE);
            ytd_arr = new int64[ytd_arr_len];
            __set_ytd_parallel(step == 6, ytd_arr);
            history_upper_rowid = HeapUpperRowId(tables[TABLE_OFFSET(TABLE_HISTORY)]);
        }

        std::thread worker_tids[workers];
        for (int i = 0; i < workers; i++) {
            uint32_t start, end;
            if (!step_6_or_7)
                GetSplitRange(workers, wh_end, i, &start, &end);
            else
                GetSplitRange(workers, history_upper_rowid, i, &start, &end, false);
            switch (step) {
                case 1: {
                    worker_tids[i] = std::thread(&TPCCBench::check_step1, this, start, end);
                    break;
                }
                case 2: {
                    worker_tids[i] = std::thread(&TPCCBench::check_step2, this, start, end);
                    break;
                }
                case 3: {
                    worker_tids[i] = std::thread(&TPCCBench::check_step3, this, start, end);
                    break;
                }
                case 4: {
                    worker_tids[i] = std::thread(&TPCCBench::check_step4, this, start, end);
                    break;
                }
                case 5:
                    worker_tids[i] = std::thread(&TPCCBench::check_step5, this, start, end);
                    break;
                case 6:
                    worker_tids[i] = std::thread(&TPCCBench::check_step6, this, start, end, ytd_arr);
                    break;
                case 7:
                    worker_tids[i] = std::thread(&TPCCBench::check_step7, this, start, end, ytd_arr);
                    break;
                default: {
                    delete[] ytd_arr;
                    return;
                }
            }
        }
        for (int i = 0; i < workers; ++i) {
            worker_tids[i].join();
        }
        for (int i = 0; i < ytd_arr_len; i++) {
            DCHECK(step_6_or_7);
            if (step == 6 && ytd_arr[i] != 300000) {
                LOG(ERROR) << "Consistency Condition 8 Failed!" << std::endl;
                break;
            } else if (step == 7 && ytd_arr[i] != 300000 / DIST_PER_WARE) {
                LOG(ERROR) << "Consistency Condition 9 Failed!" << std::endl;
                break;
            }
        }
        delete[] ytd_arr;
    }

    /* check database consistency -
     * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
     */
    void check_consistency() {
        /* consistency check divided to 7 steps */
        const uint32_t steps[] = {1, 2, 3, 4, 5, 6, 7};
        /* Step 1 to 7 */
        for (auto i : steps) {
            __check_consistency(i);
        }
        LOG(INFO) << "finish all consistency check";
    }

    void RunBench() {
        SetForceWriteBackCSN(false);
        auto statusFunc = [&](int l_runTime) {
            clearRunStat();
            run_time = 0;
            while (run_time < l_runTime) {
                sleep(1);
                run_time += 1;
                printTpccStat();
            }
        };
        if (type == 1 || type == 3) {
            std::thread worker_tids[workers];
            on_working = true;
            for (uint32_t i = 0; i < workers; i++) {
                worker_tids[i] = std::thread(&TPCCBench::tpcc_q, this, i);
            }
            const auto l_runTime = run_time;
            LOG(INFO) << "Warming up (10 sec).";
            statusFunc(10);
            LOG(INFO) << "Start TPC-C Benchmark.";
            statusFunc(l_runTime);
            LOG(INFO) << "Final results.";
            printTpccStat();
            on_working = false;
            for (int i = 0; i < workers; i++) {
                worker_tids[i].join();
            }
        }
#ifndef  TPCC_OP_ONLY
        check_consistency();
#endif
    }
};

class TPCCTest : public ::testing::Test {
protected:
    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(TPCCTest, TPCCTestMain) {
    // 要使用TPCC测试需要将 nvm_index_tuple中的 For tpcc testing 启用
    IndexBenchOpts opt = {.threads = 48, .duration = 300, .warehouse = 1024, .type = 3, .bind = true};

    TPCCBench bench("/mnt/pmem0/ycsb", opt.threads, opt.duration, opt.warehouse, opt.bind, opt.type);
    bench.InitBench();
    bench.LoadDB();
    bench.RunBench();
    bench.EndBench();
}
// backup
// cp -r /mnt/pmem0/bench2 /mnt/pmem0/bench3& cp -r /mnt/pmem1/bench2 /mnt/pmem1/bench3& cp -r /mnt/pmem2/bench2 /mnt/pmem2/bench3& cp -r /mnt/pmem3/bench2 /mnt/pmem3/bench3
// restore
// rm -rf /mnt/pmem0/bench2 /mnt/pmem1/bench2 /mnt/pmem2/bench2 /mnt/pmem3/bench2
// cp -r /mnt/pmem0/bench3 /mnt/pmem0/bench2& cp -r /mnt/pmem1/bench3 /mnt/pmem1/bench2& cp -r /mnt/pmem2/bench3 /mnt/pmem2/bench2& cp -r /mnt/pmem3/bench3 /mnt/pmem3/bench2

}
}