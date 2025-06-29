#include "common/phmap.h"
#include "tpcc_hash_index.h"
#include "nvm_init.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "random_generator.h"
#include <gtest/gtest.h>
#include <getopt.h>
#include <thread>
#include <x86intrin.h>
#include <unordered_set>
#include "common/latency_stat.h"

namespace NVMDB {

namespace TPCC_HASH {

using DRAMIndexType = MyFlatHashMap<uint64_t, RowId, SpinLock, 10>;

inline RowId UniqueSearch(Transaction *tx,
                          uint64 key,
                          DRAMIndexType& index,
                          Table *tbl,
                          RAMTuple *res) {
    RowId rowId = InvalidRowId;
    index.if_contains(key, [&](const auto& value) {
        rowId = value.second;
    });
    if (rowId == InvalidRowId) {
        return InvalidRowId;
    }
    HamStatus status = HeapRead(tx, tbl, rowId, res);
    if (status == HamStatus::OK) {
        return rowId;
    }
    return InvalidRowId;
}

/* hardwired. */
#ifdef NDEBUG
static const uint32_t Heap_SegHead[] = {2, 258, 514, 770, 1026, 1282, 1538, 1794, 2050};
#else
static const uint32_t Heap_SegHead[] = {2, 258, 514, 770, 2560, 2816, 3072, 3328, 3584};
#endif

struct IndexBenchOpts {
    int threads;
    int duration;
    int warehouse;
    int type;
    /* bind warehouses to threads with no overlap */
    bool bind;
};

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
    LatencyStat<> *g_neworder_latency_stats;
    LatencyStat<> *g_payment_latency_stats;
    /* different tableIds' index tuple funcs */
    DRAMIndexType idxs[TABLE_NUM];
    /* table heaps */
    Table **tables;
    /* secondary index of customer table */
    DRAMIndexType cus_sec_idx;

private:

    static uint64 distKey(uint64_t d_id, uint64_t d_w_id)  {
        return d_w_id * DIST_PER_WARE + d_id;
    }

    static uint64 custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
        return (distKey(c_d_id, c_w_id) * CUST_PER_DIST + c_id);
    }

    static uint64 orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
        return distKey(d_id, w_id) * CUST_PER_DIST + o_id;
    }

    static uint64 orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_num) {
        return orderPrimaryKey(w_id, d_id, o_id) * 15 + ol_num;
    }

    static uint64 custNPKey(const char * c_last, uint64_t c_d_id, uint64_t c_w_id) {
        uint64 key = 0;
        constexpr char offset = 'A';
        for (uint32_t i = 0; c_last[i] != '\0'; i++)
            key = (key << 2) + (c_last[i] - offset);
        key = key << 3;
        key += c_w_id * DIST_PER_WARE + c_d_id;
        return key;
    }

    static uint64 stockKey(uint64_t s_i_id, uint64_t s_w_id) {
        return s_w_id * MAXITEMS + s_i_id;
    }

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
        g_neworder_latency_stats = makeAlignedArray<LatencyStat<>>(workers);
        g_payment_latency_stats = makeAlignedArray<LatencyStat<>>(workers);
        bool is_init = type == 0 || type == 3;
        if (is_init) {
            InitDB(dir_config.c_str());
        } else {
            DCHECK(type == 1 || type == 2);
            LOG(INFO) << "BootStrap Start." << std::endl;
            BootStrap(dir_config.c_str());
            LOG(INFO) << "BootStrap End." << std::endl;
        }
        tables = new Table *[TABLE_NUM];
        for (uint32_t i = 0; i < TABLE_NUM; i++) {
            uint32_t table_type = TABLE_FIRST + i;
            tables[i] = new Table(table_type, TABLE_ROW_LEN(table_type));
            if (is_init) {
                tables[i]->CreateSegment();
            } else {
                tables[i]->Mount(Heap_SegHead[i]);
            }
        }
    }

    void EndBench() {
        for (uint32_t i = 0; i < TABLE_NUM; i++) {
            delete tables[i];
        }
        delete[] tables;
        delete[] g_stats;
        deleteAlignedArray(g_neworder_latency_stats, workers);
        deleteAlignedArray(g_payment_latency_stats, workers);
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
            auto whpk = i;
            InsertTupleWithIndex(tx, TABLE_WAREHOUSE, whpk, &wh);
        }
        tx->Commit();
        DestroyThreadLocalVariables();
    }

    void load_district(int wh_start, int wh_end) {
        STACK_DISTRICT(dis);
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
                auto distpk = distKey(j, i);
                InsertTupleWithIndex(tx, TABLE_DISTRICT, distpk, &dis);
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
            auto itempk = i;
            InsertTupleWithIndex(tx, TABLE_ITEM, itempk, &item);
        }
        tx->Commit();
        DestroyThreadLocalVariables();
        delete[] orig;
    }

    void load_customer(int wh_start, int wh_end) {
        STACK_CUSTOMER(cus);
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
                    auto custpk = custKey(k, j, i);
                    auto custnpk = custNPKey(c_last, j, i);
                    InsertTupleWithIndex(tx, TABLE_CUSTOMER, custpk, &cus, &custnpk);
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
                auto stockpk = stockKey(j, i);
                InsertTupleWithIndex(tx, TABLE_STOCK, stockpk, &stock);
            }
        }
        tx->Commit();
        DestroyThreadLocalVariables();
        delete[] orig;
    }

    void load_order(int wh_start, int wh_end) {
        STACK_ORDER(order);
        STACK_NEWORDER(neworder);
        STACK_ORDERLINE(orderline);

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
                        InsertTupleWithIndex(tx, TABLE_NEWORDER, 0, &neworder);
                    } else {
                        o_carrier_id = RandomNumber(1L, DIST_PER_WARE);
                        SET_COL(order, o_carrier_id, o_carrier_id);
                    }
                    auto orderpk = orderPrimaryKey(i, j, k);
                    InsertTupleWithIndex(tx, TABLE_ORDER, orderpk, &order);
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
                        InsertTupleWithIndex(tx, TABLE_ORDERLINE, 0, &orderline);
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

    // avg, p95
    std::pair<int64_t, int64_t> getNewOrderLatency() {
        static LatencyStat<> stat;
        LatencyStat<>::sumUp(stat, g_neworder_latency_stats, workers);
        return {
            stat.avg(),
            stat.percentile()
        };
    }

    // avg, p95
    std::pair<int64_t, int64_t> getPaymentLatency() {
        static LatencyStat<> stat;
        LatencyStat<>::sumUp(stat, g_payment_latency_stats, workers);
        return {
            stat.avg(),
            stat.percentile()
        };
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
        for (int k = 0; k < workers; k++) {
            g_neworder_latency_stats[k].reset();
            g_payment_latency_stats[k].reset();
        }
    }

    void printTpccStat() {
        TpccRunStat summary = getRunStat();
        auto neworder_avg_p95 = getNewOrderLatency();
        auto payment_avg_p95 = getPaymentLatency();
        uint64_t total = summary.nTotalCommitted_ + summary.nTotalAborted_;

        printf("==> Committed TPS: %lu, per worker: %lu\n\n", summary.nTotalCommitted_ / run_time,
               summary.nTotalCommitted_ / run_time / workers);

        printf("trans         #totaltran       %%ratio     #committed       #aborted       %%abort\n");
        printf("-----         ----------       ------      ----------       --------       ------\n");
        for (int i = 0; i < 2; i++) {
            const RunStat &stat = summary.runstat_[i];
            uint64_t totalpert = stat.nCommitted_ + stat.nAborted_;
            printf("%-8s     %11lu      %6.1f%%     %11lu      %9lu      %6.1f%%\n", tname[i], totalpert,
                   (totalpert * 100.0) / total, stat.nCommitted_, stat.nAborted_, (stat.nAborted_ * 100.0) / totalpert);
        }
        printf("\n");
        printf("%s        %11lu      %6.1f%%      %10lu      %9lu      %6.1f%%\n", "Total", total, 100.0,
               summary.nTotalCommitted_, summary.nTotalAborted_, (summary.nTotalAborted_ * 100.0) / total);
        printf("-----         ----------       ------      ----------       --------       ------\n");
        printf("NewOrder Latency: Avg %6ldus, P95 %6ldus.\n", neworder_avg_p95.first, neworder_avg_p95.second);
        printf("Payment  Latency: Avg %6ldus, P95 %6ldus.\n", payment_avg_p95.first, payment_avg_p95.second);
    }

    /* Assume you have inited and prepared already. */
    inline bool SelectTuple(Transaction *tx, TableType table_type, uint64 key, RAMTuple *tuple, RowId *row_id = nullptr, bool secondary = false) {
        // 只有 customer 需要插入secondary index
        RowId rowId;
        if (secondary) {
            rowId = UniqueSearch(tx, key, cus_sec_idx, tables[TABLE_OFFSET(table_type)], tuple);
        } else {
            rowId = UniqueSearch(tx, key, idxs[TABLE_OFFSET(table_type)], tables[TABLE_OFFSET(table_type)], tuple);
        }
        if (rowId == InvalidRowId) {
            return false;
        }
        if (row_id != nullptr)
            *row_id = rowId;
        return true;
    }

    /* Assume you have inited and prepared already. */
    inline void InsertTupleWithIndex(Transaction *tx, TableType table_type, uint64 key,
                                     RAMTuple *tuple, uint64* seckey = nullptr) {
        // orderline history neworder 没有索引, key = 0;
        // customer需要插入两个索引
        /* insert table segment */
        RowId rowId = HeapInsert(tx, tables[TABLE_OFFSET(table_type)], tuple);
        if (table_type == TABLE_ORDERLINE || table_type == TABLE_NEWORDER || table_type == TABLE_HISTORY) {
            return;
        }
        /* insert table index */
        idxs[TABLE_OFFSET(table_type)][key] = rowId;
        /* insert secondary index if need */
        if (table_type == TABLE_CUSTOMER) {
            cus_sec_idx[*seckey] = rowId;
        }
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
        STACK_CUSTOMER(cus);
        STACK_DISTRICT(dis);
        STACK_ORDER(order);
        STACK_NEWORDER(neworder);
        STACK_ITEM(item);
        STACK_STOCK(stock);
        STACK_ORDERLINE(orderline);

        auto tx = GetCurrentTxContext();
        tx->Begin();

        /* select from warehouse */
        auto whpk = w_id;
        CHECK(SelectTuple(tx, TABLE_WAREHOUSE, whpk, &wh));

        /* select from customer */
        auto custpk = custKey(c_id, d_id, w_id);
        CHECK(SelectTuple(tx, TABLE_CUSTOMER, custpk, &cus));

        /* update district set d_next_o_id += 1 */
        auto distpk = distKey(d_id, w_id);
        RowId disid;
        CHECK(SelectTuple(tx, TABLE_DISTRICT, distpk, &dis, &disid));
        FETCH_COL(dis, d_next_o_id, d_next_o_id);
        u_d_next_o_id = d_next_o_id + 1;
        UPDATE_COL(dis, d_next_o_id, u_d_next_o_id);
        if (HeapUpdate2(tx, tables[TABLE_OFFSET(TABLE_DISTRICT)], disid, &dis) != HamStatus::OK) {
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

        auto orderpk = orderPrimaryKey(w_id, d_id, d_next_o_id);
        InsertTupleWithIndex(tx, TABLE_ORDER, orderpk, &order);

        /* insert into neworder (3 columns, others NULL) */
        SET_COL(neworder, no_o_id, d_next_o_id);
        SET_COL(neworder, no_w_id, w_id);
        SET_COL(neworder, no_d_id, d_id);
        InsertTupleWithIndex(tx, TABLE_NEWORDER, 0, &neworder);

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
            if (!SelectTuple(tx, TABLE_ITEM, ol_i_id, &item, nullptr)) {
                DCHECK(ol_i_id == notfound);
                tx->Abort();
                return -1;
            }

            float total = 0.0;
            int s_quantity;
            price[ol_number_idx] = GET_COL_FLOAT(item, i_price);
            strncpy(iname[ol_number_idx], GET_COL(item, i_name), 25);

            RowId stockid;
            /* select from stock */
            auto stockpk = stockKey(ol_i_id, ol_supply_w_id);
            SelectTuple(tx, TABLE_STOCK, stockpk, &stock, &stockid);

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
            if (HeapUpdate2(tx, tables[TABLE_OFFSET(TABLE_STOCK)], stockid, &stock) != HamStatus::OK) {
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
            InsertTupleWithIndex(tx, TABLE_ORDERLINE, 0, &orderline);
        }

        tx->Commit();
        return 0;
    }

    int do_neword(int wh_start, int wh_end, int workerId) {
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
        std::atomic_signal_fence(std::memory_order_acq_rel);
        TestTimer timer;
        ret = neword(w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
        if (ret == 0){ g_neworder_latency_stats[workerId].insert(timer.getDurationUs()); }
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
        STACK_DISTRICT(dis);
        STACK_CUSTOMER(cus);
        STACK_HISTORY(hist);

        int64 w_ytd;
        int64 d_ytd;
        uint64 h_date = __rdtsc();
        int64 i_h_amount = h_amount;

        auto tx = GetCurrentTxContext();
        tx->Begin();

        /* select/update warehouse w_ytd += h_amount */
        RowId whid;
        auto whpk = w_id;
        CHECK(SelectTuple(tx, TABLE_WAREHOUSE, whpk, &wh, &whid));
        FETCH_COL(wh, w_ytd, w_ytd);
        w_ytd += (int)h_amount;
        UPDATE_COL(wh, w_ytd, w_ytd);
        if (HeapUpdate2(tx, tables[TABLE_OFFSET(TABLE_WAREHOUSE)], whid, &wh) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        /* select/update district d_ytd += h_amount */
        RowId disid;
        auto distpk = distKey(d_id, w_id);
        SelectTuple(tx, TABLE_DISTRICT, distpk, &dis, &disid);
        FETCH_COL(dis, d_ytd, d_ytd);
        d_ytd += (int)h_amount;
        UPDATE_COL(dis, d_ytd, d_ytd);
        if (HeapUpdate2(tx, tables[TABLE_OFFSET(TABLE_DISTRICT)], disid, &dis) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        RowId cusid;
        if (byname) {
            /* select customer by last name */
            auto custnpk = custNPKey(c_last_arg, c_d_id, c_w_id);
            SelectTuple(tx, TABLE_CUSTOMER, custnpk, &cus, &cusid, true);
        } else {
            /* select customer by id */
            auto custpk = custKey(c_id, d_id, c_w_id);
            SelectTuple(tx, TABLE_CUSTOMER, custpk, &cus, &cusid);
        }

        float c_balance = GET_COL_FLOAT(cus, c_balance) - h_amount;

        /* update customer */
        UPDATE_COL(cus, c_balance, c_balance);
        if (HeapUpdate2(tx, tables[TABLE_OFFSET(TABLE_CUSTOMER)], cusid, &cus) != HamStatus::OK) {
            tx->Abort();
            return -1;
        }

        /* insert into history (8 columns) */
        char *h_data = GET_COL(hist, h_data);
        strncpy(h_data, GET_COL(wh, w_name), 10);
        h_data[10] = '\0';
        strncpy(h_data + 11, GET_COL(dis, d_name), 10);
        h_data[20] = ' ';
        h_data[21] = ' ';
        h_data[22] = ' ';
        h_data[23] = ' ';
        h_data[24] = '\0';
        SET_COL(hist, h_c_id, c_id);
        SET_COL(hist, h_d_id, d_id);
        SET_COL(hist, h_w_id, w_id);
        SET_COL(hist, h_amount, i_h_amount);
        SET_COL(hist, h_c_d_id, c_d_id);
        SET_COL(hist, h_c_w_id, c_w_id);
        SET_COL(hist, h_date, h_date);
        InsertTupleWithIndex(tx, TABLE_HISTORY, 0, &hist);
        tx->Commit();
        return 0;
    }

    int do_payment(int wh_start, int wh_end, int workerId) {
        bool byname;
        int w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
        char c_last[17];
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

        std::atomic_signal_fence(std::memory_order_acq_rel);
        TestTimer timer;
        auto ret = payment(w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
        if (ret == 0){ g_payment_latency_stats[workerId].insert(timer.getDurationUs()); }
        return ret;
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
            if (r <= 511)
                tranid = 0;
            else
                tranid = 1;

            switch (tranid) {
                /*
                 * update district, insert order and new order,
                 * update stock, insert order line. 1% abort on item.
                 */
                case 0: {
                    ret = do_neword(start, end, wid);
                } break;
                    /*
                     * update warehouse and district,
                     * update customer, insert into history.
                     */
                case 1: {
                    ret = do_payment(start, end, wid);
                } break;
            }

            /* -1: Aborted */
            if (ret == 0)
                __sync_fetch_and_add(&g_stats[wid].runstat_[tranid].nCommitted_, 1);
            else
                __sync_fetch_and_add(&g_stats[wid].runstat_[tranid].nAborted_, 1);
        }
        DestroyThreadLocalVariables();
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
            LOG(INFO) << "Warming up (20 sec).";
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
    }
};

class TPCCHashTest : public ::testing::Test {
protected:
    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(TPCCHashTest, TPCCTestMain) {
    // 要使用TPCC测试需要将 nvm_index_tuple中的 For tpcc testing 启用
    IndexBenchOpts opt = {.threads = 48, .duration = 20, .warehouse = 1024, .type = 3, .bind = true};

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