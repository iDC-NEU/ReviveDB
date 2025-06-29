#include "nvm_init.h"
#include "nvm_table.h"
#include "transaction/nvm_transaction.h"
#include "nvm_access.h"
#include "nvmdb_thread.h"
#include "common/test_declare.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>

using namespace NVMDB;

namespace heap_test {

ColumnDesc TestColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* col_1 */
    InitColDesc(COL_TYPE_INT), /* col_2 */
};

uint64 row_len = 0;
uint32 col_cnt = 0;

void InitColumnInfo() {
    col_cnt = sizeof(TestColDesc) / sizeof(ColumnDesc);
    InitColumnDesc(&TestColDesc[0], col_cnt, row_len);
}

inline HamStatus UpdateRow(Transaction *tx, Table *table, RowId rowid, RAMTuple *tuple, int col1, int col2) {
    RAMTuple::ColumnUpdate updates[] = {{0, (char *)&col1}, {1, (char *)&col2}};
    tuple->UpdateCols(&updates[0], col_cnt);
    return HeapUpdate(tx, table, rowid, tuple);
}

inline RAMTuple *GenRow(bool value_set = false, int col1 = 0, int col2 = 0) {
    auto *tuple = new RAMTuple(&TestColDesc[0], row_len);
    if (value_set) {
        tuple->SetCol(0, (char *)&col1);
        tuple->SetCol(1, (char *)&col2);
    }
    return tuple;
}

inline bool ColEqual(RAMTuple *tuple, int col_id, int col_val) {
    return tuple->ColEqual(col_id, (char *)&col_val);
}

class HeapTest : public ::testing::Test {
protected:
    void SetUp() override {
        InitColumnInfo();
        InitDB(space_dir);
        ExitDBProcess();
        BootStrap(space_dir);
        InitThreadLocalVariables();
    }
    void TearDown() override {
        DestroyThreadLocalVariables();
        ExitDBProcess();
    }

    const std::string space_dir = "/mnt/pmem0/bench;/mnt/pmem1/bench";
};

TEST_F(HeapTest, HeapRecoveryTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    std::vector<std::pair<RowId, RAMTuple *>> ins_set;
    for (int i = 0; i < 100; i++) {
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        RAMTuple *srcTuple = GenRow(true, i + 1, i + 2);
        RowId rowid = HeapInsert(tx, &table, srcTuple);

        RAMTuple *dstTuple = GenRow();
        HeapRead(tx, &table, rowid, dstTuple);
        ASSERT_EQ(dstTuple->EqualRow(srcTuple), true);
        ASSERT_EQ(TxInfoIsCSN(dstTuple->getNVMTuple().m_txInfo), false);
        tx->Commit();

        tx->Begin();
        UpdateRow(tx, &table, rowid, srcTuple, i + 2, i + 3);
        tx->Commit();

        ins_set.emplace_back(rowid, srcTuple);
        delete dstTuple;
    }

    DestroyThreadLocalVariables();
    ExitDBProcess();
    BootStrap(space_dir);
    table.Mount(segHead);
    InitThreadLocalVariables();

    for (auto item : ins_set) {
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        RAMTuple *dstTuple = GenRow();
        HamStatus stat = HeapRead(tx, &table, item.first, dstTuple);
        ASSERT_EQ(stat, HamStatus::OK);
        ASSERT_EQ(TxInfoIsCSN(dstTuple->getNVMTuple().m_txInfo), false);
        ASSERT_EQ(dstTuple->EqualRow(item.second), true);
        tx->Commit();

        delete dstTuple;
        delete item.second;
    }
    ins_set.clear();
}

/* 单线程的增删读写 */
TEST_F(HeapTest, BasicTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    std::vector<std::pair<RowId, RAMTuple *>> ins_set;
    for (int i = 0; i < 100; i++) {
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        RAMTuple *srcTuple = GenRow(true, i + 1, i + 2);
        RowId rowid = HeapInsert(tx, &table, srcTuple);

        RAMTuple *dstTuple = GenRow();
        HeapRead(tx, &table, rowid, dstTuple);
        ASSERT_EQ(dstTuple->EqualRow(srcTuple), true);
        ASSERT_EQ(TxInfoIsCSN(dstTuple->getNVMTuple().m_txInfo), false);
        tx->Commit();

        tx->Begin();
        UpdateRow(tx, &table, rowid, srcTuple, i + 2, i + 3);
        tx->Commit();

        ins_set.emplace_back(rowid, srcTuple);
        delete dstTuple;
    }

    for (auto item : ins_set) {
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        RAMTuple *dstTuple = GenRow();
        HamStatus stat = HeapRead(tx, &table, item.first, dstTuple);
        ASSERT_EQ(stat, HamStatus::OK);
        ASSERT_EQ(TxInfoIsCSN(dstTuple->getNVMTuple().m_txInfo), false);
        ASSERT_EQ(dstTuple->EqualRow(item.second), true);
        tx->Commit();

        delete dstTuple;
        delete item.second;
    }
    ins_set.clear();
}

class ThreadSync {
    volatile int curr_step;

public:
    ThreadSync() : curr_step(0) {}

    void WaitOn(int step) {
        while (curr_step != step) {
        }
        __sync_fetch_and_add(&curr_step, 1);
    }

    void reset() {
        curr_step = 0;
    }
};

static RowId InsertRow(Table *table) {
    Transaction *tx = GetCurrentTxContext();
    tx->Begin();
    RAMTuple *srcTuple = GenRow(true, 1, 1);
    RowId rowid = HeapInsert(tx, table, srcTuple);
    tx->Commit();
    delete srcTuple;
    return rowid;
}

/* 并发的事务，其中一个读不到另外一个事务Insert的值 */
TEST_F(HeapTest, ConcurrentInsertReadTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    RowId rowid = InvalidRowId;
    ThreadSync threadSync;
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2); /* make sure two threads are concurrent */
        RAMTuple *srcTuple = GenRow(true, 1, 1);
        rowid = HeapInsert(tx, &table, srcTuple);
        tx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(1);
        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4); /* make sure the insert transaction has committed */
        ASSERT_NE(rowid, InvalidRowId);
        HamStatus status = HeapRead(tx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HamStatus::NO_VISIBLE_VERSION);
        tx->Commit();
        delete dstTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，其中一个读不到另外一个Update的值，可以读到旧值 */
TEST_F(HeapTest, ConcurrentUpdateReadTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);

        RAMTuple *srcTuple = GenRow();
        UpdateRow(tx, &table, rowid, srcTuple, 2, 2);
        tx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(1);

        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4); /* make sure the update transaction has committed */
        HamStatus status = HeapRead(tx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HamStatus::OK);
        bool cmp_res = ColEqual(dstTuple, 0, 1);
        ASSERT_EQ(cmp_res, true);
        tx->Commit();

        /* 新的事务可以看到新的值 */
        tx->Begin();
        status = HeapRead(tx, &table, rowid, dstTuple);
        ASSERT_EQ(status, HamStatus::OK);
        cmp_res = ColEqual(dstTuple, 0, 2);
        ASSERT_EQ(cmp_res, true);
        tx->Commit();
        delete dstTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，Update 同一行，会强行让其中一个回滚 */
TEST_F(HeapTest, ConcurrentUpdateUpdateTest) {
    Table table(0, row_len);
    Table table2(1, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);
    table2.CreateSegment();

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    RowId rowid2 = InsertRow(&table2);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(0);

        RAMTuple *srcTuple = GenRow();
        HamStatus status = UpdateRow(tx, &table, rowid, srcTuple, 3, 3);
        ASSERT_EQ(status, HamStatus::OK);
        threadSync.WaitOn(2); /* make sure the other transaction has started before I commit */
        tx->Commit();
        threadSync.WaitOn(3);
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(1);

        RAMTuple *srcTuple = GenRow();
        RAMTuple *dstTuple = GenRow();
        threadSync.WaitOn(4);
        HamStatus status = UpdateRow(tx, &table2, rowid2, srcTuple, 4, 4);
        ASSERT_EQ(status, HamStatus::OK);
        status = HeapRead(tx, &table2, rowid2, dstTuple);
        ASSERT_EQ(status, HamStatus::OK);
        bool cmp_res = ColEqual(dstTuple, 0, 4);
        ASSERT_EQ(cmp_res, true);

        status = UpdateRow(tx, &table, rowid, srcTuple, 4, 4);
        ASSERT_EQ(status, HamStatus::UPDATE_CONFLICT);
        status = UpdateRow(tx, &table, rowid, srcTuple, 4, 4);
        ASSERT_EQ(status, HamStatus::WAIT_ABORT);
        tx->Abort();

        /* abort is ok */
        tx->Begin();
        status = HeapRead(tx, &table2, rowid2, dstTuple);
        ASSERT_EQ(status, HamStatus::OK);
        cmp_res = ColEqual(dstTuple, 0, 1);
        ASSERT_EQ(cmp_res, true);
        tx->Commit();
        delete srcTuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发的事务，其中一个忽视另外一个的Delete，可以读到旧值 */
TEST_F(HeapTest, ConcurrentDeleteReadTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);
        HamStatus status = HeapDelete(tx, &table, rowid);
        ASSERT_EQ(status, HamStatus::OK);
        tx->Commit();
        threadSync.WaitOn(3);
        DestroyThreadLocalVariables();
    });

    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(1);
        threadSync.WaitOn(4);

        RAMTuple *dst_tuple = GenRow();
        HamStatus status = HeapRead(tx, &table, rowid, dst_tuple);
        ASSERT_EQ(status, HamStatus::OK);
        tx->Commit();

        /* 删除事务提交之后，当前事务再读，就会发现数据已经被删了 */
        tx->Begin();
        status = HeapRead(tx, &table, rowid, dst_tuple);
        ASSERT_EQ(status, HamStatus::ROW_DELETED);
        tx->Abort();

        delete dst_tuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

/* 并发事务，一个删除，一个更新，更新的会返回 conflict 错误；如果不是并发，更新会返回 row_deleted 错误 */
TEST_F(HeapTest, ConcurrentDeleteUpdateTest) {
    Table table(0, row_len);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    ThreadSync threadSync;
    RowId rowid = InsertRow(&table);
    std::thread tid1 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(0);
        threadSync.WaitOn(2);
        HamStatus status = HeapDelete(tx, &table, rowid);
        ASSERT_EQ(status, HamStatus::OK);
        tx->Commit();
        threadSync.WaitOn(3);
        DestroyThreadLocalVariables();
    });

    std::thread tid2 = std::thread([&]() {
        InitThreadLocalVariables();
        Transaction *tx = GetCurrentTxContext();
        tx->Begin();
        threadSync.WaitOn(1);
        threadSync.WaitOn(4);

        RAMTuple *dst_tuple = GenRow();
        int col1, col2;
        dst_tuple->GetCol(0, (char *)&col1);
        dst_tuple->GetCol(0, (char *)&col2);
        HamStatus status = UpdateRow(tx, &table, rowid, dst_tuple, col1, col2);
        ASSERT_EQ(status, HamStatus::UPDATE_CONFLICT);
        tx->Abort();

        tx->Begin();
        status = UpdateRow(tx, &table, rowid, dst_tuple, col1, col2);
        ASSERT_EQ(status, HamStatus::ROW_DELETED);
        tx->Abort();

        delete dst_tuple;
        DestroyThreadLocalVariables();
    });
    tid1.join();
    tid2.join();
}

static RowId PressureInsert(Table *data_tbl, Table *cnt_tbl, RowId cnt_rowid) {
    Transaction *tx = GetCurrentTxContext();
    tx->Begin();

    RAMTuple *src_tuple = GenRow(true, 0, 0);
    RowId rowid = HeapInsert(tx, data_tbl, src_tuple);
    RAMTuple *dst_tuple = GenRow();
    HamStatus status = HeapRead(tx, cnt_tbl, cnt_rowid, dst_tuple);
    int temp;
    dst_tuple->GetCol(0, (char *)&temp);
    if (rowid > temp) {
        temp = rowid;
    }
    status = UpdateRow(tx, cnt_tbl, cnt_rowid, dst_tuple, temp, 0);
    delete src_tuple;
    delete dst_tuple;

    if (status == HamStatus::OK) {
        tx->Commit();
        //        DLOG(INFO) << "Transaction " << std::hex << tx->GetTxSlotLocation() << " csn " << tx->GetCSN() <<
        //        std::dec
        //                   << " Insert one account " << rowid;
        return rowid;
    } else {
        tx->Abort();
        return InvalidRowId;
    }
}

static void PressureUpdate(Table *data_tbl, RowId rowid1, RowId rowid2, bool *commit) {
    int transfer = 1 + random() % 100;
    Transaction *tx = GetCurrentTxContext();
    tx->Begin();

    int val1, val2;
    RAMTuple *dst_tuple = GenRow();

    HamStatus status = HeapRead(tx, data_tbl, rowid1, dst_tuple);
    ASSERT_EQ(status, HamStatus::OK);
    int temp = 0;
    dst_tuple->GetCol(0, (char *)&temp);
    temp -= transfer;
    val1 = temp;
    status = UpdateRow(tx, data_tbl, rowid1, dst_tuple, temp, 0);
    if (status != HamStatus::OK) {
        tx->Abort();
        //        DLOG(INFO) << "Transaction " << std::hex << tx->GetTxSlotLocation() << " csn " << tx->GetCSN()
        //                   << " snapshot " << tx->GetSnapshot() << std::dec << " Try to update " << rowid1 << " " <<
        //                   val1
        //                   << " rollback";
        delete dst_tuple;
        *commit = false;
        return;
    }

    status = HeapRead(tx, data_tbl, rowid2, dst_tuple);
    DCHECK(status == HamStatus::OK);
    temp = 0;
    dst_tuple->GetCol(0, (char *)&temp);
    temp += transfer;
    val2 = temp;
    status = UpdateRow(tx, data_tbl, rowid2, dst_tuple, temp, 0);
    if (status != HamStatus::OK) {
        tx->Abort();
        //        DLOG(INFO) << "Transaction " << std::hex << tx->GetTxSlotLocation() << " csn " << tx->GetCSN()
        //                   << " snapshot " << tx->GetSnapshot() << std::dec << " Try to update " << rowid1 << " " <<
        //                   val1 << " "
        //                   << rowid2 << " " << val2 << " rollback";
        delete dst_tuple;
        *commit = false;
        return;
    }

    tx->Commit();

    //    DLOG(INFO) << "Transaction " << std::hex << tx->GetTxSlotLocation() << " csn " << tx->GetCSN() << "
    //    snapshot "
    //               << tx->GetSnapshot() << std::dec << " Update two account " << rowid1 << "(" << val1 << ") -> " <<
    //               rowid2
    //               << "(" << val2 << ")";
    delete dst_tuple;
    *commit = true;
}

static void PressureScanAll(Table *table, Table *table_cnt, RowId cnt_rowid, int success_update) {
    Transaction *tx = GetCurrentTxContext();
    tx->Begin();
    RAMTuple *dst_tuple = GenRow();
    HamStatus status = HeapRead(tx, table_cnt, cnt_rowid, dst_tuple);
    // std::vector<RAMTuple *> tuples;

    ASSERT_EQ(status, HamStatus::OK);

    int max_rowid = 0;
    dst_tuple->GetCol(0, (char *)&max_rowid);
    int sum = 0;
    int valid_row = 0;
    int temp = 0;
    for (int i = 0; i <= max_rowid; i++) {
        status = HeapRead(tx, table, (RowId)i, dst_tuple);
        if (status == HamStatus::OK) {
            dst_tuple->GetCol(0, (char *)&temp);
            sum += temp;
            valid_row++;
            // dst_tuple->col2 = i;
            // tuples.push_back(new TestTuple(*dst_tuple));
        }
    }
    ASSERT_EQ(sum, 0);
    tx->Commit();
    // printf("max_rowid %d, valid_row: %d, sum: %d (successful update: %d)\n", max_rowid, valid_row, sum,
    // success_update);
}

static std::pair<RowId, RowId> select_transfer_accounts(std::vector<RowId> *valid_rowids) {
    int rowid_size = valid_rowids->size();
    DCHECK(rowid_size >= 2);

    int k1 = random() % rowid_size;
    int k2 = random() % rowid_size;
    while (k1 == k2) {
        k2 = random() % rowid_size;
    }
    RowId rowid1 = (*valid_rowids)[k1];
    RowId rowid2 = (*valid_rowids)[k2];
    DCHECK(rowid1 != rowid2);
    return std::make_pair(rowid1, rowid2);
}

/* 大量的并发增删读写 */
TEST_F(HeapTest, ConcurrentPressureTest) {
    Table table(0, row_len);
    Table table_cnt(1, row_len);
    uint32 segHead = table.CreateSegment();
    uint32 segHead2 = table_cnt.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);
    ASSERT_EQ(NVMPageIdIsValid(segHead2), true);

    Transaction *tx = GetCurrentTxContext();
    tx->Begin();
    RAMTuple *cnt = GenRow(true, 0, 0);
    RowId cnt_rowid = HeapInsert(tx, &table_cnt, cnt);
    ASSERT_NE(cnt_rowid, InvalidRowId);
    tx->Commit();
    delete cnt;

    ThreadSync threadSync;
    std::vector<RowId> valid_rowids;
    std::mutex mtx;
    volatile int success_update = 0;

    static int thread_num = 16;
    std::thread tid[thread_num];
    for (int i = 0; i < thread_num; i++) {
        tid[i] = std::thread([&]() {
            InitThreadLocalVariables();
            for (int j = 0; j < 500; j++) {
                RowId rowid = PressureInsert(&table, &table_cnt, cnt_rowid);
                mtx.lock();
                if (RowIdIsValid(rowid)) {
                    valid_rowids.push_back(rowid);
                }
                mtx.unlock();

                for (int k = 0; k < 5; k++) {
                    bool ok = false;
                    mtx.lock();
                    if (valid_rowids.size() < 2) {
                        mtx.unlock();
                        continue;
                        ;
                    }
                    auto rowids = select_transfer_accounts(&valid_rowids);
                    mtx.unlock();
                    PressureUpdate(&table, rowids.first, rowids.second, &ok);
                    if (ok) {
                        __sync_fetch_and_add(&success_update, 1);
                    }
                }
            }
            DestroyThreadLocalVariables();
        });
    }

    volatile bool on_working = true;
    std::thread bg = std::thread([&]() {
        InitThreadLocalVariables();
        do {
            PressureScanAll(&table, &table_cnt, cnt_rowid, success_update);
            usleep(100 * 1000);
        } while (on_working);
        DestroyThreadLocalVariables();
    });

    for (int i = 0; i < thread_num; i++) {
        tid[i].join();
    }
    on_working = false;
    bg.join();
    PressureScanAll(&table, &table_cnt, cnt_rowid, success_update);
}

}  // namespace heap_test