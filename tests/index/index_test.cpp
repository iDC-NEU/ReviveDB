#include "index_test.h"
#include "common/test_declare.h"
#include "nvmdb_thread.h"
#include "nvm_init.h"
#include <gtest/gtest.h>
#include <thread>

using namespace NVMDB;

namespace index_test {

ColumnDesc TestColDesc[] = {
    InitColDesc(COL_TYPE_INT),        /* col_1 */
    InitColDesc(COL_TYPE_INT),        /* col_2 */
    InitVarColDesc(COL_TYPE_VARCHAR, 32) /* name */
};

uint64 row_len = 0;
uint32 col_cnt = 0;

void InitColumnInfo() {
    col_cnt = sizeof(TestColDesc) / sizeof(ColumnDesc);
    InitColumnDesc(&TestColDesc[0], col_cnt, row_len);
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

IndexColumnDesc TestIndexDesc[] = {{0}, {1}, {2}};

IndexColumnDesc TestIndexDesc2[] = {{0}};

uint64 index_len = 0;
uint32 index_col_cnt = 0;

uint64 index_len2 = 0;
uint32 index_col_cnt2 = 0;

void InitIndexInfo() {
    index_col_cnt = sizeof(TestIndexDesc) / sizeof(IndexColumnDesc);
    index_col_cnt2 = sizeof(TestIndexDesc2) / sizeof(IndexColumnDesc);
    InitIndexDesc(&TestIndexDesc[0], &TestColDesc[0], index_col_cnt, index_len);
    InitIndexDesc(&TestIndexDesc2[0], &TestColDesc[0], index_col_cnt2, index_len2);
}

inline DRAMIndexTuple *GenIndexTuple(int col1, int col2, const char *const name) {
    auto *tuple = new DRAMIndexTuple(&TestColDesc[0], &TestIndexDesc[0], index_col_cnt, index_len);
    tuple->SetCol(0, (char *)&col1);
    tuple->SetCol(1, (char *)&col2);
    tuple->SetCol(2, name, true);
    return tuple;
}

inline DRAMIndexTuple *GenIndexTuple2() {
    auto *tuple = new DRAMIndexTuple(&TestColDesc[0], &TestIndexDesc2[0], index_col_cnt2, index_len2);
    return tuple;
}

class IndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        InitColumnInfo();
        InitIndexInfo();
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

static void BasicTestUnit(int si, int ei, NVMIndex &idx) {
    std::string name("xxxxVarCharContent");
    *((uint32 *)const_cast<char*>(name.data())) = name.size()-sizeof(uint32);
    LookupSnapshot snapshot = {0, 0};
    int TEST_NUM = ei - si;
    /* 1. insert */
    auto tx = GetCurrentTxContext();
    tx->Begin();
    tx->PrepareUndo();
    for (int i = si; i < ei; i++) {
        DRAMIndexTuple *tuple = GenIndexTuple(1, i, name.c_str());
        snapshot.snapshot = tx->GetSnapshot();
        idx.Insert(tuple, i);
        /* Search the result */
        auto iter = idx.GenerateIter(tuple, tuple, snapshot, 10, false);
        ASSERT_EQ(iter->Valid(), true);
        ASSERT_EQ(iter->Curr(), i);
        delete iter;
        delete tuple;
    }
    tx->Commit();

    /* 2. scan */
    {
        DRAMIndexTuple *start = GenIndexTuple(1, si, name.c_str());
        DRAMIndexTuple *end = GenIndexTuple(1, ei - 1, name.c_str());
        auto iter = idx.GenerateIter(start, end, snapshot, 0, false);
        int fetch_num = 0;
        while (iter->Valid()) {
            ASSERT_EQ(iter->Curr(), si + fetch_num);
            iter->Next();
            fetch_num++;
        }
        ASSERT_EQ(fetch_num, TEST_NUM);
        delete iter;
        delete start;
        delete end;
    }

    /* 3. delete */
    tx->Begin();
    tx->PrepareUndo();
    for (int i = si; i < ei; i++) {
        DRAMIndexTuple *tuple = GenIndexTuple(1, i, name.c_str());
        snapshot.snapshot = tx->GetSnapshot();
        idx.Delete(tuple, i, tx->GetTxSlotLocation());
        delete tuple;
    }
    tx->Commit();

    {
        auto tx = GetCurrentTxContext();
        tx->Begin();
        snapshot.snapshot = tx->GetSnapshot();
        DRAMIndexTuple *start = GenIndexTuple(1, si, name.c_str());
        DRAMIndexTuple *end = GenIndexTuple(1, ei - 1, name.c_str());
        auto iter = idx.GenerateIter(start, end, snapshot, TEST_NUM, false);
        ASSERT_EQ(iter->Valid(), false);
        delete iter;
        delete start;
        delete end;
        tx->Commit();
    }
}

TEST_F(IndexTest, BasicTest) {
    NVMIndex idx(1);
    BasicTestUnit(0, 100, idx);
}

TEST_F(IndexTest, ConcurrentTest) {
    NVMIndex idx(1);

    static const int WORKER_NUM = 16;
    static const int SCAN_LEN = 100;
    volatile int on_working = true;

    std::thread workers[WORKER_NUM];
    for (int i = 0; i < WORKER_NUM; i++) {
        workers[i] = std::thread(
            [&](int tid) {
                InitThreadLocalVariables();
                int k = tid * SCAN_LEN;
                while (on_working) {
                    BasicTestUnit(k, k + SCAN_LEN, idx);
                    k += WORKER_NUM * SCAN_LEN;
                }
                DestroyThreadLocalVariables();
            },
            i);
    }
    sleep(1);
    on_working = false;
    for (auto & worker : workers) {
        worker.join();
    }
}

TEST_F(IndexTest, TransactionTest) {
    Table table(0, row_len);
    NVMIndex idx(1);
    uint32 segHead = table.CreateSegment();
    ASSERT_EQ(NVMPageIdIsValid(segHead), true);

    Transaction *tx = GetCurrentTxContext();
    tx->Begin();
    int si = 0, ei = 10;
    int test_num = ei - si;
    for (int i = si; i < ei; i++) {
        RAMTuple *tuple = GenRow(true, i, i + 1);
        RowId rowid = HeapInsert(tx, &table, tuple);
        DRAMIndexTuple *idx_tuple = GenIndexTuple2();
        tuple->GetCol(0, idx_tuple->GetCol(0));
        tx->IndexInsert(&idx, idx_tuple, rowid);
        delete tuple;
        delete idx_tuple;
    }
    tx->Commit();

    DRAMIndexTuple *idx_begin = GenIndexTuple2();
    DRAMIndexTuple *idx_end = GenIndexTuple2();
    idx_begin->SetCol(0, (char *)&si);
    int temp = ei - 1;
    idx_end->SetCol(0, (char *)&temp);
    int res_size;
    auto *row_ids = new RowId[test_num];
    auto **tuples = new RAMTuple *[test_num];
    for (int i = 0; i < test_num; i++) {
        tuples[i] = GenRow();
    }

    tx->Begin();
    RangeSearch(tx, &idx, &table, idx_begin, idx_end, test_num, &res_size, row_ids, tuples);

    ASSERT_EQ(res_size, test_num);
    for (int i = 0; i < res_size; i++) {
        ASSERT_EQ(ColEqual(tuples[i], 0, i + si), true);
        ASSERT_EQ(ColEqual(tuples[i], 1, i + si + 1), true);
    }

    tx->Commit();

    tx->Begin();
    RowId row_id = UniqueSearch(tx, &idx, &table, idx_begin, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    HamStatus status = HeapDelete(tx, &table, row_id);
    ASSERT_EQ(status, HamStatus::OK);
    tx->IndexDelete(&idx, idx_begin, row_id);
    tx->Commit();

    tx->Begin();
    row_id = UniqueSearch(tx, &idx, &table, idx_begin, tuples[0]);
    ASSERT_EQ(row_id, InvalidRowId);
    tx->Commit();

    tx->Begin();
    row_id = RangeSearchMin(tx, &idx, &table, idx_begin, idx_end, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    ASSERT_EQ(ColEqual(tuples[0], 0, si + 1), true);  // 第一个已经被删除了
    ASSERT_EQ(ColEqual(tuples[0], 1, si + 2), true);

    row_id = RangeSearchMax(tx, &idx, &table, idx_begin, idx_end, tuples[0]);
    ASSERT_NE(row_id, InvalidRowId);
    ASSERT_EQ(ColEqual(tuples[0], 0, ei - 1), true);  // 最后一个
    ASSERT_EQ(ColEqual(tuples[0], 1, ei), true);
    tx->Commit();

    delete[] row_ids;
    for (int i = 0; i < test_num; i++) {
        delete tuples[i];
    }
    delete[] tuples;
    delete idx_begin;
    delete idx_end;
}

}  // namespace index_test
