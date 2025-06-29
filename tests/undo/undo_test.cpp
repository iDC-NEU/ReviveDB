#include "undo/nvm_undo.h"
#include "index/nvm_index.h"
#include "transaction/nvm_transaction.h"
#include "nvmdb_thread.h"
#include "common/test_declare.h"
#include <experimental/filesystem>
#include <gtest/gtest.h>

using namespace NVMDB;

class UndoTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_dir_config = std::make_shared<NVMDB::DirectoryConfig>("/mnt/pmem0/bench;/mnt/pmem1/bench", true);
        ProcessArray::InitGlobalProcArray();
        InitGlobalThreadStorageMgr();
        InitThreadLocalStorage();
        IndexBootstrap();
    }
    void TearDown() override {
        IndexExitProcess();
        DestroyThreadLocalStorage();
        ProcessArray::DestroyGlobalProcArray();
        for (const auto & it: g_dir_config->getDirPaths()) {
            std::experimental::filesystem::remove_all(it);
        }
    }
};

TEST_F(UndoTest, BasicTest) {
    UndoCreate();
    UndoExitProcess();

    for (int i = 0; i < 10; i++) {
        UndoBootStrap();
        UndoExitProcess();
    }

    UndoBootStrap();
    InitLocalUndoSegment();

    std::vector<std::pair<UndoRecPtr, int>> undo_ptr_arr;
    std::string PREFIX = "helloworld";
    for (int i = 0; i < 4; i++) {
        PREFIX += PREFIX;  // create a long prefix
    }

    int MAX_DATA = 20;
    int MAX_TXS = UNDO_TX_SLOTS + 1024;
    uint64 TEST_CSN = MIN_TX_CSN + 1;
    char *record_cache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
    for (int i = 0; i < MAX_TXS; i++) {
        auto undoTxCtx = AllocUndoContext();
        for (int j = 0; j < MAX_DATA; j++) {
            std::string data = PREFIX + std::to_string(j);
            auto head = (UndoRecord *)record_cache;
            head->m_undoType = InvalidUndoRecordType;
            head->m_payload = data.length();
            memcpy(head->data, data.c_str(), data.length());
            UndoRecPtr undo_ptr = undoTxCtx->insertUndoRecord(head);
            undo_ptr_arr.emplace_back(undo_ptr, j);
        }
        undoTxCtx->UpdateTxSlotStatus(TxSlotStatus::COMMITTED);
        undoTxCtx->UpdateTxSlotCSN(TEST_CSN);
        if (i % 10 == 0) {
            UndoSegment *undo_segment = GetThreadLocalUndoSegment();
            undo_segment->recycleTxSlot(TEST_CSN + 1);
        }
    }

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap();
    int i = 0;
    for (auto undo_ptr : undo_ptr_arr) {
        if (i % (10 * MAX_DATA) == 0) {
            continue;
        }
        auto* record = reinterpret_cast<UndoRecord *>(record_cache);
        GetUndoRecord(undo_ptr.first, record);
        ASSERT_GT(record->m_payload, PREFIX.length());
        std::string data_prefix(record->data, PREFIX.length());
        ASSERT_STREQ(data_prefix.c_str(), PREFIX.c_str());
        std::string data(record->data + PREFIX.length(), record->m_payload - PREFIX.length());
        int digit = atoi(data.c_str());
        ASSERT_EQ(digit, undo_ptr.second);
        i++;
    }
    UndoExitProcess();
    delete[] record_cache;
}

TEST_F(UndoTest, UndoRecoveryTest) {
    UndoCreate();
    UndoExitProcess();

    UndoBootStrap();
    InitLocalUndoSegment();

    /* no undo segment switch. */
    int MAX_TXS = UNDO_TX_SLOTS;
    uint64 TEST_CSN = MIN_TX_CSN;
    char *record_cache = new char[MAX_UNDO_RECORD_CACHE_SIZE];
    for (int i = 0; i < MAX_TXS; i++) {
        auto undoTxCtx = AllocUndoContext();

        undoTxCtx->UpdateTxSlotStatus(TxSlotStatus::COMMITTED);
        undoTxCtx->UpdateTxSlotCSN(TEST_CSN + i);

        UndoSegment *undo_segment = GetThreadLocalUndoSegment();
        undo_segment->recycleTxSlot(TEST_CSN + UNDO_TX_SLOTS / 2);
    }
    TEST_CSN += MAX_TXS;

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap();
    InitLocalUndoSegment();

    /* consume the left half slots. */
    for (int i = 0; i < MAX_TXS / 2; i++) {
        auto undoTxCtx = AllocUndoContext();

        undoTxCtx->UpdateTxSlotStatus(TxSlotStatus::COMMITTED);
        undoTxCtx->UpdateTxSlotCSN(TEST_CSN + i);
    }

    DestroyLocalUndoSegment();
    UndoExitProcess();

    UndoBootStrap();
    /* switched to an empty undo segment. */
    InitLocalUndoSegment();

    UndoSegment *undo_segment = GetThreadLocalUndoSegment();
    ASSERT_EQ(undo_segment->isEmpty(), true);

    DestroyLocalUndoSegment();
    UndoExitProcess();
    delete[] record_cache;
}