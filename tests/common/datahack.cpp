#include "common/test_declare.h"
#include "nvm_init.h"
#include "nvm_access.h"
#include <cstring>
#include <cstdio>
#include <gtest/gtest.h>

#define TX_INFO_IS_CSN(txInfo) ((txInfo) >= MIN_TX_CSN)

using namespace NVMDB;

static ColumnDesc TestColDesc[] = {
    InitColDesc(COL_TYPE_INT), /* col_1 */
    InitColDesc(COL_TYPE_INT), /* col_2 */
};

static uint64 row_len = 0;
static uint32 col_cnt = 0;

inline RAMTuple *GenRow(bool value_set = false, int col1 = 0, int col2 = 0) {
    auto *tuple = new RAMTuple(&TestColDesc[0], row_len);
    if (value_set) {
        tuple->SetCol(0, (char *)&col1);
        tuple->SetCol(1, (char *)&col2);
    }
    return tuple;
}

void dump_table() {
    uint32 segHead, row_len;
    std::cin >> segHead;
    std::cin >> row_len;
    RowId start, end;
    std::cin >> start;
    std::cin >> end;

    RowIdMap *map = GetRowIdMap(segHead, row_len);
    char *undoRecordCache = new char[4096];
    for (RowId i = start; i <= end; i++) {
        auto tuple = GenRow();
        RowIdMapEntry *entry = map->GetEntry(i, false);
        entry->loadDRAMCache(RealTupleSize(tuple->getRowLen()));
        if (tuple->IsUsed()) {
            printf("%u version: %d, tx %llx ", i, tuple->getNVMTuple().m_txInfo);
            if (!TX_INFO_IS_CSN(tuple->getNVMTuple().m_txInfo)) {
                TransactionInfo txSlot;
                bool exist = GetTransactionInfo(tuple->getNVMTuple().m_txInfo, &txSlot);
                if (exist) {
                    printf("(CSN: %llx, status: %d) ", txSlot.csn, txSlot.status);
                } else {
                    printf("recycled ");
                }
            }
            printf("\n");
            int k = 0;
            while (!UndoRecPtrIsInValid(tuple->getNVMTuple().m_prev) && k < 50) {
                tuple->FetchPreVersion(undoRecordCache);
                printf("\t;version: %d, tx %llx ", k, tuple->getNVMTuple().m_txInfo);
                if (!TX_INFO_IS_CSN(tuple->getNVMTuple().m_txInfo)) {
                    TransactionInfo txSlot;
                    bool exist = GetTransactionInfo(tuple->getNVMTuple().m_txInfo, &txSlot);
                    if (exist) {
                        printf("(CSN: %llx, status: %d) ", txSlot.csn, txSlot.status);
                    } else {
                        printf("recycled ");
                    }
                }
                k++;
                printf("\n");
            }
        } else {
            printf("%u not used\n", i);
        }
        delete tuple;
    }
}

void txInfo() {
    TxSlotPtr txInfo;
    scanf("%u", &txInfo);
    TransactionInfo txSlot;
    bool exist = GetTransactionInfo(txInfo, &txSlot);
    if (exist) {
        printf("(CSN: %llx, status: %d) ", txSlot.csn, txSlot.status);
    } else {
        printf("recycled ");
    }
    printf("transaction %llx, status: %d, csn: %llx", (uint64)txInfo, txSlot.status, txSlot.csn);
}

/*
 * 支持功能：
 * 1. dump 某个表的所有数据，包括undo中的版本链
 * 2. 查看 某个事务的状态。
 */
class DataHack : public ::testing::Test {
protected:
    void SetUp() override { }

    void TearDown() override { }
};

TEST_F(DataHack, DataHackMain) {
    std::string dir;
    std::cin >> dir;
    BootStrap(dir);

    std::string cmd;
    while (std::cin >> cmd) {
        if (strcmp(cmd.c_str(), "dump_table") == 0) {
            dump_table();
        } else if (strcmp(cmd.c_str(), "tx_info") == 0) {
            txInfo();
        } else {
            std::cout << "unsupported command\n";
        }
    }

    ExitDBProcess();
}