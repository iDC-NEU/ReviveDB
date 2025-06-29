#include "heap/nvm_rowid_map.h"
#include "common/test_declare.h"
#include "nvmdb_thread.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <thread>
#include <experimental/filesystem>

using namespace NVMDB;

class RowIdMapTest : public ::testing::Test {
protected:
    static const int MAX_TABLES = CompileValue(128, 16);

    static const int TUPLE_SIZE = 127;

    static const int TUPLE_NUM = 256 * 1024;

    TableSpace *space{};

    struct FakeTables {
        uint32 segments[MAX_TABLES];
    };

protected:
    void SetUp() override {
        g_dir_config = std::make_shared<NVMDB::DirectoryConfig>("/mnt/pmem0/bench;/mnt/pmem1/bench", true);
        InitGlobalThreadStorageMgr();
        space = new TableSpace(g_dir_config);
    }

    void TearDown() override {
        space->unmount();
        for (const auto & it: g_dir_config->getDirPaths()) {
            std::experimental::filesystem::remove_all(it);
        }
    }
};

TEST_F(RowIdMapTest, BasicTest) {
    space->create();
    RowIdMap *rowIdMaps[MAX_TABLES];

    auto *tables = (FakeTables *)space->getTableMetadataPage();
    for (int i = 0; i < MAX_TABLES; i++) {
        tables->segments[i] = space->allocNewExtent(EXT_SIZE_2M);
        rowIdMaps[i] = new RowIdMap(space, tables->segments[i], TUPLE_SIZE);
    }

    std::thread workers[MAX_TABLES];

    for (int i = 0; i < MAX_TABLES; i++) {
        const auto func = [&, tid=i] {
            InitThreadLocalStorage();
            for (int j = 0; j < TUPLE_NUM; j++) {
                RowId rid = rowIdMaps[tid]->getNextEmptyRow(InvalidRowId).first;
                // ASSERT_EQ(rid, j);
                auto* entry = rowIdMaps[tid]->GetEntry(rid, false);
                entry->wrightThroughCache([&](char* tuple) {
                    reinterpret_cast<NVMTuple *>(tuple)->m_isUsed = true;
                    char *tuple_data = tuple + NVMTupleHeadSize;
                    *(reinterpret_cast<uint64*>(tuple_data)) = tid + rid*1000;
                }, NVMTupleHeadSize + sizeof(uint64));
            }
            DestroyThreadLocalStorage();
        };
        workers[i] = std::thread(func);
    }

    for (auto & worker : workers) {
        worker.join();
    }

    std::cout << "multi-thread insert finished. Start checking.\n";

    for (int i = 0; i < MAX_TABLES; i++) {
        const auto func = [&, tid=i] {
            int skip_step = CompileValue(1000, 100);
            for (int j = 0; j < TUPLE_NUM; j += skip_step) {
                auto* entry = rowIdMaps[tid]->GetEntry(j, true);
                if (entry == nullptr) {
                    continue;
                }
                auto* data = entry->loadDRAMCache<char>(NVMTupleHeadSize + sizeof(uint64));
                ASSERT_EQ(*(reinterpret_cast<uint64*>(data + NVMTupleHeadSize)), tid + j*1000);
            }
        };
        workers[i] = std::thread(func);
    }

    for (auto & worker : workers) {
        worker.join();
    }
}
