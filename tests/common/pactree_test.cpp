#include "common/pactree/pactree.h"
#include "common/test_declare.h"
#include "nvmdb_thread.h"
#include <experimental/filesystem>
#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace NVMDB;

static const int numLogsPerThread = 512;

class PACTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_dir_config = std::make_shared<NVMDB::DirectoryConfig>(space_dir, true);
        InitGlobalThreadStorageMgr();
        InitThreadLocalStorage();
    }

    void TearDown() override {
        DestroyThreadLocalStorage();
        for (const auto & it: g_dir_config->getDirPaths()) {
            std::experimental::filesystem::remove_all(it);
        }
    }
    const std::string space_dir = "/mnt/pmem0/bench;/mnt/pmem1/bench";
};

std::string LeadingZeroNumberString(int i) {
    static const int n_zero = 10;
    auto s = std::to_string(i);
    DCHECK(s.length() <= n_zero);
    return std::string(n_zero - s.length(), '0') + s;
}

std::string GenerateKey(int i) {
    return std::string("hello world") + LeadingZeroNumberString(i);
}

TEST_F(PACTreeTest, PMemTest) {
    int is_create = 0;
    root_obj *root, *sl_root;
    PMem::CreatePMemPool(&is_create, &root, &sl_root);
    NVMPtr<char> tPtr;
    PMem::alloc(sizeof(ART_ROWEX::Tree), (void **)&tPtr, &root->ptr[0]);
    char *buf = (char *)pmemobj_direct(root->ptr[0]);
    for (int i = 0; i < 128; i++) {
        buf[i] = i;
    }
    int logIdx = numLogsPerThread;
    auto plog = (OpStruct *)PMem::getOpLog(logIdx);
    plog->op = OpStruct::insert;

    PMem::UnmountPMEMPool();
    PMem::CreatePMemPool(&is_create, &root, &sl_root);
    ASSERT_EQ(is_create, 0);
    buf = (char *)pmemobj_direct(root->ptr[0]);
    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(buf[i], i);
    }
    plog = (OpStruct *)PMem::getOpLog(logIdx);
    ASSERT_EQ(plog->op, OpStruct::insert);
}

TEST_F(PACTreeTest, ARTTest) {
    int is_create = 0;
    root_obj *root = nullptr;
    root_obj *sl_root = nullptr;
    PMem::CreatePMemPool(&is_create, &root, &sl_root);
    ASSERT_EQ(is_create, 1);

    NVMPtr<ART_ROWEX::Tree> tPtr;
    PMem::alloc(sizeof(ART_ROWEX::Tree), (void **)&tPtr, &sl_root->ptr[0]);
    auto art = new (tPtr.getVaddr()) ART_ROWEX::Tree(LoadStringKeyFunction);
    auto thread_info = art->GetThreadInfo();
    static const int num_data = 10000;
    auto keys = std::vector<int>();

    static const int ndata = 100000;
    for (int i = 32; i <= ndata; i += 32) {
        NVMPtr<Key_t> dPtr;
        PMem::alloc(sizeof(Key_t), (void **)&dPtr, &sl_root->ptr[0]);
        dPtr.getVaddr()->setFromString(GenerateKey(i));

        Key k;
        k.Set(dPtr.getVaddr()->getData(), dPtr.getVaddr()->keyLength);

        art->Insert(k, (TID)dPtr.getRawPtr(), thread_info);
        keys.push_back(i);
    }

    for (auto key : keys) {
        Key k, v;
        Key_t tk;
        tk.setFromString(GenerateKey(key));
        k.Set(tk.getData(), tk.keyLength);
        auto r = art->Lookup(k, thread_info);
        ASSERT_NE(r, 0);
        LoadStringKeyFunction(r, v);
        ASSERT_EQ(k == v, true);
    }
    PMem::UnmountPMEMPool();
}

static void BasicTestUnit(PACTree *pt, int si, int ei) {
    int num_data = ei - si;
    Key_t key;
    for (int i = si; i < ei; i++) {
        key.setFromString(GenerateKey(i));
        ASSERT_EQ(pt->Insert(key, INVALID_CSN), false);
    }

    LookupSnapshot snapshot{};
    std::vector<std::pair<Key_t, Val_t>> result;

    /* case 1: 按照顺序访问都能访问到 */
    Key_t start, end;
    start.setFromString(GenerateKey(si - 1));
    end.setFromString(GenerateKey(ei + 1));

    int max_range = 7;  // 不被 num_data 整除
    int fetch_num = 0;
    while (true) {
        pt->scan(start, end, max_range, snapshot, false, result);
        for (int i = 0; i < result.size(); i++) {
            auto s = GenerateKey(si + i + fetch_num);
            key.setFromString(s);
            ASSERT_EQ(key == result[i].first, true);
        }
        fetch_num += result.size();
        if (result.size() < max_range) {
            break;
        }
        start = result.back().first;
        start.next();
    }
    ASSERT_EQ(fetch_num, num_data);

    /* case 2: 访问范围外的，返回 0 */
    start.setFromString(GenerateKey(ei));
    end.setFromString(GenerateKey(ei + 1));
    pt->scan(start, end, max_range, snapshot, false, result);
    ASSERT_EQ(result.size(), 0);

    start.setFromString(GenerateKey(si - 1));
    end.setFromString(GenerateKey(si));
    pt->scan(start, end, max_range, snapshot, false, result);
    ASSERT_EQ(result.size(), 0);

    /* case 3: 删除一个节点，然后scan的时候会过滤掉该点 */
    int skip_steps = 4;
    int skip_tuples = 0;
    for (int i = si; i < ei; i += skip_steps) {
        key.setFromString(GenerateKey(i));
        ASSERT_EQ(pt->Insert(key, MIN_TX_CSN), true);
        bool found;
        ASSERT_EQ(pt->lookup(key, &found), MIN_TX_CSN);
        ASSERT_EQ(found, true);
        skip_tuples++;
    }
    snapshot.snapshot = MIN_TX_CSN;
    snapshot.min_csn = MIN_TX_CSN;
    start.setFromString(GenerateKey(si));
    end.setFromString(GenerateKey(ei));
    pt->scan(start, end, num_data, snapshot, false, result);
    ASSERT_EQ(result.size(), num_data);

    snapshot.snapshot = MIN_TX_CSN + 1;
    pt->scan(start, end, num_data, snapshot, false, result);
    ASSERT_EQ(result.size(), num_data - skip_tuples);

    /* scan 并没有清除这些 key */
    for (int i = si; i < ei; i += skip_steps) {
        key.setFromString(GenerateKey(i));
        bool found;
        ASSERT_EQ(pt->lookup(key, &found), MIN_TX_CSN);
        ASSERT_EQ(found, true);
    }
}

TEST_F(PACTreeTest, PACTreeTest) {
    auto *pt = new PACTree();
    pt->registerThread();
    BasicTestUnit(pt, 0, 1000);
    pt->unregisterThread();
    delete pt;
}

TEST_F(PACTreeTest, RecoveryTest) {
    auto *pt = new PACTree();
    int si = 1, ed = 1000, bp = si;
    pt->registerThread();
    for (int i = si; i <= ed;) {
        if (i == bp) {
            SetPACTreeWhiteBoxBP(PACTreeWhiteBoxType::UPDATE_PERMUTATION);
        }
        try {
            Key_t key;
            key.setFromString(GenerateKey(i));
            pt->Insert(key, INVALID_CSN);
            i++;
        } catch (const std::invalid_argument &e) {
            SetPACTreeWhiteBoxBP(PACTreeWhiteBoxType::NO_BREAKPOINT);
            bp += 20;
            pt->unregisterThread();
            delete pt;
            pt = new PACTree();
            pt->registerThread();
        }
    }

    bool found;
    for (int i = 1; i < ed; i++) {
        Key_t key;
        key.setFromString(GenerateKey(i));
        Val_t value = pt->lookup(key, &found);
        ASSERT_EQ(found, true);
        ASSERT_EQ(value, INVALID_CSN);
    }
    pt->unregisterThread();
    delete pt;
}

static void workload(PACTree *pt, int wid, int nthreads, const volatile int *on_working) {
    pt->registerThread();
    std::vector<std::pair<Key_t, Val_t>> result;

    const int scan_len = 20;
    int k = wid * scan_len;
    while (*on_working) {
        // 留出边缘的空档，方便测上界和下届。
        BasicTestUnit(pt, k + 1, k + scan_len - 1);
        k += nthreads * scan_len;
    }

    pt->unregisterThread();
}

TEST_F(PACTreeTest, ConcurrentTest) {
    auto *pt = new PACTree();
    pt->registerThread();

    volatile int on_working = true;
    static const int nthreads = 2;
    std::thread worker_tids[nthreads];
    for (int i = 0; i < nthreads; i++) {
        worker_tids[i] = std::thread(workload, pt, i, nthreads, &on_working);
    }

    sleep(1);
    on_working = false;
    for (auto & worker_tid : worker_tids) {
        worker_tid.join();
    }
    pt->unregisterThread();
    delete pt;
}

TEST_F(PACTreeTest, Example) {
    const auto NUM_DATA = 1000;
    std::experimental::filesystem::remove_all(space_dir);
    std::experimental::filesystem::create_directories(space_dir);
    auto *pt = new PACTree();
    pt->registerThread();

    for (int i = 1; i < NUM_DATA; i++) {
        Key_t key(i);
        pt->Insert(key, i);
    }

    for (int i = 1; i < NUM_DATA; i++) {
        bool found;
        Key_t key(i);
        Val_t v = pt->lookup(key, &found);
        if (i != v) {
            printf("error");
            exit(1);
        }
    }
    printf("all check passed\n");
}