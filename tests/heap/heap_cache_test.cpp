#include "heap/nvm_heap_cache.h"
#include "heap/nvm_rowid_map.h"
#include "common/lightweight_semaphore.h"
#include <gtest/gtest.h>

using namespace NVMDB;

class FreeRowIdListTest : public ::testing::Test {
public:
    FreeRowIdListTest() : strings(maxSize), entries(maxSize) {
        for (int i=0; i<100; i++) {
            strings[i] = "string_at_" + std::to_string(i);
            entries[i].Init(const_cast<char *>(strings[i].data()));
        }
    }

    std::vector<std::string> strings;
    std::vector<RowIdMapEntry> entries;
    std::vector<uint64_t> dramAddrPtr;

    static constexpr int maxSize = 100;

    inline auto* getDRAMStrPtr(int index) {
        return entries[index].loadDRAMCache<char>(strings[index].size());
    }

    void SetUp() override {
        dramAddrPtr.resize(maxSize);
        for (int i=0; i<maxSize; i++) {
            // 检测加载到DRAM cache中的数据和元数据是否一样
            auto* rawStr = getDRAMStrPtr(i);
            std::string dramStr(rawStr, strings[i].size());
            ASSERT_TRUE(dramStr == strings[i]);
            // 存储缓存的地址
            dramAddrPtr[i] = reinterpret_cast<uint64_t>(getDRAMStrPtr(i));
        }
    }

    void TearDown() override {
        dramAddrPtr.clear();
    }

};

TEST_F(FreeRowIdListTest, BasicTest) {
    TupleCache<RowId, RowIdMapEntry> tupleCache(1, 1);
    entries[0].Lock();
    tupleCache.touch(0, &entries[0]);
    entries[0].Unlock();
    // 确定缓存没有被删除
    ASSERT_TRUE(dramAddrPtr[0] == reinterpret_cast<uint64_t>(getDRAMStrPtr(0)));
    {
        // 再插入两个元素, 1号元素的缓存应该被删除
        entries[1].Lock();
        tupleCache.touch(1, &entries[1]);
        ASSERT_TRUE(dramAddrPtr[0] == reinterpret_cast<uint64_t>(getDRAMStrPtr(0)));
        entries[1].Unlock();
        entries[2].Lock();
        tupleCache.touch(2, &entries[2]);
        ASSERT_TRUE(dramAddrPtr[0] != reinterpret_cast<uint64_t>(getDRAMStrPtr(0)));
        entries[2].Unlock();
    }
}

TEST_F(FreeRowIdListTest, DeadLockTest) {
    // 线程1在上锁1号元组时候释放0号元组, 同时, 线程2在上锁0号元组时候释放1号元组
    moodycamel::LightweightSemaphore semaOut(0, 0);
    moodycamel::LightweightSemaphore semaIn(0, 0);

    std::thread t1([&] {
        TupleCache<RowId, RowIdMapEntry> c1(1, 1);
        c1.touch(0, &entries[0]);
        c1.touch(2, &entries[2]);
        entries[1].Lock();
        semaOut.signal(1);
        // 等待上锁完毕
        while(!semaIn.wait());
        entries[3].Lock();
        // 此时, c1试图释放0号元组, 但是被t1持有
        c1.touch(3, &entries[3]);
        entries[3].Unlock();
        entries[1].Unlock();
    });
    std::thread t2([&] {
        TupleCache<RowId, RowIdMapEntry> c2(1, 1);
        c2.touch(1, &entries[1]);
        c2.touch(2, &entries[2]);
        entries[0].Lock();
        semaOut.signal(1);
        // 等待上锁完毕
        while(!semaIn.wait());
        entries[4].Lock();
        // 此时, c2试图释放1号元组, 但是被t0持有
        c2.touch(4, &entries[4]);
        entries[4].Unlock();
        entries[0].Unlock();
    });
    for (auto i=2; i>0; i-=(int)semaOut.waitMany((ssize_t)i));
    semaIn.signal(2);
    t1.join();
    t2.join();
}
