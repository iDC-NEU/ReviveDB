#include "table_space/nvm_table_space.h"
#include "common/test_declare.h"
#include <experimental/filesystem>
#include <gtest/gtest.h>

using namespace NVMDB;

class TableSpaceTest : public ::testing::Test {
protected:
    static const int MAX_TABLES = 3;

    struct FakeTables {
        uint32 segments[MAX_TABLES];
    };

    static const int MAX_PAGES = 100;

    struct SegmentHead {
        uint32 pages[MAX_PAGES];
    };

    TableSpace *space{};

    void SetUp() override {
        // TableSpace 用例只支持一个目录测试
        g_dir_config = std::make_shared<NVMDB::DirectoryConfig>("/mnt/pmem0/bench;/mnt/pmem1/bench", true);
        space = new TableSpace(g_dir_config);
    }

    void TearDown() override {
        space->unmount();
        for (const auto & it: g_dir_config->getDirPaths()) {
            std::experimental::filesystem::remove_all(it);
        }
    }
};

TEST_F(TableSpaceTest, TestCreateAndMount) {
    space->create();
    auto *root = static_cast<unsigned char *>(space->getTableMetadataPage());
    int testsz = 100;
    for (int i = 0; i < testsz; i++) {
        root[i] = (unsigned char)i;
    }
    space->unmount();

    space = new TableSpace(g_dir_config);
    space->mount();
    root = static_cast<unsigned char *>(space->getTableMetadataPage());
    for (int i = 0; i < testsz; i++) {
        // Check this
        ASSERT_EQ(root[i], i);
    }
}

TEST_F(TableSpaceTest, TestAllocPage) {
    space->create();
    auto *tables = (FakeTables *)space->getTableMetadataPage();

    for (int i = 0; i < MAX_TABLES; i++) {
        tables->segments[i] = space->allocNewExtent(EXT_SIZE_2M);
        /*
         * The first two pages are used as metadata of tablespace and database root,
         * thus allocated page must be numbered from 2
         * If rest space of current segment can not hold one extent, it should skip the rest space
         * and allocate from the next segment.
         */
        uint32 expectPageId = i * 256 + 2;
        if (expectPageId + 256 > space->getLogicFile().getPagesPerSegment()) {
            expectPageId += 254;
        }
        ASSERT_EQ(tables->segments[i], expectPageId);
    }

    for (unsigned int segment : tables->segments) {
        auto *segHead = (SegmentHead *)GetExtentAddr(space->getNvmAddrByPageId(segment));
        for (unsigned int & page : segHead->pages) {
            page = space->allocNewExtent(EXT_SIZE_2M, segment);
        }
    }

    /* delete all segments */
    uint32 old_hwm = space->getUsedPageCount();
    for (unsigned int & segment : tables->segments) {
        space->freeSegment(&segment);
    }

    /* ensure each allocated segment is unique */
    std::set<uint32> segSet;

    /* re-alloc segments */
    for (unsigned int & i : tables->segments) {
        i = space->allocNewExtent(EXT_SIZE_2M);
        ASSERT_EQ(segSet.count(i), 0);
        segSet.insert(i);

        auto *segHead = (SegmentHead *)GetExtentAddr(space->getNvmAddrByPageId(i));
        for (unsigned int & j : segHead->pages) {
            j = space->allocNewExtent(EXT_SIZE_2M, i);
            /* ensure all extents are re-used */
            ASSERT_LE(j, old_hwm);

            ASSERT_EQ(segSet.count(j), 0);
            segSet.insert(j);
        }
    }

    ASSERT_EQ(space->getUsedPageCount(), old_hwm);
}

TEST_F(TableSpaceTest, TestFreeExtent) {
    space->create();
    auto *tables = (FakeTables *)space->getTableMetadataPage();
    auto& segment = tables->segments[0];
    segment = space->allocNewExtent(EXT_SIZE_2M);
    // init finished
    uint32 extent[10];
    std::set<uint32> segSet;
    // only one extent
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment);
    segSet.insert(extent[0]);
    space->freeExtent(&extent[0]);
    // two extents
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment);
    CHECK(segSet.count(extent[0]));
    extent[1] = space->allocNewExtent(EXT_SIZE_2M, segment);
    segSet.insert(extent[1]);
    space->freeExtent(&extent[0]);
    space->freeExtent(&extent[1]);
    // three extents
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment);
    CHECK(segSet.count(extent[0]));
    extent[1] = space->allocNewExtent(EXT_SIZE_2M, segment);
    CHECK(segSet.count(extent[1]));
    extent[2] = space->allocNewExtent(EXT_SIZE_2M, segment);
    segSet.insert(extent[2]);
    space->freeExtent(&extent[0]);
    space->freeExtent(&extent[1]);
    space->freeExtent(&extent[2]);
}

TEST_F(TableSpaceTest, TestFreeExtentMulti) {
    space->create();
    auto *tables = (FakeTables *)space->getTableMetadataPage();
    auto& segment = tables->segments[0];
    segment = space->allocNewExtent(EXT_SIZE_2M);
    // init finished
    uint32 extent[10];
    std::set<uint32> segSet;
    // only one extent
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment, 1);
    segSet.insert(extent[0]);
    space->freeExtent(&extent[0]);
    // two extents
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment, 0);
    CHECK(segSet.count(extent[0]) == 0);
    segSet.insert(extent[0]);
    extent[1] = space->allocNewExtent(EXT_SIZE_2M, segment, 1);
    CHECK(segSet.count(extent[1]));
    space->freeExtent(&extent[0]);
    space->freeExtent(&extent[1]);
    // three extents
    extent[0] = space->allocNewExtent(EXT_SIZE_2M, segment, 0);
    CHECK(segSet.count(extent[0]));
    extent[1] = space->allocNewExtent(EXT_SIZE_2M, segment, 1);
    CHECK(segSet.count(extent[1]));
    extent[2] = space->allocNewExtent(EXT_SIZE_2M, segment, 0);
    segSet.insert(extent[2]);
    extent[3] = space->allocNewExtent(EXT_SIZE_2M, segment, 1);
    segSet.insert(extent[3]);
    space->freeExtent(&extent[0]);
    space->freeExtent(&extent[1]);
    space->freeExtent(&extent[2]);
    space->freeExtent(&extent[3]);
}
