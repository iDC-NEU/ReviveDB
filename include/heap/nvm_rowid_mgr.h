#ifndef NVMDB_ROWID_MGR_H
#define NVMDB_ROWID_MGR_H

#include "table_space/nvm_table_space.h"
#include "heap/nvm_tuple.h"
#include "common/numa.h"

namespace NVMDB {

static const ExtentSizeType HEAP_EXTENT_SIZE = EXT_SIZE_2M;

class BestTupleLenCalculator {
public:
    BestTupleLenCalculator() {
        for (int i=1; i<2048; i++) {
            calculatePercentage(i);
        }
    }

    // 根据数据大小获取最佳对齐大小
    static BestTupleLenCalculator g_btc;

    // 根据数据大小获取最佳对齐大小
    size_t getBestAlignment(size_t tupleLen, size_t maxWaste=63) {
        double minCrossBlockTimes = crossBlockTimes[tupleLen];
        size_t alignTupleLen = tupleLen;
        for (size_t i=tupleLen+1; i<=tupleLen+maxWaste; i++) {
            if (minCrossBlockTimes > crossBlockTimes[i]) {
                minCrossBlockTimes = crossBlockTimes[i];
                alignTupleLen = i;
            }
        }
        LOG(INFO) << tupleLen << " " << crossBlockTimes[tupleLen] << " VS. " << alignTupleLen << " " << minCrossBlockTimes;
        return alignTupleLen;
    }

protected:
    static unsigned int gcd(unsigned int a, unsigned int b) {
        while (b != 0) {
            unsigned int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }

    // 计算最大公倍数的函数
    static unsigned int lcm(unsigned int a, unsigned int b) {
        // 防止溢出的处理：先除后乘
        return (a / gcd(a, b)) * b;
    }

    void calculatePercentage(size_t tupleLength) {
        auto lcmTP = lcm(tupleLength, 256);
        size_t numCross = 0;
        for (size_t i=0; i<lcmTP; i+=tupleLength) {
            auto startBlock = i/256;
            auto endBlock = (i+tupleLength-1) / 256;
            numCross += endBlock - startBlock;
        }
        auto numTuple = static_cast<double>(lcmTP) / static_cast<double>(tupleLength);
        crossBlockTimes[tupleLength] = (double)numCross / numTuple;
    }

private:
    // 每个tuple跨的次数
    double crossBlockTimes[2048] = {};
};

class RowIDMgr {
public:
    // 为 Table 提供进一步抽象 rowId 为一个 Table 中的行 ID
    // segHead: Table 的入口, 为 Page id 类型, 存储所有 leaf extent 对应的 page ids
    RowIDMgr(TableSpace *tableSpace, uint32 segHead, uint32 tupleLen)
        : m_tableSpace(tableSpace),
          m_segHead(segHead),
          // heap中每行元组的大小 = NVM的header + 元组真实的数据
          m_tupleLen(NVMTupleHeadSize + tupleLen) {
        // 一个table segment的总逻辑空间 - header 除以每个tuple长度
        m_tuplesPerExtent = GetExtentSize(HEAP_EXTENT_SIZE) / m_tupleLen;
    }

    // 根据 rowid 读取NVM中Table对应的记录
    char *getNVMTupleByRowId(RowId rowId, bool append) {
        // pageId, page_offset
        const uint32 leafExtentId = rowId / m_tuplesPerExtent;
        const uint32 leafPageOffset = rowId % m_tuplesPerExtent;

        uint32 *extentIds = GetLeafPageExtentIds();
        /* 1. check leaf page existing. If not, try to allocate a new page */
        if (!NVMPageIdIsValid(extentIds[leafExtentId])) {    // pageId
            if (!append) {  // 只读请求, 不需要创建 leafPage
                return nullptr;
            }
            // CHECK(leafExtentId % 4 == NumaBinding::getThreadLocalGroupId());
            UpdateMaxPageId(leafExtentId);
            tryAllocNewPage(leafExtentId);
        }

        uint32 pageId = extentIds[leafExtentId];
        DCHECK(NVMPageIdIsValid(pageId));
        char *leafPage = m_tableSpace->getNvmAddrByPageId(pageId);
        char *leafData = (char *)GetExtentAddr(leafPage);
        char *tuple = leafData + leafPageOffset * m_tupleLen;

        return tuple;
    }

    // upper bound RowId in highest allocated range
    inline RowId getUpperRowId() const { return (GetMaxPageId() + 1) * m_tuplesPerExtent; }

    // 每个heap page extent能存储的tuple数量
    [[nodiscard]] inline uint32 getTuplesPerExtent() const { return m_tuplesPerExtent; };

protected:
    // Table Segment Header 存储 MaxPageNum 和 PageMap
    uint32 *GetLeafPageExtentIds() {
        char *rootPage = m_tableSpace->getNvmAddrByPageId(m_segHead);
        /* NVMPageHeader + MaxPageNum + Page Maps */
        return (uint32 *)GetExtentAddr(rootPage) + 1;
    }

    // pageId: 当前 Table 分配的 extent id (不一定最大)
    inline void UpdateMaxPageId(uint32 pageId) {
        char *rootPage = m_tableSpace->getNvmAddrByPageId(m_segHead);
        auto *maxPageId = reinterpret_cast<std::atomic<uint32_t>*>(GetExtentAddr(rootPage));

        uint32_t oldVal = maxPageId->load();
        while (oldVal < pageId) {
            if (maxPageId->compare_exchange_strong(oldVal, pageId)) {
                break;
            }
        }
    }

    inline uint32 GetMaxPageId() const {
        char *rootPage = m_tableSpace->getNvmAddrByPageId(m_segHead);
        return *(uint32 *)GetExtentAddr(rootPage);
    }

    // 基于 m_segHead 分配新的 extent, 并存储在 m_segHead 中
    void tryAllocNewPage(uint32 leafExtentId) {
        auto *extentIds = GetLeafPageExtentIds();
        if (NVMPageIdIsValid(extentIds[leafExtentId])) {
            return; // optimistic retry
        }
        auto pageId = m_tableSpace->fastAllocNewExtent(HEAP_EXTENT_SIZE, m_segHead, NumaBinding::getThreadLocalGroupId());
        std::lock_guard<std::mutex> lock_guard(m_tableSpaceMutex);
        while (NVMPageIdIsValid(extentIds[leafExtentId])) {
            auto spaceCount = m_tableSpace->getDirConfig()->getDirPaths().size();
            leafExtentId += spaceCount;
        }
        // use leafExtentId as a hint for space idx
        // leaf page idx is logic page number. allocate physical page from space idx.
        // CHECK(leafExtentId % 4 == NumaBinding::getThreadLocalGroupId());
        extentIds[leafExtentId] = pageId;
    }

private:
    std::mutex m_tableSpaceMutex;
    TableSpace *m_tableSpace;   // Table space, 保存真正的文件
    const uint32 m_segHead;   // 当前 Table 的 page id 入口
    const uint32 m_tupleLen;  // 实际上每行占用的NVM的长度, 每行定长
    uint32 m_tuplesPerExtent;  // 每个Table segment能保存的数组数量
};

}  // namespace NVMDB

#endif  // NVMDB_ROWID_MGR_H