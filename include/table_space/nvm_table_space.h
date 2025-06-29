#ifndef NVMDB_TABLESPACE_H
#define NVMDB_TABLESPACE_H

#include "table_space/nvm_logic_file.h"
#include "common/thread_pool_light.h"
#include <mutex>

namespace NVMDB {

namespace {
struct NVMPageHeader {
    // 抽象出一个page double list 的概念。比较多场景下，需要把page连起来，比如同一个 segment 中的page；属于同一个
    // TxSlot 的 undo page。注意这个 list 存的是页号，为了节约空间。
    struct PageDListNode {
        uint32 prev;
        uint32 next;
    };
    // 使用PageSegmentDListOffset间接访问
    PageDListNode m_pageList;
    uint8 m_pageSize;
    uint32 m_pageId;
};
}

enum ExtentSizeType {
    EXT_SIZE_8K = 0,
    EXT_SIZE_2M = 1,
};

static inline uint32 GetPageCountPerExtent(int extType) {
    // segment大小位 EXT_SIZE_8K 时, 仅能保存1个page (一个Page大小8k)
    // segment大小位 EXT_SIZE_2M 时, 能保存256个page
    static constexpr uint32 g_pageCountPerSegment[] = {1, 256};
    return g_pageCountPerSegment[extType];
}

// Tablespace 中一个 HEAP_EXTENT 的 可用空间
static inline uint32 GetExtentSize(int extType) {
//    return GetPageCountPerExtent(extType) * NVM_PAGE_SIZE - sizeof(NVMPageHeader);
    return GetPageCountPerExtent(extType) * NVM_PAGE_SIZE - 256;
}

//static inline char* GetExtentAddr(void* pageId) { return ((char *)(pageId)) + sizeof(NVMPageHeader); }

static inline char* GetExtentAddr(void* pageId) { return ((char *)(pageId)) + 256; }

struct TableSegMetaData {
    // oid为表的id, fdw转换用
    uint32 m_oid;
    // 这实际上存的是global page id
    uint32 m_seg;
};

// 第一个page为tablespace元数据页面，第二个页面为应用根页面
// 每张表一组文件作为heap, 相互独立
class TableSpace {
public:
    // 1 GB for release， 10 MB for debug
    static const size_t HEAP_SPACE_SEGMENT_LEN = CompileValue(8 * 1024ULL * 1024 * 1024, 10 * 1024ULL * 1024);
    static const size_t HEAP_SPACE_MAX_SEGMENT_NUM = 2 * 1024; /* 实际中最多16TB */

    explicit TableSpace(const std::shared_ptr<DirectoryConfig>& dirConfig, bool isInit=false)
        : m_logicFile(dirConfig,
                      "heap",
                      HEAP_SPACE_SEGMENT_LEN,
                      HEAP_SPACE_MAX_SEGMENT_NUM)
          , m_dirConfig(dirConfig) {
        auto threadPoolLight = std::make_unique<util::thread_pool_light>(m_dirConfig->size() * 7);
        moodycamel::LightweightSemaphore semaphore(0, 0);
        auto threadFunc = [&](int idxStart, int idxEnd) {
            for (int i = idxStart; i < idxEnd; i++) {
                m_logicFile.mMapFile(i, isInit);
            }
            semaphore.signal();
        };
        threadPoolLight->push_loop(0, 32, threadFunc);
        for (auto i=threadPoolLight->get_thread_count(); i>0; i-=(int)semaphore.waitMany((ssize_t)i));
        threadPoolLight.reset(nullptr);
        // 第一个page存储SpaceMetaData相关信息
        m_spaceMetadata = static_cast<SpaceMetaData *>(m_logicFile.getNvmAddrByPageId(0));
        // 防止SpaceMetaData溢出
        CHECK(m_dirConfig->size() * sizeof(SpaceMetaData) <= NVM_PAGE_SIZE);
        // TableMetaData从第二个page开始
        m_tableMetadata = static_cast<TableMetaData *>(m_logicFile.getNvmAddrByPageId(1));
    }

    [[nodiscard]] inline const auto& getDirConfig() const { return m_dirConfig; }

    [[nodiscard]] inline const auto& getLogicFile() const { return m_logicFile; }

    // 重新初始化tableSpace
    void create() {
        // first two page of space 0 kept as space meta and table meta respectively.
        for (int i = 0; i < m_dirConfig->size(); i++) {
            m_spaceMetadata[i].m_usedPageCount = 0;
            // init i th m_spaceMetadata
            getFPLRootPageIdRef(i, EXT_SIZE_8K) = NVMInvalidPageId;
            getFPLRootPageIdRef(i, EXT_SIZE_2M) = NVMInvalidPageId;
        }
        m_spaceMetadata[0].m_usedPageCount = 2; // HIGH_WATER_MARK
    }

    // 挂载所有heap相关的文件
    void mount() {
        m_logicFile.mount();
        // 遍历挂载目录
        for (uint32 i = 0; i < m_dirConfig->size(); i++) {
            // 获取目录对应的 hwm
            auto hwm = m_spaceMetadata[i].m_usedPageCount;
            // mount segments that are smaller than hwm (所以 m_spaceMetadata[0].m_usedPageCount 初始值为2)
            auto pagesPerSegment = m_logicFile.getPagesPerSegment();
            for (uint32 j = 0; j * pagesPerSegment <= hwm; j++) {
                auto pageId = getGlobalPageId(j * pagesPerSegment, i);
                m_logicFile.extend(pageId); // e.g., space id =0, mount heap.0, space id=1, mount heap.1
            }
        }
    }

    void unmount() { m_logicFile.unmount(); }

private:
    struct FreePageLists {
        uint32 m_root;
    };
    // 系统每个目录有一个SpaceMetaData
    // 每个 SpaceMetaData 有两个 m_freePageLists 链表
    struct SpaceMetaData {
        // high watermark, the first un-allocated page number,
        // it's logic page number from begin of this space.
        // free page lists store the free physical pages' num.
        // 当前分区 (目录) 中已经使用的页面数量
        uint32 m_usedPageCount;
        FreePageLists m_freePageLists[2];
    };
    // 存在第一个 page 中, TableSpace结构体存指向它的虚拟地址的指针
    // nvm ptr in heap.0 (当有两个目录时候, 其大小为2)
    SpaceMetaData *m_spaceMetadata = nullptr;

    struct TableMetaData {
        uint32 m_tableNum;
        TableSegMetaData m_segHeads[0];
    };
    // nvm ptr + blksize in heap.0
    TableMetaData *m_tableMetadata = nullptr;
    std::mutex m_tableMetadataMutex;
    std::mutex m_usedPageMutex[16]; // 最多支持16个目录分区

    std::shared_ptr<DirectoryConfig> m_dirConfig;

    LogicFile m_logicFile;

protected:
    // 可以通过 spaceId (第i个目录) 和 sizeType 唯一确定一个FreePageLists
    // 其中 sizeType 为定位符号 EXT_SIZE_8K=0 和 EXT_SIZE_2M=1
    inline uint32& getFPLRootPageIdRef(uint32 spaceId, ExtentSizeType sizeType) const {
        return m_spaceMetadata[spaceId].m_freePageLists[sizeType].m_root;
    }

    static inline bool isFPLEmpty(uint32 headPageId) { return headPageId == NVMInvalidPageId; }

    NVMPageHeader *getFPLNode(uint32 pageId) const {
        return reinterpret_cast<NVMPageHeader *>(getNvmAddrByPageId(pageId));
    }

    uint32 popFPLNode(uint32& headPageId) {
        // freePageList 链表为空
        if (isFPLEmpty(headPageId)) {
            return NVMInvalidPageId;
        }
        auto& headNode = getFPLNode(headPageId)->m_pageList;

        // freePageList 链表大小为1, 把root pop出来并返回
        if (headNode.prev == headPageId) {
            DCHECK(headNode.next == headPageId);
            auto ret = headPageId;
            headPageId = NVMInvalidPageId;
            return ret;
        }

        // pop freePageList 链表中最后一个元素, 并返回
        auto& tailNode = getFPLNode(headNode.prev)->m_pageList;
        auto& tailPrev = getFPLNode(tailNode.prev)->m_pageList;
        auto ret = headNode.prev;
        headNode.prev = tailNode.prev;
        tailPrev.next = headPageId;
        return ret;
    }

    void pushFPLNode(uint32 extentPageId, uint32& headPageId) {
        if (isFPLEmpty(headPageId)) {
            // freePageList 链表为空
            headPageId = extentPageId;
            // 初始化双向链表, rootPageId 作为 head
            auto& headNode = getFPLNode(headPageId)->m_pageList;
            headNode.next = headPageId;
            headNode.prev = headPageId;
            return;
        }
        // 将 rootPageId push 到 rootPageId 后, 形成双向链表
        auto& headNode = getFPLNode(headPageId)->m_pageList;
        auto& tailNode = getFPLNode(headNode.prev)->m_pageList;
        auto& newNode = getFPLNode(extentPageId)->m_pageList;
        newNode.prev = headNode.prev;
        newNode.next = headPageId;
        tailNode.next = extentPageId;
        headNode.prev = extentPageId;
    }

    // 根据目录本地的pageId和挂载目录的Id推导出全局PageId
    inline uint32 getGlobalPageId(const uint32 &localPageId, const uint32 &spaceId) const {
        auto pagesPerSegment = m_logicFile.getPagesPerSegment();
        // 根据localPageId推导出localSegmentId
        auto localSegmentId = localPageId / pagesPerSegment;
        auto globalSegmentId = spaceId + localSegmentId * m_dirConfig->size();
        auto globalPageId = globalSegmentId * pagesPerSegment + localPageId % pagesPerSegment;
        return globalPageId;
    }

    // space num of global physical page num.
    inline uint32 spaceIdFromGlobalPageId(const uint32& pageId) {
        return (pageId / m_logicFile.getPagesPerSegment()) % m_dirConfig->size();
    }

public:
    // 不能在运行时调用，仅供参考，测试时使用
    [[nodiscard]] inline auto getUsedPageCount(uint32 spaceId=0) const { return m_spaceMetadata[spaceId].m_usedPageCount; }

    // 返回 root page address in NVM
    [[nodiscard]] inline void *getTableMetadataPage() const { return reinterpret_cast<void *>(m_tableMetadata); }

    [[nodiscard]] inline char* getNvmAddrByPageId(uint32 globalPageId) const {
        return static_cast<char *>(m_logicFile.getNvmAddrByPageId(globalPageId));
    }

    // 为一个Table segment分配一组 pages, 返回
    // 为一个segment分配一个extent，rootPageId 为segment head，一个segment中所有page会被串起来.
    // 如果 rootPageId 为invalid值，则说明分配一个新的segment并返回。
    uint32 allocNewExtent(ExtentSizeType sizeType, uint32 rootPageId = NVMInvalidPageId, uint32 spaceIdHint=0) {
        const auto spaceId = spaceIdHint % m_dirConfig->size();
        m_usedPageMutex[spaceId].lock();
        auto& extPageId = getFPLRootPageIdRef(spaceId, sizeType);
        // pop 指定 FreePageLists 的最后一个page, 并返回 page id
        auto newPageId = popFPLNode(extPageId);
        if (isFPLEmpty(newPageId)) {
            // 当前 FreePageLists 为空, 分配一组新的 extent
            const auto pagePerSegment = m_logicFile.getPagesPerSegment();
            // 当前 segment 中已用多少 page, 同时也是下一个 page 的 local page id
            auto& totalUsedPages = m_spaceMetadata[spaceId].m_usedPageCount;
            // 1. 计算当前segment中剩余page的数量
            const auto restPages = pagePerSegment - totalUsedPages % pagePerSegment;
            // EXT_SIZE_2M需要256个page, 而EXT_SIZE_8k只需要1个page
            // 当前 segment 中的 page 数量不足以提供给 FreePageLists 链表
            const auto pagePerSeg = GetPageCountPerExtent(sizeType);
            if (restPages < pagePerSeg) {
                // The new extent of EXT_SIZE_2M should be in one segment.
                // 如果是EXT_SIZE_2M, 将这些未分配的 pages 插入对应分区的 EXT_SIZE_8K 链表中
                for (int i = 0; i < restPages; i++) {   // EXT_SIZE_8K 不会进入这个循环
                    auto tmpPageId = getGlobalPageId(totalUsedPages + i, spaceId);
                    // 获取 EXT_SIZE_8K 的 freePageList 链表
                    auto& ext8kHeadPageId = getFPLRootPageIdRef(spaceId, EXT_SIZE_8K);
                    pushFPLNode(tmpPageId, ext8kHeadPageId);
                }
                // 标记这些 page 已经被分配
                totalUsedPages += restPages;
                CHECK(totalUsedPages % pagePerSegment == 0) << "Start from a new segment failed!";
            }
            // 根据分区的 page 水位线得到分配前的 全局Page ID (因为之前pageId == NVMInvalidPageId)
            newPageId = getGlobalPageId(totalUsedPages, spaceId);
            DCHECK(!isFPLEmpty(newPageId));
            // 计算并挂载对应分配后的 segment id
            uint32 nextSegmentId = getGlobalPageId(totalUsedPages + pagePerSeg, spaceId) / pagePerSegment;
            m_logicFile.mMapFile(nextSegmentId, true);
            // 最后更新分配这些 pages 后的水位线
            totalUsedPages += GetPageCountPerExtent(sizeType);
        }
        m_usedPageMutex[spaceId].unlock();
        // 初始化这个 page id 对应的 page
        auto* pageNode = getFPLNode(newPageId);
        pageNode->m_pageId = newPageId;
        pageNode->m_pageSize = sizeType;
        // 将剩余部分初始化为 0 (注意, 例如一起分配了 256 个 pages, 仅第一个 page 有 header, 其他 pages 均为0)
        char *content = GetExtentAddr(pageNode);
        int ret = memset_s(content, GetExtentSize(sizeType), 0, GetExtentSize(sizeType));
        SecureRetCheck(ret);
        return newPageId;
    }

    uint32 fastAllocNewExtent(ExtentSizeType sizeType, uint32 rootPageId, uint32 spaceIdHint) {
        const auto spaceId = spaceIdHint % m_dirConfig->size();
        // 当前 FreePageLists 为空, 分配一组新的 extent
        const auto pagePerSegment = m_logicFile.getPagesPerSegment();
        const auto pagePerSeg = GetPageCountPerExtent(sizeType);
        m_usedPageMutex[spaceId].lock();
        auto totalUsedPages = m_spaceMetadata[spaceId].m_usedPageCount;
        const auto restPages = pagePerSegment - totalUsedPages % pagePerSegment;
        if (restPages < pagePerSeg) {
            // 这样做会存在部分page未完全利用的情况
            totalUsedPages += restPages;
        }
        auto newPageId = getGlobalPageId(totalUsedPages, spaceId);
        uint32 nextSegmentId = getGlobalPageId(totalUsedPages + pagePerSeg, spaceId) / pagePerSegment;
        m_logicFile.mMapFile(nextSegmentId, true);
        totalUsedPages += pagePerSeg;
        m_spaceMetadata[spaceId].m_usedPageCount = totalUsedPages; // 写回到NVM
        m_usedPageMutex[spaceId].unlock();

        // 初始化这个 page id 对应的 page
        auto* pageNode = getFPLNode(newPageId);
        pageNode->m_pageId = newPageId;
        pageNode->m_pageSize = sizeType;
        return newPageId;
    }

    // 会把 *pageId 对应的page回收，并且*pageId 置为 NULL。一般不会掉这个函数，直接调用freeSegment
    void freeExtent(uint32* pageId) {
        std::lock_guard<std::mutex> guard(m_tableMetadataMutex);
        auto* pageNode = getFPLNode(*pageId);
        DCHECK(*pageId == pageNode->m_pageId);
        // 找到 page 对应的 free list 链表
        auto spaceId = spaceIdFromGlobalPageId(*pageId);
        std::lock_guard<std::mutex> lg (m_usedPageMutex[spaceId]);
        auto sizeType = static_cast<ExtentSizeType>(pageNode->m_pageSize);
        auto& extHeadPageId = getFPLRootPageIdRef(spaceId, sizeType);
        // 将 page extent 插入链表
        pushFPLNode(*pageId, extHeadPageId);
        *pageId = NVMInvalidPageId;
    }

    // rootPageId 对应一个segment 的 root，回收整个 segment，并且置 *rootPageId 为null
    void freeSegment(uint32* rootPageId) {
        CHECK(false);
    }

public:
    /* 将存储oid->表地址的映射写入表空间的1号page */
    void CreateTable(uint32 pgTableOID, uint32 tableSegHead) {
        std::lock_guard<std::mutex> lockGuard(m_tableMetadataMutex);
        TableSegMetaData segAddr {
            .m_oid = pgTableOID,
            .m_seg = tableSegHead,
        };
        // 根据第i个table偏移保存对应metadata, 持久化table
        errno_t ret = memcpy_s(m_tableMetadata->m_segHeads + m_tableMetadata->m_tableNum, sizeof(segAddr),
                               &segAddr, sizeof(segAddr));
        SecureRetCheck(ret);
        m_tableMetadata->m_tableNum++;
    }

    // 根据oid在表空间中寻找表地址, 返回 Table的 segment head
    uint32 SearchTable(uint32 oid) {
        std::lock_guard<std::mutex> lockGuard(m_tableMetadataMutex);
        for (int i = 0; i < m_tableMetadata->m_tableNum; ++i) {
            TableSegMetaData *segAddr = m_tableMetadata->m_segHeads + i;
            if (segAddr->m_oid == oid) {
                return segAddr->m_seg;
            }
        }
        return 0;
    }

    /* 根据表地址将drop的表数据从表空间中删除 */
    void DropTable(uint32 oid) {
        std::lock_guard<std::mutex> lockGuard(m_tableMetadataMutex);
        int i = 0;
        TableSegMetaData *segAddr = nullptr;
        for (; i < m_tableMetadata->m_tableNum; ++i) {
            segAddr = m_tableMetadata->m_segHeads + i;
            if (segAddr->m_oid == oid) {
                break;
            }
        }

        if (segAddr == nullptr) {
            return;
        }

        size_t count = m_tableMetadata->m_tableNum - i - 1;
        if (count != 0) {
            size_t memLen = count * sizeof(TableSegMetaData);
            errno_t ret = memmove_s(m_tableMetadata->m_segHeads + i, memLen, m_tableMetadata->m_segHeads + i + 1, memLen);
            SecureRetCheck(ret);
        }
        m_tableMetadata->m_tableNum -= 1;
    }
};

}  // namespace NVMDB

#endif  // NVMDB_TABLESPACE_H
