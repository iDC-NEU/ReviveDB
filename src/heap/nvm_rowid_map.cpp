#include "heap/nvm_rowid_map.h"
#include "heap/nvm_heap.h"
#include <unordered_map>

namespace NVMDB {

DEFINE_int64(cache_size, 16384, "the max size of lru cache");
DEFINE_int64(cache_elasticity, 64, "the elasticity of lru cache");

std::atomic<bool> g_forceWriteBackCSN {true};

bool ForceWriteBackCSN() { return g_forceWriteBackCSN.load(std::memory_order_relaxed); }

void SetForceWriteBackCSN(bool flag) { g_forceWriteBackCSN.store(flag, std::memory_order_release); }

thread_local bool RowIdMap::m_isInsertInit = false;

RowIdMapEntry *RowIdMap::GetSegment(int segId) {
    auto& segmentPtr = m_segments[segId];
    if (segmentPtr == nullptr) {
        // extend the segment
        auto nextSegment = std::make_unique<std::array<RowIdMapEntry, RowIdMapSegmentLen>>();
        size_t memSize = sizeof(RowIdMapEntry) * RowIdMapSegmentLen;
        int ret = memset_s(nextSegment->data(), memSize, 0, memSize);
        SecureRetCheck(ret);
        std::lock_guard<std::mutex> lg(m_segmentsMutex);
        for (int i=segId; i<SegmentEntryLen; i++) {
            if (m_segments[i] == nullptr) {
                m_segments[i] = std::move(nextSegment);
                break;
            }
        }
    }
    return segmentPtr->begin();
}

RowIdMapEntry *RowIdMap::GetEntry(RowId rowId, bool isRead) {
    int segId = static_cast<int>(rowId / RowIdMapSegmentLen);
    RowIdMapEntry *segment = GetSegment(segId);
    RowIdMapEntry *entry = &segment[rowId % RowIdMapSegmentLen];

    if (!entry->IsValid()) {
        char *nvmTuple = m_rowidMgr->getNVMTupleByRowId(rowId, false);
        /* not valid row on nvm. */
        if (nvmTuple == nullptr) {
            DCHECK(isRead);
            return nullptr;
        }
        entry->Lock();  // init entry if not valid
        if (!entry->IsValid()) {
            entry->Init(nvmTuple);
        }
        entry->Unlock();
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    return entry;
}

static std::unordered_map<uint32, RowIdMap *> g_globalRowidMaps;
static std::mutex g_grimMtx;
thread_local std::unordered_map<uint32, RowIdMap *> g_localRowidMaps;

RowIdMap *GetRowIdMap(uint32 segHead, uint32 rowLen) {
    if (g_localRowidMaps.find(segHead) == g_localRowidMaps.end()) {
        // 本地缓存不存在对应表的segHead (page ID), 从全局中找 RowIdMap
        std::lock_guard<std::mutex> lockGuard(g_grimMtx);
        if (g_globalRowidMaps.find(segHead) == g_globalRowidMaps.end()) {
            // 为这张 table 创建 RowidMap, row id 为 table 中的行 id, 可以通过 row id 查找对应的 nvm tuple
            g_globalRowidMaps[segHead] = new RowIdMap(g_heapSpace, segHead, rowLen);
        }
        g_localRowidMaps[segHead] = g_globalRowidMaps[segHead];
    }
    RowIdMap *result = g_localRowidMaps[segHead];
    DCHECK(result->GetRowLen() == rowLen);  // rowLen 为表中一行的长度
    return result;
}

void InitGlobalRowIdMapCache() {
    g_globalRowidMaps.clear();
}

void InitLocalRowIdMapCache() {
    g_localRowidMaps.clear();
}

void DestroyGlobalRowIdMapCache() {
    for (auto entry : g_globalRowidMaps) {
        delete entry.second;
    }
    g_globalRowidMaps.clear();
}

void DestroyLocalRowIdMapCache() {
    g_localRowidMaps.clear();
}

BestTupleLenCalculator BestTupleLenCalculator::g_btc;

}  // namespace NVMDB