#include "table_space/nvm_logic_file.h"
#include "common/nvm_cfg.h"
#include <libpmem.h>
#include <fstream>

// #define UNDO_LOG_IN_DRAM

namespace NVMDB {

bool LogicFile::mMapFile(uint32 segmentId, bool init) {
    // if is mounted, return
    if (m_segmentAddr.size() > segmentId && m_segmentAddr[segmentId] != nullptr) {
        return true;
    }
    // create file if not exist
    int flags = init ? PMEM_FILE_CREATE : 0;
    size_t actualSegmentSize;
    int isPMem;
    auto fileName = segmentFilename(segmentId);

    bool fileExists = [](const std::string& filename) {
        std::ifstream file(filename);
        return file.good();
    }(fileName);

#ifdef UNDO_LOG_IN_DRAM
    void *nvmAddr;
    if (m_spaceName[0] == 'u') {
        nvmAddr = malloc(m_segmentSize);
        CHECK(nvmAddr != nullptr);
    } else {
        nvmAddr = pmem_map_file(fileName.data(),
                                m_segmentSize,
                                flags,
                                0666,
                                &actualSegmentSize,
                                &isPMem);
        if (!isPMem || nvmAddr == nullptr || actualSegmentSize != m_segmentSize) {
            if (!init) {
                return false;
            }
            CHECK(false) << "Cannot map PMem file!";
        }
    }
#else
    void *nvmAddr = pmem_map_file(fileName.data(),
                                  m_segmentSize,
                                  flags,
                                  0666,
                                  &actualSegmentSize,
                                  &isPMem);
    // NVM挂载失败
    if (!isPMem || nvmAddr == nullptr || actualSegmentSize != m_segmentSize) {
        if (!init) {
            return false;
        }
        CHECK(false) << "Cannot map PMem file!";
    }
#endif
    if (!fileExists) {
        LOG(INFO) << "Init nvm file " << fileName;
        // pmem_memset(nvmAddr, 0, m_segmentSize, PMEM_F_MEM_NONTEMPORAL | PMEM_F_MEM_WC);
    }
    // 添加nvmAddr到m_segmentAddr
    if (m_segmentAddr.size() <= segmentId) {
        m_segmentAddr.resize(segmentId + 1);
    }
    m_segmentAddr[segmentId] = nvmAddr;
    return true;
}

void LogicFile::unMMapFile(uint32 segmentId, bool destroy) {
    if (m_segmentAddr.size() <= segmentId || m_segmentAddr[segmentId] == nullptr) {
        return;
    }
    pmem_unmap(m_segmentAddr[segmentId], m_segmentSize);
    m_segmentAddr[segmentId] = nullptr;
    if (!destroy) {
        return;
    }
    unlink(segmentFilename(segmentId).data());
}

void LogicFile::reMMapFile(uint32 segmentId) {
    if (m_segmentAddr.size() <= segmentId || m_segmentAddr[segmentId] == nullptr) {
        return;
    }
    // reuse stale data
    CHECK(m_segmentAddr.size() < m_segmentAddr.capacity());
    uint32 offset = m_segmentAddr.size();
    m_segmentAddr.resize(offset + 1);
    m_segmentAddr[offset] = m_segmentAddr[segmentId];
    m_segmentAddr[segmentId] = nullptr;

#ifndef UNDO_LOG_IN_DRAM
    auto oldFileName = segmentFilename(segmentId);
    auto newFileName = segmentFilename(offset);
    CHECK(std::rename(oldFileName.data(), newFileName.data()) == 0);
    LOG(INFO) << "reLink " << oldFileName << " to " << newFileName;
#endif
}
}  // namespace NVMDB