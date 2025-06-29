#pragma once

#include <cstdint>

namespace NVMDB {

// 所有LogicFile统一编址，其中：
// 16位为nodeId（其中只有低4位被设置，因为最多扩展16台机器）
// 16位为page中的offset（最高支持64k的NVM_PAGE_SIZE）
// 32位为一个node中的pageId，与offset可以定位到字节
// 使用方法：
// 通过nodeId找到LogicFile
// 通过pageId+offset找到File内物理的Segment内的字节地址
class UnifiedNVMAddr {
private:
    uint64_t m_data{};

    static constexpr uint64_t PAGE_ID_MASK = 0xFFFFFFFF;
    static constexpr uint64_t OFFSET_MASK = 0xFFFF;
    static constexpr uint64_t NODE_ID_MASK = 0xFFFF;

    static constexpr uint64_t OFFSET_SHIFT = 32;
    static constexpr uint64_t NODE_ID_SHIFT = 48;

public:
    static UnifiedNVMAddr* CastFromUint64(uint64_t* value) { return reinterpret_cast<UnifiedNVMAddr*>(value); }

    static uint64_t* CastToUint64(UnifiedNVMAddr* pageInfo) { return &pageInfo->m_data; }


    static void Print(const UnifiedNVMAddr& pageInfo) {
        std::cout << "UnifiedNVMAddr: " << pageInfo.getPageId()
                  << ", Offset: " << pageInfo.getOffset()
                  << ", NodeId: " << pageInfo.getNodeId() << std::endl;
    }

public:
    UnifiedNVMAddr() = default;

    UnifiedNVMAddr(uint32_t pageId, uint16_t offset, uint32_t nodeId) {
        setPageId(pageId);
        setOffset(offset);
        setNodeId(nodeId);
    }

    void setPageId(uint32_t pageId) {  m_data = (m_data & ~(PAGE_ID_MASK)) | (pageId & PAGE_ID_MASK); }

    [[nodiscard]] uint32_t getPageId() const { return static_cast<uint32_t>(m_data & PAGE_ID_MASK); }

    void setOffset(uint16_t offset) {
        m_data = (m_data & ~(OFFSET_MASK << OFFSET_SHIFT)) | ((static_cast<uint64_t>(offset) & OFFSET_MASK) << OFFSET_SHIFT);
    }

    [[nodiscard]] uint16_t getOffset() const { return static_cast<uint16_t>((m_data >> OFFSET_SHIFT) & OFFSET_MASK); }

    void setNodeId(uint32_t nodeId) {
        m_data = (m_data & ~(NODE_ID_MASK << NODE_ID_SHIFT)) | ((static_cast<uint64_t>(nodeId) & NODE_ID_MASK) << NODE_ID_SHIFT);
    }

    [[nodiscard]] uint32_t getNodeId() const { return static_cast<uint32_t>((m_data >> NODE_ID_SHIFT) & NODE_ID_MASK); }

};

}