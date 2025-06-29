#ifndef NVMDB_GLOBAL_BITMAP_H
#define NVMDB_GLOBAL_BITMAP_H

#include "common/nvm_types.h"
#include "glog/logging.h"
#include <atomic>
#include <vector>
#include <cstddef>

namespace NVMDB {

// 每个Table一组GlobalBitMap, 其中每个分区对应一个GlobalBitMap
class GlobalBitMap {
public:
    constexpr static uint32 BITMAP_UNIT_SIZE = 64;

    explicit GlobalBitMap(size_t _size);

    ~GlobalBitMap();

    uint32 SyncAcquire();

    void SyncRelease(uint32 bit);

    inline uint32 get_highest_bit() const {
        DCHECK(m_highestBit >= 0 && m_highestBit < m_size);
        return m_highestBit;
    }
private:
    // 一个 dir 中, 一张 table 最多对应存储的 page 数
    std::vector<uint64> m_map;
    size_t m_size{0};
    std::atomic<uint32> m_startHint{};
    std::atomic<uint32> m_highestBit{};

    inline uint32 AryOffset(uint32 bit) const {
        DCHECK(bit < m_size);
        return bit / BITMAP_UNIT_SIZE;
    }

    inline uint32 BitOffset(uint32 bit) const {
        DCHECK(bit < m_size);
        return bit % BITMAP_UNIT_SIZE;
    }

    static uint32 FFZAndSet(uint64 *data);

    void UpdateHint(uint32 arroff, uint32 bit);
};

}  // namespace NVMDB

#endif  // NVMDB_GLOBAL_BITMAP_H