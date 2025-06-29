#include "common/nvm_global_bitmap.h"
#include "glog/logging.h"

namespace NVMDB {

GlobalBitMap::GlobalBitMap(size_t size) : m_size(size) {
    DCHECK(m_size >= BITMAP_UNIT_SIZE);
    m_map.resize(m_size / BITMAP_UNIT_SIZE, 0);
}

GlobalBitMap::~GlobalBitMap() = default;

/* Find first zero in the data and tas it, return the position [0, 63] or 64 meaning no zero */
uint32 GlobalBitMap::FFZAndSet(uint64 *data) {
    uint64 value = *data;
    while (true) {
        value = ~value;
        /*
         * ctzl 返回右起第一个1，右边的0的个数。
         *    value = .... 1110 1011
         *   ~value = .... 0001 0100
         *   ctzl() = 2, 所以 bit = 2 的是可以用的。
         */
        uint32 fftz = 64;
        if (value != 0) {
            fftz = __builtin_ctzl(value);
        }
        if (fftz != BITMAP_UNIT_SIZE) {
            DCHECK(fftz >= 0 && fftz < BITMAP_UNIT_SIZE);
            uint64 mask = 1LLU << fftz;
            uint64 oldv = __sync_fetch_and_or(data, mask);
            if (oldv & mask) {
                /* 如果已经被其他人占用了，重试 */
                value = oldv;
                continue;
            }
        }
        return fftz;
    }
}

void GlobalBitMap::UpdateHint(uint32 arroff, uint32 bit) {
    uint32 oldStart = m_startHint.load();
    while (oldStart < arroff) {
        if (m_startHint.compare_exchange_weak(oldStart, arroff)) {
            break;
        }
    }
    uint32 oldHbit = m_highestBit.load();
    while (oldHbit < bit) {
        if (m_highestBit.compare_exchange_weak(oldHbit, bit)) {
            break;
        }
    }
}

uint32 GlobalBitMap::SyncAcquire() {
restart:
    uint32 start = m_startHint;
    for (uint32 aryoff = start; aryoff < m_size / BITMAP_UNIT_SIZE; aryoff++) {
        uint32 ffz = FFZAndSet(&m_map[aryoff]);
        if (ffz == BITMAP_UNIT_SIZE) {
            continue;
        }

        uint32 bit = aryoff * BITMAP_UNIT_SIZE + ffz;
        UpdateHint(aryoff, bit);

        return bit;
    }

    /* can't find any unused bit with hinted start off, restart from beginning. */
    if (start != 0) {
        m_startHint = 0;
        goto restart;
    }

    CHECK(false) << "cannot reach here!";
}

void GlobalBitMap::SyncRelease(uint32 bit) {
    uint32 aryoff = AryOffset(bit);
    uint64 mask = 1LLU << BitOffset(bit);

    DCHECK(m_map[aryoff] & mask);
    uint64 oldv = __sync_fetch_and_and(&m_map[aryoff], ~mask);
    DCHECK(oldv & mask);

    if (aryoff < m_startHint) {
        m_startHint = aryoff;
    }
}

}  // namespace NVMDB
