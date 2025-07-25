#pragma once

#include <algorithm>
#include <cstdint>

namespace NVMDB {
namespace YCSB {
template <uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325L, uint64_t kFNVPrime64 = 1099511628211L>
inline uint64_t FNVHash64(uint64_t val)
{
    auto hash = kFNVOffsetBasis64;
    for (int i = 0; i < 8; i++) {
        auto octet = val & 0x00ff;
        val = val >> 8;

        hash = hash ^ octet;
        hash = hash * kFNVPrime64;
    }
    // to reach the same distribution with the java version
    // return Math.abs(hashVal);
    return std::abs((long)hash);
}
}  // namespace YCSB
}  // namespace NVMDB
