#ifndef NVMDB_UTILS_H
#define NVMDB_UTILS_H

#include "securec.h"

#define BITMAP_BYTE_IX(x) ((x) >> 3)
#define BITMAP_GETLEN(x) (BITMAP_BYTE_IX(x) + 1)
#define BITMAP_SET(b, x) (b[BITMAP_BYTE_IX(x)] |= (1 << ((x)&0x07)))
#define BITMAP_CLEAR(b, x) (b[BITMAP_BYTE_IX(x)] &= ~(1 << ((x)&0x07)))
#define BITMAP_GET(b, x) (b[BITMAP_BYTE_IX(x)] & (1 << ((x)&0x07)))

#ifndef likely
#define likely(x) __builtin_expect((x) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x) != 0, 0)
#endif

inline void SecureRetCheck(errno_t ret) {
    if (unlikely(ret != EOK)) {
        abort();
    }
}

namespace NVMDB {

template <typename T>
constexpr T CompileValue(T value __attribute__((unused)), T debugValue __attribute__((unused))) {
#ifdef NDEBUG
    return value;
#else
    return debugValue;
#endif
}

#define CM_ALIGN_ANY(size, align) (((size) + (align)-1) / (align) * (align))

}  // namespace NVMDB

#endif // NVMDB_UTILS_H