#ifndef RLU_ARCH_H
#define RLU_ARCH_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <cstdint>
#include <pthread.h>
#include <ctime>
#include <cerrno>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif

#ifndef read_mostly
#define read_mostly __attribute__((__section__(".data..read_mostly")))
#endif

#define PAGE_SIZE 4096
#define L1_CACHE_BYTES 64
#define INT_BIT_COUNT 32
#define RDTSCP_CHIP_OFFSET 12

#define CACHE_LINE_PREFETCH_UNIT (2)
#define CACHE_DEFAULT_PADDING ((CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES) / sizeof(long))

static inline void __attribute__((__always_inline__)) smp_mb(void) {
    __asm__ __volatile__("mfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_rmb(void) {
    __asm__ __volatile__("lfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_wmb(void) {
    __asm__ __volatile__("sfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) barrier(void) {
    __asm__ __volatile__("" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_wmb_tso(void) {
    barrier();
}

#define smp_cas(__ptr, __old_val, __new_val) __sync_bool_compare_and_swap(__ptr, __old_val, __new_val)

#define smp_cas_v(__ptr, __old_val, __new_val, __fetched_val)                       \
    ({                                                                              \
        (__fetched_val) = __sync_val_compare_and_swap(__ptr, __old_val, __new_val); \
        (__fetched_val) == (__old_val);                                             \
    })

#define smp_cas16b(__ptr, __old_val1, __old_val2, __new_val1, __new_val2)                                        \
    ({                                                                                                           \
        char result;                                                                                             \
        __asm__ __volatile__("lock; cmpxchg16b %0; setz %1"                                                      \
                             : "=m"(*(__ptr)), "=a"(result)                                                      \
                             : "m"(*(__ptr)), "d"(__old_val2), "a"(__old_val1), "c"(__new_val2), "b"(__new_val1) \
                             : "memory");                                                                        \
        (int)result;                                                                                             \
    })

#undef smp_swap
#define smp_swap(__ptr, __val) __sync_lock_test_and_set(__ptr, __val)

#undef smp_atomic_load
#define smp_atomic_load(__ptr) ({ __sync_val_compare_and_swap(__ptr, __ptr, __ptr); })

#undef smsmp_atomic_store
#define smp_atomic_store(__ptr, __val) (void)smp_swap(__ptr, __val)

#undef smp_faa
#define smp_faa(__ptr, __val) __sync_fetch_and_add(__ptr, __val)

#undef smp_fas
#define smp_fas(__ptr, __val) __sync_fetch_and_sub(__ptr, __val)

#undef cpu_relax
#define cpu_relax() asm volatile("pause\n" : : : "memory")

static inline uint64_t __attribute__((__always_inline__)) read_tsc(void) {
    uint32_t a;
    uint32_t d;
    __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
    return ((uint64_t)a) | (((uint64_t)d) << INT_BIT_COUNT);
}

static inline unsigned long read_coreid_rdtscp(int *chip, int *core) {
    unsigned long a, d, c;
    __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
    *chip = (c & 0xFFF000) >> RDTSCP_CHIP_OFFSET;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << INT_BIT_COUNT);
    ;
}

static inline uint64_t __attribute__((__always_inline__)) read_tscp(void) {
    uint32_t a, d;
    __asm __volatile("rdtscp" : "=a"(a), "=d"(d));
    return ((uint64_t)a) | (((uint64_t)d) << INT_BIT_COUNT);
}

#define cache_prefetchr_high(__ptr) __builtin_prefetch((void *)__ptr, 0, 3)

#define cache_prefetchr_mid(__ptr) __builtin_prefetch((void *)__ptr, 0, 2)

#define cache_prefetchr_low(__ptr) __builtin_prefetch((void *)__ptr, 0, 0)

#define cache_prefetchw_high(__ptr) __builtin_prefetch((void *)__ptr, 1, 3)

#define cache_prefetchw_mid(__ptr) __builtin_prefetch((void *)__ptr, 1, 2)

#define cache_prefetchw_low(__ptr) __builtin_prefetch((void *)__ptr, 1, 0)
#ifdef NO_CLWB
static inline void clwb(volatile void *p) {
    asm volatile("clflush (%0)" ::"r"(p));
}
#else
static inline void clwb(volatile void *p) {
    asm volatile(".byte 0x66; xsaveopt %0" : "+m"(p));
}
#endif

#ifdef __cplusplus
}
#endif

#endif /* RLU_ARCH_H */
