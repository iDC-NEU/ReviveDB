#include "heap/nvm_tuple.h"
#include "heap/nvm_heap_undo.h"
#include "undo/nvm_undo_segment.h"
#include <libpmem.h>

namespace NVMDB {

thread_local FlushCache FlushCache::g_rowIdCache;

inline void flush(const void *src, size_t n) {
    const auto *s = (const uint8_t *)src;
    for (size_t i = 0; i < n; i += 64) {
        _mm_clflushopt((void *)(s + i));
    }
}

void dram_to_nvm_memcpy(void *dest, const void *src, size_t n) {
    DCHECK(((uintptr_t)dest & 0x3F) == 0);   // 64字节对齐
    DCHECK(((uintptr_t)src & 0x3F) == 0);    // 64字节对齐

    auto *d = (uint8_t *)dest;
    const auto *s = (const uint8_t *)src;

    // 对于大块数据，使用AVX-512指令集
    if (n >= 64) {
        size_t blocks = n / 64;
        for (size_t i = 0; i < blocks; i++) {
            // 使用非时序存储加载和写入到NVM，因为之后不会访问这些数据
            __m512i data = _mm512_stream_load_si512((__m512i *)(s + i * 64));
            _mm512_stream_si512((__m512i *)(d + i * 64), data);
        }

        // 确保所有非时序写入已完成
        _mm_sfence();
        // 清除所有DRAM缓存行
        for (size_t i = 0; i < blocks; i++) {
            _mm_clflushopt((void *)(s + i * 64));
        }

        // 更新指针位置
        s += blocks * 64;
        d += blocks * 64;
        n -= blocks * 64;

        // 处理剩余字节
        if (n > 0) {
            memcpy(d, s, n);
            flush(s, n);
            flush(d, n);
        }
        return;
    }
    // 对于小数据，直接复制，并按情况逐出缓存
    memcpy(d, s, n);
    // 清除所有DRAM缓存行
    flush(s, n);
    // 清除NVM缓存行
    if (n > 64) {
        flush(d + 64, n - 64);
    }
}

void dram_to_nvm_memcpy_no_align(void *dest, const void *src, size_t n) {
    DCHECK(((uintptr_t)src & 0x3F) == 0);    // 64字节对齐

    auto *d = (uint8_t *)dest;
    const auto *s = (const uint8_t *)src;

    // 对于大块数据，使用AVX-512指令集
    if (n >= 64) {
        size_t blocks = n / 64;
        for (size_t i = 0; i < blocks; i++) {
            // 使用非时序存储加载和写入到NVM，因为之后不会访问这些数据
            __m512i data = _mm512_stream_load_si512((__m512i *)(s + i * 64));
            _mm512_storeu_si512((__m512i *)(d + i * 64), data);
        }

        // 确保所有非时序写入已完成
        _mm_sfence();
        for (size_t i = 0; i < blocks; i++) {
            // 清除所有DRAM缓存行
            _mm_clflushopt((void *)(s + i * 64));
            // 清除NVM缓存行
            if (i != 0) {
                _mm_clflushopt((void *)(d + i * 64));
            }
        }

        // 更新指针位置
        s += blocks * 64;
        d += blocks * 64;
        n -= blocks * 64;

        // 处理剩余字节
        if (n > 0) {
            memcpy(d, s, n);
            flush(s, n);
            flush(d, n);
        }
        return;
    }
    // 对于小数据，直接复制，并按情况逐出缓存
    memcpy(d, s, n);
    // 清除所有DRAM缓存行
    flush(s, n);
    // 清除NVM缓存行
    if (n > 64) {
        flush(d + 64, n - 64);
    }
}

//void RAMTuple::Serialize(char *nvmAddr, size_t rowLen) {
//    m_rowHeaderPtr->m_dataSize = m_rowLen;
//    if (((uintptr_t)nvmAddr & 0x3F) == 0) {
//        dram_to_nvm_memcpy(nvmAddr, m_tupleData, RealTupleSize(m_rowLen));
//    } else {
//        dram_to_nvm_memcpy_no_align(nvmAddr, m_tupleData, RealTupleSize(m_rowLen));
////        memcpy_no_flush_nt(nvmAddr, RealTupleSize(m_rowLen), m_tupleData, RealTupleSize(m_rowLen));
////        if (RealTupleSize(m_rowLen) > 64) {
////            pmem_flush(nvmAddr + 64, RealTupleSize(m_rowLen) - 64);
////        }
//    }
//}

void nvm_to_dram_memcpy_no_align(void *dest, const void *src, size_t n) {
    DCHECK(((uintptr_t)dest & 0x3F) == 0);   // 64字节对齐

    auto *d = (uint8_t *)dest;
    const auto *s = (const uint8_t *)src;

    // 使用非时序加载指令从NVM加载数据
    // 对于大块数据，使用AVX-512指令集
    if (n >= 64) {
        size_t blocks = n / 64;
        for (size_t i = 0; i < blocks; i++) {
            // 使用非时序加载从NVM读取数据
            __m512i data = _mm512_loadu_si512((__m512i *)(s + i * 64));
            // 使用时序存储写入DRAM
            _mm512_store_si512((__m512i *)(d + i * 64), data);
        }

        // 确保所有非时序写入已完成
        _mm_sfence();
        for (size_t i = 0; i < blocks; i++) {
            // 清除NVM缓存行
            _mm_clflushopt((void *)(s + i * 64));
        }

        // 更新指针位置
        s += blocks * 64;
        d += blocks * 64;
        n -= blocks * 64;
        // 处理剩余字节
        if (n > 0) {
            memcpy(d, s, n);
            flush(s, n);
        }
        return;
    }
    // 对于小数据，直接复制，并逐出NVM缓存
    memcpy(d, s, n);
    flush(s, n);
}

void nvm_to_dram_memcpy(void *dest, const void *src, size_t n) {
    DCHECK(((uintptr_t)src & 0x3F) == 0);    // 64字节对齐
    DCHECK(((uintptr_t)dest & 0x3F) == 0);   // 64字节对齐

    auto *d = (uint8_t *)dest;
    const auto *s = (const uint8_t *)src;

    // 使用非时序加载指令从NVM加载数据
    // 对于大块数据，使用AVX-512指令集
    if (n >= 64) {
        size_t blocks = n / 64;
        for (size_t i = 0; i < blocks; i++) {
            // 使用非时序加载从NVM读取数据
            __m512i data = _mm512_stream_load_si512((__m512i *)(s + i * 64));
            // 使用时序存储写入DRAM
            _mm512_store_si512((__m512i *)(d + i * 64), data);
        }
        // 更新指针位置
        s += blocks * 64;
        d += blocks * 64;
        n -= blocks * 64;
        // 处理剩余字节
        if (n > 0) {
            memcpy(d, s, n);
            flush(s, n);
        }
        // 确保所有非时序写入已完成
        _mm_sfence();
        return;
    }
    // 对于小数据，直接复制，并逐出NVM缓存
    memcpy(d, s, n);
    flush(s, n);
}

//void RAMTuple::Deserialize(const char *nvmAddr) {
//    if (((uintptr_t)nvmAddr & 0x3F) == 0) {
//        nvm_to_dram_memcpy(m_tupleData, nvmAddr, RealTupleSize(m_rowLen));
//    } else {
//        // memcpy_no_flush_nt(m_tupleData, RealTupleSize(m_rowLen), nvmAddr, RealTupleSize(m_rowLen));
//        nvm_to_dram_memcpy_no_align(m_tupleData, nvmAddr, RealTupleSize(m_rowLen));
//    }
//}

void RAMTuple::FetchPreVersion(char* buffer) {
    DCHECK(!UndoRecPtrIsInValid(m_rowHeaderPtr->m_prev));
    auto* undoRecordCache = reinterpret_cast<UndoRecord *>(buffer);
    GetUndoRecord(m_rowHeaderPtr->m_prev, undoRecordCache);
    if (undoRecordCache->m_undoType == HeapUpdateUndo) {
        UndoUpdate(undoRecordCache, this->m_rowHeaderPtr, this->m_rowDataPtr);
    } else {
        Deserialize(undoRecordCache->data);
    }
}

void RAMTuple::Serialize(char *nvmAddr, size_t rowLen) {
    m_rowHeaderPtr->m_dataSize = m_rowLen;
    memcpy_s(nvmAddr, RealTupleSize(m_rowLen), m_tupleData, RealTupleSize(m_rowLen));
}

void RAMTuple::Deserialize(const char *nvmAddr) {
    memcpy_s(m_tupleData, RealTupleSize(m_rowLen), nvmAddr, RealTupleSize(m_rowLen));
}

}  // namespace NVMDB