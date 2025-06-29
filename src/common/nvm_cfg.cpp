//
// Created by pzs1997 on 5/10/24.
//

#include "common/nvm_cfg.h"
#include <sstream>
#include <experimental/filesystem>
#include <immintrin.h>
#include <cstdint>
#include "glog/logging.h"

namespace NVMDB {

std::shared_ptr<NVMDB::DirectoryConfig> g_dir_config = nullptr;

__attribute__((noinline)) errno_t memcpy_no_flush_nt(void* dest, size_t ndest, const void* src, size_t n) {
    DCHECK(dest != nullptr && src != nullptr);
    DCHECK(n <= ndest);
    // 检查源和目标是否有重叠
    DCHECK(!(dest >= src && dest < (const char*)src + n) &&
           !(src >= dest && src < (char*)dest + n));

    auto* d = (unsigned char*)dest;
    const auto* s = (const unsigned char*)src;

    // 对于小数据量，使用简单的逐字节复制
    if (n < 32) {
        for (size_t i = 0; i < n; i++) {
            d[i] = s[i];
        }
        return 0;
    }

    // 处理未对齐的前缀部分
    size_t i = 0;
    auto d_addr = (uintptr_t)d;
    if (d_addr & 0x3F) {  // 不是 64 字节对齐
        size_t prefix_len = 64 - (d_addr & 0x3F);
        prefix_len = (prefix_len < n) ? prefix_len : n;

        // 处理小于 8 字节的对齐
        while (i < prefix_len && (d_addr + i) & 0x7) {
            d[i] = s[i];
            i++;
        }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
        // 使用 64 位整数复制
        while (i + 8 <= prefix_len) {
            *((uint64_t*)(d + i)) = *((const uint64_t*)(s + i));
            i += 8;
        }
#pragma GCC diagnostic pop

        // 处理剩余字节
        while (i < prefix_len) {
            d[i] = s[i];
            i++;
        }
    }

    // 主循环：使用 AVX-512 非时态存储
    if (i + 64 <= n) {
        size_t avx512_blocks = (n - i) / 64;

        // 对于大块数据，使用预取来提高性能
        if (avx512_blocks > 4) {
            for (size_t j = 0; j < avx512_blocks; j++, i += 64) {
                // 预取下一个缓存行
                if (j + 4 < avx512_blocks) {
                    _mm_prefetch((const char*)(s + i + 256), _MM_HINT_NTA);
                }

                // 加载 64 字节
                __m512i data = _mm512_loadu_si512((__m512i*)(s + i));

                // 使用非时态存储指令写入，绕过缓存
                _mm512_stream_si512((__m512i*)(d + i), data);
            }
        } else {
            // 对于较小的块，不使用预取
            for (size_t j = 0; j < avx512_blocks; j++, i += 64) {
                __m512i data = _mm512_loadu_si512((__m512i*)(s + i));
                _mm512_stream_si512((__m512i*)(d + i), data);
            }
        }
    }

    // 处理剩余的 32 字节块（使用 AVX/AVX2）
    if (i + 32 <= n) {
        __m256i data = _mm256_loadu_si256((__m256i*)(s + i));
        _mm256_storeu_si256((__m256i*)(d + i), data);
        i += 32;
    }

    // 处理剩余的 16 字节块（使用 SSE）
    if (i + 16 <= n) {
        __m128i data = _mm_loadu_si128((__m128i*)(s + i));
        _mm_storeu_si128((__m128i*)(d + i), data);
        i += 16;
    }

    // 处理剩余的 8 字节块
    if (i + 8 <= n) {
        *((uint64_t*)(d + i)) = *((const uint64_t*)(s + i));
        i += 8;
    }

    // 处理剩余的 4 字节块
    if (i + 4 <= n) {
        *((uint32_t*)(d + i)) = *((const uint32_t*)(s + i));
        i += 4;
    }

    // 处理最后几个字节
    while (i < n) {
        d[i] = s[i];
        i++;
    }

    // 确保非时态存储完成
    _mm_sfence();
    return 0;
}

void prefetch_from_nvm(const char *nvmAddr, size_t size) {
    const size_t CACHE_LINE_SIZE = 64;
    size_t lines = (size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;

    // 预取每一个缓存行
    for (size_t i = 0; i < lines; i++) {
        _mm_prefetch(nvmAddr + i * CACHE_LINE_SIZE, _MM_HINT_NTA);
    }
}

DirectoryConfig::DirectoryConfig(const std::string &dirPathsString, bool init) {
    const auto endWithFunc = [](const std::string& str, const std::string& suffix) {
        if (str.size() < suffix.size()) return false;
        return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    // init dirPaths
    if (endWithFunc(dirPathsString, "pg_nvm")) {
        m_dirPaths.emplace_back("/mnt/pmem0/test_folder");
        m_dirPaths.emplace_back("/mnt/pmem1/test_folder");
    } else {
        std::stringstream ss(dirPathsString);
        for (std::string dirPath; std::getline(ss, dirPath, ';'); m_dirPaths.push_back(dirPath));
    }
    // for (const auto& it: m_dirPaths) {
    //     LOG(INFO) << "NVM dir paths: " << it;
    // }
    CHECK(!m_dirPaths.empty() && m_dirPaths.size() <= NVMDB_MAX_GROUP);
    // 初始化时, 删除目录内所有文件
    if (init) {
        for (const auto &it : m_dirPaths) {
            std::experimental::filesystem::remove_all(it);
        }
    }
    // 创建文件夹
    for (const auto &it : m_dirPaths) {
        auto ret = std::experimental::filesystem::create_directories(it);
        if (!ret && init) {
            LOG(WARNING) << "Create " << it << " failed, directory may already exists!";
        }
    }
}

}