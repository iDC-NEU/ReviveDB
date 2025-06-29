#pragma once

#include "heap/nvm_rowid_map.h"
#include "common/lru.h"
#include "common/nvm_types.h"
#include <vector>

namespace NVMDB {

DECLARE_int64(cache_size);
DECLARE_int64(cache_elasticity);

namespace {
// 写成模板类, 避免和rowid_map相互引用
// 每张表有一个TupleCache
template<typename KeyType, class TupleType>
class TupleCache {
public:
    TupleCache(size_t maxSize, size_t elasticity)
        : m_tupleLruCache(maxSize, elasticity) { }

    ~TupleCache() {
        // 手动清空, 否则因为先析构m_unmanagedTuples
        // 再析构m_tupleLruCache会导致踩内存
        m_tupleLruCache.clear();
    }

    // 必须在锁定该entry之后调用
    // 当该线程访问一个元组时, 将该元组加入到lru
    void touch(KeyType rowId, TupleType* tuple) {
        if (m_tupleLruCache.contains(rowId)) {
            return;
        }
        tuple->increaseReference();

        // 创建一个包含自定义删除器的共享指针并返回, 同时增加引用计数
        // 当引用计数为0时候, 清除缓存
        // DLOG(INFO) << "Construct a new ptr, ref: " << m_referenceCount;
        auto cachedTuple = std::unique_ptr<RowIdMapEntry, CustomDeleter>(tuple, TupleCache::CustomDeleter(this));
        m_tupleLruCache.insert(rowId, std::move(cachedTuple));
    }

protected:
    class CustomDeleter {
    public:
        explicit CustomDeleter(TupleCache* owner) : m_owner(owner) { }

        inline void operator()(TupleType* tuple) const {
            auto newRef = tuple->decreaseReference() - 1;
            DCHECK(newRef >= 0);
            // 引用数量为0, 先不上锁, 乐观试一次, 最后在clearUnmanagedTuples上锁再检查一遍
            if (unlikely(newRef == 0)) {
                m_owner->m_unmanagedTuples.push_back(tuple);
                // 当前线程积攒一批一起清空
                if (m_owner->m_unmanagedTuples.size() > m_owner->m_tupleLruCache.getElasticity()) {
                    m_owner->clearUnmanagedTuples();
                }
            }
        }

    private:
        TupleCache* m_owner;
    };

    // 一个线程本地进行, 因此对m_unmanagedTuples的操作不用上锁
    inline void clearUnmanagedTuples() {
        std::vector<TupleType*> unmanagedTuples;
        unmanagedTuples.reserve(m_unmanagedTuples.size());
        unmanagedTuples.swap(m_unmanagedTuples);
        // 清理缓存并释放空间
        for (auto* tuple: unmanagedTuples) {
            bool ret = tuple->TryLock();
            if (!ret) {
                // 上锁失败, 元组正在被其他线程使用, 稍后重试
                m_unmanagedTuples.push_back(tuple);
                continue;   // 未获取锁, 不用释放锁
            }
            // 依次检测元组的引用计数
            auto refCount = tuple->getReferenceCount(std::memory_order_relaxed);
            if (refCount != 0) {
                tuple->Unlock();    // 已经成功获取锁, 需要释放锁
                continue;   // 引用计数不为0, 正在被其他线程的LRU使用, 本地线程就当作清空完毕了
            }
            // DLOG(INFO) << "Clear Buffer.";
            tuple->clearAndShrinkCache();
            tuple->Unlock();
        }
    }

private:
    using TupleLRUCache = LRUCache<KeyType, std::unique_ptr<RowIdMapEntry, CustomDeleter>>;

    TupleLRUCache m_tupleLruCache;

    std::vector<TupleType*> m_unmanagedTuples;
};

// 每个线程一个管理器, 因此不必要用锁
// ThreadLocalTupleCache不用持久化
template<typename TableIdType, typename KeyType, class TupleType>
class ThreadLocalTupleCache {
public:
    ThreadLocalTupleCache() = default;

    // 加入到LRU前需要锁定记录
    void touch(TableIdType tableId, KeyType rowId, TupleType* tuple) {
        auto iter = m_perTableCache.find(tableId);
        if (likely(iter != m_perTableCache.end())) {
            iter->second->m_tupleCache.touch(rowId, tuple);
            return;
        }
        m_perTableCache[tableId] = std::make_shared<Cache>();
    }

    static inline void Clear() { g_threadLocalTupleCache.m_perTableCache.clear(); }

    static inline void Touch(TableIdType tableId, KeyType rowId, TupleType* tuple) {
        g_threadLocalTupleCache.touch(tableId, rowId, tuple);
    }

protected:
    inline auto* getCacheOfTable(TableIdType tableId) {
        auto iter = m_perTableCache.find(tableId);
        if (likely(iter != m_perTableCache.end())) {
            return iter->second.get();
        }
        m_perTableCache[tableId] = std::make_shared<Cache>();
        return m_perTableCache.find(tableId)->second.get();
    }

public:
    struct Cache {
        Cache() : m_tupleCache(FLAGS_cache_size, FLAGS_cache_elasticity) { }

        TupleCache<KeyType, TupleType> m_tupleCache;
    };

private:
    std::unordered_map<TableIdType, std::shared_ptr<Cache>> m_perTableCache;

    static thread_local ThreadLocalTupleCache g_threadLocalTupleCache;
};

template<typename TableIdType, typename KeyType, class TupleType>
thread_local ThreadLocalTupleCache<TableIdType, KeyType, TupleType> ThreadLocalTupleCache<TableIdType, KeyType, TupleType>::g_threadLocalTupleCache = {};

// 每个线程一个管理器, 因此不必要用锁
// ThreadLocalTableCache不用持久化
template<typename TableIdType>
class ThreadLocalTableCache {
public:
    ThreadLocalTableCache() = default;

    static inline auto* GetThreadLocalTableCache(TableIdType tableId) {
        return g_threadLocalTableCache.getCacheOfTable(tableId);
    }

    static inline void Clear() { g_threadLocalTableCache.m_perTableCache.clear(); }

protected:
    inline auto* getCacheOfTable(TableIdType tableId) {
        auto iter = m_perTableCache.find(tableId);
        if (likely(iter != m_perTableCache.end())) {
            return iter->second.get();
        }
        m_perTableCache[tableId] = std::make_shared<Cache>();
        return m_perTableCache.find(tableId)->second.get();
    }
public:
    class VecRange {
    public:
        // 在该范围中搜索未使用的行
        inline void setRange(RowId start, RowId end) {
            m_start = start;
            m_end = end;
        }

        inline bool empty() const { return m_start >= m_end; }

        RowId next() {
            if (m_start < m_end) {
                return m_start++;
            }
            return InvalidRowId;
        }

    private:
        RowId m_start = {InvalidRowId};
        RowId m_end = {InvalidRowId};
    };

    class FreeRowIdList {
    public:
        void push_back(RowId rowId) {
            m_freeList.push_back(rowId);
        }

        RowId pop() {
            if (m_freeList.empty()) {
                return InvalidRowId;
            }
            RowId res = m_freeList.back();
            m_freeList.pop_back();
            return res;
        }

    private:
        // 存有本地线程释放的, 在table extent中一定未使用的行的Id
        std::vector<RowId> m_freeList;
    };

    struct Cache {
        FreeRowIdList m_rowidCache;
        VecRange m_range;
    };

private:
    std::unordered_map<TableIdType, std::shared_ptr<Cache>> m_perTableCache;

    static thread_local ThreadLocalTableCache g_threadLocalTableCache;
};

template<typename TableIdType>
thread_local ThreadLocalTableCache<TableIdType> ThreadLocalTableCache<TableIdType>::g_threadLocalTableCache = {};
}

using TLTableCache = ThreadLocalTableCache<uint32>;
using TLTupleCache = ThreadLocalTupleCache<uint32, RowId, RowIdMapEntry>;

}