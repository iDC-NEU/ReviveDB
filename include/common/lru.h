#pragma once
#include <algorithm>
#include <cstdint>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace NVMDB {
class NullMutex {
public:
    void lock() {}

    void unlock() {}

    bool try_lock() { return true; }
};

/**
 * error raised when a key not in cache is passed to get()
 */
class KeyNotFound : public std::invalid_argument {
public:
    KeyNotFound() : std::invalid_argument("key_not_found") {}
};

template<typename K, typename V>
struct KeyValuePair {
public:
    K key;
    V value;

    KeyValuePair(K k, V v) : key(std::move(k)), value(std::move(v)) {}
};

/**
 *	The LRU LRUCache class templated by
 *		Key - key type
 *		Value - value type
 *		MapType - an associative container like std::unordered_map
 *		MutexType - a mutex type derived from the Mutex class (default: NullMutex = no synchronization)
 *
 *	The default NullMutex based template is not thread-safe, however passing
 *  Mutex = std::mutex will make it thread-safe
 */
template<class Key, class Value, class Mutex = NullMutex,
          class Map = std::unordered_map<Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
class LRUCache {
public:
    typedef std::list<KeyValuePair<Key, Value>> list_type;
    typedef Mutex mutex_type;
    using Guard = std::lock_guard<mutex_type>;

    /**
         * the maxSize is the soft limit of keys and (maxSize + elasticity) is the hard limit
         * the cache is allowed to grow till (maxSize + elasticity) and is pruned back to maxSize keys
         * set maxSize = 0 for an unbounded cache (but in that case, you're better off
         * using a std::unordered_map directly anyway! :)
     */
    explicit LRUCache(size_t maxSize = 64, size_t elasticity = 10)
        : m_maxSize(maxSize), m_elasticity(elasticity) {}

    virtual ~LRUCache() = default;

    LRUCache(const LRUCache &) = delete;

    LRUCache &operator=(const LRUCache &) = delete;

    void init(size_t maxSize = 64, size_t elasticity = 10) {
        m_maxSize = maxSize;
        m_elasticity = elasticity;
    }

    size_t size() const {
        Guard g(m_mutex);
        return m_cache.size();
    }

    bool empty() const {
        Guard g(m_mutex);
        return m_cache.empty();
    }

    void clear() {
        Guard g(m_mutex);
        m_cache.clear();
        m_keys.clear();
    }

    void insert(const Key &k, Value v) {
        Guard g(m_mutex);
        const auto iter = m_cache.find(k);
        if (iter != m_cache.end()) {
            iter->second->value = std::move(v);
            m_keys.splice(m_keys.begin(), m_keys, iter->second);
            return;
        }

        m_keys.emplace_front(k, std::move(v));
        m_cache[k] = m_keys.begin();
        prune();
    }

    bool tryGetCopy(const Key &kIn, Value &vOut) {
        Guard g(m_mutex);
        Value tmp;
        if (!tryGetRef_nolock(kIn, tmp)) { return false; }
        vOut = tmp;
        return true;
    }

    bool tryGetRef(const Key &kIn, Value &vOut) {
        Guard g(m_mutex);
        return tryGetRef_nolock(kIn, vOut);
    }

    /**
         *	Maybe not thread safe!
         *	The const reference returned here is only guaranteed to be valid till the next insert/delete
         *  in multi-threaded apps use getCopy() to be threadsafe
     */
    const Value &getRef(const Key &k) {
        Guard g(m_mutex);
        return get_nolock(k);
    }

    /**
         * returns a copy of the stored object (if found) safe to use/recommended in multi-threaded apps
     */
    Value getCopy(const Key &k) {
        Guard g(m_mutex);
        return get_nolock(k);
    }

    bool remove(const Key &k) {
        Guard g(m_mutex);
        auto iter = m_cache.find(k);
        if (iter == m_cache.end()) {
            return false;
        }
        m_keys.erase(iter->second);
        m_cache.erase(iter);
        return true;
    }

    bool contains(const Key &k) const {
        Guard g(m_mutex);
        const auto iter = m_cache.find(k);
        if (iter == m_cache.end()) {
            return false;
        }
        m_keys.splice(m_keys.begin(), m_keys, iter->second);
        return true;
    }

    size_t getMaxSize() const { return m_maxSize; }

    size_t getElasticity() const { return m_elasticity; }

    size_t getMaxAllowedSize() const { return m_maxSize + m_elasticity; }

    template<typename F>
    void cwalk(F &f) const {
        Guard g(m_mutex);
        std::for_each(m_keys.begin(), m_keys.end(), f);
    }

protected:
    const Value &get_nolock(const Key &k) {
        const auto iter = m_cache.find(k);
        if (iter == m_cache.end()) {
            throw KeyNotFound();
        }
        m_keys.splice(m_keys.begin(), m_keys, iter->second);
        return iter->second->value;
    }

    bool tryGetRef_nolock(const Key &kIn, Value &vOut) {
        const auto iter = m_cache.find(kIn);
        if (iter == m_cache.end()) {
            return false;
        }
        m_keys.splice(m_keys.begin(), m_keys, iter->second);
        vOut = iter->second->value;
        return true;
    }

    size_t prune() {
        size_t maxAllowed = m_maxSize + m_elasticity;
        if (m_maxSize == 0 || m_cache.size() <= maxAllowed) {
            return 0;
        }
        size_t count = 0;
        while (m_cache.size() > m_maxSize) {
            m_cache.erase(m_keys.back().key);
            m_keys.pop_back();
            ++count;
        }
        return count;
    }

private:
    mutable Mutex m_mutex;
    Map m_cache;
    mutable list_type m_keys;
    size_t m_maxSize;
    size_t m_elasticity;
};
}