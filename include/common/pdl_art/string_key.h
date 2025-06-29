#ifndef PACTREE_COMMON_H
#define PACTREE_COMMON_H

#include "common/nvm_utils.h"
#include "glog/logging.h"
#include <libpmemobj.h>
#include <string>
#include <atomic>

#define KEYLENGTH 63

template <std::size_t keySize>
class StringKey {
public:
    unsigned char keyLength = 0;
    unsigned char data[keySize];

    StringKey() {
        int ret = memset_s(data, keySize, 0, keySize);
        SecureRetCheck(ret);
    }

    StringKey(const StringKey &other) {
        keyLength = other.keyLength;
        DCHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, other.data, keyLength);
            SecureRetCheck(ret);
        }
    }

    explicit StringKey(const char bytes[]) {
        keyLength = strlen(bytes);
        DCHECK(keySize == 0 || keySize >= keyLength);
        set(bytes, keyLength);
    }

    explicit StringKey(int k) {
        setFromString(std::to_string(k));
    }

    inline StringKey &operator=(const StringKey &other) {
        keyLength = other.keyLength;
        DCHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, other.data, keyLength);
            SecureRetCheck(ret);
        }
        return *this;
    }

    inline bool operator<(const StringKey<keySize> &other) const {
        int len = std::min(size(), other.size());
        int cmp = memcmp(data, other.data, len);
        if (cmp == 0) {
            return size() < other.size();
        } else {
            return cmp < 0;
        }
    }

    inline bool operator>(const StringKey<keySize> &other) const {
        int len = std::min(size(), other.size());
        int cmp = memcmp(data, other.data, len);
        if (cmp == 0) {
            return size() > other.size();
        } else {
            return cmp > 0;
        }
    }

    inline bool operator==(const StringKey<keySize> &other) const {
        if (size() != other.size()) {
            return false;
        }
        return memcmp(data, other.data, size()) == 0;
    }

    inline bool operator!=(const StringKey<keySize> &other) const {
        return !(*this == other);
    }

    inline bool operator<=(const StringKey<keySize> &other) const {
        return !(*this > other);
    }

    inline bool operator>=(const StringKey<keySize> &other) const {
        return !(*this < other);
    }

    size_t size() const {
        return keyLength;
    }

    inline void setFromString(const std::string& key) {
        keyLength = key.size();
        if (keySize < keyLength) {
            LOG(WARNING) << "Too long string!";
            keyLength = keySize;
        }
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, key.c_str(), keyLength);
            SecureRetCheck(ret);
        }
    }

    inline void set(const char* bytes, const std::size_t length) {
        keyLength = length;
        DCHECK(keySize == 0 || keySize >= keyLength);
        if (keyLength != 0) {
            int ret = memcpy_s(data, keyLength, bytes, keyLength);
            SecureRetCheck(ret);
        }
    }

    char* getData() { return (char *)data; }

    // 找下一个开始的Key
    void next() {
        if (keyLength <= 0) {
            return;
        }
        for (int i = keyLength - 1; i >= 0; i--) {
            data[i] += 1;
            if (data[i] != 0) {
                return;  // 没有发生溢出, 返回
            }
        }
    }
};

using Key_t = StringKey<KEYLENGTH>;
using Val_t = uint64_t;
using VarLenString = StringKey<0>;

static const int OpListNodeSize = 2152;

// pac tree index log 的结构
class OpStruct {
public:
    enum Operation { dummy, insert, remove, done };
    enum Step { initial, during_split, finish_split };
    Operation op;                       // 4
    uint16_t poolId;                    // 2
    uint8_t hash;                       // 1
    Step step;                          // 4
    std::atomic<uint8_t> searchLayers;  // 1
    Key_t key;                          //  8
    void *oldNodePtr;                   // old node_ptr 8
    PMEMoid newNodeOid;                 // new node_ptr 16
    Key_t newKey;                       // new key ; 8
    Val_t newVal;                       // new value ; 8
    uint64_t ts;                        // 8
    char oldNodeData[OpListNodeSize];

    bool operator<(const OpStruct &ops) const {
        return (ts < ops.ts);
    }
};

#endif
