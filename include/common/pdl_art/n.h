#ifndef ART_ROWEX_N_H
#define ART_ROWEX_N_H

#include "common/pdl_art/key.h"
#include "common/pdl_art/epoch.h"
#include "common/pdl_art/nvm_ptr.h"
#include "common/pdl_art/ordo_clock.h"

using TID = uint64_t;

using namespace ART;
namespace ART_ROWEX {
/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */
inline void SecureRetCheck(errno_t ret) {
    if (unlikely(ret != EOK)) {
        abort();
    }
}

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

constexpr uint8_t MAX_STORED_PREFIX_LENGTH = 7;

struct Prefix {
    uint8_t prefixCount = 0;
    uint8_t prefix[MAX_STORED_PREFIX_LENGTH] = {};
};
static_assert(sizeof(Prefix) == 8, "Prefix should be 64 bit long");

class N {
public:
    static constexpr size_t childrenIndexCount = 256;
    static constexpr size_t compactCountOffset = 16;
    static constexpr size_t n48ElementCount = 48;
    static constexpr size_t n16ElementCount = 16;
    static constexpr size_t n4ElementCount = 4;
    void SetType(NTypes type);

    NTypes GetType() const;

    uint32_t GetLevel() const;

    void SetLevel(uint32_t level);

    uint32_t GetCount() const;

    static bool VersionIsLocked(uint64_t version) {
        return ((version & 0b10) == 0b10);
    }

    void WriteLockOrRestart(bool &needRestart, uint32_t genId);

    void LockVersionOrRestart(uint64_t &version, bool &needRestart, uint32_t genId);

    void WriteUnlock();

    uint64_t GetVersion() const;

    /**
     * returns true if node hasn't been changed in between
     */
    bool CheckOrRestart(uint64_t startRead) const;
    bool ReadUnlockOrRestart(uint64_t startRead) const;

    static bool IsObsolete(uint64_t version);

    /**
     * can only be called when node is locked
     */
    void WriteUnlockObsolete() {
        typeVersionLockObsolete.fetch_add(0b11);
    }

    static N *GetChild(uint8_t k, N *node);
    static NVMPtr<N> GetChildNVMPtr(uint8_t k, N *node);

    static void InsertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val,
                                ThreadInfo &threadInfo, bool &needRestart, OpStruct *oplog, uint32_t genId);

    static void Change(N *node, uint8_t key, NVMPtr<N> val);

    static void RemoveAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo,
                                bool &needRestart, OpStruct *oplog, uint32_t genId);

    Prefix GetPrefix() const;

    void SetPrefix(const uint8_t *prefix, uint8_t length, bool flush);

    void AddPrefixBefore(N *node, uint8_t key);

    static TID GetLeaf(const N *n);

    static bool IsLeaf(const N *n);

    static N *SetLeaf(TID tid);

    static N *GetAnyChild(const N *n);
    static N *GetAnyChildReverse(const N *n);

    static TID GetAnyChildTid(const N *n);
    static TID GetAnyChildTidReverse(const N *n);

    static void DeleteChildren(N *node);

    static void DeleteNode(N *node);

    static std::tuple<NVMPtr<N>, uint8_t> GetSecondChild(N *node, uint8_t k);

    template <typename NType, typename biggerN>
    static void InsertGrow(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val, ThreadInfo &threadInfo,
                           bool &needRestart, OpStruct *oplog, uint32_t genId);

    template <typename NType>
    static void InsertCompact(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val,
                              ThreadInfo &threadInfo, bool &needRestart, OpStruct *oplog, uint32_t genId);

    template <typename NType, typename SmallerNType>
    static void RemoveAndShrink(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo,
                                bool &needRestart, OpStruct *oplog, uint32_t genId);

    static void GetChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                            uint32_t &childrenCount);
    static N *GetSmallestChild(const N *node, uint8_t start);
    static N *GetLargestChild(const N *node, uint8_t end);

public:
    N(const N &) = delete;

    N(N &&) = delete;

    N &operator=(const N &) = delete;

    N &operator=(N &&) = delete;

protected:
    N(NTypes type, uint32_t level, const uint8_t *prefix, uint8_t prefixLength) : level(level) {
        SetPrefix(prefix, prefixLength, false);
        SetType(type);
    }

    N(NTypes type, uint32_t level, const Prefix &prefix) : m_prefix(prefix), level(level) {
        SetType(type);
    }

    // 2b type 60b version 1b lock 1b obsolete
    // 2b type 28b gen id 32b version 1b lock 1b obsolete
    std::atomic<uint64_t> typeVersionLockObsolete{0b100};
    // genID?

    // version 1, unlocked, not obsolete
    std::atomic<Prefix> m_prefix;
    uint32_t level;
    uint32_t countValues = 0;  // count 2B, compactCount 2B

    static uint64_t ConvertTypeToVersion(NTypes type);
};

class N4 : public N {
public:
    const static int smallestCount = 0;
    std::atomic<uint8_t> keys[4]{};
    std::atomic<NVMPtr<N>> children[4];

public:
    void *operator new(size_t, void *location) {
        return location;
    }

    N4(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N4, level, prefix, prefixLength) {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N4(uint32_t level, const Prefix &prefix) : N(NTypes::N4, level, prefix) {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, NVMPtr<N> n);
    bool Insert(uint8_t key, NVMPtr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, NVMPtr<N> val);

    N *GetChild(uint8_t k) const;
    NVMPtr<N> GetChildNVMPtr(uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    std::tuple<NVMPtr<N>, uint8_t> GetSecondChild(uint8_t key) const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N16 : public N {
public:
    static constexpr size_t signBit = 128;
    void *operator new(size_t, void *location) {
        return location;
    }

    std::atomic<uint8_t> keys[16]{};
    std::atomic<NVMPtr<N>> children[16];

    static uint8_t FlipSign(uint8_t keyByte) {
        // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
        return keyByte ^ signBit;
    }

    static unsigned Ctz(uint16_t x) {
        // Count trailing zeros, only defined for x>0
        return __builtin_ctz(x);
    }

    std::atomic<NVMPtr<N>> *getChildPos(uint8_t k);

public:
    const static int smallestCount = 3;
    N16(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N16, level, prefix, prefixLength) {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N16(uint32_t level, const Prefix &prefix) : N(NTypes::N16, level, prefix) {
        int ret = memset_s(keys, sizeof(keys), 0, sizeof(keys));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, NVMPtr<N> n);
    bool Insert(uint8_t key, NVMPtr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void change(uint8_t key, NVMPtr<N> val);

    N *GetChild(uint8_t k) const;
    NVMPtr<N> GetChildNVMPtr(uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N48 : public N {
    std::atomic<uint8_t> childIndex[256]{};
    std::atomic<NVMPtr<N>> children[48];
    // std::atomic<N *> children[48];

    NVMPtr<N> GetChildNVMPtrByIndex(uint8_t k) const;

public:
    const static int smallestCount = 12;
    void *operator new(size_t, void *location) {
        return location;
    }

    static const uint8_t emptyMarker = 48;

    N48(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N48, level, prefix, prefixLength) {
        int ret = memset_s(childIndex, sizeof(childIndex), emptyMarker, sizeof(childIndex));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    N48(uint32_t level, const Prefix &prefix) : N(NTypes::N48, level, prefix) {
        int ret = memset_s(childIndex, sizeof(childIndex), emptyMarker, sizeof(childIndex));
        SecureRetCheck(ret);
        ret = memset_s(children, sizeof(children), 0, sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, NVMPtr<N> n);
    bool Insert(uint8_t key, NVMPtr<N> n, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, NVMPtr<N> val);

    N *GetChild(uint8_t k) const;
    NVMPtr<N> GetChildNVMPtr(uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};

class N256 : public N {
    std::atomic<NVMPtr<N>> children[256];

    NVMPtr<N> GetChildNVMPtrByIndex(uint8_t k) const;

public:
    const static int smallestCount = 38;
    void *operator new(size_t, void *location) {
        return location;
    }

    N256(uint32_t level, const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N256, level, prefix, prefixLength) {
        int ret = memset_s(children, sizeof(children), '\0', sizeof(children));
        SecureRetCheck(ret);
    }

    N256(uint32_t level, const Prefix &prefix) : N(NTypes::N256, level, prefix) {
        int ret = memset_s(children, sizeof(children), '\0', sizeof(children));
        SecureRetCheck(ret);
    }

    bool Insert(uint8_t key, NVMPtr<N> val);
    bool Insert(uint8_t key, NVMPtr<N> val, bool flush);

    template <class NODE>
    void CopyTo(NODE *n) const;

    void Change(uint8_t key, NVMPtr<N> n);

    N *GetChild(uint8_t k) const;
    NVMPtr<N> GetChildNVMPtr(uint8_t k) const;

    bool Remove(uint8_t k, bool force);

    N *GetAnyChild() const;
    N *GetAnyChildReverse() const;

    void DeleteChildren();

    void GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const;
    N *GetSmallestChild(uint8_t start) const;
    N *GetLargestChild(uint8_t end) const;
};
}  // namespace ART_ROWEX
#endif  // ART_ROWEX_N_H
