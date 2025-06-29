#include "common/pdl_art/n.h"
#include "n4.hpp"
#include "n16.hpp"
#include "n48.hpp"
#include "n256.hpp"

namespace ART_ROWEX {

constexpr int UINT_OFFSET = 63;
constexpr int UINT32_OFFSET = 32;
constexpr int N4_COMPACT_CNT = 4;
constexpr int N16_COMPACT_CNT = 16;
constexpr int N16_DEFAULT_CNT = 14;
constexpr int N48_COMPACT_CNT = 48;

void N::SetType(NTypes type) {
    typeVersionLockObsolete.fetch_add(ConvertTypeToVersion(type));
}

uint64_t N::ConvertTypeToVersion(NTypes type) {
    return (static_cast<uint64_t>(type) << (UINT_OFFSET - 1));
}

NTypes N::GetType() const {
    return static_cast<NTypes>(typeVersionLockObsolete.load(std::memory_order_relaxed) >> (UINT_OFFSET - 1));
}

uint32_t N::GetLevel() const {
    return level;
}

void N::SetLevel(uint32_t lev) {
    level = lev;
}

void N::WriteLockOrRestart(bool &needRestart, uint32_t genId) {
    uint64_t version = typeVersionLockObsolete.load(std::memory_order_relaxed);
    auto type = static_cast<NTypes>(version >> (UINT_OFFSET - 1));
    auto tmp = static_cast<uint64_t>(static_cast<uint64_t>(type) << (UINT_OFFSET - 1));

    uint32_t lockGenId = (version - tmp) >> UINT32_OFFSET;
    uint64_t newVer = tmp;
    newVer += (static_cast<uint64_t>(genId) << UINT32_OFFSET);
    newVer += 0b10;

    if (lockGenId != genId) {
        if (!typeVersionLockObsolete.compare_exchange_weak(version, newVer)) {
            needRestart = true;
            return;
        }
    } else {
        do {
            version = typeVersionLockObsolete.load();
            while (VersionIsLocked(version)) {
                version = typeVersionLockObsolete.load();
            }
            if (IsObsolete(version)) {
                needRestart = true;
                return;
            }
        } while (!typeVersionLockObsolete.compare_exchange_weak(version, version + 0b10));
    }
}

void N::LockVersionOrRestart(uint64_t &version, bool &needRestart, uint32_t genId) {
    auto type = static_cast<NTypes>(version >> (UINT_OFFSET - 1));
    auto tmp = static_cast<uint64_t>(static_cast<uint64_t>(type) << (UINT_OFFSET - 1));

    uint32_t lockGenId = (version - tmp) >> UINT32_OFFSET;
    uint64_t newVer = tmp;
    newVer += (static_cast<uint64_t>(genId) << UINT32_OFFSET);
    newVer += 0b10;

    if (IsObsolete(version)) {
        needRestart = true;
        return;
    }
    if (lockGenId != genId) {
        if (!typeVersionLockObsolete.compare_exchange_weak(version, newVer)) {
            needRestart = true;
            return;
        } else {
            version = newVer;
        }
    } else {
        if (VersionIsLocked(version)) {
            needRestart = true;
            return;
        }
        if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }
    }
}

void N::WriteUnlock() {
    DCHECK(VersionIsLocked(typeVersionLockObsolete.load()));
    typeVersionLockObsolete.fetch_sub(0b10);
}

N *N::GetAnyChild(const N *node) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<const N4 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<const N16 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<const N48 *>(node);
            return n->GetAnyChild();
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<const N256 *>(node);
            return n->GetAnyChild();
        }
    }
    CHECK(false);
    return nullptr;
}

N *N::GetAnyChildReverse(const N *node) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<const N4 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<const N16 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<const N48 *>(node);
            return n->GetAnyChildReverse();
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<const N256 *>(node);
            return n->GetAnyChildReverse();
        }
    }
    CHECK(false);
    return nullptr;
}

void N::Change(N *node, uint8_t key, NVMPtr<N> val) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            n->Change(key, val);
            return;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            n->change(key, val);
            return;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            n->Change(key, val);
            return;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            n->Change(key, val);
            return;
        }
    }
    CHECK(false);
}

template <typename NType, typename BiggerNType>
void N::InsertGrow(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val, ThreadInfo &threadInfo,
                   bool &needRestart, OpStruct *oplog, uint32_t genId) {
    if (n->Insert(key, val)) {
        n->WriteUnlock();
        return;
    }

    NVMPtr<N> nBigPtr;
    oplog->op = OpStruct::insert;

    PMem::alloc(sizeof(BiggerNType), (void **)&nBigPtr, &(oplog->newNodeOid));
    auto *nBig = (BiggerNType *)new (nBigPtr.getVaddr()) BiggerNType(n->GetLevel(), n->GetPrefix());

    n->CopyTo(nBig);
    nBig->Insert(key, val);
    CHECK(nBig->GetCount() >= BiggerNType::smallestCount);
    flushToNVM(reinterpret_cast<char *>(nBig), sizeof(BiggerNType));

    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        PMem::free((void *)nBigPtr.getRawPtr());
        n->WriteUnlock();
        return;
    }

    N::Change(parentNode, keyParent, nBigPtr);
    oplog->op = OpStruct::done;
    parentNode->WriteUnlock();

    n->WriteUnlockObsolete();
    threadInfo.GetEpoch().MarkNodeForDeletion(n, threadInfo);
}

template <typename NType>
void N::InsertCompact(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val, ThreadInfo &threadInfo,
                      bool &needRestart, OpStruct *oplog, uint32_t genId) {
    NVMPtr<N> nNewPtr;
    oplog->op = OpStruct::insert;

    PMem::alloc(sizeof(NType), (void **)&nNewPtr, &(oplog->newNodeOid));
    auto *nNew = (NType *)new (nNewPtr.getVaddr()) NType(n->GetLevel(), n->GetPrefix());

    n->CopyTo(nNew);
    nNew->Insert(key, val);
    flushToNVM(reinterpret_cast<char *>(nNew), sizeof(NType));

    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        PMem::free((void *)nNewPtr.getRawPtr());
        n->WriteUnlock();
        return;
    }

    N::Change(parentNode, keyParent, nNewPtr);
    oplog->op = OpStruct::done;
    parentNode->WriteUnlock();

    n->WriteUnlockObsolete();
    threadInfo.GetEpoch().MarkNodeForDeletion(n, threadInfo);
}

void N::InsertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key, NVMPtr<N> val, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            auto cValues = n->countValues;
            auto nCompactCount = static_cast<uint16_t>(cValues >> 16);
            auto nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N4_COMPACT_CNT && nCount <= (N4_COMPACT_CNT - 1)) {
                InsertCompact<N4>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N4, N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            auto cValues = n->countValues;
            auto nCompactCount = static_cast<uint16_t>(cValues >> 16);
            auto nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N16_COMPACT_CNT && nCount <= N16_DEFAULT_CNT) {
                InsertCompact<N16>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N16, N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            auto cValues = n->countValues;
            auto nCompactCount = static_cast<uint16_t>(cValues >> 16);
            auto nCount = static_cast<uint16_t>(cValues);
            if (nCompactCount == N48_COMPACT_CNT && nCount != N48_COMPACT_CNT) {
                InsertCompact<N48>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
                break;
            }
            InsertGrow<N48, N256>(n, parentNode, keyParent, key, val, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            n->Insert(key, val);
            node->WriteUnlock();
            break;
        }
    }
}

NVMPtr<N> N::GetChildNVMPtr(uint8_t k, N *node) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            return n->GetChildNVMPtr(k);
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            return n->GetChildNVMPtr(k);
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            return n->GetChildNVMPtr(k);
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            return n->GetChildNVMPtr(k);
        }
    }
    CHECK(false);
}

N *N::GetChild(uint8_t k, N *node) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            return n->GetChild(k);
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            return n->GetChild(k);
        }
    }
    CHECK(false);
    return nullptr;
}

void N::DeleteChildren(N *node) {
    if (N::IsLeaf(node)) {
        return;
    }
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            n->DeleteChildren();
            return;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            n->DeleteChildren();
            return;
        }
    }
    CHECK(false);
}

template <typename NType, typename SmallerNType>
void N::RemoveAndShrink(NType *n, N *parentNode, uint8_t keyParent, uint8_t key, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId) {
    if (n->Remove(key, parentNode == nullptr)) {
        n->WriteUnlock();
        return;
    }

    NVMPtr<N> nSmallPtr;
    oplog->op = OpStruct::insert;
    PMem::alloc(sizeof(SmallerNType), (void **)&nSmallPtr, &(oplog->newNodeOid));
    auto *nSmall = (SmallerNType *)new (nSmallPtr.getVaddr()) SmallerNType(n->GetLevel(), n->GetPrefix());

    DCHECK(parentNode != nullptr);
    parentNode->WriteLockOrRestart(needRestart, genId);
    if (needRestart) {
        n->WriteUnlock();
        return;
    }

    n->Remove(key, true);
    n->CopyTo(nSmall);
    flushToNVM(reinterpret_cast<char *>(nSmall), sizeof(SmallerNType));
    N::Change(parentNode, keyParent, nSmallPtr);
    oplog->op = OpStruct::done;

    parentNode->WriteUnlock();
    n->WriteUnlockObsolete();
    threadInfo.GetEpoch().MarkNodeForDeletion(n, threadInfo);
}

void N::RemoveAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent, ThreadInfo &threadInfo,
                        bool &needRestart, OpStruct *oplog, uint32_t genId) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            n->Remove(key, false);
            n->WriteUnlock();
            break;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);

            RemoveAndShrink<N16, N4>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            RemoveAndShrink<N48, N16>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            RemoveAndShrink<N256, N48>(n, parentNode, keyParent, key, threadInfo, needRestart, oplog, genId);
            break;
        }
    }
}

uint64_t N::GetVersion() const {
    return typeVersionLockObsolete.load();
}

bool N::IsObsolete(uint64_t version) {
    return (version & 1) == 1;
}

bool N::CheckOrRestart(uint64_t startRead) const {
    return ReadUnlockOrRestart(startRead);
}

bool N::ReadUnlockOrRestart(uint64_t startRead) const {
    return startRead == typeVersionLockObsolete.load();
}

uint32_t N::GetCount() const {
    uint32_t cValues = countValues;
    return static_cast<uint16_t>(cValues);
}

Prefix N::GetPrefix() const {
    return m_prefix.load();
}

void N::SetPrefix(const uint8_t *prefix, uint8_t length, bool flush) {
    if (length > 0) {
        Prefix p;
        int ret = memcpy_s(p.prefix, std::min(length, MAX_STORED_PREFIX_LENGTH), prefix,
                           std::min(length, MAX_STORED_PREFIX_LENGTH));
        SecureRetCheck(ret);
        p.prefixCount = length;
        this->m_prefix.store(p, std::memory_order_release);
    } else {
        Prefix p;
        p.prefixCount = 0;
        this->m_prefix.store(p, std::memory_order_release);
    }
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&(this->m_prefix)), sizeof(Prefix));
    }
}

void N::AddPrefixBefore(N *node, uint8_t key) {
    Prefix p = this->GetPrefix();
    Prefix nodeP = node->GetPrefix();
    uint32_t prefixCopyCount = std::min(static_cast<int>(MAX_STORED_PREFIX_LENGTH), nodeP.prefixCount + 1);
    int ret = memmove_s(p.prefix + prefixCopyCount, MAX_STORED_PREFIX_LENGTH - prefixCopyCount, p.prefix,
                        std::min(static_cast<uint32_t>(p.prefixCount), MAX_STORED_PREFIX_LENGTH - prefixCopyCount));
    SecureRetCheck(ret);
    ret = memcpy_s(p.prefix, prefixCopyCount, nodeP.prefix,
                   std::min(prefixCopyCount, static_cast<uint32_t>(nodeP.prefixCount)));
    SecureRetCheck(ret);
    if (nodeP.prefixCount < MAX_STORED_PREFIX_LENGTH) {
        p.prefix[prefixCopyCount - 1] = key;
    }
    p.prefixCount += nodeP.prefixCount + 1;
    this->m_prefix.store(p, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&(this->m_prefix)), sizeof(Prefix));
}

bool N::IsLeaf(const N *n) {
    return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << UINT_OFFSET)) ==
           (static_cast<uint64_t>(1) << UINT_OFFSET);
}

N *N::SetLeaf(TID tid) {
    return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << UINT_OFFSET));
}

TID N::GetLeaf(const N *n) {
    return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << UINT_OFFSET) - 1));
}

std::tuple<NVMPtr<N>, uint8_t> N::GetSecondChild(N *node, uint8_t key) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            return n->GetSecondChild(key);
        }
        case NTypes::N16:
        case NTypes::N48:
        case NTypes::N256:
        default:
            CHECK(false);
    }
    return {};
}

void N::DeleteNode(N *node) {
    if (N::IsLeaf(node)) {
        return;
    }
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<N4 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<N16 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<N48 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<N256 *>(node);
            PMem::freeVaddr((void *)n);
            return;
        }
    }
    PMem::freeVaddr(node);
}

TID N::GetAnyChildTid(const N *n) {
    const N *nextNode = n;

    while (true) {
        const N *node = nextNode;
        nextNode = GetAnyChild(node);

        DCHECK(nextNode != nullptr);
        if (IsLeaf(nextNode)) {
            return GetLeaf(nextNode);
        }
    }
}

TID N::GetAnyChildTidReverse(const N *n) {
    const N *nextNode = n;

    while (true) {
        const N *node = nextNode;
        nextNode = GetAnyChildReverse(node);

        DCHECK(nextNode != nullptr);
        if (IsLeaf(nextNode)) {
            return GetLeaf(nextNode);
        }
    }
}

void N::GetChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                    uint32_t &childrenCount) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<const N4 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<const N16 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<const N48 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<const N256 *>(node);
            n->GetChildren(start, end, children, childrenCount);
            return;
        }
    }
}
N *N::GetSmallestChild(const N *node, uint8_t start) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<const N4 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<const N16 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<const N48 *>(node);
            return n->GetSmallestChild(start);
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<const N256 *>(node);
            return n->GetSmallestChild(start);
        }
    }
    CHECK(false);
    return nullptr;
}
N *N::GetLargestChild(const N *node, uint8_t start) {
    switch (node->GetType()) {
        case NTypes::N4: {
            auto* n = reinterpret_cast<const N4 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N16: {
            auto* n = reinterpret_cast<const N16 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N48: {
            auto* n = reinterpret_cast<const N48 *>(node);
            return n->GetLargestChild(start);
        }
        case NTypes::N256: {
            auto* n = reinterpret_cast<const N256 *>(node);
            return n->GetLargestChild(start);
        }
    }
    CHECK(false);
    return nullptr;
}
}  // namespace ART_ROWEX
