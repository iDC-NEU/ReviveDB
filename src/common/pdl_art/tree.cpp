#include "common/pdl_art/tree.h"

namespace ART_ROWEX {
void Tree::Recover() {
    for (int i = 0; i < oplogsCount; i++) {
        if (oplogs[i].op == OpStruct::insert) {
            pmemobj_free(&oplogs[i].newNodeOid);
        }
    }
}

Tree::Tree(LoadKeyFunction loadKey) : loadKey(loadKey) {
    NVMPtr<OpStruct> ologPtr;
    PMEMoid oid;
    PMem::alloc(sizeof(OpStruct), (void **)&ologPtr, &oid);
    OpStruct *olog = ologPtr.getVaddr();
    NVMPtr<N> nRootPtr;
    PMem::alloc(sizeof(N256), (void **)&nRootPtr, &(olog->newNodeOid));
    oplogsCount = 1;
    flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
    smp_wmb();

    genId = defaultGenId;
    N256 *rootRawPtr = (N256 *)new (nRootPtr.getVaddr()) N256(0, {});

    flushToNVM(reinterpret_cast<char *>(rootRawPtr), sizeof(N256));
    smp_wmb();
    this->loadKey = loadKey;
    root = nRootPtr;
    flushToNVM(reinterpret_cast<char *>(this), sizeof(Tree));
    smp_wmb();
}

Tree::~Tree() = default;

ThreadInfo Tree::GetThreadInfo() {
    return ThreadInfo(this->epoch);
}

TID Tree::Lookup(const Key &k, ThreadInfo &threadEpochInfo) const {
    EpochGuardReadonly epochGuard(threadEpochInfo);
    NVMPtr<N> nodePtr = root;
    N *node = (N *)nodePtr.getVaddr();
    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {
        switch (CheckPrefix(node, k, level)) {  // increases level
            case CheckPrefixResult::NO_MATCH:
                return 0;
            case CheckPrefixResult::OPTIMISTIC_MATCH:
                optimisticPrefixMatch = true;
                // fallthrough
            case CheckPrefixResult::MATCH: {
                if (k.GetKeyLen() <= level) {
                    return 0;
                }
                node = N::GetChild(k[level], node);
                if (node == nullptr) {
                    return 0;
                }
                if (N::IsLeaf(node)) {
                    TID tid = N::GetLeaf(node);
                    if (level < k.GetKeyLen() - 1 || optimisticPrefixMatch) {
                        return CheckKey(tid, k);
                    } else {
                        return tid;
                    }
                }
            }
        }
        level++;
    }
}

TID Tree::CheckKey(const TID tid, const Key &k) const {
    Key kt;
    this->loadKey(tid, kt);
    if (k == kt) {
        return tid;
    }
    return 0;
}

void Tree::Insert(const Key &k, TID tid, ThreadInfo &epochInfo) {
    EpochGuard epochGuard(epochInfo);
restart:
    bool needRestart = false;

    NVMPtr<N> nextNodePtr = root;

    N *node = nullptr;
    uint8_t nodeKey = 0;
    NVMPtr<N> nodePtr;
    N *nextNode = (N *)root.getVaddr();
    uint32_t level = 0;

    while (true) {
        N* parentNode = node;
        uint8_t parentKey = nodeKey;
        NVMPtr<N> parentPtr = nodePtr;
        node = nextNode;
        nodePtr = nextNodePtr;

        auto v = node->GetVersion();

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey;
        Prefix remainingPrefix;
        switch (CheckPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix, this->loadKey)) {  // increases level
            case CheckPrefixPessimisticResult::SKIPPED_LEVEL:
                goto restart;
            case CheckPrefixPessimisticResult::NO_MATCH: {
                DCHECK(nextLevel < k.GetKeyLen());  // prevent duplicate key
                node->LockVersionOrRestart(v, needRestart, genId);
                if (needRestart) {
                    goto restart;
                }
                // 1) Create new node which will be parent of node, Set common prefix, level to this node
                Prefix prefix = node->GetPrefix();
                prefix.prefixCount = nextLevel - level;

                NVMPtr<N> newNodePtr;
                oplogs[oplogsCount].op = OpStruct::insert;
                oplogs[oplogsCount].oldNodePtr = (void *)parentPtr.getRawPtr();
                PMem::alloc(sizeof(N4), reinterpret_cast<void **>(&newNodePtr), &(oplogs[oplogsCount].newNodeOid));
                oplogsCount++;
                flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
                smp_wmb();

                N4 *newNode = (N4 *)new (newNodePtr.getVaddr()) N4(nextLevel, prefix);

                NVMPtr<N> pTid(0, (unsigned long)N::SetLeaf(tid));

                // 2)  add node and (tid, *k) as children

                newNode->Insert(k[nextLevel], pTid, false);
                newNode->Insert(nonMatchingKey, nodePtr, false);
                flushToNVM(reinterpret_cast<char *>(static_cast<void *>(newNode)), sizeof(N4));
                smp_wmb();

                // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                CHECK(parentNode != nullptr);
                parentNode->WriteLockOrRestart(needRestart, genId);
                if (needRestart) {
                    PMem::free((void *)newNodePtr.getRawPtr());
                    node->WriteUnlock();
                    goto restart;
                }
                N::Change(parentNode, parentKey, newNodePtr);
                oplogs[oplogsCount].op = OpStruct::done;
                parentNode->WriteUnlock();

                // 4) update prefix of node, unlock
                node->SetPrefix(remainingPrefix.prefix, node->GetPrefix().prefixCount - ((nextLevel - level) + 1), true);

                oplogsCount = 0;
                flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
                smp_wmb();
                node->WriteUnlock();
                return;
            }
            case CheckPrefixPessimisticResult::MATCH:
                break;
        }
        level = nextLevel;
        nodeKey = k[level];
        nextNodePtr = N::GetChildNVMPtr(nodeKey, node);
        nextNode = nextNodePtr.getVaddr();
        if (nextNode == nullptr) {
            node->LockVersionOrRestart(v, needRestart, genId);
            if (needRestart) {
                goto restart;
            }

            NVMPtr<N> pTid(0, (unsigned long)N::SetLeaf(tid));

            N::InsertAndUnlock(node, parentNode, parentKey, nodeKey, pTid, epochInfo, needRestart,
                               &oplogs[oplogsCount], genId);
            oplogsCount = 0;
            flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
            smp_wmb();
            if (needRestart) {
                goto restart;
            }
            return;
        }
        if (N::IsLeaf(nextNode)) {
            node->LockVersionOrRestart(v, needRestart, genId);
            if (needRestart) {
                goto restart;
            }

            Key key;
            loadKey(N::GetLeaf(nextNode), key);

            if (key == k) {
                NVMPtr<N> pTid(0, (unsigned long)N::SetLeaf(tid));
                N::Change(node, k[level], pTid);
                oplogsCount = 0;
                flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
                smp_wmb();
                node->WriteUnlock();
                return;
            }

            level++;
            DCHECK(level < key.GetKeyLen());  // prevent inserting when prefix of key exists already

            uint32_t prefixLength = 0;
            while (key[level + prefixLength] == k[level + prefixLength]) {
                prefixLength++;
            }

            NVMPtr<N> n4Ptr;
            oplogs[oplogsCount].op = OpStruct::insert;
            oplogs[oplogsCount].oldNodePtr = (void *)nodePtr.getRawPtr();
            PMem::alloc(sizeof(N4), (void **)&n4Ptr, &(oplogs[oplogsCount].newNodeOid));
            oplogsCount++;
            flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
            smp_wmb();

            N4 *n4 = (N4 *)new (n4Ptr.getVaddr()) N4(level + prefixLength, &k[level], prefixLength);
            NVMPtr<N> pTid(0, (unsigned long)N::SetLeaf(tid));

            n4->Insert(k[level + prefixLength], pTid, false);
            n4->Insert(key[level + prefixLength], nextNodePtr, false);
            flushToNVM(reinterpret_cast<char *>(n4), sizeof(N4));
            smp_wmb();
            N::Change(node, k[level - 1], n4Ptr);
            oplogs[oplogsCount].op = OpStruct::done;

            oplogsCount = 0;
            flushToNVM(reinterpret_cast<char *>(&oplogsCount), sizeof(uint64_t));
            smp_wmb();
            node->WriteUnlock();
            return;
        }
        level++;
    }
}

void Tree::Remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
    EpochGuard epochGuard(threadInfo);
restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root.getVaddr();
    uint8_t nodeKey = 0;
    uint32_t level = 0;
    NVMPtr<char> valuePtr;
    valuePtr.setRawPtr((void *)tid);

    while (true) {
        N *parentNode = node;
        uint8_t parentKey = nodeKey;
        node = nextNode;
        auto v = node->GetVersion();

        switch (CheckPrefix(node, k, level)) {
            case CheckPrefixResult::NO_MATCH:
                if (N::IsObsolete(v) || !node->ReadUnlockOrRestart(v)) {
                    goto restart;
                }
                return;
            case CheckPrefixResult::OPTIMISTIC_MATCH:
                // fallthrough
            case CheckPrefixResult::MATCH: {
                nodeKey = k[level];
                nextNode = N::GetChild(nodeKey, node);
                if (nextNode == nullptr) {
                    if (N::IsObsolete(v) || !node->ReadUnlockOrRestart(v)) {
                        goto restart;
                    }
                    return;
                }
                if (N::IsLeaf(nextNode)) {
                    node->LockVersionOrRestart(v, needRestart, genId);

                    if (needRestart) {
                        goto restart;
                    }
                    if (N::GetLeaf(nextNode) != (unsigned long)valuePtr.getVaddr()) {
                        node->WriteUnlock();
                        return;
                    }
                    DCHECK(parentNode == nullptr || node->GetCount() != 1);

                    if (node->GetCount() == removeConditionCount && node != (N *)(root.getVaddr())) {
                        // 1. check remaining entries
                        N *secondNodeN;
                        NVMPtr<N> secondNodePtr;
                        uint8_t secondNodeK;
                        std::tie(secondNodePtr, secondNodeK) = N::GetSecondChild(node, nodeKey);
                        secondNodeN = secondNodePtr.getVaddr();
                        CHECK(parentNode != nullptr);
                        if (N::IsLeaf(secondNodeN)) {
                            parentNode->WriteLockOrRestart(needRestart, genId);
                            if (needRestart) {
                                node->WriteUnlock();
                                goto restart;
                            }
                            N::Change(parentNode, parentKey, secondNodePtr);

                            parentNode->WriteUnlock();
                            node->WriteUnlockObsolete();
                            this->epoch.MarkNodeForDeletion(node, threadInfo);
                        } else {
                            uint64_t vChild = secondNodeN->GetVersion();
                            secondNodeN->LockVersionOrRestart(vChild, needRestart, genId);
                            if (needRestart) {
                                node->WriteUnlock();
                                goto restart;
                            }
                            parentNode->WriteLockOrRestart(needRestart, genId);
                            if (needRestart) {
                                node->WriteUnlock();
                                secondNodeN->WriteUnlock();
                                goto restart;
                            }

                            N::Change(parentNode, parentKey, secondNodePtr);
                            secondNodeN->AddPrefixBefore(node, secondNodeK);

                            parentNode->WriteUnlock();
                            node->WriteUnlockObsolete();
                            this->epoch.MarkNodeForDeletion(node, threadInfo);
                            secondNodeN->WriteUnlock();
                        }
                    } else {
                        N::RemoveAndUnlock(node, k[level], parentNode, parentKey, threadInfo, needRestart, &oplogs[0], genId);
                    }
                    if (needRestart) {
                        goto restart;
                    }
                    return;
                }
                level++;
            }
        }
    }
}

Tree::CheckPrefixResult Tree::CheckPrefix(N *n, const Key &k, uint32_t &level) {
    if (k.GetKeyLen() <= n->GetLevel()) {
        return CheckPrefixResult::NO_MATCH;
    }
    Prefix p = n->GetPrefix();
    if (p.prefixCount + level < n->GetLevel()) {
        level = n->GetLevel();
        return CheckPrefixResult::OPTIMISTIC_MATCH;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->GetLevel());
             i < std::min(p.prefixCount, MAX_STORED_PREFIX_LENGTH); ++i) {
            if (p.prefix[i] != k[level]) {
                return CheckPrefixResult::NO_MATCH;
            }
            ++level;
        }
        if (p.prefixCount > MAX_STORED_PREFIX_LENGTH) {
            level += p.prefixCount - MAX_STORED_PREFIX_LENGTH;
            return CheckPrefixResult::OPTIMISTIC_MATCH;
        }
    }
    return CheckPrefixResult::MATCH;
}

Tree::CheckPrefixPessimisticResult Tree::CheckPrefixPessimistic(N *n,
                                                                const Key &k,
                                                                uint32_t &level,
                                                                uint8_t &nonMatchingKey,
                                                                Prefix &nonMatchingPrefix,
                                                                LoadKeyFunction loadKeyFunc) {
    Prefix p = n->GetPrefix();
    if (p.prefixCount + level < n->GetLevel()) {
        return CheckPrefixPessimisticResult::SKIPPED_LEVEL;
    }
    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        Key kt;
        for (uint32_t i = ((level + p.prefixCount) - n->GetLevel()); i < p.prefixCount; ++i) {
            if (i == MAX_STORED_PREFIX_LENGTH) {
                loadKeyFunc(N::GetAnyChildTid(n), kt);
            }
            uint8_t curKey = i >= MAX_STORED_PREFIX_LENGTH ? kt[level] : p.prefix[i];
            if (curKey != k[level]) {
                nonMatchingKey = curKey;
                if (p.prefixCount > MAX_STORED_PREFIX_LENGTH) {
                    if (i < MAX_STORED_PREFIX_LENGTH) {
                        loadKeyFunc(N::GetAnyChildTid(n), kt);
                    }
                    for (uint32_t j = 0; j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                                      static_cast<uint32_t>(MAX_STORED_PREFIX_LENGTH));
                         ++j) {
                        nonMatchingPrefix.prefix[j] = kt[level + j + 1];
                    }
                } else {
                    for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                        nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                    }
                }
                return CheckPrefixPessimisticResult::NO_MATCH;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::MATCH;
}

typename Tree::PCCompareResults Tree::CheckPrefixCompare(const N *n,
                                                         const Key &k,
                                                         uint32_t &level,
                                                         LoadKeyFunction loadKeyFunc) {
    Prefix p = n->GetPrefix();
    if (p.prefixCount + level < n->GetLevel()) {
        return PCCompareResults::SKIPPED_LEVEL;
    }
    if (p.prefixCount > 0) {
        Key kt;
        for (uint32_t i = ((level + p.prefixCount) - n->GetLevel()); i < p.prefixCount; ++i) {
            if (i == MAX_STORED_PREFIX_LENGTH) {
                loadKeyFunc(N::GetAnyChildTid(n), kt);
            }
            uint8_t kLevel = (k.GetKeyLen() > level) ? k[level] : 0;

            uint8_t curKey = i >= MAX_STORED_PREFIX_LENGTH ? kt[level] : p.prefix[i];
            if (curKey < kLevel) {
                return PCCompareResults::SMALLER;
            } else if (curKey > kLevel) {
                return PCCompareResults::BIGGER;
            }
            ++level;
        }
    }
    return PCCompareResults::EQUAL;
}

typename Tree::PCEqualsResults Tree::CheckPrefixEquals(const N *n,
                                                       uint32_t &level,
                                                       const Key &start,
                                                       const Key &end,
                                                       LoadKeyFunction loadKeyFunc) {
    Prefix p = n->GetPrefix();
    if (p.prefixCount + level < n->GetLevel()) {
        return PCEqualsResults::SKIPPED_LEVEL;
    }
    if (p.prefixCount > 0) {
        Key kt;
        for (uint32_t i = ((level + p.prefixCount) - n->GetLevel()); i < p.prefixCount; ++i) {
            if (i == MAX_STORED_PREFIX_LENGTH) {
                loadKeyFunc(N::GetAnyChildTid(n), kt);
            }
            uint8_t startLevel = (start.GetKeyLen() > level) ? start[level] : 0;
            uint8_t endLevel = (end.GetKeyLen() > level) ? end[level] : 0;

            uint8_t curKey = i >= MAX_STORED_PREFIX_LENGTH ? kt[level] : p.prefix[i];
            if (curKey > startLevel && curKey < endLevel) {
                return PCEqualsResults::CONTAINED;
            } else if (curKey < startLevel || curKey > endLevel) {
                return PCEqualsResults::NO_MATCH;
            }
            ++level;
        }
    }
    return PCEqualsResults::BOTH_MATCH;
}

void Tree::Copy(TID *result,
                std::size_t &resultsFound,
                std::size_t &resultSize,
                TID &toContinue,
                N *node) const {
    if (N::IsLeaf(node)) {
        if (resultsFound == resultSize) {
            toContinue = N::GetLeaf(node);
            return;
        }
        result[resultsFound] = N::GetLeaf(node);
        resultsFound++;
    } else {
        N *child = N::GetSmallestChild(node, 0);
        Copy(result, resultsFound, resultSize, toContinue, child);
    }
}

void Tree::CopyReverse(TID *result,
                       std::size_t &resultsFound,
                       std::size_t &resultSize,
                       TID &toContinue,
                       N *node) const {
    if (N::IsLeaf(node)) {
        if (resultsFound == resultSize) {
            toContinue = N::GetLeaf(node);
            return;
        }
        result[resultsFound] = N::GetLeaf(node);
        resultsFound++;
    } else {
        N *child = N::GetLargestChild(node, 255);
        CopyReverse(result, resultsFound, resultSize, toContinue, child);
    }
}

bool Tree::FindStart(TID *result,
                     std::size_t &resultsFound,
                     std::size_t &resultSize,
                     const Key &start,
                     TID &toContinue,
                     N *node, N *parentNode,
                     uint32_t level,
                     uint32_t parentLevel) const {
    if (N::IsLeaf(node)) {
        Copy(result, resultsFound, resultSize, toContinue, node);
        return false;
    }

    PCCompareResults prefixResult = CheckPrefixCompare(node, start, level, loadKey);
    switch (prefixResult) {
        case PCCompareResults::BIGGER: {
            N *childNode = nullptr;
            if (start[parentLevel] != 0) {
                childNode = N::GetLargestChild(parentNode, start[parentLevel] - 1);
            }
            if (childNode != nullptr) {
                CopyReverse(result, resultsFound, resultSize, toContinue, childNode);
            } else {
                Copy(result, resultsFound, resultSize, toContinue, node);
            }
            break;
        }
        case PCCompareResults::SMALLER:
            CopyReverse(result, resultsFound, resultSize, toContinue, node);
            break;
        case PCCompareResults::EQUAL: {
            uint8_t startLevel = (start.GetKeyLen() > level) ? start[level] : 0;
            N *childNode = N::GetChild(startLevel, node);
            if (childNode != nullptr) {
                if (start[level] != 0) {
                    FindStart(result, resultsFound, resultSize, start, toContinue, childNode, node, level + 1, level);
                } else {
                    FindStart(result, resultsFound, resultSize, start, toContinue, childNode, parentNode, level + 1,
                              parentLevel);
                }
            } else {
                N *child = N::GetLargestChild(node, startLevel);
                if (child != nullptr) {
                    CopyReverse(result, resultsFound, resultSize, toContinue, child);
                } else {
                    childNode = nullptr;
                    if (start[parentLevel] != 0) {
                        childNode = N::GetLargestChild(parentNode, start[parentLevel] - 1);
                    }
                    if (childNode != nullptr) {
                        CopyReverse(result, resultsFound, resultSize, toContinue, childNode);
                    } else {
                        child = N::GetSmallestChild(node, startLevel);
                        Copy(result, resultsFound, resultSize, toContinue, child);
                    }
                }
            }
            break;
        }
        case PCCompareResults::SKIPPED_LEVEL:
            return true;
    }
    return false;
};

TID Tree::LookupNext(const Key &start, ThreadInfo &threadEpochInfo) const {
    EpochGuardReadonly epochGuard(threadEpochInfo);
    TID toContinue = 0;
    std::size_t resultsFound = 0;
    std::size_t resultSize = 1;
    TID result[resultSize];

restart:
    resultsFound = 0;

    uint32_t level = 0;
    NVMPtr<N> nodePtr = root;
    N *node = nodePtr.getVaddr();
    if (FindStart(result, resultsFound, resultSize, start, toContinue, node, node, level, level)) {
        goto restart;
    }
    if (resultsFound == 0) {
        return 0;
    }
    return result[0];
}

}  // namespace ART_ROWEX
