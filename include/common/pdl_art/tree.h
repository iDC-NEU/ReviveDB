#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H

#include "common/pdl_art/n.h"

using namespace ART;

namespace ART_ROWEX {

class Tree {
public:
    static constexpr uint64_t removeConditionCount = 2;
    static constexpr uint64_t defaultGenId = 3;
    using LoadKeyFunction = void (*)(TID tid, Key &key);
    uint64_t genId{0};
    LoadKeyFunction loadKey{nullptr};

    void Copy(TID *result, std::size_t &resultsFound, std::size_t &resultSize, TID &toContinue, N *node) const;
    void CopyReverse(TID *result, std::size_t &resultsFound, std::size_t &resultSize, TID &toContinue, N *node) const;
    bool FindStart(TID *result, std::size_t &resultsFound, std::size_t &resultSize, const Key &start, TID &toContinue,
                   N *node, N *parentNode, uint32_t level, uint32_t parentLevel) const;

    enum class CheckPrefixResult : uint8_t { MATCH, NO_MATCH, OPTIMISTIC_MATCH };

    enum class CheckPrefixPessimisticResult : uint8_t { MATCH, NO_MATCH, SKIPPED_LEVEL };

    enum class PCCompareResults : uint8_t { SMALLER, EQUAL, BIGGER, SKIPPED_LEVEL };
    enum class PCEqualsResults : uint8_t { PARTIAL_MATCH, BOTH_MATCH, CONTAINED, NO_MATCH, SKIPPED_LEVEL };
    static CheckPrefixResult CheckPrefix(N *n, const Key &k, uint32_t &level);

    static CheckPrefixPessimisticResult CheckPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                               uint8_t &nonMatchingKey, Prefix &nonMatchingPrefix,
                                                               LoadKeyFunction loadKey);

    static PCCompareResults CheckPrefixCompare(const N *n, const Key &k, uint32_t &level, LoadKeyFunction loadKey);

    static PCEqualsResults CheckPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                             LoadKeyFunction loadKey);

    void Recover();
    explicit Tree(LoadKeyFunction loadKey);

    Tree(const Tree &) = delete;

    Tree &operator=(const Tree &) = delete;

    Tree(Tree &&t) noexcept : root(t.root), loadKey(t.loadKey) {}

    Tree &operator=(Tree &&t) noexcept {
        root = t.root;
        loadKey = t.loadKey;
        return *this;
    }

    ~Tree();

    ThreadInfo GetThreadInfo();

    TID Lookup(const Key &k, ThreadInfo &threadEpochInfo) const;

    TID LookupNext(const Key &k, ThreadInfo &threadEpochInfo) const;

    void Insert(const Key &k, TID tid, ThreadInfo &epochInfo);
    void Remove(const Key &k, TID tid, ThreadInfo &epochInfo);

private:
    NVMPtr<N> root;

    TID CheckKey(TID tid, const Key &k) const;

    Epoch epoch{256};
    OpStruct oplogs[10000];
    uint64_t oplogsCount{0};
};

}  // namespace ART_ROWEX
#endif  // ART_ROWEX_TREE_H
