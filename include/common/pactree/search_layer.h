#ifndef SEARCHLAYER_H
#define SEARCHLAYER_H

#include "common/pdl_art/tree.h"

constexpr int DEFAULT_INSERT_NUM = 2;

inline void LoadIntKeyFunction(TID tid, Key &k) {
    k.SetInt(*reinterpret_cast<uint64_t *>(tid));
}

inline void LoadStringKeyFunction(TID tid, Key &k) {
    auto pkey = reinterpret_cast<Key_t *>(tid);
    k.Set(pkey->getData(), pkey->keyLength);
}

class PDLARTIndex {
public:
    int group{0};
    uint8_t grpMask{0};
    PDLARTIndex() {
        PMEMoid oid;
        PMem::alloc(sizeof(ART_ROWEX::Tree), (void **)&idxPtr, &oid);
        idx = new (idxPtr.getVaddr()) ART_ROWEX::Tree(LoadStringKeyFunction);
        dummyIdx = new ART_ROWEX::Tree(LoadIntKeyFunction);
    }

    ~PDLARTIndex() {
        delete dummyIdx;
    }

    void Init() {
        idx = (ART_ROWEX::Tree *)(idxPtr.getVaddr());
        idx->genId++;
        idx->loadKey = LoadStringKeyFunction;
        dummyIdx = new ART_ROWEX::Tree(LoadIntKeyFunction);
    }

    void SetGroupId(int nma) {
        this->group = nma;
        this->grpMask = 1 << nma;
    }

    void SetKey(Key &k, uint64_t key) {
        k.SetInt(key);
    }

    void SetKey(Key &k, StringKey<KEYLENGTH> key) {
        k.Set(key.getData(), key.keyLength);
    }

    bool Insert(Key_t key, void *ptr) {
        auto t = dummyIdx->GetThreadInfo();
        Key k;
        SetKey(k, key);
        idx->Insert(k, reinterpret_cast<unsigned long>(ptr), t);
        if (key < curMin)
            curMin = key;
        numInserts++;
        return true;
    }

    bool remove(Key_t key, void *ptr) {
        auto t = dummyIdx->GetThreadInfo();
        Key k;
        SetKey(k, key);
        idx->Remove(k, reinterpret_cast<unsigned long>(ptr), t);
        numInserts--;
        return true;
    }

    // Gets the value of the key if present or the value of key just less than/greater than key
    void *lookup(Key_t &key) {
        auto t = dummyIdx->GetThreadInfo();
        Key endKey;
        SetKey(endKey, key);

        auto result = idx->LookupNext(endKey, t);
        return reinterpret_cast<void *>(result);
    }

    void *lookup2(Key_t key) {
        if (key <= curMin) {
            return nullptr;
        }
        auto t = dummyIdx->GetThreadInfo();
        Key endKey;
        SetKey(endKey, key);

        auto result = idx->Lookup(endKey, t);
        return reinterpret_cast<void *>(result);
    }

    // Art segfaults if range operation is done when there are less than 2 keys
    bool IsEmpty() {
        return (numInserts < DEFAULT_INSERT_NUM);
    }

    uint32_t Size() {
        return numInserts;
    }
private:
    Key minKey;
    Key_t curMin;
    NVMPtr<ART_ROWEX::Tree> idxPtr;
    ART_ROWEX::Tree *idx{nullptr};
    ART_ROWEX::Tree *dummyIdx{nullptr};
    uint32_t numInserts{0};
};

using SearchLayer = PDLARTIndex;

#endif
