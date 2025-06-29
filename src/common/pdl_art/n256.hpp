#include "common/pdl_art/n.h"

namespace ART_ROWEX {

NVMPtr<N> N256::GetChildNVMPtrByIndex(uint8_t k) const {
    NVMPtr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

void N256::DeleteChildren() {
    for (uint64_t i = 0; i < childrenIndexCount; ++i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

bool N256::Insert(uint8_t key, NVMPtr<N> val) {
    return Insert(key, val, true);
}

bool N256::Insert(uint8_t key, NVMPtr<N> val, bool flush) {
    val.markDirty();  // DL
    children[key].store(val, std::memory_order_release);
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&children[key]), sizeof(NVMPtr<N>));
        smp_wmb();
    }
    uint32_t increaseCountValues = (1 << 16);
    countValues += increaseCountValues + 1;  // visible point
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
        smp_wmb();
    }
    val.markClean();  // DL
    children[key].store(val, std::memory_order_release);
    return true;
}

template <class NODE>
void N256::CopyTo(NODE *n) const {
    for (int i = 0; i < childrenIndexCount; ++i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            n->Insert(i, child, false);
        }
    }
}

void N256::Change(uint8_t key, NVMPtr<N> n) {
    n.markDirty();  // DL
    children[key].store(n, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[key]), sizeof(NVMPtr<N>));
    smp_wmb();
    n.markClean();  // DL
    children[key].store(n, std::memory_order_release);
}

NVMPtr<N> N256::GetChildNVMPtr(uint8_t k) const {
    NVMPtr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

N *N256::GetChild(uint8_t k) const {
    NVMPtr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    N *rawChild = child.getVaddr();
    return rawChild;
}

bool N256::Remove(uint8_t k, bool force) {
    auto count = static_cast<uint16_t>(countValues);
    if (count == smallestCount - 1 && !force) {
        return false;
    }
    DCHECK(force || count > smallestCount - 1);

    NVMPtr<N> nullPtr(0, 0);
    nullPtr.markDirty();  // DL
    children[k].store(nullPtr, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[k]), sizeof(NVMPtr<N>));
    smp_wmb();
    countValues -= 1;  // visible point
    flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
    smp_wmb();
    nullPtr.markClean();  // DL
    children[k].store(nullPtr, std::memory_order_release);
    return true;
}

N *N256::GetAnyChild() const {
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < childrenIndexCount; ++i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            } else {
                anyChild = rawChild;
            }
        }
    }
    return anyChild;
}

N *N256::GetAnyChildReverse() const {
    N *anyChild = nullptr;
    for (int i = 255; i >= 0; --i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            } else {
                anyChild = rawChild;
            }
        }
    }
    return anyChild;
}

void N256::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            children[childrenCount] = std::make_tuple(i, rawChild);
            childrenCount++;
        }
    }
}

N *N256::GetSmallestChild(uint8_t start) const {
    N *smallestChild = nullptr;
    for (int i = start; i < childrenIndexCount; ++i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            return rawChild;
        }
    }
    return smallestChild;
}

N *N256::GetLargestChild(uint8_t end) const {
    N *largestChild = nullptr;
    for (int i = end; i >= 0; --i) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            return rawChild;
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
