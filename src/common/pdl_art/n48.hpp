#include "common/pdl_art/n.h"

namespace ART_ROWEX {

NVMPtr<N> N48::GetChildNVMPtrByIndex(uint8_t k) const {
    NVMPtr<N> child = children[k].load();
    while (child.isDirty()) {  // DL
        child = children[k].load();
    }
    return child;
}

bool N48::Insert(uint8_t key, NVMPtr<N> n) {
    return Insert(key, n, true);
}

bool N48::Insert(uint8_t key, NVMPtr<N> n, bool flush) {
    auto compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    if (compactCount == n48ElementCount) {
        return false;
    }
    n.markDirty();  // DL
    children[compactCount].store(n, std::memory_order_release);
    if (flush)
        flushToNVM(reinterpret_cast<char *>(&children[compactCount]), sizeof(NVMPtr<N>));
    childIndex[key].store(compactCount, std::memory_order_release);
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&childIndex[key]), sizeof(uint8_t));
        smp_wmb();
    }
    uint32_t increaseCountValues = (1 << 16) + 1;
    countValues += increaseCountValues;  // visible point
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
        smp_wmb();
    }
    n.markClean();  // DL
    children[compactCount].store(n, std::memory_order_release);
    return true;
}

template <class NODE>
void N48::CopyTo(NODE *n) const {
    for (unsigned i = 0; i < childrenIndexCount; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker) {
            n->Insert(i, GetChildNVMPtrByIndex(index), false);
        }
    }
}

void N48::Change(uint8_t key, NVMPtr<N> val) {
    uint8_t index = childIndex[key].load();
    DCHECK(index != emptyMarker);
    val.markDirty();  // DL
    children[index].store(val, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[index]), sizeof(NVMPtr<N>));
    smp_wmb();
    val.markClean();  // DL
    children[index].store(val, std::memory_order_release);
}

NVMPtr<N> N48::GetChildNVMPtr(uint8_t k) const {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        NVMPtr<N> nullPtr(0, 0);
        return nullPtr;
    } else {
        return GetChildNVMPtrByIndex(index);
    }
}

N *N48::GetChild(uint8_t k) const {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
        NVMPtr<N> child = GetChildNVMPtrByIndex(index);
        N *rawChild = child.getVaddr();
        return rawChild;
    }
}

bool N48::Remove(uint8_t k, bool force) {
    auto count = static_cast<uint16_t>(countValues);
    if (count == smallestCount && !force) {
        return false;
    }
    DCHECK(childIndex[k] != emptyMarker);
    NVMPtr<N> nullPtr(0, 0);
    nullPtr.markDirty();
    children[childIndex[k]].store(nullPtr, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&children[childIndex[k]]), sizeof(NVMPtr<N>));
    childIndex[k].store(emptyMarker, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(&childIndex[k]), sizeof(uint8_t));
    smp_wmb();
    countValues -= 1;  // visible point
    flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
    smp_wmb();
    DCHECK(GetChild(k) == nullptr);
    nullPtr.markClean();
    children[childIndex[k]].store(nullPtr, std::memory_order_release);
    return true;
}

N *N48::GetAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < n48ElementCount; i++) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            }
            anyChild = rawChild;
        }
    }
    return anyChild;
}

N *N48::GetAnyChildReverse() const {
    N *anyChild = nullptr;
    for (int i = 47; i >= 0; i--) {
        NVMPtr<N> child = GetChildNVMPtrByIndex(i);
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            if (N::IsLeaf(rawChild)) {
                return rawChild;
            }
            anyChild = rawChild;
        }
    }
    return anyChild;
}

void N48::DeleteChildren() {
    for (unsigned i = 0; i < childrenIndexCount; i++) {
        if (childIndex[i] != emptyMarker) {
            NVMPtr<N> child = GetChildNVMPtrByIndex(i);
            N *rawChild = child.getVaddr();
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

void N48::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            NVMPtr<N> child = this->children[index].load();
            while (child.isDirty()) {  // DL
                child = this->children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                children[childrenCount] = std::make_tuple(i, rawChild);
                childrenCount++;
            }
        }
    }
}

N *N48::GetSmallestChild(uint8_t start) const {
    N *smallestChild = nullptr;
    for (int i = start; i < childrenIndexCount; ++i) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            NVMPtr<N> child = children[index].load();
            while (child.isDirty()) {  // DL
                child = children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
    }
    return smallestChild;
}
N *N48::GetLargestChild(uint8_t end) const {
    N *largestChild = nullptr;
    for (int i = end; i >= 0; --i) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker) {
            NVMPtr<N> child = children[index].load();
            while (child.isDirty()) {  // DL
                child = children[index].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                return rawChild;
            }
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
