#include "common/pdl_art/n.h"

namespace ART_ROWEX {

bool N16::Insert(uint8_t key, NVMPtr<N> n) {
    return Insert(key, n, true);
}
bool N16::Insert(uint8_t key, NVMPtr<N> n, bool flush) {
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    if (compactCount == n16ElementCount) {
        return false;
    }

    n.markDirty();  // DL
    children[compactCount].store(n, std::memory_order_release);
    if (flush) {
        flushToNVM(reinterpret_cast<char *>(&children[compactCount]), sizeof(NVMPtr<N>));
        smp_wmb();
    }

    keys[compactCount].store(FlipSign(key), std::memory_order_release);
    uint32_t increaseCountValues = (1 << n16ElementCount) + 1;
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
void N16::CopyTo(NODE *n) const {
    uint16_t compactCount = static_cast<uint16_t>(countValues >> compactCountOffset);
    for (unsigned i = 0; i < compactCount; i++) {
        NVMPtr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            n->Insert(FlipSign(keys[i]), child, false);
        }
    }
}

void N16::change(uint8_t key, NVMPtr<N> val) {
    auto childPos = getChildPos(key);
    DCHECK(childPos != nullptr);
    val.markDirty();  // DL
    childPos->store(val, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(childPos), sizeof(NVMPtr<N>));
    smp_wmb();
    val.markClean();  // DL
    childPos->store(val, std::memory_order_release);
}

std::atomic<NVMPtr<N>> *N16::getChildPos(uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(FlipSign(k)), _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = Ctz(bitfield);
        NVMPtr<N> child = children[pos].load();
        N *rawChild = child.getVaddr();

        if (rawChild != nullptr) {
            return &children[pos];
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
}

NVMPtr<N> N16::GetChildNVMPtr(uint8_t k) const {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(FlipSign(k)), _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = Ctz(bitfield);
        NVMPtr<N> child = children[pos].load();
        while (child.isDirty()) {  // DL
            child = children[pos].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[pos].load() == FlipSign(k)) {
            return child;
        }
        bitfield = bitfield ^ (1 << pos);
    }

    NVMPtr<N> nullPtr(0, 0);
    return nullPtr;
}

N *N16::GetChild(uint8_t k) const {
    __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(FlipSign(k)), _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = Ctz(bitfield);
        NVMPtr<N> child = children[pos].load();
        while (child.isDirty()) {  // DL
            child = children[pos].load();
        }
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr && keys[pos].load() == FlipSign(k)) {
            return rawChild;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
}

bool N16::Remove(uint8_t k, bool force) {
    uint16_t count = static_cast<uint16_t>(countValues);
    if (count == smallestCount && !force) {
        return false;
    }
    auto leafPlace = getChildPos(k);
    DCHECK(leafPlace != nullptr);
    NVMPtr<N> nullPtr(0, 0);
    nullPtr.markDirty();
    leafPlace->store(nullPtr, std::memory_order_release);
    flushToNVM(reinterpret_cast<char *>(leafPlace), sizeof(NVMPtr<N>));
    smp_wmb();
    countValues -= 1;  // visible point
    flushToNVM(reinterpret_cast<char *>(this), L1_CACHE_BYTES);
    smp_wmb();
    nullPtr.markClean();
    leafPlace->store(nullPtr, std::memory_order_release);
    return true;
}

N *N16::GetAnyChild() const {
    N *anyChild = nullptr;
    for (int i = 0; i < n16ElementCount; ++i) {
        NVMPtr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
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

N *N16::GetAnyChildReverse() const {
    N *anyChild = nullptr;
    for (int i = 15; i >= 0; --i) {
        NVMPtr<N> child = children[i].load();
        while (child.isDirty()) {  // DL
            child = children[i].load();
        }
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

void N16::DeleteChildren() {
    uint16_t compactCount = static_cast<uint16_t>(countValues >> 16);
    for (std::size_t i = 0; i < compactCount; ++i) {
        NVMPtr<N> child = children[i].load();
        N *rawChild = child.getVaddr();
        if (rawChild != nullptr) {
            N::DeleteChildren(rawChild);
            N::DeleteNode(rawChild);
        }
    }
}

void N16::GetChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children, uint32_t &childrenCount) const {
    childrenCount = 0;
    uint16_t compactCount = static_cast<uint16_t>(countValues >> 16);
    for (int i = 0; i < compactCount; ++i) {
        uint8_t key = FlipSign(this->keys[i]);
        if (key >= start && key <= end) {
            NVMPtr<N> child = this->children[i].load();
            while (child.isDirty()) {  // DL
                child = this->children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                children[childrenCount] = std::make_tuple(key, rawChild);
                childrenCount++;
            }
        }
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) { return std::get<0>(first) < std::get<0>(second); });
}
N *N16::GetSmallestChild(uint8_t start) const {
    N *smallestChild = nullptr;
    uint8_t minKey = 255;
    for (int i = 0; i < n16ElementCount; ++i) {
        uint8_t key = FlipSign(this->keys[i]);
        if (key >= start && key <= minKey) {
            NVMPtr<N> child = this->children[i].load();
            while (child.isDirty()) {  // DL
                child = this->children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                minKey = key;
                smallestChild = rawChild;
            }
        }
    }
    return smallestChild;
}
N *N16::GetLargestChild(uint8_t end) const {
    N *largestChild = nullptr;
    uint8_t maxKey = 0;
    for (int i = 0; i < n16ElementCount; ++i) {
        uint8_t key = FlipSign(this->keys[i]);
        if (key <= end && key >= maxKey) {
            NVMPtr<N> child = this->children[i].load();
            while (child.isDirty()) {  // DL
                child = children[i].load();
            }
            N *rawChild = child.getVaddr();
            if (rawChild != nullptr) {
                maxKey = key;
                largestChild = rawChild;
            }
        }
    }
    return largestChild;
}
}  // namespace ART_ROWEX
