#ifndef NVMDB_INDEX_ITER_H
#define NVMDB_INDEX_ITER_H

#include "common/pactree/pactree.h"
#include "common/serializer.h"

namespace NVMDB {

PACTree *GetGlobalPACTree();

class NVMIndexIter {
public:
    NVMIndexIter(Key_t &begin, Key_t &end, LookupSnapshot snapshot, int maxSize, bool reverse)
        : cursor(0), m_keyEnd(end), m_snapshot(snapshot), m_maxSize(maxSize), m_reverse(reverse) {
        if (maxSize == 0) {
            // no size limit, use m_defaultRange
            search(begin, end, m_defaultRange, snapshot, reverse);
            return;
        }
        search(begin, end, maxSize, snapshot, reverse);
    }

    void Next() {
        cursor++;
        if (cursor < m_result.size()) {
            return;
        }
        // 超过范围, 并且 exceed range limit, set invalid
        if (m_maxSize != 0) {
            m_valid = false;
            return;
        }
        Key_t new_kb = LastKey();
        new_kb.next();
        search(new_kb, m_keyEnd, m_defaultRange, m_snapshot, m_reverse);
        cursor = 0;
    }

    [[nodiscard]] bool Valid() const { return m_valid; }

    RowId Curr() {
        DCHECK(m_valid);
        auto& key = m_result[cursor].first;
        BinaryReader reader(key.getData());
        reader.set_position(key.keyLength - 1 - sizeof(uint32));
        CHECK(reader.read_uint8() == CODE_ROWID);
        return (RowId)reader.read_uint32();
    }

    [[nodiscard]] Key_t &LastKey() { return m_result.back().first; }

protected:
    void search(Key_t &kb, Key_t &ke, int max_range, LookupSnapshot snapshot, bool reverse) {
        auto* pt = GetGlobalPACTree();
        pt->scan(kb, ke, max_range, snapshot, reverse, m_result);
        m_valid = !m_result.empty();
    }

private:
    std::vector<std::pair<Key_t, Val_t>> m_result;

    // 搜索结果是否存在
    bool m_valid = false;

    int cursor;

    // maxSize = 0, means no limit
    int m_maxSize;

    bool m_reverse;

    Key_t m_keyEnd;

    LookupSnapshot m_snapshot;

    static constexpr auto m_defaultRange = 6;
};

}  // namespace NVMDB

#endif  // NVMDB_INDEX_ITER_H
