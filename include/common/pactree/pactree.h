#ifndef pactreeAPI_H
#define pactreeAPI_H

#include "common/pactree/pactree_impl.h"

namespace NVMDB {

class PACTree {
public:
    explicit PACTree() {  // dir not used, get from g_dirPaths
        pt = InitPT();
    }

    ~PACTree() {
        pt->~PACTreeImpl();
        pt = nullptr;
        PMem::UnmountPMEMPool();
        LNodeReport();
    }

    bool Insert(Key_t &key, Val_t val) {
        return pt->Insert(key, val);
    }

    Val_t lookup(Key_t &key, bool *found) {
        return pt->Lookup(key, found);
    }

    void scan(Key_t &startKey,
              Key_t &endKey,
              int max_range,
              LookupSnapshot snapshot,
              bool reverse,
              std::vector<std::pair<Key_t, Val_t>> &result) {
        pt->Scan(startKey, endKey, max_range, snapshot, reverse, result);
    }

    void registerThread() {
        pt->RegisterThread();
    }

    void unregisterThread() {
        pt->UnregisterThread();
    }

private:
    PACTreeImpl *pt;
};

}  // namespace NVMDB

#endif
