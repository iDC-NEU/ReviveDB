#include "index/nvm_index.h"

namespace NVMDB {

static PACTree *g_pt = nullptr;

void IndexBootstrap() {
    DCHECK(g_pt == nullptr);
    g_pt = new PACTree();
}

PACTree *GetGlobalPACTree() {
    DCHECK(g_pt != nullptr);
    return g_pt;
}

void IndexExitProcess() {
    delete g_pt;
    g_pt = nullptr;
}

void InitLocalIndex() {
    DCHECK(g_pt != nullptr);
    g_pt->registerThread();
}

void DestroyLocalIndex() {
    DCHECK(g_pt != nullptr);
    g_pt->unregisterThread();
}

}  // namespace NVMDB