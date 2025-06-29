#include "heap/nvm_heap.h"

namespace NVMDB {

TableSpace *g_heapSpace = nullptr;

// pass in all dirs, HEAP_FILENAME:"heap"
// TableSpace is also a logical file
void HeapCreate(const std::shared_ptr<DirectoryConfig>& dirConfig) {
    g_heapSpace = new TableSpace(dirConfig, true);
    g_heapSpace->create();
}

void HeapBootStrap(const std::shared_ptr<DirectoryConfig>& dirConfig) {
    g_heapSpace = new TableSpace(dirConfig, false);
    g_heapSpace->mount();
}

void HeapExitProcess() {
    if (g_heapSpace != nullptr) {
        g_heapSpace->unmount();
        delete g_heapSpace;
        g_heapSpace = nullptr;
    }
}

}  // namespace NVMDB