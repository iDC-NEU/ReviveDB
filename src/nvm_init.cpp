#include "nvm_init.h"
#include "undo/nvm_undo.h"
#include "heap/nvm_heap.h"
#include "index/nvm_index.h"
#include "nvmdb_thread.h"

namespace NVMDB {

void InitDB(const std::string& dir) {
    g_dir_config = std::make_shared<DirectoryConfig>(dir, true);
    InitGlobalVariables();
    UndoCreate();
    HeapCreate(g_dir_config);    // heap is one table space
    IndexBootstrap();
}

void BootStrap(const std::string& dir) {
    g_dir_config = std::make_shared<DirectoryConfig>(dir, false);
    InitGlobalVariables();
    HeapBootStrap(g_dir_config);
    IndexBootstrap();
    UndoBootStrap(); // mount the heap so we can undo the logs
}

void ExitDBProcess() {
    IndexExitProcess();
    HeapExitProcess();
    UndoExitProcess();
    DestroyGlobalVariables();
}

}  // namespace NVMDB