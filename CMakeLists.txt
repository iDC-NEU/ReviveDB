cmake_minimum_required(VERSION 3.19)
project(ReviveDB)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH FALSE)

if ((CMAKE_BUILD_TYPE STREQUAL "Debug"))
    set(NVM_MACRO_OPTIONS ${NVM_MACRO_OPTIONS} -DDEBUG)
else()
    set(NVM_MACRO_OPTIONS ${NVM_MACRO_OPTIONS} -DNDEBUG)
endif()

# ---- Add dependencies via CPM ----
# see https://github.com/TheLartians/CPM.cmake for more info
include(vendor/CPM.cmake)

### gflags support
find_package(gflags_install
        CONFIG
        REQUIRED
        PATHS ${CMAKE_CURRENT_SOURCE_DIR}/vendor
)

### glog support
find_package(glog_install
        CONFIG
        REQUIRED
        PATHS ${CMAKE_CURRENT_SOURCE_DIR}/vendor
)

### greg7mdp/gtl gel template repo
# 并行Hashmap, 用于Delta表索引
find_package(phmap_install
        CONFIG
        REQUIRED
        PATHS ${CMAKE_CURRENT_SOURCE_DIR}/vendor
)

include_directories(SYSTEM ${CMAKE_BINARY_DIR}/include)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/tbb/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/pmdk/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/boost/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/Huawei_Secure_C/comm/include)
set(SECUREDYNAMICLIB_HOME ${CMAKE_CURRENT_SOURCE_DIR}/lib/Huawei_Secure_C/Dynamic_Lib)
set(TBB_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib/tbb/lib)
set(PMDK_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib/pmdk/lib)

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE CLWB)
if(CLWB)
    message(STATUS "Use clwb")
    set(nvm_core_DEF_USE_CLWB)
else()
    message(STATUS "Use clflush instead of clwb")
    set(nvm_core_DEF_USE_CLWB -DNO_CLWB)
endif()

if ((CMAKE_BUILD_TYPE STREQUAL "Debug"))
    set(nvm_core_COMPILE_OPTIONS "-pipe;-pthread;-fno-aggressive-loop-optimizations;-fno-expensive-optimizations;-fno-omit-frame-pointer;-fno-strict-aliasing;-freg-struct-return;-msse4.2;-mcx16;-fwrapv;-std=c++14;-fnon-call-exceptions;-O0;-g;-fPIE;-fno-common;-fstack-protector;-Wmissing-format-attribute;-Wno-attributes;-Wno-unused-but-set-variable;-Wno-write-strings;-Wpointer-arith;-march=native")
else()
    set(nvm_core_COMPILE_OPTIONS "-pipe;-pthread;-fno-aggressive-loop-optimizations;-fno-expensive-optimizations;-fno-omit-frame-pointer;-fno-strict-aliasing;-freg-struct-return;-msse4.2;-mcx16;-fwrapv;-std=c++14;-fnon-call-exceptions;-O2;-g3;-fPIE;-fno-common;-fstack-protector;-Wmissing-format-attribute;-Wno-attributes;-Wno-unused-but-set-variable;-Wno-write-strings;-Wpointer-arith;-march=native")
endif()

set(nvm_core_inc_list ${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE nvm_core_src_list "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# tests
add_subdirectory(tests)
