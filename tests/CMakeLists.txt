cmake_minimum_required(VERSION 3.14...3.22)

project(ReviveDBTests LANGUAGES CXX)

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fprofile-arcs -ftest-coverage")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -ffast-math -march=native")

# ---- Options ----

#option(ENABLE_TEST_COVERAGE "Enable test coverage" OFF)

# ---- Add GreeterTests ----
include(FetchContent)
FetchContent_Declare(
        googletest
        # Specify the commit you depend on and update it regularly.
        URL https://github.com/google/googletest/archive/fa6de7f4382f5c8fb8b9e32eea28a2eb44966c32.zip
        DOWNLOAD_EXTRACT_TIMESTAMP FALSE
)
set(BUILD_GMOCK FALSE)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# ---- Create binary ----

file(GLOB_RECURSE sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(ReviveDBTests ${headers} ${sources} ${nvm_core_src_list})
target_include_directories(ReviveDBTests PUBLIC ${nvm_core_inc_list} ${PROJECT_INCLUDE_DIR})

find_package(GTest)
enable_testing()
#include_directories(${GTEST_INCLUDE_DIRS})

gtest_discover_tests(ReviveDBTests)

target_link_options(ReviveDBTests PUBLIC -std=c++14)
target_link_directories(ReviveDBTests PRIVATE
        ${CMAKE_BINARY_DIR}/lib
        ${SECUREDYNAMICLIB_HOME}
        ${TBB_LIB_PATH}
        ${PMDK_LIB_PATH})
# The -Wl,-R part tells the resulting binary to also look for the library in
# /usr/local/lib at runtime before trying to use the one in /usr/lib/.
target_link_libraries(ReviveDBTests PRIVATE glog gflags gtest gtest_main numa
        "-Wl,-R/usr/local/lib64" ibverbs
        "-Wl,-R${SECUREDYNAMICLIB_HOME}" securec
        "-Wl,-R${TBB_LIB_PATH}" tbb
        ${CMAKE_DL_LIBS} 
        pmemobj pmem dash
        stdc++fs ndctl daxctl pthread)

# "-l:libtbb.so.12.11"
target_compile_options(ReviveDBTests PRIVATE ${nvm_core_COMPILE_OPTIONS})

target_include_directories(
        ReviveDBTests PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include/${PROJECT_NAME}-${PROJECT_VERSION}>
)
# ---- code coverage ----

#if(ENABLE_TEST_COVERAGE)
#    target_compile_options(gausskernel_storage_nvm_core PUBLIC -O0 -g -fprofile-arcs -ftest-coverage)
#    target_link_options(gausskernel_storage_nvm_core PUBLIC -fprofile-arcs -ftest-coverage)
#endif()
