cmake_minimum_required(VERSION 3.14...3.22)

set(RDMA_CORE_LIB "${CMAKE_BINARY_DIR}/include/infiniband/verbs.h")

if(NOT EXISTS "${RDMA_CORE_LIB}")
    CPMAddPackage(
            NAME rdma-core
            URL https://github.com/linux-rdma/rdma-core/releases/download/v51.0/rdma-core-51.0.tar.gz
            DOWNLOAD_ONLY True
    )

    message("Start configure rdma-core")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DNO_MAN_PAGES=1 -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rdma-core_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for rdma-core failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rdma-core_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for rdma-core failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rdma-core_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for rdma-core failed: ${result}")
    endif()
endif()
