cmake_minimum_required(VERSION 3.14...3.22)

set(RPMA_LIB "${CMAKE_BINARY_DIR}/include/librpma.h")

if(NOT EXISTS "${RPMA_LIB}")
    CPMAddPackage(
            NAME rpma
            GITHUB_REPOSITORY pmem/rpma
            VERSION 1.3.0
            GIT_TAG f52c00d18821ac573a71e9f23a6d2e8695086e95
            DOWNLOAD_ONLY True
    )
    list(APPEND RPMA_PREFIX_PATH ${PMDK_HOME})

    message("Start configure rpma")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(
            COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_EXAMPLES=OFF -DBUILD_DOC=OFF -DBUILD_TESTS=OFF -DBUILD_DEVELOPER_MODE=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_PREFIX_PATH=${RPMA_PREFIX_PATH}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rpma_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for rpma failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rpma_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for rpma failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rpma_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for rpma failed: ${result}")
    endif()
endif()
