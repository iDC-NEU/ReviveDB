cmake_minimum_required(VERSION 3.14...3.22)

# We only need the header, the other parts are not useful
set(GTEST_LIB "${CMAKE_BINARY_DIR}/include/gtest/gtest.h")

if(NOT EXISTS "${GTEST_LIB}")
    CPMAddPackage(
            NAME googletest
            GITHUB_REPOSITORY google/googletest
            GIT_TAG v1.13.0
            VERSION 1.13.0
            OPTIONS "INSTALL_GTEST OFF"
            GIT_SHALLOW TRUE
    )

    message("Start configure googletest")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -Dgtest_force_shared_crt=OFF -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for googletest failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for googletest failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for googletest failed: ${result}")
    endif()
endif()
