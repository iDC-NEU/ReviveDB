cmake_minimum_required(VERSION 3.14...3.22)

set(PHMAP_LIB "${CMAKE_BINARY_DIR}/include/parallel_hashmap/phmap.h")

if(NOT EXISTS "${PHMAP_LIB}")
    CPMAddPackage(
            NAME phmap
            GITHUB_REPOSITORY greg7mdp/parallel-hashmap
            VERSION v1.3.12
            GIT_TAG e5b892baed478513adcb6425773cae1eda033057
            DOWNLOAD_ONLY True
    )

    execute_process(
            COMMAND ${CMAKE_COMMAND} -E copy_directory "${phmap_SOURCE_DIR}/parallel_hashmap" "${CMAKE_BINARY_DIR}/include/parallel_hashmap"
            RESULT_VARIABLE copy_result
            ERROR_VARIABLE copy_error
    )

    if(NOT copy_result EQUAL "0")
        message(FATAL_ERROR "Failed to copy directory: ${copy_error}")
    else()
        message(STATUS "Successfully copied ${SOURCE_DIR} to ${TARGET_DIR}")
    endif()
endif()
