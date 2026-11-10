cmake_minimum_required(VERSION 3.28)

project(GlamDB VERSION 0.1.0 LANGUAGES C CXX)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR "Use an out-of-source build: cmake --preset local")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CTest)

option(GLAMDB_BUILD_DEPENDENCIES "Build and install pinned dependencies" OFF)
if(GLAMDB_BUILD_DEPENDENCIES)
    add_subdirectory(cmake/dependencies)
    return()
endif()

include(GNUInstallDirs)
include(cmake/CompilerOptions.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Quality.cmake)

add_subdirectory(core)
add_subdirectory(apps)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

add_custom_target(dependency-check
    COMMAND "${CMAKE_COMMAND}" -P
            "${PROJECT_SOURCE_DIR}/cmake/dependencies/VerifySubmodules.cmake"
    VERBATIM
)
