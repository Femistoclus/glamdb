if(NOT EXISTS "${PROJECT_SOURCE_DIR}/third_party/quill/CMakeLists.txt")
    message(FATAL_ERROR "Logging requires the pinned Quill submodule: git submodule update --init third_party/quill")
endif()
find_package(Threads REQUIRED)
add_subdirectory("${PROJECT_SOURCE_DIR}/third_party/quill" "${PROJECT_BINARY_DIR}/third_party/quill"
    EXCLUDE_FROM_ALL SYSTEM)
add_library(glamdb_logging STATIC structured_log.cpp)
# Quill headers are private to this archive; installed consumers need its thread dependency.
target_link_libraries(glamdb_logging PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>"
    "$<BUILD_INTERFACE:quill::quill>" Threads::Threads)
target_include_directories(glamdb_logging PRIVATE "${PROJECT_SOURCE_DIR}")
