add_executable(glamdb_core_test core_test.cpp)
target_link_libraries(glamdb_core_test PRIVATE
    glamdb_io glamdb_build_options GTest::gtest_main
)

add_executable(glamdb_io_test
    io/local_fs_test.cpp
    io/sha256_test.cpp
    io/store_uri_test.cpp
    io/store_dirs_test.cpp
    io/io_attempt_test.cpp
    io/durable_file_test.cpp
    io/durable_copy_test.cpp
    io/fault_fs_test.cpp
    io/durability_trace_test.cpp
    ../support/fault_fs.cpp
    ../support/durability_trace.cpp
)
target_include_directories(glamdb_io_test PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_io_test PRIVATE
    glamdb_io glamdb_build_options GTest::gtest_main
)
gtest_discover_tests(glamdb_io_test
    TEST_PREFIX "glamdb.unit."
    PROPERTIES LABELS "unit" TIMEOUT 10
)

if(GLAMDB_BUILD_STORAGE)
    add_subdirectory(storage)
endif()
gtest_discover_tests(glamdb_core_test
    TEST_PREFIX "glamdb.unit."
    PROPERTIES LABELS "unit" TIMEOUT 10
)
