add_executable(glamdb_storage_test
    table_profile_test.cpp metadata_validation_test.cpp capacity_test.cpp format_verifier_test.cpp
    durable_local_file_io_test.cpp append_stager_test.cpp
    publication_guard_test.cpp
    data_file_writer_test.cpp
    "${PROJECT_SOURCE_DIR}/tests/support/fault_fs.cpp"
)
target_include_directories(glamdb_storage_test PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_storage_test PRIVATE
    glamdb_storage glamdb_build_options GTest::gtest_main
)
gtest_discover_tests(glamdb_storage_test
    TEST_PREFIX "glamdb.unit."
    PROPERTIES LABELS "unit" TIMEOUT 30
)
