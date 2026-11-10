file(GLOB_RECURSE storage_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
add_executable(glamdb_storage_test
    ${storage_test_sources}
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
