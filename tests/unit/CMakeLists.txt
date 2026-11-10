add_executable(glamdb_core_test core_test.cpp)
target_link_libraries(glamdb_core_test PRIVATE
    glamdb_io glamdb_build_options GTest::gtest_main
)

file(GLOB_RECURSE io_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/io/*.cpp")
add_executable(glamdb_io_test
    ${io_test_sources}
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
