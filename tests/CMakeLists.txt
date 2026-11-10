add_test(NAME glamdb.smoke COMMAND glamdb)
set_tests_properties(glamdb.smoke PROPERTIES LABELS "smoke" TIMEOUT 10)

find_package(GTest 1.17 CONFIG REQUIRED)
add_executable(glamdb_unit unit/api_test.cpp unit/catalog_test.cpp unit/arrow_input_test.cpp
    unit/coordinator_test.cpp unit/outcome_json_test.cpp support/fake_storage.cpp)
target_link_libraries(glamdb_unit PRIVATE glamdb::core GTest::gtest_main glamdb_build_options
    SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static glamdb_json)
target_include_directories(glamdb_unit PRIVATE "${PROJECT_SOURCE_DIR}")
include(GoogleTest)
gtest_discover_tests(glamdb_unit TEST_PREFIX glamdb.unit. PROPERTIES LABELS unit TIMEOUT 30)

add_executable(glamdb_fault fault/coordinator_fault_test.cpp support/fake_storage.cpp support/journal_fault_vfs.cpp)
target_include_directories(glamdb_fault PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_fault PRIVATE glamdb::core glamdb_build_options GTest::gtest_main
    SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
gtest_discover_tests(glamdb_fault TEST_PREFIX glamdb.fault. PROPERTIES LABELS fault TIMEOUT 30)

add_test(NAME glamdb.cli COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/cli_test.py" "$<TARGET_FILE:glamdb>")
set_tests_properties(glamdb.cli PROPERTIES LABELS integration TIMEOUT 30)
add_executable(glamdb_recovery_driver integration/recovery_driver.cpp support/fake_storage.cpp)
target_include_directories(glamdb_recovery_driver PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_recovery_driver PRIVATE glamdb_json glamdb_build_options SQLite::SQLite3
    iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
add_test(NAME glamdb.process_recovery COMMAND "${Python3_EXECUTABLE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/integration/recovery_test.py" "$<TARGET_FILE:glamdb_recovery_driver>")
set_tests_properties(glamdb.process_recovery PROPERTIES LABELS integration TIMEOUT 90)
add_executable(glamdb_sqlcatalog_driver integration/sqlcatalog_driver.cpp)
target_include_directories(glamdb_sqlcatalog_driver PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(glamdb_sqlcatalog_driver PRIVATE glamdb::core glamdb_build_options
    iceberg::iceberg_sql_catalog_static iceberg::iceberg_bundle_static SQLite::SQLite3)
add_test(NAME glamdb.real_sqlcatalog COMMAND "${Python3_EXECUTABLE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/integration/sqlcatalog_test.py" "$<TARGET_FILE:glamdb_sqlcatalog_driver>")
set_tests_properties(glamdb.real_sqlcatalog PROPERTIES LABELS integration TIMEOUT 90)
add_executable(glamdb_public_consumer_compile installed_consumer/main.cpp)
target_link_libraries(glamdb_public_consumer_compile PRIVATE glamdb::core glamdb_build_options)
if(NOT GLAMDB_ENABLE_ASAN AND NOT GLAMDB_ENABLE_UBSAN)
    add_test(NAME glamdb.installed_consumer COMMAND "${CMAKE_COMMAND}"
        "-DENGINE_BINARY_DIR=${PROJECT_BINARY_DIR}"
        "-DTEST_BINARY_DIR=${PROJECT_BINARY_DIR}/installed-consumer"
        "-DTEST_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer"
        "-DDEPENDENCY_PREFIX=${PROJECT_SOURCE_DIR}/.deps/install"
        "-DTEST_TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE}"
        "-DTEST_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer/run.cmake")
    set_tests_properties(glamdb.installed_consumer PROPERTIES LABELS installed TIMEOUT 120)
endif()
