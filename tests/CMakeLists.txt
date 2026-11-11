if(NOT EXISTS "${PROJECT_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
    message(FATAL_ERROR
        "Unit tests require the pinned GoogleTest submodule. "
        "Run: git submodule update --init third_party/googletest "
        "(or configure with -DBUILD_TESTING=OFF)."
    )
endif()

set(BUILD_GMOCK OFF)
set(INSTALL_GTEST OFF)
add_subdirectory("${PROJECT_SOURCE_DIR}/third_party/googletest"
                 "${PROJECT_BINARY_DIR}/third_party/googletest" EXCLUDE_FROM_ALL SYSTEM)
include(GoogleTest)
add_subdirectory(unit)

# The compiler-configuration tests take the active compiler through the environment.
add_test(NAME glamdb.tooling
    COMMAND "${CMAKE_COMMAND}" -E env
        "CC=${CMAKE_C_COMPILER}" "CXX=${CMAKE_CXX_COMPILER}"
        "GLAMDB_TEST_CMAKE=${CMAKE_COMMAND}" "GLAMDB_TEST_GENERATOR=${CMAKE_GENERATOR}"
        "GLAMDB_TEST_TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE}" "GLAMDB_TEST_PREFIX=${CMAKE_PREFIX_PATH}"
        "${Python3_EXECUTABLE}" -m unittest discover
        -s "${CMAKE_CURRENT_SOURCE_DIR}/tooling" -p "test_*.py" -v
)
set_tests_properties(glamdb.tooling PROPERTIES LABELS "tooling" TIMEOUT 240)

if(NOT GLAMDB_IO_ONLY)
    add_test(NAME glamdb.smoke COMMAND glamdb_cli)
    set_tests_properties(glamdb.smoke PROPERTIES LABELS "smoke" TIMEOUT 10)

    file(GLOB_RECURSE unit_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cpp")
    list(FILTER unit_test_sources EXCLUDE REGEX "/unit/(io|storage)/|/unit/core_test\\.cpp$")
    add_executable(glamdb_unit ${unit_test_sources} support/fake_storage.cpp)
    target_link_libraries(glamdb_unit PRIVATE glamdb::core GTest::gtest_main glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static glamdb_json)
    target_include_directories(glamdb_unit PRIVATE "${PROJECT_SOURCE_DIR}")
    target_compile_definitions(glamdb_unit PRIVATE "GLAMDB_TEST_STORAGE=$<BOOL:${GLAMDB_BUILD_STORAGE}>")
    gtest_discover_tests(glamdb_unit TEST_PREFIX glamdb.unit. PROPERTIES LABELS unit TIMEOUT 30)

    if(GLAMDB_BUILD_STORAGE)
        # Planner, cursor and storage error classification over an in-memory
        # FileIO: real encodings in RAM, no filesystem access (label mock).
        file(GLOB_RECURSE mock_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/mock/*.cpp")
        add_executable(glamdb_mock ${mock_test_sources})
        target_include_directories(glamdb_mock PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_mock PRIVATE glamdb::core glamdb_storage glamdb_build_options GTest::gtest_main)
        gtest_discover_tests(glamdb_mock TEST_PREFIX glamdb.mock. PROPERTIES LABELS "mock;scan" TIMEOUT 30)

        # Public API with the real storage backend: Parquet, Iceberg metadata and SQLite.
        file(GLOB_RECURSE vertical_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/vertical/*.cpp")
        add_executable(glamdb_vertical ${vertical_test_sources} support/fault_fs.cpp support/journal_fault_vfs.cpp)
        target_include_directories(glamdb_vertical PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_vertical PRIVATE glamdb::core glamdb_storage glamdb_build_options
            GTest::gtest_main SQLite::SQLite3)
        gtest_discover_tests(glamdb_vertical TEST_PREFIX glamdb.vertical.
            PROPERTIES LABELS "vertical;scan" TIMEOUT 60)
    endif()

    file(GLOB_RECURSE fault_test_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fault/*.cpp")
    add_executable(glamdb_fault ${fault_test_sources} support/fake_storage.cpp support/journal_fault_vfs.cpp)
    target_include_directories(glamdb_fault PRIVATE "${PROJECT_SOURCE_DIR}")
    target_link_libraries(glamdb_fault PRIVATE glamdb::core glamdb_build_options GTest::gtest_main
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static Arrow::arrow_static)
    gtest_discover_tests(glamdb_fault TEST_PREFIX glamdb.fault. PROPERTIES LABELS fault TIMEOUT 30)

    if(TARGET glamdb_admin)
        add_test(NAME glamdb.admin COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/cli_test.py" "$<TARGET_FILE:glamdb_admin>")
        set_tests_properties(glamdb.admin PROPERTIES LABELS integration TIMEOUT 30)
    endif()
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
    if(GLAMDB_BUILD_STORAGE)
        add_executable(glamdb_storage_catalog_driver integration/storage_catalog_driver.cpp)
        target_include_directories(glamdb_storage_catalog_driver PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_storage_catalog_driver PRIVATE glamdb::core glamdb_storage glamdb_build_options
            iceberg::iceberg_sql_catalog_static SQLite::SQLite3)
        add_test(NAME glamdb.durable_storage_catalog COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/integration/sqlcatalog_test.py" "$<TARGET_FILE:glamdb_storage_catalog_driver>")
        set_tests_properties(glamdb.durable_storage_catalog PROPERTIES LABELS integration TIMEOUT 90)
        add_executable(glamdb_scan_driver integration/scan_driver.cpp)
        target_include_directories(glamdb_scan_driver PRIVATE "${PROJECT_SOURCE_DIR}")
        target_link_libraries(glamdb_scan_driver PRIVATE glamdb::core glamdb_storage glamdb_build_options)
        add_test(NAME glamdb.scan_process COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/integration/scan_process_test.py" "$<TARGET_FILE:glamdb_scan_driver>")
        set_tests_properties(glamdb.scan_process PROPERTIES LABELS "integration;scan" TIMEOUT 120)
    endif()
    add_executable(glamdb_public_consumer_compile installed_consumer/main.cpp)
    target_link_libraries(glamdb_public_consumer_compile PRIVATE glamdb::core glamdb_build_options Arrow::arrow_static)
    if(NOT GLAMDB_ENABLE_ASAN AND NOT GLAMDB_ENABLE_UBSAN)
        add_test(NAME glamdb.installed_consumer COMMAND "${CMAKE_COMMAND}"
            "-DENGINE_BINARY_DIR=${PROJECT_BINARY_DIR}"
            "-DTEST_BINARY_DIR=${PROJECT_BINARY_DIR}/installed-consumer"
            "-DTEST_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer"
            "-DDEPENDENCY_PREFIX=${CMAKE_PREFIX_PATH}"
            "-DTEST_GENERATOR=${CMAKE_GENERATOR}"
            "-DTEST_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
            "-DTEST_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
            "-DTEST_TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE}"
            "-DTEST_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            "-DTEST_STORAGE_ENABLED=${GLAMDB_BUILD_STORAGE}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/installed_consumer/run.cmake")
        set_tests_properties(glamdb.installed_consumer PROPERTIES LABELS installed TIMEOUT 120)
    endif()
endif()

if(TARGET glamdb_server)
    set(GLAMDB_TEST_CLI_EXECUTABLE "" CACHE FILEPATH "Optional external CLI for server integration tests")
    set(glamdb_test_cli "$<TARGET_FILE:glamdb_cli>")
    if(GLAMDB_TEST_CLI_EXECUTABLE)
        set(glamdb_test_cli "${GLAMDB_TEST_CLI_EXECUTABLE}")
    endif()
    add_test(NAME glamdb.native_metadata COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_metadata_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.native_metadata PROPERTIES LABELS "integration;http;metadata" TIMEOUT 180)
    add_test(NAME glamdb.native_http COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_http_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    add_test(NAME glamdb.http_review COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/http_review_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui")
    set_tests_properties(glamdb.http_review PROPERTIES LABELS "integration;http" TIMEOUT 120)
    add_test(NAME glamdb.database_limits COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/database_limits_test.py" "$<TARGET_FILE:glamdb_server>")
    set_tests_properties(glamdb.database_limits PROPERTIES LABELS "integration;http" TIMEOUT 120)
    set_tests_properties(glamdb.native_http PROPERTIES LABELS "integration;http" TIMEOUT 120)
    add_test(NAME glamdb.cli_network COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/client/cli_network_test.py"
        "${glamdb_test_cli}" "$<TARGET_FILE:glamdb_server>")
    set_tests_properties(glamdb.cli_network PROPERTIES LABELS "integration;http;cli" TIMEOUT 90)
    add_test(NAME glamdb.cli_workload_bridge COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/tools/workload/tests/bridge_test.py" "${glamdb_test_cli}")
    set_tests_properties(glamdb.cli_workload_bridge PROPERTIES LABELS "integration;cli" TIMEOUT 30)
    add_test(NAME glamdb.scan_http COMMAND "${CMAKE_COMMAND}" -E env
        "PYTHONPATH=${PROJECT_SOURCE_DIR}/sdk/python/src"
        "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/server/scan_http_test.py"
        "$<TARGET_FILE:glamdb_server>" "${glamdb_test_cli}" "${PROJECT_SOURCE_DIR}/ui"
        "$<TARGET_FILE:glamdb_resource_profile_driver>")
    set_tests_properties(glamdb.scan_http PROPERTIES LABELS "integration;http;scan" TIMEOUT 420)
endif()
if(TARGET glamdb_transport)
    add_executable(glamdb_transport_test server/transport_test.cpp server/outcome_conformance_test.cpp)
    target_link_libraries(glamdb_transport_test PRIVATE glamdb_transport glamdb_build_options GTest::gtest_main)
    gtest_discover_tests(glamdb_transport_test TEST_PREFIX glamdb.transport.
        PROPERTIES LABELS "unit;http" TIMEOUT 120)
endif()

if(TARGET glamdb_routes)
    add_executable(glamdb_native_fault_driver server/native_fault_driver.cpp
        "${PROJECT_SOURCE_DIR}/apps/server/server.cpp" support/journal_fault_vfs.cpp)
    target_link_libraries(glamdb_native_fault_driver PRIVATE glamdb_routes glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    add_test(NAME glamdb.native_http_fault COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/native_fault_test.py" "$<TARGET_FILE:glamdb_native_fault_driver>")
    set_tests_properties(glamdb.native_http_fault PROPERTIES LABELS "integration;http;fault" TIMEOUT 180)
    add_executable(glamdb_routes_test server/admission_test.cpp server/routes_test.cpp server/metadata_pages_test.cpp server/scan_sessions_test.cpp server/databases_test.cpp server/database_limits_test.cpp support/fake_storage.cpp support/journal_fault_vfs.cpp)
    target_link_libraries(glamdb_routes_test PRIVATE glamdb_routes glamdb_build_options GTest::gtest_main
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    gtest_discover_tests(glamdb_routes_test TEST_PREFIX glamdb.routes.
        PROPERTIES LABELS "unit;http;fault" TIMEOUT 120)
endif()
if(TARGET glamdb_routes)
    add_executable(glamdb_http_lifecycle_driver server/lifecycle_driver.cpp
        "${PROJECT_SOURCE_DIR}/apps/server/server.cpp" support/fake_storage.cpp)
    target_link_libraries(glamdb_http_lifecycle_driver PRIVATE glamdb_routes glamdb_build_options
        SQLite::SQLite3 iceberg::iceberg_sql_catalog_static)
    add_test(NAME glamdb.http_lifecycle COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/lifecycle_test.py" "$<TARGET_FILE:glamdb_http_lifecycle_driver>")
    set_tests_properties(glamdb.http_lifecycle PROPERTIES LABELS "integration;http;fault" TIMEOUT 30)
endif()

if(TARGET glamdb_routes)
    add_executable(glamdb_resource_profile_driver server/resource_profile_driver.cpp
        "${PROJECT_SOURCE_DIR}/apps/server/server.cpp")
    target_link_libraries(glamdb_resource_profile_driver PRIVATE glamdb_routes glamdb_build_options)
    add_test(NAME glamdb.resource_profile COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/server/resource_profile_test.py" "$<TARGET_FILE:glamdb_resource_profile_driver>")
    set_tests_properties(glamdb.resource_profile PROPERTIES LABELS "integration;http;resources" TIMEOUT 240)
endif()
