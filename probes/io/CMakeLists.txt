find_package(Iceberg CONFIG REQUIRED)

add_executable(io-probe main.cpp probe.cpp
    "${PROJECT_SOURCE_DIR}/tests/support/fault_fs.cpp"
    "${PROJECT_SOURCE_DIR}/tests/support/durability_trace.cpp"
)
target_include_directories(io-probe PRIVATE "${PROJECT_SOURCE_DIR}")
target_link_libraries(io-probe PRIVATE
    glamdb_storage glamdb_build_options iceberg::iceberg_sql_catalog_static)
