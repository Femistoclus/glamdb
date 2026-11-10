find_package(Arrow 25.0.0 EXACT CONFIG REQUIRED)
find_package(Parquet 25.0.0 EXACT CONFIG REQUIRED)
find_package(Iceberg CONFIG REQUIRED)
find_package(SQLite3 CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

add_library(glamdb_storage STATIC
    table_profile.cpp metadata_validation.cpp capacity.cpp format_verifier.cpp base_state.cpp
    publication_guard.cpp durable_local_file_io.cpp append_stager.cpp
    data_file_writer.cpp
    format_registry.cpp recovery_visited.cpp
)
target_include_directories(glamdb_storage
    PRIVATE "${PROJECT_SOURCE_DIR}"
    PUBLIC "${PROJECT_SOURCE_DIR}/public"
)
target_link_libraries(glamdb_storage
    PUBLIC glamdb_io Arrow::arrow_static Parquet::parquet_static iceberg::iceberg_bundle_static
    PRIVATE ZLIB::ZLIB SQLite::SQLite3
    PRIVATE glamdb_build_options
)
