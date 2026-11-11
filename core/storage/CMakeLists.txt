find_package(Arrow 25.0.0 EXACT CONFIG REQUIRED)
find_package(Parquet 25.0.0 EXACT CONFIG REQUIRED)
find_package(Iceberg CONFIG REQUIRED)
find_package(SQLite3 CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

add_library(glamdb_storage STATIC
    table_profile.cpp metadata_validation.cpp capacity.cpp format_verifier.cpp base_state.cpp
    publication_guard.cpp durable_local_file_io.cpp append_stager.cpp
    data_file_writer.cpp scan_index.cpp
    format_registry.cpp recovery_visited.cpp
)
target_include_directories(glamdb_storage
    PRIVATE "${PROJECT_SOURCE_DIR}"
    PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/public>"
           "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(glamdb_storage
    PUBLIC glamdb_io Arrow::arrow_static Parquet::parquet_static iceberg::iceberg_bundle_static
    PRIVATE ZLIB::ZLIB SQLite::SQLite3
    PRIVATE "$<BUILD_INTERFACE:glamdb_build_options>"
)

if(NOT GLAMDB_IO_ONLY)
    # glamdb::core links this static library privately, so installed consumers need it.
    set_target_properties(glamdb_storage PROPERTIES EXPORT_NAME storage)
    install(TARGETS glamdb_storage EXPORT GlamDBTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
endif()
