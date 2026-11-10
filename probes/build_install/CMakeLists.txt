cmake_minimum_required(VERSION 3.28)
project(GlamDBDependencyConsumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Arrow 25.0.0 EXACT CONFIG REQUIRED)
find_package(Parquet 25.0.0 EXACT CONFIG REQUIRED)
find_package(Iceberg CONFIG REQUIRED)
find_package(SQLite3 3.54.1 EXACT CONFIG REQUIRED)
get_property(imported_targets DIRECTORY PROPERTY IMPORTED_TARGETS)
message(STATUS "Imported targets: ${imported_targets}")
if(TARGET iceberg::arrow_static OR TARGET iceberg::parquet_static)
    message(FATAL_ERROR "Unexpected second Arrow/Parquet runtime")
endif()

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE
    iceberg::iceberg_bundle_static iceberg::iceberg_sql_catalog_static
    Arrow::arrow_static Parquet::parquet_static SQLite::SQLite3)
target_compile_options(consumer PRIVATE -Wall -Wextra -Wpedantic -Werror)

enable_testing()
add_test(NAME glamdb.build-install.api COMMAND consumer)
set_tests_properties(glamdb.build-install.api PROPERTIES LABELS "build-install" TIMEOUT 30)
