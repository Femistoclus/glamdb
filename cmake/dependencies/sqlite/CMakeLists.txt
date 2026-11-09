cmake_minimum_required(VERSION 3.28)

if(NOT GLAMDB_SQLITE_SOURCE_DIR OR NOT GLAMDB_SQLITE_VERSION)
    message(FATAL_ERROR "Use the GlamDB dependency build to configure SQLite")
endif()
project(SQLite3 VERSION "${GLAMDB_SQLITE_VERSION}" LANGUAGES C)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)

add_library(sqlite3 STATIC "${GLAMDB_SQLITE_SOURCE_DIR}/sqlite3.c")
add_library(SQLite::SQLite3 ALIAS sqlite3)
set_target_properties(sqlite3 PROPERTIES EXPORT_NAME SQLite3)
target_compile_definitions(sqlite3 PRIVATE SQLITE_THREADSAFE=1)
target_include_directories(sqlite3 PUBLIC
    "$<BUILD_INTERFACE:${GLAMDB_SQLITE_SOURCE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(sqlite3 PUBLIC Threads::Threads ${CMAKE_DL_LIBS})

install(TARGETS sqlite3 EXPORT SQLite3Targets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(FILES "${GLAMDB_SQLITE_SOURCE_DIR}/sqlite3.h"
              "${GLAMDB_SQLITE_SOURCE_DIR}/sqlite3ext.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(EXPORT SQLite3Targets NAMESPACE SQLite::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/SQLite3")
configure_package_config_file(SQLite3Config.cmake.in SQLite3Config.cmake
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/SQLite3")
write_basic_package_version_file(SQLite3ConfigVersion.cmake
    VERSION "${PROJECT_VERSION}" COMPATIBILITY SameMajorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/SQLite3Config.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/SQLite3ConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/SQLite3")
