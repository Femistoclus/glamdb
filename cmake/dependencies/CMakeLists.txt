include(ExternalProject)
include("${CMAKE_CURRENT_LIST_DIR}/VerifySubmodules.cmake")

set(GLAMDB_DEPENDENCY_PREFIX "${PROJECT_SOURCE_DIR}/.deps/install"
    CACHE PATH "Installation prefix for pinned dependencies")
set(GLAMDB_DEPENDENCY_JOBS 2 CACHE STRING "Parallel jobs per dependency")

set(dependency_args
    "-DCMAKE_INSTALL_PREFIX:PATH=${GLAMDB_DEPENDENCY_PREFIX}"
    "-DCMAKE_INSTALL_LIBDIR:STRING=lib"
    "-DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}"
    "-DCMAKE_C_COMPILER:FILEPATH=${CMAKE_C_COMPILER}"
    "-DCMAKE_CXX_COMPILER:FILEPATH=${CMAKE_CXX_COMPILER}"
    "-DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON"
    "-DBUILD_SHARED_LIBS:BOOL=OFF"
)
if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND dependency_args "-DCMAKE_TOOLCHAIN_FILE:FILEPATH=${CMAKE_TOOLCHAIN_FILE}")
endif()
foreach(variable IN ITEMS CMAKE_OSX_ARCHITECTURES CMAKE_OSX_SYSROOT CMAKE_OSX_DEPLOYMENT_TARGET)
    if(DEFINED ${variable} AND NOT "${${variable}}" STREQUAL "")
        list(APPEND dependency_args "-D${variable}:STRING=${${variable}}")
    endif()
endforeach()

set(thrift_url "$ENV{ARROW_THRIFT_URL}")
if(NOT thrift_url)
    file(STRINGS "${PROJECT_SOURCE_DIR}/third_party/arrow/cpp/thirdparty/versions.txt"
        thrift_version REGEX "^ARROW_THRIFT_BUILD_VERSION=")
    string(REGEX REPLACE "^[^=]+=" "" thrift_version "${thrift_version}")
    set(thrift_url "https://archive.apache.org/dist/thrift/${thrift_version}/thrift-${thrift_version}.tar.gz")
endif()

set(arrow_platform_args)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$"
   AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
   AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19)
    # Clang 18 crashes while compiling Arrow's ARM SVE bit-packing kernels.
    list(APPEND arrow_platform_args -DARROW_RUNTIME_SIMD_LEVEL=NONE)
endif()

ExternalProject_Add(glamdb_arrow
    SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party/arrow/cpp"
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow"
    DOWNLOAD_COMMAND ""
    UPDATE_COMMAND ""
    STEP_TARGETS configure
    CONFIGURE_COMMAND "${CMAKE_COMMAND}" -E env "ARROW_THRIFT_URL=${thrift_url}"
        "${CMAKE_COMMAND}" -S <SOURCE_DIR> -B <BINARY_DIR> -G "${CMAKE_GENERATOR}"
        ${dependency_args}
        ${arrow_platform_args}
        -DARROW_BUILD_SHARED=OFF
        -DARROW_BUILD_STATIC=ON
        -DARROW_BUILD_TESTS=OFF
        -DARROW_BUILD_BENCHMARKS=OFF
        -DARROW_DEPENDENCY_SOURCE=BUNDLED
        -DARROW_IPC=ON
        -DARROW_COMPUTE=ON
        -DARROW_FILESYSTEM=ON
        -DARROW_PARQUET=ON
        -DARROW_JSON=ON
        -DARROW_S3=OFF
        -DARROW_WITH_ZLIB=ON
        -DARROW_WITH_ZSTD=ON
        -DARROW_WITH_SNAPPY=ON
        -DARROW_WITH_LZ4=ON
    BUILD_COMMAND "${CMAKE_COMMAND}" --build <BINARY_DIR> --parallel ${GLAMDB_DEPENDENCY_JOBS}
    INSTALL_COMMAND "${CMAKE_COMMAND}" --install <BINARY_DIR>
)

string(JSON sqlite_url GET "${GLAMDB_DEPENDENCY_LOCK}" archives sqlite url)
string(JSON sqlite_sha256 GET "${GLAMDB_DEPENDENCY_LOCK}" archives sqlite sha256)
string(JSON sqlite_version GET "${GLAMDB_DEPENDENCY_LOCK}" archives sqlite version)
set(sqlite_args ${dependency_args})
list(FILTER sqlite_args EXCLUDE REGEX "^-DCMAKE_CXX_COMPILER:")
ExternalProject_Add(glamdb_sqlite
    URL "${sqlite_url}"
    URL_HASH "SHA256=${sqlite_sha256}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/sqlite"
    CONFIGURE_COMMAND "${CMAKE_COMMAND}"
        -S "${CMAKE_CURRENT_SOURCE_DIR}/sqlite" -B <BINARY_DIR>
        -G "${CMAKE_GENERATOR}" ${sqlite_args}
        "-DGLAMDB_SQLITE_SOURCE_DIR:PATH=<SOURCE_DIR>"
        "-DGLAMDB_SQLITE_VERSION:STRING=${sqlite_version}"
    BUILD_COMMAND "${CMAKE_COMMAND}" --build <BINARY_DIR> --parallel ${GLAMDB_DEPENDENCY_JOBS}
    INSTALL_COMMAND "${CMAKE_COMMAND}" --install <BINARY_DIR>
)

# Iceberg builds from a patched copy of the pinned submodule; see patches/iceberg-cpp.
file(GLOB iceberg_patches LIST_DIRECTORIES false "${CMAKE_CURRENT_SOURCE_DIR}/patches/iceberg-cpp/*.patch")
ExternalProject_Add(glamdb_iceberg
    DEPENDS glamdb_arrow glamdb_sqlite
    SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/iceberg-src"
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/iceberg"
    DOWNLOAD_COMMAND "${CMAKE_COMMAND}"
        "-DSOURCE=${PROJECT_SOURCE_DIR}/third_party/iceberg-cpp"
        "-DDESTINATION=${CMAKE_CURRENT_BINARY_DIR}/iceberg-src"
        "-DPATCH_DIR=${CMAKE_CURRENT_SOURCE_DIR}/patches/iceberg-cpp"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/PrepareSource.cmake"
    UPDATE_COMMAND ""
    STEP_TARGETS configure
    CMAKE_ARGS ${dependency_args}
        "-DCMAKE_PREFIX_PATH:PATH=${GLAMDB_DEPENDENCY_PREFIX}"
        "-DArrow_DIR:PATH=${GLAMDB_DEPENDENCY_PREFIX}/lib/cmake/Arrow"
        "-DParquet_DIR:PATH=${GLAMDB_DEPENDENCY_PREFIX}/lib/cmake/Parquet"
        "-DSQLite3_INCLUDE_DIR:PATH=${GLAMDB_DEPENDENCY_PREFIX}/include"
        "-DSQLite3_LIBRARY:FILEPATH=${GLAMDB_DEPENDENCY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}sqlite3${CMAKE_STATIC_LIBRARY_SUFFIX}"
        -DCMAKE_REQUIRE_FIND_PACKAGE_Arrow=ON
        -DCMAKE_REQUIRE_FIND_PACKAGE_Parquet=ON
        -DFETCHCONTENT_TRY_FIND_PACKAGE_MODE=ALWAYS
        -DICEBERG_BUILD_STATIC=ON
        -DICEBERG_BUILD_SHARED=OFF
        -DICEBERG_BUILD_BUNDLE=ON
        -DICEBERG_BUILD_TESTS=OFF
        -DICEBERG_BUILD_BENCHMARKS=OFF
        -DICEBERG_BUILD_SQL_CATALOG=ON
        -DICEBERG_SQL_SQLITE=ON
        -DICEBERG_BUILD_REST=OFF
        -DICEBERG_BUILD_HIVE=OFF
        -DICEBERG_S3=OFF
    BUILD_COMMAND "${CMAKE_COMMAND}" --build <BINARY_DIR> --parallel ${GLAMDB_DEPENDENCY_JOBS}
)
# Re-copy and re-patch the source when a recorded patch or the copy script changes.
ExternalProject_Add_StepDependencies(glamdb_iceberg download
    "${CMAKE_CURRENT_SOURCE_DIR}/PrepareSource.cmake" ${iceberg_patches})

# CivetWeb shutdown needs synchronized stop-flag access; keep the pin unchanged.
file(GLOB prometheus_patches LIST_DIRECTORIES false "${CMAKE_CURRENT_SOURCE_DIR}/patches/prometheus-cpp/*.patch")
ExternalProject_Add(glamdb_prometheus_cpp
    SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/prometheus-src"
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/prometheus-cpp"
    DOWNLOAD_COMMAND "${CMAKE_COMMAND}"
        "-DSOURCE=${PROJECT_SOURCE_DIR}/third_party/prometheus-cpp"
        "-DDESTINATION=${CMAKE_CURRENT_BINARY_DIR}/prometheus-src"
        "-DPATCH_DIR=${CMAKE_CURRENT_SOURCE_DIR}/patches/prometheus-cpp"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/PrepareSource.cmake"
    UPDATE_COMMAND ""
    CMAKE_ARGS ${dependency_args}
        -DENABLE_PULL=ON
        -DENABLE_PUSH=OFF
        -DENABLE_TESTING=OFF
        -DENABLE_COMPRESSION=OFF
        -DTHIRDPARTY_CIVETWEB_WITH_SSL=OFF
        -DOVERRIDE_CXX_STANDARD_FLAGS=OFF
        -DCMAKE_CXX_STANDARD=23
    BUILD_COMMAND "${CMAKE_COMMAND}" --build <BINARY_DIR> --parallel ${GLAMDB_DEPENDENCY_JOBS}
    INSTALL_COMMAND "${CMAKE_COMMAND}" --install <BINARY_DIR>
)
ExternalProject_Add_StepDependencies(glamdb_prometheus_cpp download
    "${CMAKE_CURRENT_SOURCE_DIR}/PrepareSource.cmake" ${prometheus_patches})
