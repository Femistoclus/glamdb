#!/bin/sh
set -eu

if [ "$#" -ne 0 ]; then
    printf '%s\n' "Usage: $0" >&2
    exit 2
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
for source in arrow/cpp iceberg-cpp googletest prometheus-cpp prometheus-cpp/3rdparty/civetweb; do
    if [ ! -f "$root/third_party/$source/CMakeLists.txt" ]; then
        printf '%s\n' "Initialize dependencies first: git submodule update --init --recursive third_party/${source%%/*}" >&2
        exit 1
    fi
done

docker build --tag glamdb-dev:clang18 - < "$root/docker/dev/Dockerfile"
docker build --tag glamdb-deps:clang18 - < "$root/docker/deps/Dockerfile"
exec docker run --rm --name glamdb-deps-build --volume "$root:/src:ro" \
    --volume "${GLAMDB_DEPS_VOLUME:-glamdb-deps-clang18}:/deps" \
    --env CMAKE_BUILD_PARALLEL_LEVEL=2 --env GLAMDB_DEPS_CLEAN="${GLAMDB_DEPS_CLEAN:-0}" \
    --workdir /src glamdb-deps:clang18 sh -eu -c '
        # A changed dependency recipe can invalidate the incremental build tree.
        if [ "$GLAMDB_DEPS_CLEAN" = 1 ]; then
            rm -rf /deps/build /deps/install
        fi
        for dependency in arrow iceberg-cpp googletest cli11 prometheus-cpp prometheus-cpp/3rdparty/civetweb; do
            git config --global --add safe.directory "/src/third_party/$dependency"
        done
        cmake --preset deps-local -B /deps/build \
            -DGLAMDB_DEPENDENCY_PREFIX=/deps/install -DGLAMDB_DEPENDENCY_JOBS=2 \
            -DCMAKE_INSTALL_PREFIX=/deps/install -DCMAKE_PREFIX_PATH=/deps/install
        cmake --build /deps/build --parallel 1
    '
