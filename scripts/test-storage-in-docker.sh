#!/bin/sh
set -eu

profile=${1-local}
if [ "$#" -gt 1 ]; then
    printf '%s\n' "Usage: $0 [local|release|asan-ubsan]" >&2
    exit 2
fi
case "$profile" in
    local|release|asan-ubsan) ;;
    *) printf '%s\n' "Unknown build profile: $profile" >&2; exit 2 ;;
esac

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ ! -f "$root/third_party/googletest/CMakeLists.txt" ]; then
    printf '%s\n' 'Initialize tests first: git submodule update --init third_party/googletest' >&2
    exit 1
fi

exec docker run --rm --network none --volume "$root:/src:ro" \
    --mount "type=volume,src=${GLAMDB_DEPS_VOLUME:-glamdb-deps-clang18},dst=/prefix,readonly,volume-subpath=install" \
    --workdir /src glamdb-deps:clang18 sh -eu -c '
        if [ "$1" = asan-ubsan ]; then
            export CXXFLAGS="${CXXFLAGS-} -D_LIBCPP_HAS_NO_ASAN"
        fi
        cmake --preset "$1" -B /build -DCMAKE_INSTALL_PREFIX=/build/install \
            -DGLAMDB_BUILD_STORAGE=ON -DGLAMDB_IO_ONLY=OFF -DCMAKE_PREFIX_PATH=/prefix \
            -DCMAKE_EXE_LINKER_FLAGS=-static-libstdc++
        cmake --build /build --parallel 2
        ctest --test-dir /build --output-on-failure --no-tests=error -L unit
        ctest --test-dir /build --output-on-failure --no-tests=error
        cmake --build /build --target format-check tidy-check quality-negative-check
    ' sh "$profile"
