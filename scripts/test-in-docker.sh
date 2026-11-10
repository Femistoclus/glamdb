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

docker build --tag glamdb-dev:clang18 - < "$root/docker/dev/Dockerfile"
exec docker run --rm --network none --volume "$root:/src:ro" \
    --workdir /src glamdb-dev:clang18 sh -eu -c '
        cmake --preset "$1" -B /build -DCMAKE_INSTALL_PREFIX=/build/install -DCMAKE_PREFIX_PATH= -DGLAMDB_IO_ONLY=ON
        cmake --build /build --parallel
        ctest --test-dir /build --output-on-failure --no-tests=error -L unit
        ctest --test-dir /build --output-on-failure --no-tests=error
        cmake --build /build --target format-check tidy-check quality-negative-check
    ' sh "$profile"
