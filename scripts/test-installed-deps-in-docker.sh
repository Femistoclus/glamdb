#!/bin/sh
set -eu

if [ "$#" -ne 0 ]; then
    printf '%s\n' "Usage: $0" >&2
    exit 2
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec docker run --rm --network none \
    --volume "$root/probes/build_install:/consumer-src:ro" \
    --volume "$root/.clang-tidy:/quality/.clang-tidy:ro" \
    --volume "$root/.clang-format:/quality/.clang-format:ro" \
    --mount "type=volume,src=${GLAMDB_DEPS_VOLUME:-glamdb-deps-clang18},dst=/prefix,readonly,volume-subpath=install" \
    --workdir /consumer-src glamdb-deps:clang18 sh -eu -c '
        cmake -S /consumer-src -B /build -G Ninja \
            -DCMAKE_PREFIX_PATH=/prefix -DCMAKE_BUILD_TYPE=Release
        cmake --build /build --parallel 2
        ctest --test-dir /build --output-on-failure --no-tests=error -L build-install
        clang-format-18 --style=file:/quality/.clang-format --dry-run --Werror /consumer-src/main.cpp
        clang-tidy-18 /consumer-src/main.cpp -p /build --config-file=/quality/.clang-tidy --warnings-as-errors=*
        /build/consumer
        ldd /build/consumer
        readelf -d /build/consumer
    '
