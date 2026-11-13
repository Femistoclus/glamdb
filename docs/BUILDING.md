# Build and install

Native builds require Linux, CMake 3.28+, Ninja, Python 3.10+, OpenSSL and zlib
development headers, and a C++23 compiler and standard library supporting
`std::expected`: GCC 14+ or Clang 18+ with a matching STL. Build the engine and
all C++ dependencies with the same compiler, standard library and ABI.
The [dependency lock](../third_party/lock.json) records exact revisions.

## Native installation

From the repository root:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
cmake --install build/local
```

Dependencies install to `.deps/install`; the local preset installs GlamDB to
`build/install/local`. Installation includes `bin/glamdb`, `bin/glamdb-cli`,
`share/glamdb/ui`, public `storage/*.h` headers and the CMake package.
Use the `release` or `asan-ubsan` preset for the corresponding application build.
After dependency recipes or patches change, rerun the dependency configure and
build before rebuilding the engine. See [dependencies](../third_party/README.md).

Start the installed service with an existing canonical parent directory and a
token supplied through the environment:

```sh
export GLAMDB_TOKEN="$(openssl rand -hex 32)"
build/install/local/bin/glamdb --data-dir /absolute/path/to/new-service
```

The server discovers its installed UI relative to the executable on Linux.
It starts without databases; create one through HTTP or the workbench.
[Server options](HTTP_SERVER.md) describe ownership, limits and TLS deployment.
The [local helper](../README.md#get-started) builds and runs the service for you.

To include the offline administrator, configure with
`cmake --preset local -DGLAMDB_BUILD_ADMIN=ON`, rebuild and reinstall.
`glamdb-admin` requires exclusive store ownership. It cannot open a store while
the server or a retained embedded handle owns it.

For a build without tests, use a separate directory:

```sh
cmake -S . -B build/no-tests -G Ninja -DBUILD_TESTING=OFF \
  -DCMAKE_PREFIX_PATH="$PWD/.deps/install" -DCMAKE_BUILD_TYPE=Release
cmake --build build/no-tests
cmake --install build/no-tests --prefix "$PWD/build/install/no-tests"
```

The [standalone CLI build](../apps/cli/README.md) needs no engine dependency
installation. The [Python SDK](../sdk/python/README.md) also installs independently.

## Embed the library

Consumers link the installed public target and use Arrow batches:

```cmake
cmake_minimum_required(VERSION 3.28)
project(example LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(GlamDB CONFIG REQUIRED)
add_executable(example main.cpp)
target_link_libraries(example PRIVATE glamdb::core)
```

Configure the consumer with both `build/install/local` and `.deps/install` in
`CMAKE_PREFIX_PATH`. Include `<storage/engine.h>` and explicitly create or open
a store with its expected identity. See the [native storage contract](STORAGE_CONTRACT.md)
for contract negotiation, supported operations and lifetime requirements.
The interface is a C++ source contract; it does not promise a stable binary ABI.

## Docker

With a running Docker daemon, including on macOS:

```sh
./scripts/build-deps-in-docker.sh
./scripts/test-storage-in-docker.sh asan-ubsan
```

The storage runner uses the installed prefix from the `glamdb-deps-clang18`
volume. `GLAMDB_DEPS_VOLUME` selects another compatible volume. Dependencies
must be rebuilt when their recipes change. The separate
`scripts/test-in-docker.sh` runner covers only the independent local I/O module.

## Checks

Documentation changes need factual, command and link checks. For C++ changes,
run the relevant CTest suites and LLVM 18 quality targets:

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
```

Client/browser checks require Python 3.13, Node 22+, OpenSSL and Chromium:

```sh
bash scripts/bootstrap-workbench.sh
npm --prefix ui exec -- playwright install chromium
bash scripts/check-workbench.sh
```

On Linux, use Playwright's `--with-deps` option when browser system libraries
are missing. [Contribution requirements](../CONTRIBUTING.md) describe CI and review.
