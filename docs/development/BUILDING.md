# Building

## Requirements

This page covers C++. For the implemented Python SDK/mock and browser UI, use
[WORKBENCH.md](WORKBENCH.md). Those packages do not link the C++ scaffold or
require an Arrow/Iceberg source build.

- CMake 3.28 or newer, Ninja, and Python 3.9 or newer.
- Linux and a C++23 compiler **and standard library** with `std::expected`:
  GCC 14+ or Clang 18+ with a matching STL. Clang with an older system
  libstdc++ may lack this feature. Use the same C++ ABI for all dependencies.
- LLVM 18 `clang-format` and `clang-tidy` for quality targets.

## Build and install

First install the [pinned dependencies](../../third_party/README.md):

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
```

```sh
cmake --preset local
cmake --build --preset local
ctest --preset local
./build/local/apps/glamdb/glamdb --help
```

The executable creates or inspects an empty store with an explicit identity.
It links the installed SQLite, Arrow and Iceberg dependencies. Tests also link
GoogleTest. The production durable file backend and scanner remain pending;
the executable does not serve HTTP or write user-data files.

Use `release` or `asan-ubsan` in place of `local` for the other build profiles.

```sh
cmake --install build/local
./build/install/local/bin/glamdb --help
```

The install contains the executable, `storage/*.h`, `glamdb::core` static library
and `GlamDBConfig.cmake`. Consumers use `find_package(GlamDB CONFIG REQUIRED)`
with both the GlamDB and dependency prefixes in `CMAKE_PREFIX_PATH`.
`glamdb.installed_consumer` installs to a disposable build prefix and compiles
and runs a separate public-header consumer. It exercises create/open and the
missing-collection error from `GetCollection` and `FindAppendOutcome`, without
source-tree headers or HTTP/Python dependencies. Scan/predicate
acceptance awaits the scanner. The installed-consumer test is omitted from
sanitizer builds; instrumented static libraries require matching consumer flags.

## Quality

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
cmake --build --preset local --target format
```

Tools must report LLVM major version 18. Versioned binaries such as
`clang-tidy-18` take precedence over unversioned binaries. Override their paths
with `GLAMDB_CLANG_FORMAT` and `GLAMDB_CLANG_TIDY`.

Checks cover project sources and headers, excluding generated and third-party
files. C++ formatting uses a 120-column limit. `tidy-check` uses
`compile_commands.json` and rejects missing translation
units. `quality-negative-check` verifies that a formatting violation and a null
dereference in a project header both fail their respective checks.

## Dependencies

See [the dependency guide](../../third_party/README.md). The separate
`deps-local` build installs dependencies into `.deps/install`.

The initial durability qualification targets Linux x86-64 on a dedicated local
ext4 volume. Successful builds and process-crash tests do not establish
power-loss durability.

## Validation scope

| Check | What it establishes |
| --- | --- |
| Build and `glamdb.smoke` | The entry point compiles, starts, and exits successfully. |
| Formatting, tidy, and negative controls | Configured C++ checks execute and reject deliberate violations. |
| Unit and VFS fault suites | Canonical input, catalog transactions, bindings/outcomes and coordinator state transitions. Storage production uses a test double. |
| `glamdb.process_recovery` | Separate-process reopen, interrupted operations, SQLite hot-journal recovery and exclusive ownership. |
| `glamdb.real_sqlcatalog` | Real Iceberg create/property-update ordering, outer rollback, exact base and fresh-process load. Local FileIO is test-only. |
| `glamdb.installed_consumer` | Installed CMake package and public facade work without private headers. |
| Future environment qualification | Durability and supported limits for the specifically tested platform and storage configuration. |

See [COORDINATOR.md](COORDINATOR.md) for native implemented behavior and current
evidence. Independent fixture, receipt/model and network-client/browser suites
run separately, as recorded in [WORKBENCH_VALIDATION.md](WORKBENCH_VALIDATION.md).
They do not qualify production file/scanner integration or physical durability.
The [roadmap](../ROADMAP.md) defines the remaining evidence. Describe checks as
designed, run with PASS/FAIL, or NOT RUN; a configured target alone is not a pass.
Do not use proposed test labels or executable names as if they exist already.

Use `local`, `release`, and `asan-ubsan` with the configure/build/test commands
above. Lifetime changes require sanitizer
coverage; reopen and recovery require a fresh process. The eventual qualification
also needs calibrated fault modeling and dedicated-volume power cuts, which
ordinary CTest or a macOS build cannot establish on its own.

Record the toolchain and dependency pins, command, exit code, environment,
fixture/seed, and measured results for reproducibility. Store raw generated
output under ignored `artifacts/` and retain a reviewable summary or stable
artifact reference with the change. Keep commands and this status description
current as targets and suites are added.
