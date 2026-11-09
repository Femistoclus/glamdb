# Building

## Requirements

- CMake 3.28 or newer, Ninja, and Python 3.9 or newer.
- C++23 compiler: GCC 14+ or Clang 18+ on Linux. Recent Apple Clang supports
  development builds on macOS.
- LLVM 18 `clang-format` and `clang-tidy` for quality targets.

## Scaffold

```sh
cmake --preset local
cmake --build --preset local
ctest --preset local
./build/local/apps/glamdb/glamdb
```

The executable has a no-op `main`. This build does not fetch or link engine
dependencies. The smoke test verifies that it starts and exits successfully.

Use `release` or `asan-ubsan` in place of `local` for the other build profiles.

```sh
cmake --install build/local
./build/install/local/bin/glamdb
```

The install currently contains the executable. A public library package and its
consumer test will be added with the engine API.

The future consumer must build against the installed prefix without source-tree
or private headers and without HTTP/Python dependencies. Its acceptance scenario
opens an existing store, resolves a collection, obtains its schema, constructs a
public predicate, and inspects a fixed-snapshot, zero-limit scan. The reserved
`tests/installed_consumer/` directory is not yet a buildable consumer project.

## Quality

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
cmake --build --preset local --target format
```

Tools must report LLVM major version 18. Versioned binaries such as
`clang-tidy-18` take precedence over unversioned binaries. Override their paths
with `GLAMDB_CLANG_FORMAT` and `GLAMDB_CLANG_TIDY`.

Checks cover project sources and headers, excluding generated and third-party
files. `tidy-check` uses `compile_commands.json` and rejects missing translation
units. `quality-negative-check` verifies that a formatting violation and a null
dereference in a project header both fail their respective checks.

## Dependencies

See [the dependency guide](../../third_party/README.md). The separate
`deps-local` build installs dependencies into `.deps/install`.

The initial durability qualification targets Linux x86-64 on a dedicated local
ext4 volume. A successful scaffold build, including on macOS, does not validate
engine behavior or durability.

## Validation scope

| Check | What it establishes |
| --- | --- |
| Scaffold build and `glamdb.smoke` | The current entry point compiles, starts, and exits successfully. |
| Formatting, tidy, and negative controls | Configured C++ checks execute and reject deliberate violations. |
| Dependency build | The selected dependency recipes build/install; engine integration still needs its own probes. |
| Future integration/fault suites | Engine behavior at the tested boundaries, with recorded inputs and results. |
| Future environment qualification | Durability and supported limits for the specifically tested platform and storage configuration. |

Only the first three categories have build infrastructure in this checkout.
The [roadmap](../ROADMAP.md) defines the remaining evidence. Describe checks as
designed, run with PASS/FAIL, or NOT RUN; a configured target alone is not a pass.
Do not use proposed test labels or executable names as if they exist already.

Use `local`, `release`, and `asan-ubsan` with the configure/build/test commands
above when the corresponding suites exist. Lifetime changes require sanitizer
coverage; reopen and recovery require a fresh process. The eventual qualification
also needs calibrated fault modeling and dedicated-volume power cuts, which
ordinary CTest or a macOS build cannot establish on its own.

Record the toolchain and dependency pins, command, exit code, environment,
fixture/seed, and measured results for reproducibility. Store raw generated
output under ignored `artifacts/` and retain a reviewable summary or stable
artifact reference with the change. Keep commands and this status description
current as targets and suites are added.
