# Dependency build and installed API probe

The dependency build is optional for the core unit tests described in
[Building](BUILDING.md). It supplies a Linux prefix for storage adapters and
probes without installing Linux libraries into the macOS checkout.

## Run

Start a Docker engine, then run from the repository root:

```sh
git submodule update --init third_party/arrow third_party/iceberg-cpp third_party/googletest
./scripts/build-deps-in-docker.sh
./scripts/test-installed-deps-in-docker.sh
```

The scripts honor `DOCKER_CONFIG` and the selected Docker context. The build
creates `glamdb-deps:clang18` from the development image and uses Clang 18 with
libc++ 18: the default libstdc++ 13 does not expose the `std::expected` interface
required by Iceberg with this compiler. Image construction compiles and runs an
`expected`/`format` check before dependency compilation.

Sources are mounted read-only at `/src`. The named volume
`glamdb-deps-clang18` retains `/deps/build` and `/deps/install`; subsequent builds
reuse it. Each dependency uses at most two build jobs, and the outer build runs
one dependency at a time. Set `GLAMDB_DEPS_VOLUME` to choose a different named
volume. Use a new volume after changing dependency pins, compiler, standard
library, or architecture; the cached prefix belongs to its original configuration.

The installed probe does not rebuild dependencies. It mounts only the volume's
`install` subdirectory at `/prefix`, read-only, using Docker's
[`volume-subpath` option](https://docs.docker.com/engine/storage/volumes/#mount-a-volume-subdirectory).
Its container has neither the checkout nor dependency build trees, and has no
network. Only the probe sources and quality configuration are available. Its
build is discarded with the container. This also checks prefix relocation.

The standalone project can be used with an existing compatible installed prefix:

```sh
cmake -S probes/build_install -B build/build-install -G Ninja \
    -DCMAKE_PREFIX_PATH=/path/to/prefix
cmake --build build/build-install --parallel 2
ctest --test-dir build/build-install --no-tests=error --output-on-failure -L build-install
```

Use the same compiler and standard library as the prefix. The Docker runner also
runs LLVM 18 formatting and tidy checks, prints dependency versions, and records
dynamic dependencies with `ldd` and `readelf`. Root `format-check` includes the
probe; root `tidy-check` excludes `probes/build_install` and the separate
[`probes/toolchain` compatibility reproducer](BUILDING.md#storage-adapters).
Their compilation databases belong to standalone projects. Missing compilation
entries for other enabled project sources remain errors.

## Executed development profile, 10 November 2026

| Component | Observed value |
| --- | --- |
| Environment | Linux aarch64, Colima VM on macOS; kernel 6.8.0-117-generic |
| Base image | Ubuntu 24.04, digest in `docker/dev/Dockerfile` |
| Compiler and LLVM tools | Clang/clang-format/clang-tidy 18.1.3, Ubuntu `1:18.1.3-1ubuntu1` |
| Standard library | libc++/libc++abi 18.1.3 |
| Build tools | CMake 3.28.3, Ninja 1.11.1 |
| Arrow / Parquet | 25.0.0, `59bea6ec485e7fe351d1aa6753f964f6a6bc353a` |
| Iceberg | 0.4.0, `64dc66f159bd62c9817a96d74cbce767268934d0` |
| GoogleTest | 1.17.0, `52eb8108c5bdec04579160ae17225d66034bd723` |
| SQLite | 3.54.1; archive SHA-256 `0a9c8e34352dad46006aa82836d5383bff9dc6bc9ce5a60553c05aa30e6ad1f9` |
| Avro, selected by Iceberg | `209a3735ec330679790824da54b7558db55e4e7f` |

The direct pins remain governed by [lock.json](../../third_party/lock.json).
Arrow builds as C++20 upstream; Iceberg and the consumer build as C++23, all using
the same libc++ ABI. APT packages are not a frozen repository snapshot, so the
Dockerfile alone is not a fully reproducible qualified toolchain receipt.

The first Arrow build failed in Clang's ARM SVE backend with
`Cannot scalarize scalable vector loads` in bit-packing kernels. The dependency
recipe disables `ARROW_RUNTIME_SIMD_LEVEL` only for ARM64 Clang versions below
19. NEON remains enabled; other compiler/platform configurations are unchanged.
The repeated complete build passed. No upstream source or direct pin changed.

The Ubuntu libc++/libc++abi packages also reproduce an ASan allocation/deallocation
mismatch when destroying `std::runtime_error`, independently of GlamDB or its
dependencies. This matches the reported
[LLVM packaging issue](https://github.com/llvm/llvm-project/issues/59432).
For sanitizer builds with this development prefix, configure executables with
`-DCMAKE_EXE_LINKER_FLAGS=-static-libstdc++`. Despite its name, Clang applies this
option to the selected `-stdlib=libc++`: the resulting executable links libc++
statically. The minimal exception reproducer and all eight TableProfile tests
passed with ASan/UBSan and this option, without disabling sanitizer checks;
`ldd` confirmed no dynamic C++ runtime. Evidence is in ignored
`artifacts/table-profile-check/`. This workaround is specific to the development
toolchain, not a change to the storage error contract.

The installed probe checks these exported targets:

- `Arrow::arrow_static`, `Arrow::arrow_bundled_dependencies`, `Parquet::parquet_static`;
- `iceberg::iceberg_static`, `iceberg::iceberg_data_static`,
  `iceberg::iceberg_bundle_static`, `iceberg::iceberg_sql_catalog_static`;
- `SQLite::SQLite3` and Iceberg's exported private dependency targets.

Iceberg does not install a package version file: use
`find_package(Iceberg CONFIG REQUIRED)`, with the version checked from its public
header. A versioned `find_package(Iceberg 0.4.0 EXACT ...)` fails despite a valid
installation. The probe also rejects separate `iceberg::arrow_static` and
`iceberg::parquet_static` targets, which would indicate another bundled runtime.

Executed consumer coverage includes public FileIO/CatalogStore, format Writer
registry and Parquet/Avro registration, MetricsConfig, DataWriter, FastAppend,
DataTableScan, and creation of an in-memory SQLite catalog. Linkage uses only
installed headers and libraries. Observed dynamic dependencies contain libc++,
libc++abi, libunwind, zlib, libm, libgcc_s, and libc; no libstdc++, Python, or HTTP
runtime was linked. The installed prefix occupies approximately 173 MiB.

Raw local evidence is retained under ignored `artifacts/deps/`: `build.log`,
`clang18-sve-failure.log`, `installed-consumer.log`, `standalone-consumer.log`,
`environment.txt`, and `image.json`. The scripts and standalone project are the
reproducible regression; these logs describe this execution.

This establishes a development build and installed API boundary. It does not
validate writer metrics on files, durable FileIO, catalog transaction semantics,
memory limits, or power-loss behavior. P-IO/P-WRITER and the Linux x86-64/ext4
qualification remain separate checks.
