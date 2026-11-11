# Dependencies

| Dependency | Version | Source |
| --- | --- | --- |
| Arrow / Parquet | 25.0.0 | `arrow/` submodule; Parquet is part of Arrow. |
| Iceberg C++ | 0.4.0 | `iceberg-cpp/` submodule. |
| GoogleTest | 1.17.0 | `googletest/` submodule; built with project tests. |
| CLI11 | 2.7.2 | `cli11/` submodule; header-only CLI parser. |
| Asio | 1.36.0 | `asio/` submodule; standalone application networking. |
| nlohmann/json | 3.12.0 | `json/` submodule; application JSON codec. |
| reproc/reproc++ | 14.2.7 | `reproc/` submodule; CLI child-process lifecycle, MIT. |
| SQLite | 3.54.1 | Official amalgamation archive. |

Exact commits, URLs, and SQLite checksums are recorded in [lock.json](lock.json).
SQLite's archive was checked against the SHA3-256 published on its
[download page](https://www.sqlite.org/download.html).

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
```

This explicitly builds and installs Arrow/Parquet, Iceberg and SQLite into
`.deps/install`. GoogleTest is built directly from the pinned submodule when
`BUILD_TESTING=ON`; the dependency build does not compile or install it again.
GoogleMock is disabled. CLI11 is consumed directly from its pinned submodule by
the CLI target; it adds no runtime library or public engine dependency. Its tests,
examples, and installation are disabled when included as a subproject.
The dependency build needs
network access for SQLite and upstream transitive dependencies, plus the build
prerequisites of Arrow and Iceberg, including zlib development files. Set
`GLAMDB_DEPENDENCY_JOBS` when configuring to change parallelism within each
dependency build; the default is two.

The application build requires this installation (or an equivalent installation
of the locked dependencies). Check the local submodule revisions without building dependencies:

```sh
cmake -P cmake/dependencies/VerifySubmodules.cmake
```

Arrow enables IPC, compute, filesystem, Parquet, JSON, and compression codecs.
Iceberg uses the same installed Arrow/Parquet and the pinned SQLite library.
SQL catalog support is enabled; REST, Hive, and S3 are disabled. CMake 3.28 is
required by Iceberg's built-in SQL connector.

The lock covers direct dependencies. Transitive dependency recipes belong to the
pinned upstream sources; a complete Linux toolchain and integration receipt is
part of G1. A configured superbuild is not evidence of an installed engine API
or a passing dependency integration probe.

To update a submodule, change its checkout and matching lock entry together.
For SQLite, verify the upstream archive checksum and update the version, URL,
hashes, and source ID. Preserve upstream licenses and notices in the submodules.

## Engine integration requirements

These boundaries include implemented catalog integration and pending production
file I/O, writer and scanner work. See the
[architecture](../docs/ARCHITECTURE.md) and [G1 requirements](../docs/ROADMAP.md#g1-integration-experiments).

- Use one installed Arrow/Parquet runtime shared with Iceberg. C++ dependencies
  come from the locked source builds, not a PyArrow wheel or a second bundled
  Arrow. Verify exported CMake targets with a real installed consumer.
- Prefer Iceberg's public `DataWriter` interface, subject to checks of field
  IDs, real manifest metrics, error propagation, and allocations. A direct
  Parquet writer is a fallback if the probe demonstrates a need; it also owns
  the correct construction of Iceberg file metadata. Neither choice can bypass
  durable metadata I/O.
- Integrate durable local I/O through generic Iceberg `FileIO`. Verify that
  Parquet, Avro manifests/lists, and root JSON all use it, without a native Arrow
  filesystem shortcut. Preserve exclusive creation even if an adapter asks for
  overwrite, and retain the first error across repeated close calls.
- Use the public `SqlCatalog`/`CatalogStore` boundary for the narrow SQLite
  adapter. Verify the actual connection's open flags and durability settings,
  exact prepared base, same-connection outer transaction, atomic outcome update,
  and final commit result. A default catalog factory or an inner Iceberg commit
  is not sufficient evidence for GlamDB's publication guarantee.
- Measure allocations outside Arrow as well as its memory pool. Lazy planning
  does not eliminate materialized metadata, retained history, or executor and
  buffer ownership requirements.

If an interface needs a patch, keep it narrow, pin the affected revision, and
repeat the relevant experiment. Patches live in
`cmake/dependencies/patches/<dependency>/` with the pinned revision and the
reason in their header; `cmake/dependencies/PrepareSource.cmake` copies the
submodule into the build tree and applies them, so the submodule stays clean.
The CI dependency cache key covers these files. Current patches:
`iceberg-cpp/0001-build-json-lists-and-maps-linearly.patch` (table metadata
JSON serialization was quadratic in retained snapshots). The same change is
upstream as
[`36e0d1a`](https://github.com/apache/iceberg-cpp/commit/36e0d1a791f8868bb827986ae48199cced0be52f)
([apache/iceberg-cpp#1111](https://github.com/apache/iceberg-cpp/pull/1111)),
which is after v0.4.0 and not yet released. Delete the patch in the change
that moves the pin to a revision containing that commit: the patch no longer
applies there, so the dependency build fails until it is removed.

After pulling changes to dependency recipes or patches, rerun the `deps-local`
configure and build commands above before rebuilding the application. Building
`local` alone uses the existing installed libraries and does not refresh them.
If an older Iceberg build reports that `iceberg-src/CMakeLists.txt` does not
match the cached `third_party/iceberg-cpp/CMakeLists.txt`, discard only its
generated CMake cache and rerun the dependency build:

```sh
cmake -E rm -f build/deps-local/cmake/dependencies/iceberg/CMakeCache.txt
cmake --build --preset deps-local --parallel 2
cmake --build --preset local --parallel 2
```

Preserve the required semantics rather than
silently changing catalog type or weakening the commit boundary. Record the
SQLite source/build/VFS and complete toolchain with integration results; the
direct dependency lock alone is not that evidence.
