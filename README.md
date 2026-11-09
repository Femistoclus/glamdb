# GlamDB

An open-source C++23 columnar key-value storage engine in early development.

GlamDB combines Arrow batches, immutable Parquet files, Iceberg snapshots, and a
persistent SQLite catalog. The C++ core owns writes, commit ordering, recovery,
and snapshot reads; future network clients use that same core.

The first engine milestone is an embedded, synchronous **append store** on
Linux and local persistent storage, with one owning process and one writer.
Append preserves duplicate keys. Key-value upserts and deletes follow after
local durability qualification, with HTTP, SDK/UI, and S3 as separate extensions.
See the [architecture](docs/ARCHITECTURE.md) for semantics and the
[roadmap](docs/ROADMAP.md) for scope and acceptance criteria.

## Current status

The repository currently contains a build scaffold, a no-op executable, a smoke
test, quality tooling, and pinned dependency build recipes. Engine APIs,
persistence, recovery, and durability qualification are not implemented yet.
The commands below build and test the scaffold; they do not run a storage server
or demonstrate data durability.

```sh
cmake --preset local
cmake --build --preset local
ctest --preset local
```

- [Project structure](docs/PROJECT_STRUCTURE.md)
- [Build and quality checks](docs/development/BUILDING.md)
- [Dependencies](third_party/README.md)
- [Agent guide](AGENTS.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
