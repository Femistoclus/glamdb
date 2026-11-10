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

The SQLite catalog, write/recovery coordinator, canonical Arrow input, public
C++ facade, outcome JSON and store-management CLI are implemented. Persistent
operation bindings and outcomes, atomic publication, uncertain-commit recovery,
and the real Iceberg catalog seam have executable tests. The production durable
file backend and scanner remain pending; there is no storage HTTP server yet.
See the [coordinator guide](docs/development/COORDINATOR.md) for the supported
boundary and validation limits. Full append-store durability is not qualified.

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
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
