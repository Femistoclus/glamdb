# GlamDB

An open-source C++23 columnar key-value storage engine in early development.

GlamDB combines Arrow batches, immutable Parquet files, Iceberg snapshots, and a
persistent SQLite catalog. The C++ core owns writes, commit ordering, recovery,
and snapshot reads; the native HTTP server and clients use that same core.

The first engine milestone is an embedded, synchronous **append store** on
Linux and local persistent storage, with one owning process and one writer.
Append preserves duplicate keys. Key-value upserts and deletes follow after
local durability qualification, with further network capabilities and S3
as separate extensions.
See the [architecture](docs/ARCHITECTURE.md) for semantics and the
[roadmap](docs/ROADMAP.md) for scope and acceptance criteria.

## Current status

The SQLite catalog, write/recovery coordinator, canonical Arrow input, public
C++ facade, outcome JSON and network CLI are implemented. Independent
storage primitives implement durable local I/O, Parquet writing, Iceberg append
staging, publication validation, reachable-file recovery, and capacity checks.
Tests cover real files, injected failures and fresh-process recovery.

A storage backend connecting these primitives to the coordinator and a minimal
scanner (`Engine::Scan`) are implemented and tested through `glamdb_vertical`;
see [SCANNER.md](docs/development/SCANNER.md).
See the [coordinator guide](docs/development/COORDINATOR.md)
and [storage guide](docs/development/STORAGE_MODULE.md) for supported boundaries.
Power-loss and load qualification are deferred.

Available now: independent golden fixtures, a typed SQL-NULL reference oracle,
receipt/fault-evidence tools, a network Python SDK, and a configurable pink
browser workbench. The [native HTTP server](docs/development/HTTP_SERVER.md)
connects them to persistent storage and serves the same UI. It supports typed
create/append/scan operations, JSON and Arrow, with explicit capabilities for
unavailable metadata/explain APIs. A separate **disposable in-memory mock**
remains available for development. Its data files,
snapshots and commits are synthetic and disappear on restart.

The workbench also has a **public browser demo** requiring no token or Python
server. Each page instance owns its sample data; visitors can create/append,
filter, explore snapshots and export JSON. Reload resets only that visitor's
demo. See [public demo and publishing](docs/development/PUBLIC_DEMO.md).
The supplied wordmark/transparent mark are used in login, sidebar, avatar and
favicon; artwork paths and sizes remain runtime theme settings.

**[Open the public demo](https://nnrpi.github.io/glamdb/)** — anyone can try it.

For persistent data, start `glamdb --data-dir /absolute/data/path` with
`GLAMDB_TOKEN` set, then create and manage databases in the workbench or API.
No offline initialization is required. Follow the
[native server quickstart](docs/development/HTTP_SERVER.md).
Launch the disposable mock workbench (Python 3.13 and Node 22+):

```sh
bash scripts/bootstrap-workbench.sh
export GLAMDB_TOKEN='choose-a-local-development-token'
.venv/bin/python -m glamdb_mock --ui-dir ui --seed-demo
```

Open **http://127.0.0.1:8765** and enter that token. Edit
[`ui/config/theme.json`](ui/config/theme.json) for live colors, fonts, layout,
branding and bows/sparkles/kisses. No compilation or restart is needed.

See the [workbench guide](docs/development/WORKBENCH.md),
[SDK](sdk/python/README.md), [independent tools](tools/verification/README.md),
[adapter contract](docs/development/HTTP_MOCK_CONTRACT.md), and
[validation report](docs/development/WORKBENCH_VALIDATION.md).

Build/test the native C++ core separately on Linux with its pinned dependencies:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

On macOS, `./scripts/test-in-docker.sh asan-ubsan` tests independent I/O in Linux
without the full dependency prefix. To include the engine and storage adapters:

```sh
./scripts/build-deps-in-docker.sh
./scripts/test-storage-in-docker.sh asan-ubsan
```

The full build installs `glamdb::core` and public headers. Storage adapters are
enabled with `GLAMDB_BUILD_STORAGE=ON`; `GLAMDB_IO_ONLY=ON` provides a separate
lightweight build. See the build guide for Docker setup.

- [Project structure](docs/PROJECT_STRUCTURE.md)
- [Build and quality checks](docs/development/BUILDING.md)
- [C++ style guide](docs/development/CPP_STYLE_GUIDE.md)
- [Dependencies](third_party/README.md)
- [Agent guide](AGENTS.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
