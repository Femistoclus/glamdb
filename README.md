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

Available now: independent golden fixtures, a typed SQL-NULL reference oracle,
receipt/fault-evidence tools, a network Python SDK, and a configurable pink
browser workbench. A separate **disposable in-memory mock** supplies missing
engine/HTTP interfaces. Its HTTP/Arrow transport is real; its data files,
snapshots and commits are synthetic and disappear on restart.

The workbench also has a **public browser demo** requiring no token or Python
server. Each page instance owns its sample data; visitors can create/append,
filter, explore snapshots and export JSON. Reload resets only that visitor's
demo. See [public demo and publishing](docs/development/PUBLIC_DEMO.md).
The supplied wordmark/transparent mark are used in login, sidebar, avatar and
favicon; artwork paths and sizes remain runtime theme settings.

**[Open the public demo](https://nnrpi.github.io/glamdb/)** — anyone can try it.

Launch the workbench (Python 3.13 and Node 22+):

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

- [Project structure](docs/PROJECT_STRUCTURE.md)
- [Build and quality checks](docs/development/BUILDING.md)
- [Dependencies](third_party/README.md)
- [Agent guide](AGENTS.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
