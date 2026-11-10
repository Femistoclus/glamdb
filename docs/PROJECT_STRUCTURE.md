# Project Structure

```text
glamdb/
├── AGENTS.md
├── CMakeLists.txt
├── CMakePresets.json
├── .clang-format
├── .clang-tidy
├── .editorconfig
├── .gitmodules
├── apps/
│   ├── mock_server/
│   └── glamdb/
│       ├── CMakeLists.txt
│       └── main.cpp
├── public/
│   └── storage/
├── core/
├── cmake/
│   ├── CompilerOptions.cmake
│   ├── Sanitizers.cmake
│   ├── Quality.cmake
│   └── dependencies/
│       └── sqlite/
├── tests/
│   ├── verification/
│   ├── unit/
│   ├── integration/
│   ├── fault/
│   └── installed_consumer/
├── fixtures/
├── tools/verification/
├── probes/
├── benchmarks/
├── sdk/
├── ui/
├── scripts/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PROJECT_STRUCTURE.md
│   ├── ROADMAP.md
│   └── development/
└── third_party/
    ├── arrow/
    ├── iceberg-cpp/
    ├── googletest/
    ├── lock.json
    └── README.md
```

| Path | Purpose |
| --- | --- |
| `AGENTS.md` | Repository guidance for coding agents, maintained alongside implementation changes. |
| `public/storage/` | Public C++ API headers. |
| `core/` | Engine implementation and private headers. Keep each component's `.h` and `.cpp` together. |
| `apps/glamdb/` | Store-management CLI and transport-only outcome JSON. |
| `cmake/` | Compiler options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Unit, process recovery, real Iceberg catalog, VFS fault, and installed-library consumer tests. |
| `fixtures/` | Shared deterministic test inputs and expected outputs. |
| `apps/mock_server/` | Disposable in-memory adapter with real HTTP/Arrow and runtime config. |
| `tools/verification/` | Literal canonical vectors, SQL 3VL oracle and receipt/fault-evidence tools. |
| `probes/` | Isolated dependency and platform experiments. |
| `benchmarks/` | Performance and capacity measurements. |
| `sdk/python/` | Independently installable network SDK, unit tests and quickstart. |
| `ui/` | Source-served HTML/CSS/ES modules, live theme JSON, Node unit and Playwright browser tests. |
| `scripts/` | Developer tooling. |
| `docs/development/` | Build and tooling instructions. |
| `docs/ARCHITECTURE.md` | Intended engine semantics, persistence boundaries, and resource ownership. |
| `docs/ROADMAP.md` | Implementation stages, prerequisites, and acceptance evidence. |
| `third_party/` | Pinned upstream submodules and the direct dependency lock. |

The catalog and coordinator are implemented under `core/api/`, `core/catalog/`
and `core/engine/`; production file I/O and scanning are pending. Client and
verification code uses an explicit mock seam. Read [ARCHITECTURE.md](ARCHITECTURE.md)
for engine responsibilities, [ROADMAP.md](ROADMAP.md) for acceptance criteria,
and [the workbench guide](development/WORKBENCH.md) for runnable client paths.

Generated files belong in ignored `build/`, `.deps/`, and `artifacts/` directories.

## Engine boundaries

Add components when their behavior is implemented rather than creating empty
files for every anticipated module. The intended responsibilities are:

| Area | Responsibility |
| --- | --- |
| Public API | Store/collection operations, requests/results, typed predicates, and scan cursors with schema and snapshot information. |
| Write and recovery coordination | Serialized writes, prepared bases, operation identities/outcomes, publication, and reconciliation. |
| Local I/O | Immutable file creation, durable close, path validation, file receipts, and a traceable fault-injection boundary. |
| Catalog | Persistent SQLite connection, exact-base checks, atomic pointer/outcome publication, and recovery fencing. |
| Storage integration | Arrow/Parquet writers and readers plus Iceberg metadata, snapshots, and metrics. |
| Scanning | Fixed-view planning, conservative pruning, projection, and exact filtering. |
| Runtime | Shared budgets, cancellation, resource leases, and store ownership. |

These are logical boundaries, not prescribed filenames or existing CMake
targets. Public headers must install without private implementation headers.
Transport, SDK, and UI dependencies stay outside `public/storage/` and `core/`;
all write paths still pass through the engine.

## Validation layout

`tests/unit/` is intended for local contracts such as types, predicates, and
state transitions. `tests/integration/` covers embedded append/read/reopen and
API behavior. `tests/fault/` covers persistence failures, recovery, and corruption
using disposable stores. `tests/installed_consumer/` exercises the installed
public package without access to source-tree headers.

`probes/` holds bounded dependency/platform experiments, while `benchmarks/`
holds workload and capacity measurements. Neither a dependency probe nor a
benchmark replaces correctness tests. Share deterministic inputs and expected
results through `fixtures/`; put generated stores, traces, and measurements in
ignored output directories. See [BUILDING.md](development/BUILDING.md) for
current commands and [COORDINATOR.md](development/COORDINATOR.md) for test scope.

Independent Python suites live in `tests/verification/`, `sdk/python/tests/`,
`apps/mock_server/tests/` and `tests/integration/test_network_sdk.py`; browser
tests live in `ui/tests/`. Their results do not qualify the full native storage integration.
