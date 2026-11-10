# GlamDB agent guide

This file applies to this repository. All paths and commands below are relative
to its root. A nested `AGENTS.md` provides additional guidance for its subtree.

## Keeping this guide current

Update this file in the same change whenever implementation makes its project
status, layout, commands, conventions, or validation guidance outdated. Replace
obsolete statements, including scaffold-only and placeholder descriptions, and
add guidance for new capabilities that agents need to work with. Remove
superseded instructions instead of accumulating a history of them.

Keep the affected README and repository documentation consistent with those
updates. Derive implementation status from the code and validation status from
recorded checks; implementing a feature does not by itself qualify its durability
or complete a roadmap gate. Keep this guide concise, with detailed design and
validation material in repository-local documentation. Its links and instructions
must remain usable from a standalone checkout.

## Project context

GlamDB is an open-source C++23 columnar key-value storage engine in early
development. The checkout implements the SQLite catalog, synchronous write and
recovery coordinator, canonical Arrow input, public C++ facade, outcome JSON,
and store-management CLI. The production file producer, durable FileIO and
scanner are pending; collection/data operations require that internal backend.
See [the coordinator guide](docs/development/COORDINATOR.md) for the integration
contract and tested boundaries. Full engine durability remains unqualified.

The first milestone is an embedded, synchronous, durable **append store** on
local disk. A store has one owning process and one serialized writer; reads may
run concurrently. Collections are fixed-schema, unpartitioned Iceberg v2 tables.
Append preserves every input row, including duplicate keys. It provides neither
key uniqueness nor replacement semantics.

Key-value upserts and deletes are a subsequent extension using copy-on-write
and an explicit KV collection mode. HTTP, a network Python SDK, a browser UI,
and object storage are later extensions. Compaction, snapshot expiration,
garbage collection, asynchronous admission, replication, multiple writers, and
schema evolution are outside the initial append scope. Consult
[docs/ROADMAP.md](docs/ROADMAP.md) when assessing feature scope.

## Architecture and correctness boundaries

| Component | Responsibility |
| --- | --- |
| Arrow | Typed in-memory batches and columnar buffers. |
| Parquet | Immutable user-data files. |
| Iceberg C++ | Table schemas, manifests, file membership, and snapshots. |
| SQLite | Persistent catalog pointer, operation identities, and outcomes. |
| GlamDB core | Write ordering, publication, recovery, scanning, and resource ownership. |

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before changing persistence,
recovery, scan semantics, or resource ownership.

Preserve these boundaries when adding or changing engine behavior:

- **One write authority.** All writes and Iceberg commits go through the C++
  core. Transport and client layers adapt its API; they do not own independent
  writers. Keep HTTP, Python, and UI dependencies outside the public API and core.
- **Explicit store identity.** Creation and opening are distinct operations.
  `OpenExisting` validates the expected store identity and existing catalog;
  missing or corrupt state is an error, never an implicit empty-store creation.
  Store ownership remains locked while dependent operations or handles exist.
- **Durable publication.** A committed response requires durable Parquet,
  manifests, manifest lists, and metadata JSON before the final outer SQLite
  transaction commits. File finalization, buffer flush, file sync, checked close,
  and directory sync all matter. SQLite commits do not sync external files;
  successful internal Iceberg updates do not authorize a committed response.
  Preserve the first I/O failure through retries, repeated close, and cleanup.
  The initial catalog profile uses `journal_mode=DELETE` and `synchronous=EXTRA`,
  verified on the actual working connection.
- **Exact commit base.** A prepared write retains its metadata location, table
  UUID, and optional snapshot identity through publication. Reject a changed
  base instead of silently rebasing. A snapshot-only precondition cannot detect
  metadata-only changes.
- **Explicit outcomes and durable retries.** Distinguish `Committed`,
  `DefinitelyNotCommitted`, and `CommitUnknown`. An uncertain commit blocks
  further writes until authoritative recovery and a durability fence resolve
  it; a timeout alone does not prove failure. Bind a collection-scoped operation
  ID to its canonical content hash before file I/O. Persist the committed
  outcome atomically with the catalog pointer. The same ID and hash return the
  original terminal outcome, including failure; a different hash conflicts.
  Outcomes survive restart and do not expire. The initial engine has no input
  payload journal or asynchronous job queue.
- **Snapshot reads.** Resolve the selected view once and enumerate its files
  through Iceberg manifests. Read neither uncommitted nor orphan files by
  scanning warehouse directories. Retain historical snapshots; an initial empty
  view stays empty after later writes. Pruning is conservative and followed by
  exact filtering with SQL three-valued NULL logic; only TRUE matches survive.
- **Types and ownership.** Preserve stable field IDs and validate against the
  fixed schema. Timestamps use UTC microseconds in years 0001 through 9999;
  preserve integer precision across interfaces. Cursors and returned buffers
  retain their allocator, accounting, executor, and I/O resources until the last
  dependent handle is released, even after the engine or cursor is closed.
- **Bounded resources and retained data.** Account for data, metadata, and
  control memory separately, including allocations outside Arrow. Check that a
  published state can be reopened within the same limits. Capacity exhaustion
  produces backpressure before publication, not partial success or hidden
  deletion of history. Orphan files consume capacity; automatic cleanup is
  outside the initial scope. Detected corruption closes readiness; leave SQLite
  journal recovery to SQLite rather than deleting or truncating its journal.

## Repository navigation

| Path | Use |
| --- | --- |
| `public/storage/` | Installed C++ types and synchronous engine facade. |
| `core/` | Canonical input, catalog, coordinator, and internal storage-backend seam. |
| `apps/glamdb/` | Store-management CLI and transport-only outcome serialization. |
| `cmake/` | Build options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Unit, new-process recovery, real SqlCatalog, SQLite VFS fault, and installed-consumer suites. |
| `fixtures/`, `probes/`, `benchmarks/` | Deterministic inputs, isolated experiments, and measurements. |
| `scripts/` | Developer tooling, including quality checks. |
| `third_party/` | Pinned upstream submodules and `lock.json`. |

Read [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) when placing new
components. `sdk/` and `ui/` are reserved empty directories and may be absent
from a fresh clone. Add components as needed by the task; the reserved layout
does not imply implemented functionality.

## Build and development commands

Requirements: CMake 3.28+, Ninja, Python 3.9+, Linux, and a C++23 compiler and
standard library with `std::expected` (GCC 14+ or Clang 18+ with a matching STL).
Quality targets require LLVM major version 18 for both `clang-format` and
`clang-tidy`.

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

The application links the pinned SQLite, Arrow and Iceberg installation. Tests
also require GoogleTest. Use `release` or `asan-ubsan` in place of `local` for
the corresponding configure, build, and test presets. Core protocol tests use a
test-only backend; the real SqlCatalog test verifies the catalog integration
separately. Neither establishes durable user-file production or power-loss safety.

After configuring `local`, check project C++ sources and headers with:

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
```

`quality-negative-check` verifies that deliberate formatting and analyzer
violations are rejected. `cmake --build --preset local --target format` rewrites
project C++ formatting. See
[docs/development/BUILDING.md](docs/development/BUILDING.md) for installation,
tool path overrides, and build details.

For dependency work, read [third_party/README.md](third_party/README.md).
Verify checked-out revisions without building them:

```sh
cmake -P cmake/dependencies/VerifySubmodules.cmake
```

When a task needs the dependency build, initialize submodules and use the
separate preset. It downloads upstream dependencies and installs to
`.deps/install`:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
```

## Code and dependency conventions

- Use C++23 with the checked-in `.clang-format`, `.clang-tidy`, and
  `.editorconfig` as the formatting and diagnostic authorities. Project compiler
  warnings and configured tidy diagnostics are errors. The C++ line limit is
  120 columns.
- Keep each component's private `.h` and `.cpp` together in `core/`. Installed
  public headers belong in `public/storage/` and must be usable without private
  headers, source-tree paths, or transport dependencies.
- Make ownership and error propagation explicit. Recoverable I/O, validation,
  and capacity failures must remain reportable outcomes, not process aborts or
  fabricated successful results.
- Prefer public dependency APIs. Check the pinned source before relying on
  upstream behavior, especially I/O hooks, close semantics, and transaction
  boundaries. Record any necessary upstream patch with its dependency revision.
- Treat `third_party/lock.json` as the authority for direct dependency pins.
  Change submodule revisions and matching lock entries together; preserve
  archive verification and upstream licenses. Keep unrelated vendor edits out
  of application changes.
- Keep generated builds, dependencies, and experiment output in the ignored
  `build/`, `.deps/`, and `artifacts/` directories.

## Validation and reporting

- For behavior changes, add and run focused tests in the appropriate suite.
  Storage tests need independent expected results, including duplicate rows,
  fixed snapshots, retries, and failures at the affected persistence boundary.
  Use a fresh process for reopen/recovery tests and sanitizers for lifetime work.
- Run the relevant build, tests, and quality checks for C++ changes. For
  documentation-only changes, verify factual claims, commands, and links;
  engine or dependency builds are unnecessary unless the change concerns them.
  Read `.github/workflows/` when changing CI; the current workflow-security
  checks apply to every pull request, including documentation changes.
- Separate designed behavior, source inspection, executed PASS/FAIL, and
  checks not run. Report exact commands, results, and missing prerequisites.
  A smoke test does not establish engine correctness or durability.
- Process crashes and power loss require different evidence. `kill -9` does
  not discard the OS cache. Initial durability qualification targets Linux
  x86-64 on a dedicated local ext4 volume with verified flush behavior; macOS
  builds and simulated faults alone do not qualify that environment. Record
  environment details with measurements and durability results.
- Run destructive fault tests only on disposable test stores or dedicated test
  volumes. Keep credentials and private data out of fixtures, traces, and logs.

## Contributions and security

For branch, pull request, or dependency publication work, read
[CONTRIBUTING.md](CONTRIBUTING.md). Development branches belong in forks;
upstream keeps only `main`. Use the repository's pull request template to
describe behavior, validation, and any storage compatibility impact.

When changing workflows, preserve SHA-pinned Actions, read-only PR permissions,
and separation from privileged release credentials. For vulnerabilities, follow
[SECURITY.md](SECURITY.md) and report privately rather than in public issues.
