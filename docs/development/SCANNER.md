# Storage backend and scanner MVP

`core/backend/` connects the coordinator seam in `core/engine/storage_backend.h`
to the [durable storage primitives](STORAGE_MODULE.md). `core/scan/` implements
the public `Engine::Scan` declared in `public/storage/scan.h`. Both are built
when `GLAMDB_BUILD_STORAGE=ON`, the default unless `GLAMDB_IO_ONLY=ON`. Without
them the public factories install no backend and collection creation, appends
and scans report `DEPENDENCY_UNAVAILABLE`.

> [!IMPORTANT]
> This code has not yet been built or run against the pinned Iceberg
> dependency. Only the Arrow-only parts (`storage_mapping`, `predicate`) have
> executed unit tests. See [Validation status](#validation-status).

## Write path

`CreateStore`/`OpenExisting` construct `TLocalStorageBackend` after the store
lease has created or validated `root/` and `warehouse/`
(`EngineFactory::CreateWith/OpenWith`). The internal factories that take a
ready backend remain for coordinator tests.

- **Create:** `MakeTableProfile` maps the fixed `SchemaSpec` (field IDs 1..N).
  The backend creates the collection directories, binds a `CreateCollection`
  attempt, writes `metadata/00000-<attempt>.metadata.json` through the durable
  FileIO and returns the Iceberg table UUID. `Publish` inserts the
  `columnar.<exact name>` pointer; the catalog guard runs
  `TStoragePublicationGuard` before that insert.
- **Append:** normalized coordinator rows are converted into one owned Arrow
  batch, so stored values are exactly the hashed canonical content. The backend
  writes `data/<operation>-<attempt>-00000.parquet`, stages a fast append with
  summary keys `glamdb.operation-id` and `glamdb.content-hash`, and writes the
  next numbered root. `Publish` is one CAS from the exact prepared root. The
  receipt carries the staged snapshot ID, parent, row count, one data file, its
  size and the snapshot timestamp in microseconds.
- `iceberg::Table` requires a catalog; the backend passes `TInertCatalog`,
  whose every operation fails, so no Iceberg commit can bypass the coordinator.
- Guard candidates are tentative. A later binding whose base is the candidate
  root proves the outer COMMIT and promotes it; otherwise the backend
  re-verifies the committed root with `VerifyReachable`.
- `VerifyRetained` verifies every collection root, its schema, and a one-to-one
  mapping between retained snapshots and committed receipts. Missing or
  inconsistent committed state is `CORRUPTION`, which closes readiness.

Known limits: the writer accepts 1..10000 rows and at most 16 MiB per append,
and capacity preflight runs inside `PrepareAppend`. Both are checked after the
coordinator has bound the operation, so a rejected append becomes a permanent
`DefinitelyNotCommitted` outcome for that operation ID. The default storage
limits require free-space reserves (see `TStorageLimits`). Orphan files from
failed attempts remain.

## Scan path

`Engine::Scan` validates the collection, resolves the confirmed SQLite view
once, and opens a cursor through the backend's read-only FileIO view; it never
joins a writer attempt or holds a SQLite read transaction.

- `SnapshotSelector`: `Latest` (resolved at open), `Id`, `InitialEmpty`.
  `ScanInfo.selector_resolved` reports `Id(snapshot)` or `InitialEmpty`.
- Planning reads the committed root, the selected manifest list and live
  manifest entries. Warehouse directories are never listed. Delete manifests,
  partitioned specs and non-Parquet files are `UNSUPPORTED_TABLE_FEATURE`.
- `Predicate` supports `True` and one comparison (`Eq`, `Ne`, `Lt`, `Le`, `Gt`,
  `Ge`) of a field with a literal of exactly the field type. A NULL cell is
  UNKNOWN and never returned, which equals SQL three-valued logic for a single
  comparison. Strings compare by unsigned UTF-8 bytes; literal `-0.0` matches
  stored `0.0`; NaN/Inf and out-of-range timestamps are `INVALID_ARGUMENT`.
- Each file is read whole (at most 32 MiB in this profile), opened with
  `parquet::arrow`, checked against the field IDs 1..N and the manifest row
  count, then filtered, projected and limited. Batches have at most
  `batch_rows` rows and the exact `Info().output_schema`. `limit = 0` and empty
  views return `nullptr` from the first `Next()`.
- The cursor retains the engine state, and therefore the store lease, until it
  is released. `Cancel()` or `Engine::Shutdown()` makes `Next()` return
  `CANCELLED`; `Info()` remains valid. Read failures are errors, never EOS.

Not implemented: file/row-group pruning, compound predicates (`And`, `Or`,
`Not`, `In`, `IsNull`), streaming planning, runtime memory leases and quotas.

## Validation status

| Check | Status |
| --- | --- |
| `cmake --preset local && cmake --build --preset local` (GCC 14.2.0, `GLAMDB_BUILD_STORAGE=ON`) | PASS |
| `tests/unit/storage_mapping_test.cpp`, `tests/unit/scan_predicate_test.cpp` (`glamdb_unit`) | 10/10 PASS |
| `tests/vertical/engine_vertical_test.cpp` (`glamdb_vertical`, CTest labels `vertical;scan`) | 10/10 PASS |
| `ctest --preset local` (`unit`, `vertical`, `fault`, `integration`, `installed_consumer`, `tooling`, `smoke`) | 364/364 PASS (standalone `RecoveryChildProcess` helper skipped as expected; executed by its parent test) |
| `cmake --build --preset local --target format-check` and `clang-tidy-18` on new/modified TUs | PASS (`format-check` 117/117 files; full-repo `tidy-check` and sanitizer presets not run in this session) |
