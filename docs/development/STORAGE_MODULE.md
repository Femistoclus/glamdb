# Durable storage module

The module supplies local durable files and Arrow/Parquet/Iceberg integration
for the append-store MVP. The catalog/coordinator and installed public API are
separate components; `core/backend/` bridges them to these primitives and
`core/scan/` reads them, as described in [SCANNER.md](SCANNER.md). See
[the coordinator guide](COORDINATOR.md) for the integration contract.
The [shared resource budget matrix](RESOURCE_BUDGETS.md) distinguishes persisted
file/history limits and disk preflight checks from shared runtime memory and
device reservations, including completion and recovery capacity.

`glamdb_io` contains the independent local I/O components. `glamdb_storage`
adds the pinned dependency adapters; `GLAMDB_BUILD_STORAGE` enables it and
defaults to `ON` unless `GLAMDB_IO_ONLY=ON`. See [Building](BUILDING.md#storage-adapters) for the
Docker runner and [dependency validation](DEPENDENCY_VALIDATION.md) for the
installed-prefix consumer and toolchain constraints.

## Integration sequence

The coordinator retains the process/store ownership lock and serializes writers.
It opens or creates directories through `core/io/store_dirs.h`, constructs the
adapter in `core/storage/durable_local_file_io.h`, and retains the confirmed base
from `VerifyReachable` or a previously confirmed publication.

1. Run `CheckWriteCapacity` before binding a new operation. It checks the root
   identity, accounts for the warehouse including orphan files, and reserves space
   for the next attempt, journal, and filesystem headroom. The catalog owner
   supplies aggregate storage usage separately to the publication guard. The
   backend uses a conservative incremental inventory on supported Linux local
   filesystems; other adapters retain full traversal. `PrepareAppend` reuses the
   admission bound (binding writes only the catalog) and rechecks root identity
   and free space. Full walks use a private descriptor. See
   [warehouse inventory](WAREHOUSE_INVENTORY.md) for fallback, deadline, memory
   bounds and the reviewed change to external-mutation detection.
2. The coordinator owns operation identity, canonical hashing, durable binding,
   and input normalization. Pass owned, immutable, normalized Arrow batches to
   `WriteDataFile`; their buffers must remain unchanged until it returns. The
   writer validates bounded input against the loaded table schema and derives
   metrics from its properties. It does not perform the coordinator's copy or
   canonical-hash step. The pinned Parquet writer closes a buffered row group
   at exactly `write.parquet.max-row-group-rows` consecutive rows, across input
   batches. The writer picks the largest such count for which every resulting
   group's summed row estimates plus one 4-byte offset per string field stay
   within `MaxRowGroupBytes`; one wide row no longer sizes every group, and
   uniform rows keep the previous largest-row quotient. Variable-size groups
   would need a row-group flush hook in the Iceberg Parquet wrapper. A `TParquetWriteSpec`
   selects the file-level codec/level and per-column overrides (exact top-level
   field names); an empty spec adds no writer properties and keeps the pinned
   defaults (zstd, default level). The writer rejects unknown columns,
   unavailable codecs and out-of-range levels before creating the file, and
   after closing it checks that every column chunk of the footer records the
   requested codec. Parquet does not record levels, so levels are verified only
   by range. The spec also selects the file-level and per-column dictionary
   use, the dictionary page limit and per-column value encodings; the footer
   check requires a dictionary page exactly where a dictionary applies (never
   for `bool`) and data page encodings of `rle_dictionary` with its `plain`
   fallback there, or exactly the configured encoding (default `plain`)
   elsewhere.
3. Create an attempt and hold its `BindAttempt` scope across data writing,
   `StageAppend`, metadata writing, and publication checking. The attempt owns
   sticky errors, budgets, file state, durable receipts, and format verification.
   Delayed handles retain their original attempt; releasing the scope deactivates it.
4. `StageAppend` commits the Iceberg append update to its transaction and returns
   the staged transaction, snapshot identity, and manifest paths. It does not
   perform the outer transaction commit. The catalog/coordinator owner passes
   expected operation fields in the summary map and the guard's `ExpectedSummary`;
   this module does not define that owner's persistent receipt schema.
5. The catalog calls `BeforePointerChange` after the candidate metadata JSON is
   durable and before changing the catalog pointer. The guard seals the attempt,
   verifies the exact prepared base, metadata delta, reachable new files,
   retained manifests, expected summary fields, and aggregate logical capacity limits.
   For an append the backend serializes the staged root once (the exact bytes of
   `TableMetadataUtil::Write`) and passes both forms; the guard requires the
   durable root to equal those bytes and runs every validation on the in-memory
   staged metadata instead of parsing the file. This relies on parsing our roots
   reproducing the staged metadata, which `vertical/root_round_trip_test.cpp`
   establishes for roots written by real appends (byte-identical reserialization
   and structural equality). `VerifyStagedRoot` (on by default without `NDEBUG`,
   `GLAMDB_VERIFY_ROUNDTRIP=1|0` overrides it; the vertical CTest suite sets it)
   additionally parses the durable root and requires that equality before
   publication. Collection creation still parses its root.
   It rechecks the physical warehouse cap after production, before allowing
   publication: for an append it adds the physical size of every durable file
   of the sealed attempt (the larger of length and allocated blocks, recorded
   after the file sync) and the growth of the collection's data/metadata
   directories to the conservative pre-production inventory, including orphans.
   Ordinary notified growth outside the writer after admission is first rejected
   by the next admission under the inventory ownership contract. Any failed
   estimate, including a conservative capacity refusal, falls back to an exact
   walk before rejecting publication. Collection creation, and `WalkWarehouseAtPublication=true`, still
   walk the whole warehouse at publication.
6. `GetCandidate()` returns a tentative verified state. Install it as the next
   confirmed base only after the caller confirms its outer catalog COMMIT.
   Atomic pointer/outcome publication, uncertain-commit recovery, fencing, and
   client acknowledgements remain catalog/coordinator responsibilities.

On open and reconciliation, the backend checks physical warehouse usage (including
orphans) independently of reachable metadata/history usage. Recovery sums each
verified collection with overflow and aggregate limit checks before admitting the
next collection. It requires no disk reserve for a future write. A lower physical
limit fails with `RESOURCE_EXHAUSTED`; retained logical usage outside the selected
profile fails closed as `CORRUPTION`. Neither failure deletes retained files.

For collection creation, `MakeTableProfile` supplies the schema and exact table
properties used with the public Iceberg create API. The adapter and guard check
the resulting durable root. Public collection lifecycle and catalog registration
remain external. Schema names are nonempty UTF-8, at most 128 bytes, and contain
no NUL; the optional `key` field must be a required string. Field IDs are stable
1..N, with no Iceberg identifier fields. Publication and recovery both require the
canonical schema and exact properties returned by `MakeTableProfile`; caller
property extensions and changes to that profile are rejected before publication.
Snapshot-log timestamps refer to their snapshots. The root update timestamp is
independent and is preserved on recovery, including when the wall clock moves.
Staging, publication and recovery reject snapshot timestamps outside UTC years
0001 through 9999 before callers convert milliseconds to microseconds.

Failures that the engine must classify carry stable message prefixes from
`core/storage/iceberg_error_boundary.h`: `CORRUPTION: ` marks a violated storage
invariant, `CAPACITY: ` a configured storage or file limit (candidate store
usage, candidate root size, append accumulators and the Parquet file limit) and
`RESOURCE: ` memory or recovery-workspace exhaustion (`not_enough_memory` from
the FileIO, `std::bad_alloc` caught at the Iceberg boundary, a full workspace).
Recovery charges each retained commit's manifest and list to `CommitBytes`
and the final root only to the current commit, matching what each publication
guard checked, so any accepted history reopens under the same limits.
The durable FileIO reports store-layout violations as `kInvalid` with the
corruption prefix: a symlink (ELOOP with `O_NOFOLLOW`), a non-directory or
cross-device entry, and, when opening an existing file, a non-regular file or
one larger than its file-kind limit. See
[SCANNER.md](SCANNER.md#error-classification) for the engine mapping.

## Manifest merging

Ordinary and bulk appends use the pinned Iceberg C++ `NewMergeAppend` inside
the existing append transaction. The upstream defaults target 8 MiB manifests
and merge a bin containing the newest manifest at 100 manifests. Full bins can
merge earlier. No separate maintenance commit, data rewrite, table-property
change or storage-format migration is required. Existing FastAppend histories
are still accepted, and a subsequent append can merge their small manifests.

The shared closure verifier matches unchanged manifest descriptors exactly,
then checks removed parent manifests against the new manifests. Every old live
entry must occur exactly once as EXISTING, with identical data-file metadata,
metrics, snapshot ID and sequence numbers. Deletes, missing/duplicate/invented
entries and changed counters fail closed. Only ADDED entries contribute new
data, summary counters, footer usage and scan membership; ordinary appends still
add exactly one file and bulk appends at most 256.

Before materializing changed manifests, publication and recovery check their
combined input/output bytes, manifest-list bytes and root against 32 MiB; only
the current commit is charged the final root on recovery. Each manifest remains
at most 8 MiB, each rewrite's entries are bounded by the store file cap, and at
most eight new reachable manifests are accepted. The FileIO attempt independently
limits all newly written manifests, including an unreferenced staging manifest.
Retained usage counts new reachable outputs once; old manifests stay reachable
through history. Unreferenced staging files remain charged to physical capacity.

The latest list is bounded by target-sized bins and fewer than 100 small
manifests under the finite metadata profile. For the default capacity workload,
the current membership fits one target bin, so the latest list cycles from
one through 99 manifests. With every snapshot retained, merging every K appends
retains O(N²/K) file descriptors in merged manifests in this single-bin regime,
while retained manifest-list entries become O(KN). Root JSON still rewrites
retained history; snapshot expiration and #168 remain separate work. See
[capacity measurements](CAPACITY_CAMPAIGN.md) for the executed comparison.

## Read, recovery, and copy boundaries

Reads on the binding thread participate in its active attempt and I/O budget.
Scan callers must retain an independent `ReadOnlyView()`; they must not reuse
the writer's implicit binding. The adapter rejects writes through that view and
keeps its filesystem resources alive. Snapshot selection, planning, pruning,
filtering, and returned Arrow buffer ownership belong to the scanner owner.

`VerifyReachable` validates a selected root without requiring receipts from the
creating process. It checks the supported table profile, retained snapshot chain,
manifest lists/manifests, data file lengths and Parquet footers, and storage
usage. It registers the required format readers in a fresh process. The catalog
owner selects the authoritative root and performs SQLite recovery/fencing.
This verification does not checksum every byte of historical data pages.

Recovery checks snapshot transitions with a two-snapshot window and retains the
loaded root for the final consistency check. Previously seen manifest-list and
data paths use a private temporary SQLite database capped by the selected
recovery workspace budget (256 MiB by default), with an
eight-page cache. It does not modify the catalog or warehouse; workspace I/O or
capacity failures fail recovery. SQLite must use disk-backed temporary databases
and its temporary directory must have sufficient space on local disk. The
bounded snapshot-ID set and current manifest list remain in memory; total RSS
and reference-host recovery time still require qualification.

`CopyDurableFile` copies an exact known length through a fixed-size buffer,
checks source identity/size and source close, and finalizes the target durably.
The caller must hold an offline snapshot/lock and prepare a durable destination
directory. The primitive does not orchestrate a whole-store backup or publish
backup readiness. The separate [whole-store acceptance/backup tool](NATIVE_ACCEPTANCE.md)
uses closed SQLite plus an independent retained directory lease; it is not a
property of the collection-copy primitive.

Warehouse accounting uses the larger of logical size and allocated blocks for
regular files, plus directory blocks; hard links are conservatively counted per
entry. The walk rejects symlinks, special files, cross-device entries, and depth
above 64. It creates no files and performs no orphan cleanup. Limits are
configurable development candidates pending measured qualification.

## Tests and evidence

The following files under `tests/unit/` cover the corresponding implementation.
They are registered with CTest label `unit`; storage tests require the optional
target.

| Component | Tests |
| --- | --- |
| Filesystem, URI, directory identity, SHA-256 | `io/local_fs_test.cpp`, `io/store_uri_test.cpp`, `io/store_dirs_test.cpp`, `io/sha256_test.cpp` |
| Attempt state, durable close, first errors, receipts | `io/io_attempt_test.cpp`, `io/durable_file_test.cpp` |
| Fault matching and durability ordering | `io/fault_fs_test.cpp`, `io/durability_trace_test.cpp` |
| Durable copy and source/target failure boundaries | `io/durable_copy_test.cpp` |
| Schema/profile and permitted metadata changes | `storage/table_profile_test.cpp`, `storage/metadata_validation_test.cpp` |
| FileIO binding, reads, denied deletes, lifetime | `storage/durable_local_file_io_test.cpp` |
| Real formats, bounded decoding, writer metrics and failures | `storage/format_verifier_test.cpp`, `storage/data_file_writer_test.cpp` |
| Writer compatibility and entirely absent footer statistics | `storage/writer_compatibility_test.cpp` |
| Staging, manifest round trips, no implicit publication | `storage/append_stager_test.cpp` |
| Guard, closure/recovery, retained history, invalid candidates, staged-root byte/round-trip checks | `storage/publication_guard_test.cpp` |
| Recovery visited index, exact duplicates, disk spill and workspace failures | `storage/recovery_visited_test.cpp` |
| Capacity boundaries, orphans, preflight faults | `storage/capacity_test.cpp`, `io/local_fs_test.cpp` |

The native `vertical/aggregate_capacity_test.cpp` suite additionally covers
cross-collection root/history limits, uncertain COMMIT success and rollback,
physical orphan usage on publication/reconciliation/reopen, and fresh-process
rejection under lower limits. `vertical/append_preflight_cost_test.cpp` bounds
warehouse traversal to zero walks per steady append on a supported filesystem
(one per admission in strict/fallback mode), and prints per-append
root/manifest-list/manifest opens. `io/tree_inventory_test.cpp` checks incremental
physical accounting, mutation races, invalidation, overflow fallback and deadlines. `vertical/append_history_bench_test.cpp` is a
manual benchmark (skipped unless `GLAMDB_APPEND_HISTORY_BENCH=<appends>`) that
reports one-row append latency per bucket of retained history.

The fault adapter also injects errors after a successful native file or directory
sync. Nested traces distinguish the successful syscall from the reported error;
all four file kinds retain the first error, issue no receipt and block further
writes. This exercises conservative error handling, not power loss.

`TManifestMetricsTest.RoundTripsIndependentMetricsThroughRealParquetAndAvro`
checks serialized field IDs, value/null/NaN counts, numeric bounds, full ASCII
and 1024-byte UTF-8 key bounds, and column sizes in a real Avro manifest against
independent row expectations and its Parquet footer. An all-NULL field exercises
omitted min/max bounds.

`TWriterCompatibilityTest` compares the complete no-key DataFile, physical bytes
and decoded rows against the upstream DataWriter. Its second test uses the public
Parquet API to disable statistics for every column, then checks format validation,
decoded rows, Avro round-trip of absent metrics and recovery. The production
writer has no test-only statistics switch. These checks passed with ASan/UBSan
and LLVM 18 analysis; writer RSS measurements remain pending. Run them with:

```sh
ctest --preset asan-ubsan --output-on-failure --no-tests=error \
    -R 'TManifestMetricsTest|TWriterCompatibilityTest'
```

`WritesTwoAppendsAndReopensEveryRetainedFile` in the publication guard suite writes
1,000 and then 10 rows with duplicate keys through the real Parquet writer,
stages real Avro manifests/lists, writes metadata, checks publication, and reopens
the closure in a fresh Linux process. Its upstream in-memory catalog is a test
double for the catalog integration boundary. The child helper is skipped when CTest
invokes it alone and is explicitly executed by this pipeline test.

With the full build and storage enabled, `glamdb.durable_storage_catalog` combines
the durable FileIO, real writers and publication guard with the real SqlCatalog
and SQLite catalog store. It verifies create, appends of 1000 and 10 duplicate
rows, guard rejection, outer rollback and three actual intervening base changes.
An independent SQLite connection checks that inner commits remain tentative.
An observing catalog forwards real commits unchanged and checks the emitted
table UUID and `main` snapshot requirements, including the first append's absent
snapshot. A competing staged create is rejected before its publication body or
guard can run, preserving the winning name, UUID and root.
A new process selects its root from the reopened catalog, compares exact stored
outcomes with caller-result fixtures, and reads historical/current rows through
the manifests. Failed candidates remain unreachable; this raw catalog test leaves
their bindings unresolved rather than claiming coordinator reconciliation.
It does not implement the production StorageBackend or scanner. Run it with:

```sh
ctest --preset asan-ubsan --output-on-failure --no-tests=error -R '^glamdb.durable_storage_catalog$'
```

The full Linux ARM64 Docker baseline registered 380 tests: 379 passed with ASan/UBSan,
and the child-process dispatcher skipped its standalone invocation. The parent
pipeline executed it successfully in a fresh process. Regressions for delayed
Iceberg commits and unsupported profile extensions failed against the original
implementation and passed after the fixes. LLVM 18 formatting checked 109 project
files; changed C++ analyzer checks passed. The Release installed-package consumer
was verified separately. Run the current checkout with:

```sh
./scripts/test-storage-in-docker.sh asan-ubsan
./scripts/test-installed-deps-in-docker.sh
```

The storage sanitizer profile has the documented libc++ compatibility restriction
in [Building](BUILDING.md#storage-adapters); it is not a fully instrumented
dependency qualification. Product integration, load/capacity measurement,
long-running fault campaigns, and Linux x86-64/ext4 power-loss qualification are
deferred. Simulated failures and fresh-process recovery do not establish
power-loss durability.

Recovery tests cover the temporary index, nine retained-history cases and reopening
at exact usage limits; reducing each positive usage limit rejects the same store
without modifying it. Collection-copy tests use real Parquet/Avro/JSON files,
remove their disposable source and reopen old/current roots in new processes.
Interrupted copies fail on the injected Parquet corruption. These component tests
do not cover catalog backup, backup leases or restore through the engine API.
Both copy cases also passed with source temporary files on tmpfs and the destination
on a different filesystem; deleting the source does not require a cross-device rename.
The follow-up publication/recovery run registered 57 tests: 56 passed; the child
dispatcher skipped its standalone invocation and ran through its parent tests.
The real catalog integration also passed separately.
The bounded file-I/O probe and its separately checked syscall trace are described in
[qualification preparation](QUALIFICATION.md#bounded-file-io-probe).

Raw generated logs include `artifacts/qualification-full-engine-check.log`,
`artifacts/after-success-io.log` and `artifacts/p-io-final-evidence.log`.
Earlier regression and installed-package evidence is retained in
`artifacts/review-regression-before.log` and `artifacts/merged-install.log`.

## Bulk append extension

[Bulk ingestion](BULK_INGESTION.md) uses an explicit bounded I/O attempt profile,
file rollover and `StageBulkAppend` to stage multiple verified files in one
merge append. Supported bulk markers permit bounded multiple ADDED entries;
ordinary unmarked transitions keep the one-added-file rule even when a merged
manifest carries older EXISTING entries. Publication and recovery
check actual aggregate rows/files/bytes against the summary and receipt.
