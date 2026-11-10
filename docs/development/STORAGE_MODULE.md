# Durable storage module

The module supplies local durable files and Arrow/Parquet/Iceberg integration
for the append-store MVP. The catalog/coordinator and installed public API are
separate components; `core/backend/` bridges them to these primitives and
`core/scan/` reads them, as described in [SCANNER.md](SCANNER.md). See
[the coordinator guide](COORDINATOR.md) for the integration contract.

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
   identity, measures the warehouse including orphan files, and reserves space
   for the next attempt, journal, and filesystem headroom. The catalog owner
   supplies aggregate storage usage separately to the publication guard.
2. The coordinator owns operation identity, canonical hashing, durable binding,
   and input normalization. Pass owned, immutable, normalized Arrow batches to
   `WriteDataFile`; their buffers must remain unchanged until it returns. The
   writer validates bounded input against the loaded table schema and derives
   metrics from its properties. It does not perform the coordinator's copy or
   canonical-hash step.
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
   retained manifests, expected summary fields, and capacity limits.
6. `GetCandidate()` returns a tentative verified state. Install it as the next
   confirmed base only after the caller confirms its outer catalog COMMIT.
   Atomic pointer/outcome publication, uncertain-commit recovery, fencing, and
   client acknowledgements remain catalog/coordinator responsibilities.

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

`CopyDurableFile` copies an exact known length through a fixed-size buffer,
checks source identity/size and source close, and finalizes the target durably.
The caller must hold an offline snapshot/lock and prepare a durable destination
directory. The primitive does not orchestrate a whole-store backup or publish
backup readiness.

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
| Staging, manifest round trips, no implicit publication | `storage/append_stager_test.cpp` |
| Guard, closure/recovery, retained history, invalid candidates | `storage/publication_guard_test.cpp` |
| Capacity boundaries, orphans, preflight faults | `storage/capacity_test.cpp`, `io/local_fs_test.cpp` |

`WritesTwoAppendsAndReopensEveryRetainedFile` in the publication guard suite writes
1,000 and then 10 rows with duplicate keys through the real Parquet writer,
stages real Avro manifests/lists, writes metadata, checks publication, and reopens
the closure in a fresh Linux process. Its upstream in-memory catalog is a test
double for the catalog integration boundary. The child helper is skipped when CTest
invokes it alone and is explicitly executed by this pipeline test.

The Linux ARM64 Docker run registered 346 tests: 345 passed with ASan/UBSan,
and the child-process dispatcher skipped its standalone invocation. The parent
pipeline executed it successfully in a fresh process. Regressions for delayed
Iceberg commits and unsupported profile extensions failed against the original
implementation and passed after the fixes. The delayed-commit and existing
publication pipelines each passed twenty consecutive runs. The Release
installed-package consumer was also verified separately. Run the current
checkout with:

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

LLVM 18 formatting checked all 101 project files, component and incoming-source
analyzer checks passed, and both quality negative controls passed. Raw generated
logs include `artifacts/pr-review-fixed.log`, `artifacts/review-regression-before.log`
and `artifacts/merged-install.log`.
