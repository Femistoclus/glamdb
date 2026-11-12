# Retained history validation

This campaign exercises [issue #156](https://github.com/glamourdb/glamdb/issues/156)
through ordinary public appends in one database. The
[profile and implementation guide](SCALABLE_HISTORY.md) describes the budgets;
the [machine-readable evidence](evidence/scalable-history-2026-11-12.json)
records the exact profile, inputs, binary fingerprint, checkpoints and results.

## Workload and environment

All eight TPC-H SF=10 tables use Int64 identifiers, Float64 quantities/money and
exact strings for text/dates. The loader submits at most 10,000 rows per append,
with a distinct permanent operation and snapshot for every successful request.
It also binds one failed precondition per collection before the first success.
There is no bulk operation or request coalescing.

The input comes from the YDB-vendored TPC-H dbgen 2.17.2 at commit
`03e5c5e370aeeee02b81d0ce87b7a3cde8ed3d83`, generated with `dbgen -s 10` and
the matching `dists.dss`. The evidence includes each input's byte count and SHA-256.
These are ingestion/history measurements, not TPC-H SQL benchmark results.

The measured binary uses official base `0dbffa72b683d34704a73f1900ae54445df5ed8d`,
Clang 18, libc++18, C++23, Release `-O3 -DNDEBUG`, and the pinned dependencies.
The final implementation also integrates official base
`6adb50c3a9be239b27deefa09c77b1a4db226ec3`; its separate build and compatibility
checks are recorded below. The workload keeps its original numeric/string
schema rather than switching to the newly integrated Decimal/Date types.
Measured ingest timings also predate the later incremental warehouse-admission
optimization; the final integration is validated separately.
The host is Linux x86-64, kernel `5.4.210-39.1.pagevecsize`, on local ext4.
Builds, tests and analysis shared the host during the campaign. Every recovery
probe starts a new executable image; the OS page cache remains populated.

## Measured campaign

<!-- measured-results:start -->
The campaign commits **86,586,082 rows in 8,661 ordinary appends**
and retains **8,669 permanent operations**, including eight failed bindings.
All eight source tables match the latest native rows exactly in a fresh process.
The full oracle also verifies duplicate multiplicity and source exhaustion.

| Table | Rows | Ordinary commits |
| --- | ---: | ---: |
| region | 5 | 1 |
| nation | 25 | 1 |
| supplier | 100,000 | 10 |
| customer | 1,500,000 | 150 |
| part | 2,000,000 | 200 |
| partsupp | 8,000,000 | 800 |
| orders | 15,000,000 | 1,500 |
| lineitem | 59,986,052 | 5,999 |

Total time inside successful append calls is **3,268.823 seconds**;
append p50/p95/p99 are **376.976 / 744.077 / 965.467 ms**.
The loader's wall time is 5,480.447 seconds, including input
parsing and the checkpoint shutdown/reopen sequence. All 17 checkpoint recovery
processes succeed. Selected storage checkpoints follow; the JSON includes all 17.

| Commits | Physical warehouse GiB | Catalog MiB | Roots MiB | Manifests MiB | Retained lists MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 500 | 0.215 | 0.488 | 0.443 | 1.710 | 2.159 |
| 4,000 | 1.611 | 3.547 | 3.518 | 14.532 | 90.266 |
| 8,000 | 3.564 | 7.035 | 7.043 | 30.785 | 577.473 |
| 8,500 | 3.848 | 7.469 | 7.484 | 32.817 | 679.010 |
| 8,661 | 3.941 | 7.613 | 7.626 | 33.471 | 713.632 |

| Commits | Append elapsed seconds | Loader peak RSS MiB | Fresh open seconds | Fresh peak RSS MiB |
| ---: | ---: | ---: | ---: | ---: |
| 500 | 53.900 | 84.117 | 1.265 | 27.871 |
| 4,000 | 697.317 | 93.148 | 19.816 | 63.059 |
| 8,000 | 2,952.588 | 166.617 | 101.090 | 136.320 |
| 8,500 | 3,192.856 | 174.883 | 111.154 | 170.270 |
| 8,661 | 3,268.823 | 177.383 | 112.546 | 218.445 |

The final row in the recovery table uses the full verification process; its
peak includes scanning and the row oracle, while checkpoint peaks cover open only.
The final fresh open takes **112.546 seconds**; complete verification
takes **2,473.396 seconds** with **218.445 MiB** peak RSS.
All **12 lower-profile probes reject** with their expected errors.
Restoration then verifies all saved outcomes and historical views in 135.500
seconds (open 116.627 seconds; peak 193.758 MiB).
End-to-end campaign time is 8,102.001 seconds.
<!-- measured-results:end -->

Every 500 successful appends, the loader saves its independent receipt ledger,
closes and releases the engine, starts a separate native open-only process,
records recovery time and memory, then reopens the writer and resumes its input
stream and operation sequence. These probes validate every accepted state at
the recorded checkpoint. Append latency excludes checkpoint shutdown/reopen and
source parsing; campaign wall time includes them and full row verification.
Append statistics cover successful calls; collection creation and the deliberate
failed bindings are included in wall time.

After ingestion, another fresh process checks every original outcome against
the ledger. For each collection it retries the original failed binding once and
the first successful append ten times, requiring exactly the saved outcome. It
checks the initial empty view and first committed snapshot, compares latest
rows with the complete source, and verifies that retries change neither latest
row counts nor retained snapshot counts.
The failed-binding retry uses the original rows with an unrestricted snapshot
precondition: the original permanent failure must win before evaluating that
new precondition, as required by the existing retry contract.

The source oracle parses numbers independently from the loader, canonicalizes
negative zero, and compares exact numeric bits/string bytes. GNU sort uses a
64 MiB buffer and one worker; sorted-row comparison verifies duplicate
multiplicity without assuming scan order. Full latest verification also requires
source exhaustion. Oracle files are streamed to separate scratch space and are
excluded from warehouse measurements.

Linux peak RSS uses current-image `/proc/self/status` `VmHWM`, including for
checkpoint children. `warehouse_file_bytes` sums logical regular-file sizes.
`warehouse_physical_bytes` accounts for the larger of logical size and
`st_blocks * 512` per file, plus allocated directory blocks. The catalog is
reported separately. These are process observations and filesystem accounting,
not a universal allocator bound or dedicated-device throughput qualification.

Twelve fresh-process probes lower operation, catalog, snapshot, manifest,
data-file, manifest-byte, manifest-list-byte, footer-byte, aggregate-root,
individual-root, per-commit or recovery-workspace budgets. Operation/catalog
rejections use `CONFIG_TOO_SMALL`; retained-storage profile violations preserve
the existing `CORRUPTION` classification. Restoring the original profile must
recover all original outcomes, retries and initial/historical views without
changing the store.

## Regression and failure coverage

| Invariant | Executed evidence |
| --- | --- |
| Inventory exceeds the old 8,192-row query ceiling | Catalog regression reopens 8,193 bound records, rejects a smaller limit, and restores every binding. Production inventory/recovery uses 256-record keyset pages. |
| Permanent success/failure identity and duplicate multiplicity | Six native campaign cases mix distinct values and duplicate pairs across batches, all saved outcomes, successful/failed retries, initial/historical views, and lower-profile restoration. |
| Oracle detects incorrect input | Regressions reject changed values, a missing duplicate, an unconsumed suffix, and a numeric suffix beyond the retried prefix; negative zero remains canonical. |
| Larger root remains readable | A valid root padded beyond 16 MiB reopens with a 32 MiB allowance and exercises history, statistics, file details and Explain without closing readiness. |
| Public limits persist and remain discoverable | HTTP database-limit cases include normalized larger budgets, restart, exhaustion without binding a refused ID, and a complete legacy seven-field registry, alongside the integrated collection-profile cases. |
| Accepted state remains recoverable | Publication/recovery share aggregate checks and the conservative visited-workspace envelope; 17 fresh-process probes run during the full campaign. |
| Healthy lookup stays indexed | `BackendRetryScopeTest` and `TerminalRetryScopeTest` verify terminal lookups/retries do not enumerate unrelated history or read warehouse files. |
| Lost publication results remain uncertain until reconciliation | The full native suite includes binding/publication/recovery-fence faults and `TCombinedStorageFaultTest` cases covering file failures combined with lost SQLite results and fresh-process recovery. |
| Memory ownership and shared saturation preserve outcomes | Focused ASan/UBSan with leak detection covers catalog inventory, larger roots, retries, shared resources, HTTP restart and checkpoint recovery. |

The measurement revision's Release suite reports **968 passed, 11 skipped, 2 disabled and
zero failures** (981 registered CTest entries). Skips are child helpers and
manual measurement fixtures; disabled cases are existing manual latency/layout
measurements. All **29 focused ASan/UBSan/LSan entries pass**. The final six-case
history campaign also runs after the checkpoint/RSS corrections.

LLVM 18 formatting covers 274 files at the measurement revision; the deliberate formatting/header-analyzer
negative checks pass. The new benchmark passes targeted clang-tidy. The full
177-translation-unit analyzer run reports diagnostics outside the changed logic;
the evidence lists them instead of claiming a clean global analyzer result.

On the final integrated base `6adb50c`, the Release build and full suite pass:
**1,108 passed, 11 skipped, 4 disabled, zero failures** (1,123 registered entries,
71.84 seconds). All **76 enabled ASan/UBSan/LSan checks pass** in
73.03 seconds; two existing bulk fixtures are disabled. The selection includes
history, bulk/Decimal/Date, collection profiles, warehouse admission and Explain.
Formatting covers 293 files and both negative checks pass. Targeted
clang-tidy passes for the benchmark, engine, catalog, HTTP configuration, scan
cursor and native backend translation units. The mixed-value checkpoint fixture
also passes separately in Release and sanitizer builds, then in this final suite.
Prior integration results are retained separately in the machine-readable evidence.

The final executable opens the complete measured SF=10 store and verifies all
8,669 saved outcomes, retries, initial/historical views, row statistics and
retained-history counts. This compatibility check takes 111.274 seconds to open and
125.213 seconds overall, with 194.664 MiB peak RSS. It does not repeat
the measured executable's complete row-multiset comparison; the evidence keeps
the two measurements distinct.

The native test environment uses Python 3.12. The database-drop CLI fixture needs
the optional local workload package; its installation bypasses that package's
Python 3.13 minimum solely for this exercised fixture. General workload support
below Python 3.13 is not qualified.

Reproduction commands:

```sh
cmake --build build/release
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/release --output-on-failure --no-tests=error -j 8
ASAN_OPTIONS=detect_leaks=1 ctest --test-dir build/asan-ubsan \
  -R 'CatalogTest|NativeHistoryLimits|BackendRecoveryLimits|BackendRetryScope|TerminalRetryScope|SharedResources|BulkAppend|DecimalDate|CollectionProfiles|WarehouseInventory|Explain|glamdb\.(history_campaign|database_limits)' \
  --output-on-failure --no-tests=error -j 4
cmake --build build/release --target format-check quality-negative-check
GLAMDB_TIDY_JOBS=8 cmake --build build/release --target tidy-check
python3 benchmarks/history.py --data /absolute/sf10-tables \
  --output /absolute/new-disposable-campaign
```

## Independent review and critique

Independent review found and prompted fixes for four substantive issues:

- Legacy persisted HTTP limits omitted the new history object and needed
  compatible normalization while present history objects remain complete.
- Public metadata readers still enforced a fixed 16 MiB root ceiling, despite
  publication accepting a larger root. The selected root allowance and temporary
  memory envelope now reach every reader.
- The full source oracle initially allowed an unconsumed suffix. Full-source
  EOF, strict numeric parsing, multiplicity and negative-zero regressions now
  protect the measurement.
- Linux `ru_maxrss` inherited the parent writer's high-water mark after exec.
  Fresh-image `VmHWM` measures checkpoint memory independently.

Focused follow-up review found no remaining substantive issue in these fixes or
their integration with Arrow-owned input, scan cache, pruning, encoding, bulk
append, Decimal/Date, database drop and the 64-collection profile. The final
integration review also verifies incremental warehouse admission and invalidation,
Explain reservation reuse, footer verification, cache OOM handling, SDK request
caps and the GCC atomic-shared-pointer fallback. The review
checked the combined resource reservations and compatibility with legacy HTTP
limits alongside the current registry format.

The measured producer has quadratic retained manifest-list growth. The newer
target branch includes [PR #183](https://github.com/glamourdb/glamdb/pull/183),
which bounds latest manifest lists; the recorded measurements and earlier
integration results do not qualify that newer producer's growth or performance.
Retained snapshots and manifest rewrites still consume explicit budgets.
A normalized snapshot index could accelerate recovery, but would introduce
another publication/recovery invariant and migration. A dedicated equality query
could avoid scanning unrelated collections during filtered inventories; the
current keyset query remains bounded and correct with at most 64 collections.

The measured state qualifies 8,661 ordinary commits and 8,669 permanent
operations under this profile and workload. Configured headroom of 12,000
snapshots/manifests and 20,000 operations is not a measured capacity ceiling.
Different schemas can exhaust individual-file, aggregate-byte or shared-owner
budgets earlier. This campaign does not qualify arbitrary larger profiles,
physical power loss or a universal RSS bound. Existing physical durability
qualification requirements remain unchanged.
