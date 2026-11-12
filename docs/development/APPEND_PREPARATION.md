# Native append preparation

This is a focused follow-up for [#107](https://github.com/glamourdb/glamdb/issues/107)
after [#134](https://github.com/glamourdb/glamdb/pull/134) introduced owned
canonical Arrow columns. It does **not** close #107's shared preparation-admission
or full concurrency-campaign acceptance.

## Implementation and invariants

Each traversal retains one immutable Arrow schema mapping, replacing it when
the next batch uses a different schema. Mapping reads schema types directly,
so it does not box and retain an `arrow::Array` for every input column/slice.
Inspection also counts non-NULL string bytes per column; the owned copy uses
those counts instead of visiting every batch again to size string buffers.
Scratch remains O(fields), including inputs with 10,000 distinct batch schemas.

Names over the fixed schema's 128-byte limit reject before hashing/copying the
name. Unsupported schema fields reject before full array validation. Every
actual batch still receives structural and full validation, including when its
mapping was cached. Actual timestamp unit/timezone and decimal precision/scale
are checked directly against the fixed schema before Arrow's type fingerprints
or error formatting can allocate variable-sized diagnostics.

The initial fingerprint still sizes the existing reservation; normalization
still performs a **fresh** post-admission inspection. Copying retains its source
range and destination-capacity checks. The coordinator compares the owned hash
before binding. Caller buffers must remain immutable throughout the synchronous
call; mutation tests exercise the boundary between fingerprinting and copying,
not arbitrary concurrent mutation during a raw-buffer read.

Canonical bytes, exact values, NULLs, duplicates, field IDs, scalar parameters,
limits and the reservation formula are unchanged. Old terminal receipts still
precede new execution reservations and snapshot checks. Writer serialization,
exact prepared-base checks, unknown-outcome barriers, file sync, outer catalog
publication and recovery ordering are unchanged. The tracked backend writer
copy and its allocator lifetime are retained. No public API, dependency or
persistent-format migration is required.

## Repeated stage profile

The [machine-readable measurements](evidence/append-preparation-2026-11-12.json)
include every per-run result. Baseline production code is official `8677619`.
The baseline substitutes only official `arrow_input.cpp` into a copied core
archive; all other Release objects, benchmark code and dependencies are identical.
Both versions use the same deterministic data: required string
key, nullable fields cycling over int64, float64, string, bool and UTC timestamp,
10% NULLs, two warm-up appends and five measured appends per shape. Three runs
alternate baseline and changed executables. Each append uses one operation ID
and produces its own snapshot; no new-ID write retries are used.

Environment: Linux x86-64, 48 available CPUs, Clang/libc++ 18.1.3, Release
`-O3 -DNDEBUG`, pinned Arrow 25, local ext4 at the canonical temporary directory.
These are same-host runs with other activity, not isolated device measurements.
Medians below are medians of the three per-run medians, in milliseconds.

| Fields | Rows | Batches | Baseline append | Changed append | Change |
| --- | --- | --- | --- | --- | --- |
| 2 | 1,000 | 1 | 20.276 | 20.137 | -0.7% |
| 16 | 1,000 | 1 | 26.079 | 24.661 | -5.4% |
| 64 | 1,000 | 1 | 44.342 | 45.133 | +1.8% |
| 64 | 10,000 | 1 | 163.813 | 172.493 | +5.3% |
| 2 | 1,000 | 1,000 | 20.722 | 20.262 | -2.2% |
| 16 | 1,000 | 1,000 | 45.071 | 34.558 | -23.3% |
| 64 | 1,000 | 1,000 | 141.202 | 75.418 | -46.6% |
| 64 | 10,000 | 10,000 | 1,175.692 | 450.729 | -61.7% |

For 64 fields × 10,000 one-row batches, fingerprint time falls from 409.470 to
153.074 ms and normalization from 623.475 to 165.238 ms. Their combined measured
work falls from 103,295 to 31,831 ns/row, or 1,614 to 497 ns/cell. These stages
are independent microbenchmarks outside the real append; their medians are not
an additive partition of append wall time. The native driver calls the public
append method with the real backend through a private timing factory; installed
public-header consumption and fresh-process reopen are checked separately.

The same process's peak RSS changes from 1,622,336 to 1,274,112 KiB in that largest
fragmented shape. VmHWM includes prebuilt caller inputs and preceding shapes;
it is not a per-append allocation or a process memory qualification. At 64
fields × 10,000 rows, tracked owned/writer Arrow peaks remain about 4,972/4,971
KiB, within the unchanged 42,287 KiB reservation. The benefit is strongest for
fragmented inputs. Single-batch and narrow results do not establish a general
throughput improvement.

Run the opt-in native measurements after building `glamdb_vertical`:

```sh
GLAMDB_APPEND_STAGE_BENCH=5 \
GLAMDB_APPEND_STAGE_SHAPES=2x1000,16x1000,64x1000,64x10000 \
GLAMDB_APPEND_STAGE_BATCH_ROWS=1 \
build/release/tests/glamdb_vertical \
  --gtest_filter=AppendStageBench.NativeAppendStagesByShape

GLAMDB_APPEND_STAGE_BENCH=1 \
GLAMDB_APPEND_STAGE_SHAPES=2x1000,16x1000,64x1000,64x10000 \
build/release/tests/glamdb_vertical \
  --gtest_filter=AppendStageBench.OwnedInputBytesByShape
```

Use `GLAMDB_APPEND_STAGE_BATCH_ROWS=10000` for the single-batch comparison and
repeat each version three times. `GLAMDB_APPEND_STAGE_DIR` selects a different
existing canonical parent/filesystem for disposable native stores.

## HTTP and SDK boundary

`benchmarks/append_preparation_http.py` starts a real disposable native server,
prebuilds deterministic rows, and measures SDK preparation separately from the
complete HTTP append call. After timing it replays the exact original receipt
and consumes every Arrow scan batch. Each shape uses a fresh server/store.
Startup has an absolute deadline, bounded output and owned-child cleanup.

Three alternating runs of five measured 1,000-row appends give these medians:

| Fields | SDK preparation, before/after ms | HTTP call, before/after ms | Total, before/after ms |
| --- | --- | --- | --- |
| 2 | 3.562 / 3.539 | 27.176 / 23.923 | 30.674 / 28.101 |
| 16 | 24.213 / 22.203 | 34.419 / 37.695 | 60.117 / 61.268 |
| 64 | 81.549 / 84.190 | 59.548 / 65.155 | 141.107 / 147.985 |

The Python preparation code is unchanged. These measurements do not establish
an HTTP throughput improvement. JSON transport creates a normal
single batch, so it does not exercise the native fragmented-schema benefit.
The raw report also records server peak RSS, which includes verification scans.

```sh
PYTHONPATH=sdk/python/src python benchmarks/append_preparation_http.py \
  --server build/release/apps/server/glamdb \
  --output artifacts/append-http.json --appends 5
```

Use a Python environment with the repository SDK dependencies. The script has
no mock fallback, automatic write retry, load scheduler or durable client journal.
It is a bounded latency comparison; the broader workload/concurrency harness
remains the owner of sustained system-capacity qualification.

## Rejection regressions and independent review

The field-name regression first failed against `31a8db6` (the same upstream
mapping implementation retained at `8677619`): a one-row batch
with an 8 MiB unknown field name allocated 8,392,952/8,388,880 bytes of C++ heap
during fingerprinting/normalization. The guarded implementation uses 208/208
bytes for both 128 KiB and 8 MiB names.

Independent review found an additional rejection path through an actual timestamp
array with an 8 MiB timezone behind a valid UTC batch schema. Before the actual
type-parameter guard its fingerprint/normalization peaks were
37,749,312/29,360,632 bytes; after the guard they are 184/184 bytes. Both cases
were observed failing before their fixes and passing afterwards, with
`INVALID_ARGUMENT` and `FixInput`.

The review also checked per-batch validation with a shared schema, alternating
field order and omitted NULLs against explicit expected rows/hashes, mutation
boundaries, resource refusal before binding, old receipts, exact bases,
publication and reopen. Benchmark startup cleanup and zero-row configuration
handling were corrected. No remaining introduced correctness defect was found.

The one-entry cache and inspected sizes are the smallest measured extension of
the existing representation. Possible later experiments are a schema-sized
index vector instead of the temporary map, or combining inspection and initial
hashing after a cheap header/row-count pass. Each needs its own profile and must
retain fresh owned-input validation. Removing the tracked backend copy changes
allocator-refusal/lifetime contracts and requires separate design.

## Verification and remaining boundaries

Final executed checks:

- Release CTest: zero failures among 1,141 enabled tests, with 12 skipped
  opt-in/child cases and 4 disabled tests; 86.50 seconds. Native HTTP/TLS,
  publication faults, historical reads and the installed consumer/reopen passed.
- ASan/UBSan unit and vertical CTest: zero failures among 528 enabled tests,
  with 11 skipped opt-in/child cases and 4 disabled tests; 88.26 seconds.
- LLVM 18 formatting: all 293 project C++ files; quality-negative probes rejected.
  Clang-tidy passed on all four changed C++ translation units.
- All nine direct dependency pins verified; benchmark Python compiles. The
  partial-line startup timeout reaps its child, and zero-row benchmark shapes
  reject before slicing.

The machine-readable evidence records the commands and counts.
ASan/UBSan use the development prefix's documented `_LIBCPP_HAS_NO_ASAN` and
static libc++ compatibility settings from [BUILDING](BUILDING.md). LeakSanitizer
is disabled because the execution environment rejects its ptrace-based check;
leak qualification is not claimed. The 10,000-batch memory stress test builds its
caller values once and slices them; distinct-schema empty batches separately
ensure mappings cannot accumulate. Its measured preparation assertions are
unchanged, while caller setup stays within sanitizer test deadlines.

Fingerprinting still precedes pending-call admission, as documented in
[resource budgets](RESOURCE_BUDGETS.md). This patch moves no preparation outside
the writer and adds no shared CPU/queue admission guarantee. A nonblocking refusal
before hashing cannot currently supply the truthful canonical digest required
by an unknown outcome, and a definite refusal must not contradict a queued
same-ID publication. That protocol/admission decision and one/two/many-session
CPU, queue-age and combined-memory qualification remain open under #107.
TSan/MSan and physical power-loss qualification are not established by these runs.
