# Physical warehouse admission

Issue [#167](https://github.com/glamourdb/glamdb/issues/167) identified one full
warehouse traversal per append, including every collection and orphan entry.
The local backend now uses a conservative incremental inventory on Linux
ext4, XFS, Btrfs and tmpfs. Other filesystems/platforms and adapters without
tracking retain full traversal. Persistent formats and operation receipts do
not change; this optimization does not delete orphans or extend history limits.

## Ownership and external mutations

The fast path requires exclusive ownership of the open warehouse: data and
metadata are immutable after publication, and filesystem changes belong to
the engine. This explicitly narrows the former unconditional detection of
arbitrary external growth at the next admission. Ordinary local create,
write, truncate and removal events through watched paths are still observed;
the regression suite checks external orphan creation and existing-file growth.
These checks are additional diagnostics, not permission for another writer.

Linux inotify does not cover writes through external hardlink aliases, mmap
changes, remote filesystem changes or mounting another tree over a watched
directory; these limitations are documented in the
[Linux inotify manual](https://man7.org/linux/man-pages/man7/inotify.7.html).
Do not perform these operations while the store is owned. Offline
maintenance must release all engine/cursor/buffer leases first. Detected
hardlinks disable tracking and select full traversal; creating an external
alias after baseline is also outside the fast-path contract. Filesystems not
on the local allowlist use full traversal.

`TLocalStorageOptions::WalkWarehouseAtAdmission=true` retains the former
unconditional traversal contract, including observing external changes at the
next admission. This is an internal backend option, not an HTTP/server startup
setting. `WalkWarehouseAtPublication=true` independently forces a second full
walk before publication. Neither mode makes concurrent external mutations
atomic with publication. Open and reconciliation always measure the full tree,
including orphans, and enforce lower reopen limits without an attempt reserve.

## Accounting and fallback

The monitor establishes directory watches before scanning their children,
then calls the ordinary authoritative measurement. This initial setup performs
directory discovery as well as the baseline traversal. It retains directory
descriptors and allocation bounds, but no map proportional to regular files.
Events arriving during setup are replayed conservatively, never discarded.

At admission, each dirty regular filename is statted once. Its entire current
`max(logical length, allocated blocks * 512)` is added, even when part of its
space was already counted. Directory allocation is also added conservatively.
Repeated events are deduplicated within one batch. Deletions do not refund the
inventory: successful superseded-root cleanup can overcount until refresh;
failed cleanup and abandoned/failed/uncertain attempt files retain their charge.
Accounting does not depend on a successful catalog commit.

A rename, new directory, lost/unknown watch, event overflow or uncertain state
invalidates the baseline and rebuilds it. Watch/descriptor limits or unavailable
tracking fall back to full traversal. A conservative total that cannot fit the
warehouse cap **plus the next attempt reserve** triggers an exact refresh before
admission refusal. Publication still adds every durable attempt file and
directory growth to admission's bound; any failed estimate triggers an exact
walk before refusing publication. Integer overflow and physical measurement
errors remain fail-closed. Every admission still checks root identity and
current filesystem free space, including filesystem, attempt and journal
reserves.

Recovery invalidates the inventory. A new process never trusts a persisted
counter, and reconciliation independently verifies physical usage and retained
metadata/receipts. This optimization neither changes outcome lookup semantics
nor qualifies durability.

## Work and memory bounds

The backend optionally reserves 4 MiB of shared metadata memory for the monitor
at the first append admission, after recovery has admitted the retained roots.
Opening/reading an existing store never spends this optional reservation. If it
is unavailable, admission uses full traversal. The monitor retains at most 1,024 watched
directory descriptors and processes at most 4,096 notifications per replay (at most two replays per admission),
with filename length bounded to `NAME_MAX`. Exceeding these bounds selects a
refresh or full traversal. Kernel watches/event queues and file descriptors are
separate from the userspace memory envelope.

Ordinary steady append admission performs work proportional to changed entries.
The serialized writer's append deadline, bulk cancellation flag and engine
shutdown flag reach discovery, notification replay and fallback traversal.
Native full traversal polls every 64 entries; timeout/cancellation returns
`DEADLINE_EXCEEDED`/`CANCELLED` before binding the operation ID. A blocking
filesystem syscall can still outlive the deadline or cancellation request. Recovery/creation have no
append deadline and remain full-inventory operations; legacy injected adapters
only gain polling inside their traversal if they forward the budget overload;
the default adapter checks cancellation and deadline before and after its call.

## Validation and measurements

`tests/unit/io/tree_inventory_test.cpp` uses an independent POSIX stat oracle
for sparse files, directory blocks, multiple directories and empty/tiny files.
It exercises notification overload, setup races, deletion, invalidation,
hardlinks, symlinks and expired deadlines. The vertical suites check admission
before binding, retained receipts/snapshots, uncertain outcomes, publication
fallback and lower-limit reopen. The steady preflight test expects no full
traversal when tracking is available, while strict mode expects one.

`glamdb_capacity_bench` reports append-only full-walk calls/time, total tracked
inventory time, per-append latencies, useful rows/s and before/after physical
population. Its optional limits JSON field `empty_orphan_files` creates an
orphan fixture before append counters begin. Startup, fixture generation,
verification and reporting are outside those counters. Exact evidence and
executed results are recorded below.

For one fresh 100-append sample, after the normal Release build:

```sh
mkdir artifacts/inventory-50000
printf '%s\n' '{"operations":4096,"snapshots":4096,"warehouse_bytes":1099511627776,"attempt_reserve_bytes":268435456,"empty_orphan_files":50000}' > artifacts/inventory-50000/profile.json
printf '%s\n' '[]' > artifacts/inventory-50000/ledger.json
build/release/benchmarks/glamdb_capacity_bench segment \
  artifacts/inventory-50000/store artifacts/inventory-50000/profile.json \
  artifacts/inventory-50000/ledger.json 100 > artifacts/inventory-50000/sample.json
```

Use a new output directory for each sample; change `empty_orphan_files` to zero
for the control. The driver checks rows, original receipts and retained views
before returning. For fresh-process verification, save `sample.json`'s
`receipts` array as the ledger and invoke `verify` instead of `segment`, with
count zero. The standard `benchmarks/capacity.py` supervisor additionally
automates checkpoints and lower-limit reopen refusals.

### Executed on 2026-11-12

The [machine-readable evidence](evidence/warehouse-inventory-2026-11-12.json)
compares base `081dcb2c909e3689f0329db56fde0832a08be248` (measurement-only
benchmark additions) with this implementation, using the same dependency
prefix, Release Clang 18/libc++, Linux aarch64/ext4 and shared VM. Each fixture
runs 100 appends of 1,000 rows across two collections, in three repetitions with
alternating fixture order. The OS cache is retained; this is not a dedicated
device throughput guarantee. Every value below is independently the median of
the corresponding per-run values.

| Empty orphan files | Mean append, base → changed (ms) | Total inventory / 100 appends, base → changed (ms) | MeasureTree calls, base → changed |
| ---: | ---: | ---: | ---: |
| 0 | 8.764 → 7.007 | 24.254 → 1.478 | 100 → 1 |
| 50,000 | 75.545 → 8.193 | 6,687.296 → 134.602 | 100 → 1 |

Changed inventory time includes notification handling **and initial watch
discovery plus the baseline full traversal**. The call counter counts only
`MeasureTree`; it does not count discovery as another measurement. First setup
is inside append time, not hidden in fixture preparation.

| 50,000-orphan fixture | Base | Changed |
| --- | ---: | ---: |
| Useful committed rows/s | 13,237 | 122,062 |
| p50 append (ms) | 74.413 | 6.507 |
| p95 append (ms) | 84.649 | 9.503 |
| p99 append (ms) | 89.583 | 14.781 |
| Maximum append (ms) | 95.978 | 139.880 |

The maximum increases because first admission establishes the watches and
baseline. Steady admission removes the repeated inventory cost; startup and
recovery still enumerate the warehouse. The approximately 9.2× mean-latency
improvement applies to this orphan-rich workload. The smaller control difference
must not all be attributed to inventory: its measured inventory saving is only
about 0.23 ms/append, and shared-host latency varies.

Both variants begin with 50,002 regular files and finish with 50,302 (100 data
files, 100 manifests, 100 manifest lists and two current roots, plus orphans).
Physical use is approximately 1.08 MB before and 2.43 MB after, including
directory blocks. The empty files isolate entry count; these are not 50,000
committed snapshots. Per-run byte populations and latency summaries are retained
in the evidence JSON.

The [raw evidence](evidence/warehouse-inventory-2026-11-12-raw.json) additionally
retains all per-append latency arrays, checkpoint populations, binary hashes and
verification outcomes for independent recalculation.

All 1,200 appends across both matrices committed. Independent exact rows and
duplicates, all original receipts, historical/initial views, retained buffers,
fresh-process reopen and lower-operation/snapshot/physical-limit refusals passed.

A matched default-profile campaign used 512-append fresh-process segments and a
4,200-attempt budget, with no injected orphans. Both builds accepted exactly
4,096 appends / 4,096,000 rows and refused attempt 4,097 without binding.
Original receipts, exact rows, historical/initial views, retained buffers,
same-profile reopen and all three lower-limit refusals passed in both campaigns.

| Default-profile campaign | Base | Changed |
| --- | ---: | ---: |
| Append wall time (s), excluding verification/reopen | 111.173 | 66.445 |
| Useful committed rows/s | 36,843 | 61,645 |
| Total append inventory time (s) | 41.944 | 0.241 |
| MeasureTree calls within append segments | 4,097 | 9 |
| Accepted append p50 / p95 / p99 (ms) | 25.141 / 49.105 / 58.089 | 16.628 / 25.917 / 29.737 |
| Accepted append maximum (ms) | 92.659 | 55.165 |

The nine baseline inventories correspond to nine freshly opened segments;
discovery is also charged in changed inventory time. Both finish with 12,290
regular files and approximately 206 MB of physical warehouse use. Small byte
differences arise from generated metadata, not deletion of history. This is
one matched full-profile run per build, not a repeated throughput guarantee.
The implementation was subsequently rebased onto `2654609` (collection profiles);
the original measurements remain explicitly associated with the `081dcb2` base.
The full listed Release suites and fresh-process scan were rerun after rebase
with the same pass counts and seven matching Parquet failures, and no new failures.
A separate post-rebase 100-append/50,000-orphan sample also passed exact data,
receipt, view, reopen and lower-limit checks with one `MeasureTree` baseline;
it is verification evidence, not another comparative performance campaign.

The Release checks executed 199 passing I/O tests, 189 passing storage tests,
342 passing vertical tests, 21 passing mock tests and the fresh-process scan
check. Seven Parquet dictionary/encoding failures (three storage, four vertical)
were independently reproduced on the same base revision/prefix; they are not
reported as passes. Storage had one child-only skip; vertical had ten skips and
three disabled manual tests. Bulk tests require their documented >8 GiB device
reservation and passed using a 16 GiB tmpfs; insufficient free ext4 space caused
matching failures in both variants before that environment correction.

Changed C++ formatting passed clang-format 18. Clang-tidy 18 passed new/changed
logic after fixes; the untouched `MakeBatch(schema, rows, seed)` in
`append_stage_bench_test.cpp:94` still reports the base revision's
`bugprone-easily-swappable-parameters` warning. This is not an all-repository
clean clang-tidy claim.

All 21 new inventory cases also passed ASan+UBSan with leak detection and no
suppressions. The full I/O sanitizer suite hit an unrelated container
libc++/libc++abi allocation/deallocation mismatch during exception cleanup; an
independent program containing only `throw`/`catch` of `std::invalid_argument`
reproduced it. This does not establish a passing full sanitizer suite.
