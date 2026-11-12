# Combined capacity, ingestion and cold recovery

Issue [#63](https://github.com/glamourdb/glamdb/issues/63) adds a reproducible
native campaign for the synchronous append profile. It complements the
[shared resource contract](RESOURCE_BUDGETS.md), [scan benchmark](SCAN_INDEX.md)
and [network stress suite](../../tests/stress/README.md).

The integration baseline was rechecked: PR #19's final head is `ee480ec9`, merged
as `87b557e`. This work builds on official `main` at `135651d`, including the
aggregate-capacity, pre-binding-admission, bounded-lookup and retained-scan fixes
tracked by #35, #37, #40 and #47.

## Reproduction

Build the pinned dependencies and a Release engine as described in
[BUILDING.md](BUILDING.md), then run from the repository root:

```sh
cmake --build build/release --target glamdb_capacity_bench --parallel 4
python3 benchmarks/capacity.py --output artifacts/capacity-default
python3 benchmarks/capacity.py --profile snapshots --limit 8 --attempts 20 --checkpoint 4 \
  --output artifacts/capacity-snapshots
python3 benchmarks/capacity.py --profile outcomes --limit 8 --attempts 20 --checkpoint 4 \
  --output artifacts/capacity-outcomes
python3 benchmarks/capacity.py --profile combined --limit 8 --attempts 20 --checkpoint 4 --trace-syncs \
  --output artifacts/capacity-combined
```

Python 3.10+ and the native binary are required; no Python SDK, HTTP service or
mock is involved. `--trace-syncs` additionally requires Linux `strace` and
records **all** process `fsync`/`fdatasync` calls, including SQLite. Tracing
changes timings and must be identified in comparisons. The default driver is
`build/release/benchmarks/glamdb_capacity_bench`; `--driver` selects another build.

Every output directory must be new. The runner preserves its native store,
profile, original receipts, commands, stdout/stderr, raw checkpoint data,
environment and final summary. It never attaches to an existing service or
reuses a campaign directory. `--timeout` bounds each native subprocess, including
recovery; timeout and interruption terminate its process group. Incomplete runs
retain evidence and fail; they do not establish backpressure or success.

The full default campaign is manual. The short `glamdb.capacity_campaign` CTest
runs real dependency integration at three-append boundaries:

```sh
ctest --test-dir build/release -R '^glamdb.capacity_campaign$' --output-on-failure
```

## Workload and invariants

The default budget is 5,000 attempted appends, each with exactly 1,000 rows.
Two collections receive alternating appends so usage and operation limits are
tested across collections. A batch contains 500 identical duplicate-key/large
int64 rows, 250 UTF-8-key/NULL rows and 250 other-key/negative-int64 rows. The
read oracle checks each literal value and its multiplicity independently of
file or snapshot ordering. This narrow, highly compressible workload measures
retained history and small-file costs, rather than a representative wide schema.

A new native process runs each segment of at most 100 successful appends; every
checkpoint includes the final partial segment. An explicit capacity refusal
ends ingestion immediately. The default operation, snapshot, manifest, root,
file and memory caps remain in effect. The runner never raises a history cap,
expires a snapshot, removes an outcome, or recycles an operation ID.

Each segment verifies the entire current row multiset, both initial-empty
views, each collection's first historical snapshot and all original committed
receipts. Exact first-operation retries must return the original complete
receipt after later appends and process restart. Ten additional healthy retries
measure lookup latency and require zero external-file opens/writes/syncs.
Verified snapshot and data-file counts must equal successful appends.

The supervisor distinguishes exhausting its attempt budget from reaching
capacity. Only a definitely-not-committed `RESOURCE_EXHAUSTED` append counts as
backpressure; uncertainty, corruption, dependency failure, unexpected exits and
failed verification fail the campaign. The final store must reopen and verify
in another process under the original profile.

Separate fresh processes lower each of the operation, aggregate snapshot and
physical warehouse limits below the accepted state. They must fail closed with
the current documented results: `CONFIG_TOO_SMALL`, `CORRUPTION` and
`RESOURCE_EXHAUSTED`, respectively. The original profile is checked again after
these rejections, proving the accepted state remains readable.

The combined profile first leaves unused snapshot and operation slots, adds and
syncs an unreferenced file, then opens with a warehouse cap that includes that
orphan but cannot reserve another attempt. The remaining logical slots make
physical refusal observable independently of the history/outcome caps. New
appends must fail before binding;
old scans, snapshots and retries must remain valid. A retained Arrow slice is
also held while the remaining data envelope is occupied: new scan admission
must refuse, old exact retries still work, and reads resume when the pressure
reservation releases. Retained slices must keep their values and accounting
after engine shutdown and release the final data charge when dropped. The
orphan stays in the disposable evidence store and remains invisible to scans.
Ingestion then continues to fill the finite snapshot/outcome limits, with the
orphan still consuming physical space.

## Measurement boundaries

| Field | Meaning |
| --- | --- |
| `append_seconds`, `append_rows_per_second` | Time in synchronous append segments, including rejected attempts; excludes engine open and the subsequent verification scans. No client/network scheduler is included. |
| `append_latencies_ms` | Every append attempt in the checkpoint, including the final refusal. |
| `open_ms`, `cold_process_reopen_ms` | Engine open and recovery in a fresh process. OS page cache is retained; this is not device-cold I/O. The first sample measures store creation. |
| `rss_peak_bytes` | Process high-water RSS, including engine, Arrow/Iceberg/SQLite, the benchmark ledger and verification. It is neither engine-only RSS nor a hard memory bound. |
| `resource_memory_peak_bytes` | Shared-owner reservation peaks for data, metadata, control, completion and recovery; these conservative envelopes differ from RSS. |
| `usage` logical fields | Verified aggregate reachable roots, snapshots, data files, manifests, manifest lists and footers from the production backend. |
| `usage.latest_manifest_counts` | Number of manifests in each collection's current snapshot, separate from the aggregate count retained by history. |
| `warehouse_physical_bytes` | Larger of logical length and allocated blocks per regular file, plus directory blocks; includes orphans. |
| `warehouse_file_bytes`, `warehouse_files`, `catalog_bytes` | Logical lengths and count of all warehouse regular files; SQLite catalog length separately. |
| `io` | Successful external-file bytes read/written and attempted external-file/directory sync calls **during appends**, excluding the SQLite VFS. The adapter keeps counters, not an event history. |
| `process_io` | Linux `/proc/self/io` deltas during append segments, including SQLite; syscall byte counts and block-accounted bytes are distinct. Cache hits do not imply physical-device reads. |
| `*.syncs.txt` | Optional syscall summary for each complete subprocess, including recovery, verification and SQLite. It covers a different interval from append-only counters. |
| `retry_us` | Mean of ten exact first-operation retries, including canonical input fingerprinting and catalog lookup; it is not an HTTP latency percentile. |

Recorded environment includes kernel/platform, filesystem/mount, build settings,
revision/dirty status and binary SHA-256. Stores, original receipts and raw logs
remain under the chosen output directory. Published results retain a compact
measurement record; they do not require committing a generated warehouse.

## Executed measurements, 2026-11-12

The [machine-readable record](evidence/capacity-2026-11-12.json) retains every
checkpoint, original/lowered profiles, binary hashes and summary measurements.
The runs used official base `135651d` with the campaign changes, Release Clang
18, pinned native dependencies, Python 3.12.3, Linux x86-64 kernel
`5.4.210-39.1.pagevecsize`, and ext4 mounted
`rw,nosuid,nodev,relatime,errors=panic`. The device is a shared virtual disk;
hardware flush behavior was not qualified. All four commands exited **0**.

| Profile | Successful / attempted appends | Peak RSS, MiB | Physical warehouse, MiB | Fresh-process reopen, ms | Exact retry, us |
| --- | --- | --- | --- | --- | --- |
| Default, budget 5,000 | 4,096 / 4,097 | 71.00 | 198.03 | 18,991.42 | 342.95 |
| Snapshot cap 8 | 8 / 9 | 28.06 | 0.14 | 27.97 | 297.04 |
| Outcome cap 8 | 8 / 9 | 28.24 | 0.14 | 30.81 | 313.20 |
| Combined cap 8, sync tracing enabled | 8 / 9 | 28.02 | 0.14 | 125.90 | 741.08 |

The default run accepted 4,096,000 rows across two collections. Attempt 4,097
returned unbound `RESOURCE_EXHAUSTED` at the aggregate snapshot limit; the
operation limit also remained at 4,096. The run stopped at that documented
backpressure rather than spending the rest of its 5,000-attempt budget. All
4,096 original receipts, latest rows, first historical snapshots and initial
views verified after reopening; lowered operation, snapshot and warehouse
profiles rejected explicitly. Another original-profile reopen still passed.

Append time totaled 323.52 seconds, or 12,660.78 committed rows/s, excluding
checkpoint verification and reopening. All-attempt append p50/p95/p99 were
77.77 / 138.88 / 163.94 ms. These are observations on this host and schema.
Retained metadata growth and repeated root/manifest-list writes make performance
depend on history length:

| Successful appends | Peak RSS, MiB | Physical warehouse, MiB | Reachable manifest lists, MiB | Checkpoint open, ms |
| --- | --- | --- | --- | --- |
| 100 | 26.11 | 1.32 | 0.23 | 15.03 (create) |
| 1,000 | 37.68 | 21.06 | 10.12 | 2,745.57 |
| 2,000 | 46.39 | 59.74 | 37.82 | 5,552.26 |
| 3,000 | 58.19 | 115.97 | 83.16 | 9,834.83 |
| 4,000 | 71.00 | 190.03 | 146.20 | 18,198.76 |
| 4,096 | 69.38 | 198.03 | 153.18 | 18,308.91 |

At exhaustion, verified logical usage was 3,743,674 root bytes, 4,096 data files,
4,096 manifests, 12,601,920 manifest bytes, 160,624,365 manifest-list bytes and
1,368,064 footer bytes. The warehouse contained 12,290 regular files; SQLite
occupied 3,801,088 bytes separately. Append-only external I/O totaled
8,172,406,100 bytes read, 4,007,192,689 bytes written, 16,384 file sync calls and
16,384 directory sync calls. Linux process block-accounted writes, including
SQLite, were 4,362,334,208 bytes. The separately traced combined campaign
observed 237 `fsync` calls across its complete subprocesses; no `fdatasync`
calls occurred. These scopes are intentionally separate.

The combined run proved physical-orphan refusal with logical headroom, then
retained the orphan while filling history/outcomes. Its scans, exact retries,
retained slice accounting and same/lowered-profile reopens passed. A negative
control that made warehouse measurement falsely return zero caused the physical
probe to fail; restoring real POSIX measurements restored the passing checks.

## Executed validation

```sh
cmake --build build/release --parallel 6
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/release --output-on-failure --no-tests=error -j 6
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  ctest --test-dir build/asan-ubsan -R '^glamdb.capacity_campaign$' --output-on-failure
cmake --build build/release --target format-check quality-negative-check
clang-tidy-18 -p build/release benchmarks/capacity.cpp --quiet
```

All exited **0**. Normal CTest reported 760 tests with zero failures, seven
expected skips and two disabled benchmarks (762 registered). Native vertical
coverage and the 20 in-memory mock cases remain separately labelled; no mock
result substitutes for a campaign measurement. The new CTest drives five Python
regression cases, including real native boundary profiles and supervisor
termination/RSS reporting controls. They pass normally and with ASan/UBSan and
leak detection. The sandbox prevents LeakSanitizer process inspection; that
initial attempt failed at the runtime level, and the passing sanitizer run used
the unrestricted test environment, without disabling leak detection.

The independent review's physical-probe isolation, process cleanup and RSS
aggregation findings each have a failing-before/passing-after regression.
Formatting covers all 236 project C++ files; clang-tidy covered the changed
benchmark translation unit. Power-loss, device-cold and dedicated cgroup
campaigns were not run.

## Manifest merging comparison, 2026-11-12

Issue [#169](https://github.com/glamourdb/glamdb/issues/169) switches ordinary and
bulk staging to Iceberg merge append inside the same atomic publication. Its
100-manifest threshold and 8 MiB target keep the latest list below 100 manifests
per collection for this workload. The verifier checks exact preservation of
rewritten entries and charges both rewrite inputs and outputs to the existing
32 MiB commit bound. See [the storage contract](STORAGE_MODULE.md#manifest-merging).

The [before/after record](evidence/manifest-merging-2026-11-12.json) contains
both environments, binary hashes, unchanged profiles, summaries and every
checkpoint. Both runs started from official base `37b58fa`, used Release Clang
18 on the same Linux x86-64/ext4 host, and ran the unmodified default 5,000-attempt,
1,000-row workload with 100-append checkpoints. The baseline binary was preserved
before changing production code. Both binaries used the same installed native
dependencies. That installation preceded refreshing the repository's existing
Decimal PLAIN patch for final test validation; this string/int64 workload does
not exercise that option. Concurrent builds/tests shared the host, so timing
differences are observations rather than an isolated throughput comparison.

Both campaigns exited **0**, accepted 4,096 appends (4,096,000 rows), and stopped
on the expected unbound capacity refusal at attempt 4,097. Every checkpoint and
final fresh process verified latest rows, historical and initial views, original
receipts and exact retries. Lowered operation/snapshot/warehouse profiles failed
closed, followed by another successful original-profile reopen. The fixed
verifier also opened and verified all 4,096 baseline FastAppend operations.

| Measurement after 4,096 appends | FastAppend baseline | Merge append |
| --- | ---: | ---: |
| Latest manifests per collection | 2,048 / 2,048 | 68 / 68 |
| Retained manifest-list bytes | 160,814,027 | 13,617,787 |
| Retained manifest bytes | 12,601,876 | 14,377,913 |
| Retained manifests across history | 4,096 | 4,096 |
| External bytes read during appends | 8,173,049,414 | 7,780,064,828 |
| External bytes written during appends | 4,007,419,613 | 3,857,024,037 |
| External read opens | 77,820 | 109,848 |
| External file sync calls | 16,384 | 16,424 |
| Physical warehouse bytes | 207,814,656 | 62,046,208 |
| Append time, seconds | 399.36 | 386.06 |
| Fresh-process final reopen, ms | 22,270.88 | 14,903.26 |

Manifest-list storage fell **91.53%**, and physical warehouse use fell **70.14%**.
Retained manifest bytes increased **14.09%**; merge inputs and staging add read
opens and forty additional durable staging files. The adapter deliberately
refuses deletion, so unreferenced staging files remain physical orphans. The
eight-new-manifest attempt limit and permanent history/outcome caps remain
unchanged. Metadata root history still dominates I/O: external read/write bytes
fell only **4.81% / 3.75%**.

| Successful appends | Baseline list bytes | Merged list bytes |
| --- | ---: | ---: |
| 1,000 | 10,622,693 | 3,305,086 |
| 2,000 | 39,698,010 | 6,632,269 |
| 3,000 | 87,296,065 | 9,969,583 |
| 4,096 | 160,814,027 | 13,617,787 |

The retained lists grow approximately linearly here; current membership fits one
target bin, and each latest list cycles between one and 99 entries. Per-append
root serialization and verification remain linear in retained history length.
Keeping all historical snapshots also retains O(N²/K) descriptors in merged
manifests in this single-bin regime (K=99 appends between later merges), versus
O(KN) retained manifest-list entries. Snapshot expiration and #168 remain
separate work; this change does not remove the history/outcome cap.

Validation on the measurement base reported **1,105 CTest cases, zero failures**.
After rebasing onto official `ad898d7`, the complete suite exercised 1,110 cases
with twelve expected standalone child/manual skips and four disabled tests
(1,114 registered). All functional/integration cases passed. The existing
`IcebergMetadataJsonCost.SerializationIsLinearInRetainedSnapshots` timing check
measured 3.008× against a strict 3× limit under parallel load; five consecutive
isolated reruns passed (ratios 1.88, 2.46, 2.10, 2.03 and 1.43). It was not
changed or disabled. Real repeated-merge tests check every latest manifest count
through 205 appends; five fault cases cover the original/merged manifest,
manifest-list and SQLite commits before/after journal unlink, followed by
fresh-process rows/history and committed/failed exact retries. Adversarial tests
reject seven changed EXISTING-entry cases and verify exact commit-byte boundaries,
identical recovery usage and ordinary appends after bulk ingestion. All 288 C++
files passed formatting, intentional negative quality checks passed, and the
five affected translation units passed clang-tidy.

An independent source review found no blocking implementation defect and prompted
the failed-operation retry check at a merge boundary. It favored the upstream
merge implementation over a separate maintenance commit, which would complicate
operation identity and publication. Multi-bin execution with EXISTING-only outputs
and maximum-width metadata expansion remain broader coverage limits; a bounded
scratch comparison is a possible future option if expansion warrants it. No
power-loss or RSS qualification is added.

## Qualification limits

This campaign uses real SQLite, Arrow, Parquet and Iceberg dependencies. Its
results are native evidence, separate from in-memory mocks and HTTP/Locust
measurements. Same-host filesystem/cache behavior and a narrow compressible
schema limit generalization. A larger fixture, different row width, failed
operation population, retained-reader population or filesystem can reach a
different bound first.

No physical power cut, model cut, OS-cache eviction, dedicated-device flush/FUA
verification, general RSS bound or large-volume bulk-ingestion qualification is
implied. G4 and physical power-loss durability remain **NOT RUN / NOT VALIDATED**.
