# Bulk append validation and independent review

Recorded on 2026-11-12, Linux x86-64, Clang/libc++ 18, Release with the pinned
Arrow 25 / Iceberg / SQLite installation. This is a shared development host with
concurrent compilation/tests. Timing is illustrative; no dedicated-device,
cache-eviction, physical power-loss or 1 TiB qualification was performed.

## Data larger than the process memory limit

Both ordinary and bulk processes generated the same 1,048,576 rows, paired
duplicate keys, deterministic 1,024-byte payloads and nullable precise int64s.
Canonical input was 1,113,437,978 bytes (over 1 GiB). Every ingestion and reopen
process had `RLIMIT_AS=805306368` (768 MiB), including its libraries and allocator.
No complete input dataset or source file was materialized. This ceiling bounds
address space; it is not an RSS reservation or a process-wide governor claim.

| Measurement | Ordinary append | Bulk append |
| --- | ---: | ---: |
| Source chunk rows | 4,096 | 4,096 |
| Source passes | 1 | 2 |
| Committed operations / snapshots | 256 / 256 | 1 / 1 |
| Parquet files / row groups | 256 / 256 | 128 / 256 |
| Ingestion peak RSS | 83,415,040 bytes | 91,308,032 bytes |
| Ingestion time | 37.812 s | 36.669 s |
| Canonical throughput | 28.08 MiB/s | 28.96 MiB/s |
| Reachable Iceberg JSON/Avro | 2,630,712 bytes | 14,980 bytes |
| Parquet footer bytes | 652,544 | 636,672 |
| Catalog bytes | 290,816 | 53,248 |
| External file / directory sync calls | 1,024 / 1,024 | 131 / 131 |
| Final response interval | 137.61 ms | 28.46 ms |
| Fresh-process engine open | 667.86 ms | 30.85 ms |
| Matching retry | 37.76 ms | 9,710.68 ms |

Bulk validation took 9.869 s and production 26.771 s. Its retry rereads/hashes
the entire source; callers resolving a known operation can instead use
`FindAppendOutcome`. Both retry paths performed zero warehouse-file I/O or syncs.
The response intervals have explicit different boundaries: ordinary measures
from its last batch being ready to its last append returning; bulk measures
from second-pass EOF to the synchronous bulk call returning. They are not
measurements of identical amounts of final work. Sync counters exclude SQLite's
VFS. Fresh-process open retains the OS cache and is not a cold-device result.

The independent oracle verified every key/payload/value/NULL and duplicate count,
initial empty and historical views, all permanent receipts and exact retries.
The first ordinary historical view contains 4,096 rows; bulk's single snapshot
contains all rows. Both reopened with unchanged limits. The process crash campaign
also killed source consumption and execution immediately after a real Parquet
file sync: two fresh opens preserved an empty view and permanent `INTERRUPTED`
failure without replaying data.

Reproduce the small process suite and the manual large campaign:

```sh
cmake --build --preset release --parallel 4
ctest --test-dir build/release -R '^glamdb.bulk_ingestion$' --output-on-failure
python3 tests/integration/bulk_ingestion_test.py \
  --driver build/release/benchmarks/glamdb_bulk_ingestion_bench \
  --large --output /tmp/glamdb-bulk-measurement --address-space-mib 768
```

The output directory must be new. The runner canonicalizes an existing parent
alias, preserving the engine's rejection of a symlink used as the store itself.
Raw JSON and executed logs from this run are retained locally under the ignored
`artifacts/bulk-ingestion/2026-11-12/` directory.

The original measurement candidate used base `47cc955`. Final integration also includes
`85f54cf` (owned Arrow columns and buffered canonical hashing, positional reads,
pruning and metadata cache, Parquet compression/dictionary/encoding settings,
HTTP scan quotas, recovery coverage, observability and managed database deletion).
Measurements use the default writer encoding;
nondefault compression levels were not included in this memory/timing campaign.
The final integration repeats correctness checks and independently checks every
bulk footer, settings changes, exact retries and reopen with catalog schema version 2.

The large campaign was repeated on the reviewed `382d192` candidate (base
`0dbffa7`) after the owned-column, buffered-hash, pruning and encoding integration.
Both methods again verified all 1,048,576 rows, original receipts and historical
views in ingestion and fresh-process reopen under the same 768 MiB address-space
ceiling. Bulk retained one snapshot/receipt, 128 files and 256 row groups, with
74,821,632 bytes ingestion peak RSS; ordinary used 256 snapshots/receipts/files
and 77,471,744 bytes peak RSS. Reachable metadata was 14,988 versus 2,636,722
bytes, with 131 versus 1,024 external file syncs. Concurrent builds/tests affected
timings (bulk ingestion 39.934 s, ordinary 114.682 s); this is a correctness and
memory repeat, not a controlled performance comparison. Raw JSON is retained in
`artifacts/bulk-ingestion/2026-11-12/review-main-0dbffa7/`.

## Correctness and quality checks

- Complete Release build and CTest after integration through `85f54cf`:
  **zero failures across 1,038 enabled cases**, including native HTTP/SDK,
  installed consumer, protocol/fault, storage, I/O, scanner and native metrics suites.
  Eleven child/measurement entrypoints skipped without their opt-in environment;
  four manual measurements/capacity cases were disabled. The complete suite was
  repeated after the final footer-validator cleanup with the same result.
- Complete Python suite at base `c5ca585`: **1,030 passed**, one expected skip and
  one external Starlette deprecation warning. After observability integration
  (`9536a3d`), the affected SDK receipt/client/stream, verifier and workload suites
  passed **542** cases. The fresh review integration through `85f54cf` passed
  **600** affected SDK, verifier and workload cases and **208** Node unit cases.
  The Python network cases require localhost access: the restricted-sandbox run
  reported 23 socket-related failures/errors, then the complete affected selection
  passed with sockets enabled.
- ASan/UBSan after the fresh review integration: unit **135**, vertical **339**
  (including all **17** active bulk cases) and storage **175** passed; eleven
  expected child/measurement entrypoints skipped across vertical/storage.
  After the final footer-validator cleanup, all 175 storage cases and the bulk
  codec/retry/reopen regression passed again on the rebuilt sanitizer binaries.
  Earlier I/O **178** cases passed before the compression integration.
  Existing measurements and the two manual capacity campaigns were excluded
  from sanitizer execution.
  Leak detection was disabled because LeakSanitizer cannot run under the local
  ptrace-based sandbox; no LSan or TSan result is claimed here.
- LLVM 18 formatting across **280** project files, clang-tidy on the **10**
  translation units affected by the fresh integration, and quality negative checks
  passed. Both diagnostic-bearing translation units were corrected and rechecked.
  The original feature's 17 changed translation units were checked earlier.
- Literal shared fixtures cover canonical chunk equivalence, receipt version 2,
  unsupported versions, ordinary limits, precise timestamps and 17 invalid
  receipt mutations in native, SDK, UI and independent verifier tests.

Focused native cases exercise ordinary/bulk matching and conflicting retries,
source interruption before binding, changed production content, duplicate rows,
initial/historical views, schema/scalar validation, odd 9,999-row chunks across
100,000-row file boundaries, 2,051 one-row chunks coalesced into one file, parent
buffer retention, second-file sync failure, uncertain outer COMMIT, shutdown,
and cancellation before/during/after publication. A committed bulk retry succeeds
with a one-byte owned-input limit and a device reservation ceiling too small for
a new bulk operation.

Manual operation-wide boundary campaigns are excluded from normal CI because
they hash several GiB. Run them explicitly in Release:

```sh
build/release/tests/glamdb_vertical --gtest_also_run_disabled_tests \
  --gtest_filter='TBulkAppendTest.DISABLED_*'
```

The canonical-byte case streams 1,047,298 repeated rows of 4,101 canonical bytes
each: 4,294,969,098 bytes, just beyond 4 GiB. The file-count case uses 800,000 rows
below that byte cap and forces more than 256 files. Both cases passed in Release, asserting rejection
before publication, unchanged views and identity/failure persistence as applicable.
These do not replace combined-profile/RSS or physical-storage qualification.

## Independent review, critique and alternatives

A separate reviewer inspected source, tests, publication/recovery invariants,
client validators and measurement design without implementing the feature.
It found one material race: cancellation could arrive during closure verification
after the first flag check and still reach the outer COMMIT. The reproducer
failed by committing; a final cancellation/deadline check under the publication
gate makes it fail durably with no snapshot. That regression and the separate
after-COMMIT cancellation case pass. A forged duplicate data path also received
a RED-to-GREEN publication-closure regression during implementation.

No additional production blocker was established. The reviewer checked bounded
normalization/coalescing, complete canonical hash, immutable prepared base,
three outcomes, permanent failure/retry identity, old ordinary one-file guards,
actual aggregate rows/files/bytes, bulk-marker/receipt correspondence and equal
publication/recovery bounds. Shutdown and constrained-admission tests were added
in response to its evidence checklist. Existing corruption-generation tests
exercise the shared coordinator; no new bulk-specific corruption-race campaign
or maximum-width NULL-heavy RSS qualification is claimed.

A second independent review examined the integration with the newly merged
Parquet settings and recovery coverage. It found no additional code blocker:
settings are refreshed under the writer lock, both producers apply them to every
file, terminal retries precede codec admission, and catalog schema version 2 is
independent of result receipt version 2. It clarified the catalog outcome-blob
encoding wording. Building the immutable writer specification once per operation
and calibrating reservations across compression levels are further improvements;
the footer/retry/reopen regression covers the implemented integration.

The final observability integration was independently reviewed as well. Both
append entry points share exactly one operation metric scope, every returned
outcome is recorded once, and pending-call/owned-byte gauges retain balanced RAII
increments and decrements across retries, admission refusals and all outcomes.
The final cancellation/deadline check remains under the publication gate. No
additional blocker was found in this review.

A fresh independent review checked the integration with owned Arrow columns,
buffered hashing, scan pruning and dictionary/encoding settings. It found that
the verified scan index now retains each file's metric maps, whose memory is
not bounded by root JSON size when one bulk snapshot adds many files. The fix
adds a generation-pinned descriptor/index charge and checks the producer's
descriptor sum against its 32 MiB envelope. A regression first failed the
retained-charge assertions. Review of that fix exposed a second issue: the
same five-file history can have vector capacity six after publication and
eight after recovery. Charging actual capacity made reconciliation fail under
saturated metadata. The extended three-plus-two-file regression reproduced that
failure; stable two-times-element-count vector allowances now let it reconcile,
return both original receipts and reopen under unchanged limits. The final
independent re-review found no further actionable blocker.

Conflict resolution preserves owned column lifetimes, hashes the same canonical
preimage across source chunks and applies the current writer specification to
every bulk file. Local builds also caught a stale footer-validator name and two
hash tests that still used the earlier hasher interface; those were corrected.
Static-analysis diagnostics prompted explicit root/index size fields and a local
optional reference in the footer validator without changing its settings logic.

The two-pass choice avoids a payload spool and its recovery/cleanup protocol,
but requires replayable input, repeats hashing on retries and conservatively
reserves over 8 GiB of device growth even for small bulk calls. Alternatives
considered for later work:

1. Derive proved, tighter admission envelopes from bounded first-pass statistics,
   while allowing chunk boundaries to change.
2. Normalize directly in bounded Arrow buffers and later stream row groups to
   reduce conversion/copy cost, preserving exact canonical and lifetime rules.
3. Design one-pass ingestion around a caller-supplied digest with final mismatch
   failure, or a separately budgeted durable spool with explicit interruption,
   recovery and cleanup semantics.

Those are future extensions. The append-only scope, replayable source and finite
development profile are deliberate choices for this implementation.
