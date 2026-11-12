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


## Remote bulk follow-up (#172)

The native HTTP adapter, Python SDK and standalone CLI now upload one bulk
operation with declared total rows and bounded LF-terminated NDJSON batches.
The transport stages replay in anonymous disk-backed scratch; the core keeps its
existing two-pass hash, exact retry, publication and recovery protocol. No new
persistent format, source replay during recovery or automatic retry is introduced.
The organization gateway masks the native bulk feature until it has a compatible
streaming write-admission adapter; browser import remains outside this change.

Independent review checked framing, scalar/hash identity, pre-binding failure,
publication, resource ownership, admission, cleanup and caller ambiguity. It
confirmed the anonymous spool fits the existing source protocol, and found:

- Scheduling allocation failures could terminate a spill worker or retain a
  session. Enqueue now restores ownership on failure; atomic completion remains
  observable if notification fails. Dedicated fault tests cover both boundaries.
- Final scratch close could execute on the listener. Cleanup now runs on spill
  workers with reservations and permits retained through acknowledgement; a
  blocked-close test checks health progress and resource release.
- SDK scratch seek/read and schema-lookup errors omitted the original identity.
  Regressions fail before the fix, then verify both UUIDs, unlocked replay,
  sanitized errors and failures after the final transmitted block.
- Gateway capabilities advertised an unavailable route. Its capability response
  now masks the feature and upload limits, with a before/after regression.
- Memory-backed temporary storage could charge the operation size outside fixed
  memory admission. The Linux server rejects tmpfs/ramfs scratch; a real native
  test verifies refusal before binding and continued process health.

The design critique compared server scratch with in-memory buffering, client
resend phases, persistent upload sessions and an Arrow IPC transport. In-memory
buffering violates the operation-size-independent envelope; resend phases would
couple the writer to a second network exchange; durable upload sessions add a
new recovery/cleanup authority. NDJSON reuses the strict precise row codec and
requires no new client dependency. Arrow IPC could be a separate future encoding;
it does not remove replay or lifecycle requirements.

The measured profile is Linux x86-64 with Clang/libc++ 18 and the pinned native
prefix, on a shared development filesystem. The native tests cover 12,001 exact
rows including duplicates, chunk-independent retries, changed-input conflict,
initial snapshot and fresh-process receipt/retry, invalid tail and interrupted
upload without a binding, a deliberately unconsumed committed POST reply,
SDK/CLI uploads beyond ordinary append, verified TLS, global/per-database upload
limits and available lifecycle/readiness controls. A 42,320,552-byte body is
streamed while the last record is withheld; outcome lookup remains absent and
statistics remain zero before execution. Staging RSS is sampled separately from
core execution. The Python preparation test also spills over 16 MiB while
keeping traced preparation heap below 8 MiB.

The initial remote validation used official `ad898d7`; recorded checks on 2026-11-12:

| Check | Executed result |
| --- | --- |
| Debug native server, standalone CLI, admin, transport/routes tests and native fault driver | Build passed |
| `python -m pytest -q` with the hash-pinned development packages | 1,499 passed, 31 skipped, 12 subtests passed; two external deprecation warnings |
| CTest routes/transport/native HTTP, bulk, TLS security, scalars, CLI and native fault suites | 152/152 passed |
| `format-check`, `quality-negative-check` | 294 project files checked; intentional violations rejected |
| LLVM 18 clang-tidy | All 11 changed translation units passed |
| 42,320,552-byte native upload before final record/core entry | Staging RSS growth 757,760 bytes; no operation binding or visible rows |
| ASan/UBSan upload job faults, including blocked cleanup, with leak detection | 3/3 passed with the compatibility setting below |
| ASan/UBSan SDK/CLI bulk, verified TLS, admission, media type and tmpfs refusal | 5/5 passed with the same setting |

Reproduce the ordinary native contour after configuring/building the requested
targets (the native fault test also requires `glamdb_native_fault_driver`):

```sh
python -m pytest -q
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --test-dir build/local --output-on-failure -j 4 \
  -R '^(glamdb\.(bulk_http|native_http|http_review|http_security|native_scalar_types|cli_network|cli_data)|glamdb\.routes\.|glamdb\.transport\.)'
```

The local ASan/UBSan build reuses ordinary pinned dependencies and the existing
Clang/libc++ 18 toolchain. It needed `detect_container_overflow=0` because
instrumented libc++ container headers coexist with uninstrumented upstream code;
GoogleTest discovery itself otherwise reports container poisoning. Heap/lifetime,
undefined-behavior and leak checks remain enabled. The three upload job cases
pass with `ASAN_OPTIONS=detect_container_overflow=0:detect_leaks=1:halt_on_error=1`
and `UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1`.

The complete local native bulk sanitizer attempt is **not qualified**: an
eight-case attempt completed five cases but could not complete three exception
paths because of an allocation/deallocation mismatch in the reused
libc++/libc++abi runtime. An independent exception-only program, with no GlamDB
code, reproduces `operator new` versus `free` when throwing a
`std::runtime_error(std::string(...))` with this runtime/library combination.
That check was not disabled. The production RSS case is a separate unsanitized
measurement; no ASan RSS or complete local sanitizer campaign is claimed.
Required clean CI sanitizer profiles remain separate evidence.

This evidence does not qualify maximum-size decode RSS, disk-controller behavior,
physical power loss, a 1 TiB load or a universal cgroup memory governor. Source
callbacks remain cooperative, and CLI callers must preserve their input file and
UUID for exact retries.

A further independent review before merge found that the standalone CLI discarded
an available early HTTP refusal when sending a bulk body larger than the server's
32 MiB rejection-drain ceiling. It reported an unknown outcome instead of the
complete pre-execution admission or authentication refusal. The client now attempts
its existing bounded response read after a socket write failure. Only a complete,
valid response resolves the outcome; absent or malformed replies remain uncertain.
The native regression first failed all four HTTP/verified-TLS and
admission/authentication combinations, then passed after the fix, with no operation
binding or visible rows. It also passed with the CLI under ASan/UBSan and leak
detection using the compatibility setting above. A follow-up independent inspection
confirmed the callback lifetimes and unchanged response/deadline validation.
Official `6adb50c` was incorporated without conflicts before the final checks.

On that combined revision, the native build, full Python suite (1,499 passed,
31 skipped, 12 subtests; two external deprecation warnings), and the 152-case
native contour above all passed. LLVM 18 formatting checked 298 project files,
and clang-tidy passed for the changed client translation unit. The new four-case
early-refusal regression passed with the ordinary CLI and with the instrumented
CLI; this focused sanitizer result does not expand the earlier broad sanitizer
qualification.

Before merging, official `8677619` added shared SHA acceleration and manifest
merging. The roadmap conflict was resolved preserving both upstream evidence and
remote bulk coverage; independent review confirmed unchanged canonical preimages,
bulk receipt counters, membership and publication/recovery checks. A post-conflict
run exposed a test assuming that a malformed-upload reply implied cleanup had
released its permit. That loop now retries its exact request for at most five
seconds only on `503 SERVER_BUSY` with `execution_started: false`; parsing errors
and operation-absence checks remain strict. This test correction was independently
reviewed. The final affected contour passed 75 tests, with one child-process helper
skipped and two manual capacity tests disabled. It includes native HTTP/CLI, bulk
publication/recovery, canonical hashes, forced SHA paths and manifest closure.
The final native build and formatting check (300 project files) passed.
