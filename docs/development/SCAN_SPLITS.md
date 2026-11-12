# Snapshot-bound native scan splits

Contract **1.2** adds `ScanSplits`, `PlanScan`, `NextScanSplits`, `ReadSplit`
and `CancelScanPlan` for the native local scanner in append and KV modes.
Default negotiation remains 1.0; exact 1.0 and 1.1 requests keep their previous
features. These C++ handles are process-local, opaque and nonserializable.
Descriptor `TScanSplit::Version` is 1, independent of the negotiated contract,
package, file and receipt versions. No HTTP, Flight, remote token or stable ABI
is implied.

## Planning and scheduling

Acquire a [read view](READ_VIEWS.md), then call
`Engine::PlanScan(view, entry, request, authorizationScope, options)`. The plan
retains the original shared view handle, validates projection, predicate and
batch size through the scanner, and exposes `Info()` with the output Arrow
schema and resolved identity. Even an initial/empty plan has a schema.

Call `NextScanSplits(plan, scope)` until `Done`. A successful call advances one
shared plan position; concurrent calls on that plan serialize. A failed call
leaves its position unchanged, including resource refusal after constructing
some descriptors. Keep returned descriptors to replay work; calling the page
method again advances rather than replaying a successful page.

Each page examines at most 4,096 verified inventory entries and emits at most
`SplitsPerPage` descriptors (default 64, range 1–256). A KV inventory window
containing only retired files may produce an empty nonterminal page. Paging
never copies the full inventory or opens data files. Append selects a prefix
of the original verified membership; KV selects its original half-open file
lifetimes at the captured sequence. No warehouse directory enumeration or
re-resolution of Latest supplies membership.

`TargetBytes` is a soft grouping target of committed compressed whole-file
sizes, default 128 MiB, range 1 byte–1 GiB. A group ends after reaching the target
or the page's examination bound. A file is indivisible, including all its row
groups; the final file can exceed the remaining target or be larger than the
target itself. `EstimatedBytes()` sums selected live files before projection,
filtering or pruning; it predicts neither decoded bytes nor physical I/O.
`FileCount()` counts live files, excluding retired KV inventory entries.

`ReadSplit(split, scope)` creates a fresh independent, schema-bearing cursor.
It reuses the normal exact typed predicate, conservative file/row-group pruning,
projection, SQL NULL semantics and Arrow ownership. Repeated execution replays
the same work. Different splits in a completed plan select disjoint physical
membership and their results form exactly the serial selected row multiset,
including duplicate multiplicity and zero-column cardinality. Fully pruned
splits may return no batches. Physical work can be uneven.

There is no promised global order, offset or end-to-end exactly-once execution.
Any `request.Limit`, including zero, returns `UNSUPPORTED_FEATURE`: a query-global
limit cannot be independently applied to every worker. Compute owns final sort,
limit, reduction and task-attempt deduplication. The current request accepts the
scanner's exact predicates; it does not claim a separate residual negotiation API.

## Validity and cancellation

Plans and descriptors strongly retain the original read-view handle when its
caller drops that handle. They do not retain an Engine facade or independently
retain a verified generation outside the read-view registry. Every planning
page and split read resolves the original view against engine incarnation,
store identity, exact authorization scope and active lifetime. It never falls
back to Latest. TTL and renewal follow the original view (30 seconds by default,
at most five minutes from acquisition); no independent split TTL is introduced.

Explicit view release/cancellation, observed expiry, facade destruction or
shutdown invalidates future admissions and releases registry pins. A surviving
plan/descriptor still owns its charged request/schema/control objects, but does
not keep a closed store locked. An already admitted cursor has its own lease:
view release, expiry and plan cancellation do not cancel that cursor. Cursor
`Cancel()` and engine shutdown use the existing scanner cancellation path;
retained batches, arrays and buffers retain their independent leases.

`CancelScanPlan` stops further page and split admissions for that plan with
`SCAN_PLAN_CANCELLED`. It is idempotent while the original view remains active.
It does not cancel other plans or the shared view. The plan's original view
reference and immutable request/schema remain owned until the last plan or
split is destroyed; explicitly release the view to revoke its pin sooner.
Identity and view-state errors take precedence over plan cancellation.

Other errors use the read-view codes (`READ_VIEW_SCOPE_MISMATCH`,
`READ_VIEW_WRONG_ENGINE`, `READ_VIEW_RESTARTED`, `READ_VIEW_RELEASED`,
`READ_VIEW_CANCELLED`, `READ_VIEW_EXPIRED`, `READ_VIEW_CLOSED`), ordinary scanner
validation/I/O errors, and `RESOURCE_EXHAUSTED`. Invalid entry/page/target sizes
are `INVALID_ARGUMENT`. Unsupported native backends do not advertise splits.
Publication, receipts and write outcome semantics are unchanged.

## Resource ownership

The canonical [budget matrix](RESOURCE_BUDGETS.md) includes every owner.
A plan reserves 4 KiB ordinary Control and 256 KiB plus twice the measured
predicate capacity in Metadata before copying the request. Measured predicate
capacity is capped at 16 MiB; existing depth/node/literal/type limits still
apply. Projection is at most 128 fields with names at most 128 bytes.
Its independently retained schema keeps the scanner's 16 MiB Metadata envelope.
Plan validation briefly admits an ordinary unopened cursor and releases its
other working/control/data reservations on return.

Each split reserves 4 KiB Control before allocating the descriptor, including
its page-vector slot. Handles share plan request/schema ownership; no per-split
copy of the inventory or predicate is made. Partial pages refund every new
charge on failure. Plans/splits have no separate count quota beyond bounded
pages, read-view quotas and finite shared Control/Metadata admission. Their
retained bytes continue competing after cancellation/expiry until their last
owner releases them. Independent cursor/data/result charges remain the normal
[shared scanner charges](SHARED_RESOURCES.md). These are conservative reservation
envelopes, not measured RSS or protection-class borrowing.

## Focused validation and measurements

`ScanSplit.*` in `tests/vertical/scan_split_test.cpp` exercises real native stores,
parallel literal multiset oracles, duplicates/NULLs, empty/historical/initial and
pruned results, zero-column cardinality, native two-row-group files, delayed
retention/KV replacement/compaction replay, identity/lifetime/cancellation and
partial reservation cleanup. `StorageContract.*` checks version/backend gating.
The installed public-only consumer checks delayed split replay after append,
limits/discovery, authorization and admitted-cursor lifetime after view release.

`ScanSplit.NativeWorkerMeasurements` measures planning, throughput, successful
`Next()` batch latency and cursor positional-read bytes with 1/2/4 workers over
four files/4,000 rows. Its first pass runs after native writes (OS cache is warm);
subsequent passes also reuse the engine footer cache. It does not clear caches,
measure device I/O, qualify production scaling or establish physical durability.

A focused Linux x86-64 Debug/Clang 18 run on 2026-11-13, without concurrent
builds, produced the following small native sample. Latency is the arithmetic
mean over successful nonempty `Next()` calls; throughput includes worker cursor
open/read/cleanup and scheduling, while planning includes plan creation and all
pages. All six runs returned exactly 4,000 rows.

| Workers | Pass | Plan µs | Rows/s | Successful Next mean µs | Positional bytes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0 | 162.669 | 631,988 | 1,167.71 | 32,678 |
| 1 | 1 | 203.549 | 625,118 | 1,198.82 | 29,838 |
| 2 | 0 | 179.259 | 1,014,730 | 1,439.00 | 29,838 |
| 2 | 1 | 170.899 | 1,382,830 | 972.94 | 29,838 |
| 4 | 0 | 212.869 | 2,130,730 | 1,081.21 | 29,838 |
| 4 | 1 | 186.280 | 1,556,470 | 1,203.97 | 29,838 |

Only the first row starts with unpopulated scanner footer-cache entries; every
row has warm OS caches from the preceding writes. The difference in positional
bytes is metadata/footer-cache reuse, not evidence about physical device reads.
These are six observations, not statistical performance bounds. Run with:

```sh
GLAMDB_VERIFY_ROUNDTRIP=1 build/local/tests/glamdb_vertical \
  --gtest_filter=ScanSplit.NativeWorkerMeasurements
```

Executed focused validation: native split/read-view/cursor-lifetime suites
39/39; ASan+UBSan+LeakSanitizer 39/39 with no diagnostics; contract tests 10/10
in normal and storage-disabled builds; installed consumer 1/1 in both profiles.
After refining the benchmark's measured batch-count denominator and adding
contract assertions, the normal split and contract suites each passed 10/10.
No CI scripts, broad sanitizer campaigns or physical durability tests were run.
