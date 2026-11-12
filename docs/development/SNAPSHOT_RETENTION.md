# Explicit snapshot retention

The embedded C++ API supports explicit, opt-in expiration. Existing collections
retain all history until an application installs a policy and runs maintenance.
There is no background task, implicit append expiration, or HTTP/SDK/UI
maintenance route in this extension.

```cpp
SnapshotRetentionPolicy policy;
policy.keep_last = 8;
policy.min_age = std::chrono::hours(24);
auto result = engine->ExpireSnapshots({collection_id, policy});
// Later maintenance reuses the persisted policy.
auto next = engine->ExpireSnapshots({collection_id});
auto effective = engine->GetSnapshotRetention(collection_id);
```

`keep_last` accepts 1–2^40; `min_age` accepts nonnegative milliseconds through
253402300799999. Retention protects the union of the newest `keep_last`
snapshots, snapshots younger than `min_age`, and the current snapshot. A snapshot
at exactly the minimum age can expire. Future-dated snapshots remain protected
by a positive age guarantee. Only a contiguous oldest prefix expires, so a clock
regression cannot create a hole in ancestry. A policy with neither a count nor
a positive age retains everything; explicitly passing that policy disables future
expiration without restoring any expired snapshot. Policies persist even on an
empty collection. Callers choose maintenance frequency and must run it before
reaching retained-history capacity; protected history can still exhaust capacity.

The result reports the effective policy, snapshots expired by this call and
metadata files still queued for cleanup. The current snapshot ID and every
Parquet row remain unchanged. Expiration reclaims old manifest lists and manifests
whose last retained snapshot reference disappears. A merged manifest can replace
its immutable input manifests; those inputs remain until every retained snapshot
that needs them expires. Current manifests and Parquet files are never deleted.
General orphan collection and compaction remain outside this feature.

## Historical reads, preconditions and receipts

New scans, statistics, selected snapshot histories, file listings/details and
explain requests for an expired
ID return `SNAPSHOT_EXPIRED` with `FixInput`; HTTP error serialization maps the
code to 410. Unknown IDs retain `NOT_FOUND`. A new append with an expired
snapshot precondition returns `SNAPSHOT_EXPIRED`; a matching terminal operation
retry still returns its original receipt before considering a new precondition.
Initial-empty reads always stay empty.

An already planned cursor retains its immutable file membership and continues
reading the original view, including after expiration. Metadata planning against
any older root delays cleanup for that collection. Fixed metadata pages already
materialized keep their original inventory. New requests use the newly confirmed
retained suffix. The HTTP statistics adapter reports `retained_snapshots: null`
when a concurrent publication prevents it from obtaining a count from the same
root as the statistics; it never substitutes a later root's count.

Append receipts and their ID/hash bindings never expire. Receipt lookup does not
need old snapshot metadata. Retained snapshots are cross-checked against their
original receipts; expired IDs are also bound to exactly one committed receipt.
Recovery starts from the oldest retained snapshot's complete inventory. It checks
each file's original snapshot ID, sequence and file sequence against the permanent
receipts, verifies per-operation row/file/byte totals and ordinary/bulk limits,
and validates Parquet footers. Later retained transitions still require exact
EXISTING-entry rewrites. Expired lists and obsolete manifests are not needed.
Advisory column metrics retain the existing immutable-storage trust contract;
receipts authenticate operation identity and totals rather than file contents.

## Publication, recovery and compatibility

Maintenance owns the serialized writer, checks the exact root/table base and
writes a new immutable root through the same durable FileIO as appends. The
closure guard permits only a policy change and removal of an oldest prefix;
current identity, schema, data membership and immutable snapshots stay fixed.
Only the outer SQLite COMMIT publishes the root, expiration ledger and cleanup
queue together. An uncertain result returns `RECOVERY_REQUIRED`, closes
admission and requires `Reconcile()` followed by repeating the request. Neither
an error nor a timeout authorizes deletion.

Cleanup checks each queued path against the verified union of all retained
manifests and lists in the same collection. It waits for collection metadata planners, unlinks the file,
synchronizes the metadata directory and then acknowledges the queue in SQLite.
Failures leave cleanup pending. Each writer/recovery pass handles at most 8192
queued paths per collection; the reported pending count includes the entire
backlog. Later writes or maintenance resume remaining batches. Exclusive reopen
validates the selected root and receipts, executes the durability fence and
resumes cleanup. Missing expired
ledger entries, invalid ancestry or a missing current data file are corruption.
Interrupted maintenance can leave an ordinary orphan root; automatic orphan
collection is not introduced.

Catalog versions 1 and 2 remain readable and unchanged until the first policy
publication. Version 3 is introduced transactionally and includes the existing
write-settings schema plus expiration and cleanup tables. Older binaries reject
version 3. The immutable store and append receipt formats remain unchanged.

The default permanent operation limit remains 4096. To exceed it, select adequate
explicit `Limits::max_operations`, catalog bytes and `Limits::history` budgets,
and pass the same profile when reopening; see [configurable history](SCALABLE_HISTORY.md).
Retention never raises or lowers configured capacity. Successful history is bounded
by the lesser of the operation and data-file budgets: above 4096 such slots,
admission reserves an additional 16 KiB metadata and 64 KiB recovery memory per
slot. This covers expired ancestry even when the retained root is small; streamed
failed operations do not inflate this reservation. Catalog operation and
expiration inventories use bounded keyset pages, so a ledger beyond 8192 entries
remains recoverable. Data files, roots, manifests, lists, catalog bytes, warehouse
capacity and shared reservations remain finite. Retention cannot make an
append-only data inventory independent of the number of appends.

Maintenance checks its new root together with the latest verified append's manifest
and list bytes against `max_commit_bytes` before publication. A policy-only change
can exceed this budget even when no snapshots expire. Maintenance conservatively
preserves that verification charge; reclaimed rewrite inputs can remain charged
until reconciliation rebuilds the retained checkpoint.

## Validation

Focused native tests are `SnapshotRetention.*`, `TSnapshotRetentionTest.*` and
`SupersededRootTest.ExpirationWaitsForPlannersOfEveryOlderRoot`. Fault tests use
disposable stores and real POSIX/SQLite delegation. The child test executes a
new process for uncertain-COMMIT exits and reopen verification. These tests do
not qualify physical power loss.

The manual >4096 campaign is excluded from default CTest:

```sh
build/local/tests/glamdb_vertical --gtest_also_run_disabled_tests \
  --gtest_filter=SnapshotRetention.DISABLED_MoreThan4096AppendsBoundRootAndVerifyAllReceiptsInFreshProcess
```

It runs 4100 real three-row appends, keeps eight snapshots with maintenance every
16 appends, checks a 16 KiB retained-root ceiling, and compares a digest of every
original serialized receipt after fresh-process reopen. It prints maximum root
bytes and elapsed/reopen time.

Executed on 2026-11-12 with Clang 18, Linux x86-64, a Debug build and disposable
`/tmp` storage (`statfs` reports ext2/ext3) on a shared host:

| Check | Result |
| --- | --- |
| 4,100 committed appends, 12,300 rows | PASS; 310,465 ms through the final maintenance |
| Maximum root after maintenance | 8,644 bytes; eight snapshots retained |
| Fresh-process reopen | 4,324 ms; every original serialized receipt verified |
| Full native/SDK/HTTP suite before the final HTTP integration | PASS; 0 failures out of 1,183 enabled entries, with 13 helper/measurement skips |
| Latest upstream HTTP integration suite | 1,191 enabled entries; one new HTTP timing test failed initially and passed on isolated retry; all other entries passed or were helper skips |
| Full local suite after the independent byte-budget review fix | PASS; 0 failures out of 1,194 enabled entries, with 13 helper/measurement skips; 198.64 seconds |
| Tight commit-byte policy changes | PASS; live publication, ordinary reopen and expired checkpoint reopen |
| Focused retention, planner, adapter and catalog compatibility regressions | PASS; 25 native tests |
| ASan/UBSan/LSan retention, adapter, settings and cursor lifetime checks | PASS; 108 tests across native, storage and catalog suites |
| LLVM 18 formatting / changed translation units / quality negative check | PASS; 306 files / 18 translation units / deliberate violations rejected |

The full-suite command was
`GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 ctest --test-dir build/local --output-on-failure --parallel 4`.
The manifest-merging/configurable-history run completed in 225.93 seconds without
retries. A subsequent merge of upstream HTTP changes completed 1191 entries in
171.53 seconds, with one failure in the newly introduced concurrent reader test:
an operation lookup raced the catalog mutex held by statistics/history and
returned `OPERATION_IN_PROGRESS`. The same locking path exists in the upstream
base. The entire `glamdb.native_http` entry passed when rerun alone (7.94 seconds).
That unrelated test and runtime behavior are unchanged by this extension.
Five manual campaigns remain disabled in default CTest; the retention campaign
above was executed before that final HTTP merge. Its retention implementation is
unchanged; those timings do not measure the newer HTTP/input preparation code. Earlier
runs before manifest merging also passed, but their timings do not measure this
producer. The runs did not control cache warmth or qualify combined capacity,
dedicated-device behavior, or physical power loss. G4 remains NOT RUN.

The sanitizer build uses `GLAMDB_ENABLE_ASAN=ON` and `GLAMDB_ENABLE_UBSAN=ON`;
the focused executable runs with `ASAN_OPTIONS=detect_leaks=1` and
`UBSAN_OPTIONS=halt_on_error=1`. Dependencies use the existing pinned install.

## Design review

Independent review exposed two compatibility pitfalls now covered by
regressions: every catalog-version preflight must accept version 3, and installing
a retention policy must preserve configured storage budgets. Opting one collection
into retention cannot admit an unreadable state in another collection. Reopen
also checks aggregate usage independently of collection creation order. Integration
with configurable history added paged expiration inventories, overflow-safe queue
bounds, larger keep-last values and owned receipts across streaming callbacks.

The adapter review added deterministic regressions for an expired history
selector and expiration between statistics and history reads. A keep-last-one
race used to manufacture a not-found error; a keep-last-two race silently
undercounted the earlier root. Both now preserve the computed statistics and
report an unavailable retained count when the root identities differ.

Review of upstream manifest merging added a complete retained checkpoint for
recovery, union-based last-reference manifest cleanup, and bounded cleanup batches.
Regressions cover the 99/100 to 100/101 reference transition, original bulk files,
identical publication/recovery accounting, malformed file provenance, early Avro
record limits and a cleanup queue larger than 8192 paths. Fresh-process tests
also cover format-reader initialization before checkpoint recovery. A forced
snapshot-ID collision proves that append publication rejects IDs reserved by the
expiration ledger; a randomly reused expired ID cannot become an unreadable
committed snapshot. Resource-admission tests cover successful-history bounds,
large operation settings and refusal before creating a store.

A further independent review found that a policy-only root could exceed the
current append's recovery verification budget after publication. Maintenance now
checks that budget before COMMIT using the latest verified non-root byte charge.
Regressions cover refusal at the exact limit, acceptance with room for the policy,
and ordinary and expired-checkpoint reopen under the same limits. Refusal preserves
the previous policy and original permanent receipts.

The retained root and redundant list history are bounded by explicit maintenance;
permanent receipts and the current append-only file inventory still grow within
the configured finite limits. Cleanup runs on writers and recovery, so a stalled
metadata planner can delay reclamation for its collection. Compaction or a
separately designed orphan collector could address those different growth
sources. A future cleanup worker could batch directory fences and queue
acknowledgments, but must retain publication authority and planner protection.
