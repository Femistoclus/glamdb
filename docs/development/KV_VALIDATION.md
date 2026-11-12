# KV validation and independent review

The implementation for issue #101 preserves the synchronous append contract and
uses the same coordinator, durable file writer, exact-base closure and SQLite
publication authority. The [KV guide](KV_MODE.md) defines the supported profile;
the [layout measurements](KV_LAYOUT_MEASUREMENTS.md) and
[native measurements](KV_NATIVE_MEASUREMENTS.md) record separate experiments.

## Independent review

A separate reviewer examined the whole change and ran public-API reproductions
without editing the implementation. Six initial findings were corrected and checked:

| Finding | Correction and regression evidence |
| --- | --- |
| A canceled exact migration retry hid its committed population receipt. | Owner-proven terminal replay precedes source reads, cancellation and new budgets; canceled, 1 ms timeout and pressured-memory retries return the original receipt. |
| Public compaction accepted 16 MiB while the backend accepted only 8 MiB. | One 16 MiB upper bound, boundary test and conflicting-options exact retry. |
| SDK/UI accepted impossible upsert physical counters. | Matching native arithmetic and shared invalid receipt vectors; compact no-ops also require zero input rows. |
| An unrelated write could claim a canceled migration's population UUID. | Creation reserves that UUID, binding atomically links its payload digest, and replay checks durable ownership and operation provenance. |
| A busy migration lookup fabricated an unknown receipt with an empty digest. | A blocked identity lookup returns a distinct resolution error; genuine pending populations retain their original validated context. |
| A reentrant replay could read an unconfirmed catalog transaction. | Replay refuses publication/transaction reentry, with a transaction callback regression. |

Independent native reprobes verified original receipt replay, reserved UUID
protection, corrected target rows, busy lookup classification, and policy/source
view conflicts. This is a bounded review; it is not a proof that every possible
bug or durability failure has been excluded.

## Review of the later official retention integration

Official main then added append snapshot retention and wider schemas. KV now
uses catalog version 4, preserving official version-3 retention stores without
reinterpretation; the first KV creation upgrades transactionally. Three
additional cross-feature findings were corrected:

| Finding | Correction and regression evidence |
| --- | --- |
| An unbound migration population could report its existing target creation as uncommitted after source expiry. | Owner-proven creation is retained before source reads; source expiry refuses population while creation remains committed, including after reopen. Terminal populations replay their original receipt without source reads. |
| An append cleanup queue could be interpreted against an incomplete KV manifest union. | KV refuses retention policy/expiration, rejects expiration ledgers and cleanup queues, and rejects direct cleanup before I/O. Storage profiles, closure, recovery and scan planning independently reject retention/checkpoint state. |
| Physical inventory omitted historical KV manifests and permanently pinned no-op roots. | Verified full manifest unions and no-op roots enter referenced-byte accounting; mixed-mode cleanup, independent file sums and reopen checks cover classification. Path admission is checked before each insertion. |

Independent native probes additionally cover official v3 reopen followed by
lazy v4 KV creation, 128 fields with the key at column 128 and 127 decimal fields
with 128-byte names, exact values beyond binary64 integer precision, null
replacement, historical rows, no-op replay and exact physical metadata sums.
The 520-operation/260-root probe also checks referenced/unreferenced bytes before
and after reopen. These are finite correctness checks, not larger-history or
power-loss qualification.

## Follow-up lookup and workbench regressions

`tests/vertical/kv_lookup_test.cpp` rewrites a retained Parquet file with duplicate
keys while preserving its schema, row count and file length. Current and historical
lookups must report corruption and close read/write admission. Scan creation,
receipt lookup and a missing-key delete remain blocked; reconciliation fails until
the original bytes are restored. Successful reconciliation restores lookup, the
original receipt and new writes. Invalid keys and lookup result-memory exhaustion
are separate controls that must leave a healthy store available.

`ui/tests/e2e/native.spec.mjs` covers imported delete and compaction requests with
both committed and definitely-not-committed receipts, resolved through exact retry
or outcome lookup. Transport failures keep the pending operation's original kind,
ID and payload. Terminal outcomes restore an editable upsert form without losing
the query draft, including when the post-commit metadata refresh fails. Each case
submits a fresh upsert with a new ID, reads its row back from the native server and
checks that the original terminal receipt is unchanged. The export/reload/import
scenario also checks that returning to Data studio does not resurrect a pending
panel for a completed delete.

## Invariants exercised

| Contract | Executable coverage |
| --- | --- |
| Immutable mode; append compatibility | Mode/create retries, raw append refusal without consuming an ID, old receipt byte fixtures, existing ordinary/bulk suites. |
| Full-row uniqueness | Last input wins across chunks, non-first UTF-8 keys, all seven scalar families, omitted nullable fields, duplicate/missing deletes, deleting every live file. |
| Snapshot isolation | Untouched range descriptors, retained old rows, held cursors, interval membership, initial empty views and fresh-process reopen. |
| No-op authority | Unchanged upsert, missing/empty delete and compaction; exact original root proof, permanent receipts, no phantom history and lost-COMMIT/rollback recovery. |
| Changed publication | Six real upsert/delete/compaction lost-COMMIT and rollback cases, exact receipts and fresh-process production reopen; shared external-file fault matrix remains enabled. |
| Migration | Explicit policies, fixed source, chronological cross-chunk/full-row FirstWins/LastWins, Reject failures, creation/population uncertainty, preexisting-target refusal and population provenance. |
| Bounded work and ownership | Read requested-byte charging including partial/failed reads, cancellation/deadline latching, result lifetime, retained capacity refusal, scratch page/I/O limits and collision-safe cleanup. |
| Native clients | HTTP, SDK, CLI and browser mode/operation parity, exact saved retries, all wire scalars, historical lookup, receipt export/import/reload and native restart. |

## Architecture critique

Sorted disjoint full-key ranges meet the measured locality target and avoid a
new per-key index. An index could reduce candidate discovery but cannot reduce
the files physically containing scattered updates. Overlapping sorted runs would
need a different read/merge and tombstone contract. They were measured as a
baseline, rather than silently mixed with this visibility model.

Copy-on-write rewrites surviving values in touched ranges and retains immutable
history. Bounded scatter may refuse; compaction can increase physical retained
usage and does not reclaim snapshots. The conservative four-to-one contiguous
compactor can leave fragmentation. A later multi-output compactor, richer range
directory, or delayed scratch reservation after proving a no-op each needs its
own resource and recovery contract.

The component p95/p99 values have five samples and equal the observed maximum;
OS caches were not flushed. Native runs use a shared development host and one
exercise sample per size, with raw I/O and RSS. These are measurements, not
random-write latency guarantees. Physical power loss, 1 TiB qualification,
KV retention/GC, remote storage and multiwriter operation remain outside this work.

## Recorded checks after upstream integration

The complete feature integrates official main
`8532c4966fa1da5ce03c54bb5e0067311648fa29`. Integration preserves streamed HTTP
bulk append, cached warehouse inventory, merge-append, streamed catalog history,
configured reader admission, hardened footers and optional cache admission, plus
append retention and 128-field schemas.

- Full Release build and CTest: 1,293 enabled entries, zero failures. Fourteen
  standalone child or opt-in measurement entries skipped; five existing manual
  campaigns remain disabled. Native HTTP KV/all-scalar/restart, streamed HTTP
  bulk, ordinary/bulk append, fault recovery, admin, tooling and the installed
  consumer all passed.
- Storage-disabled installed consumer: passed separate write/reopen processes
  and explicit dependency-error expectations after rebuilding the public API.
- Python SDK: 945 passed. UI unit: 255 passed. Native Chromium: 13 passed.
- Combined ASan/UBSan with leak detection: 179 unit, 238 storage, 24 mock and
  139 selected vertical cases passed. Four standalone child/measurement entries
  skipped; parent fault tests invoke their recovery children separately. The
  selected native set covers KV, bulk, faults, snapshots, scan/index admission
  and owned-input lifetime. No runtime sanitizer error was suppressed.
- Both 4/16 GiB native campaigns were rebuilt from scratch on the integrated
  implementation; clustered mutation, historical full-row lookup, no-ops,
  compaction and fresh-process reopen passed. The raw record identifies the
  final source, executable hashes and catalog-v4 schema. Creation, exercise,
  reopen and the final rebuilt benchmark have the same executable hash.
- LLVM 18 formatting passed across all 339 project files. Quality negative
  checks rejected deliberate format/analyzer violations. Integrated KV
  recovery, scan, producer, API, catalog, native server and CLI units and new
  regressions passed individual tidy checks after resolving their diagnostics.
  The earlier full-project tidy campaign was not green: it also reports
  existing diagnostics in transport, pruning, verifier, vendor code and legacy
  tests, including the existing write-settings suite. This work does not claim
  a clean whole-project analyzer baseline.

Independent integration reviews also ran seven fresh v4 native probes. The longer
probe executed 520 operations across catalog pages, retained 260 distinct no-op
roots and receipts, held the original cursor across 259 COW transitions, and
verified latest/history plus exact receipt replay after reopening. All probes
passed, with no residual concrete finding in the bounded review.

Integration regressions cover actual merged-manifest migration chronology,
unknown metric fields and contradictory physical column sizes, the exact
rewritten-input metadata budget, twelve recovery transitions retaining old
origins/intervals, shared warehouse inventory reuse and route-copy admission
before output files. The new history test's first run correctly refused its
tiny recovery workspace through the shared capacity guard; its diagnostic
assertion was corrected and rerun. The missing direct test reader include and
three integrated analyzer diagnostics were also corrected before acceptance.
The inherited HTTP test for rejecting 65 fields now tests 129 fields against the
new 128-field bound. Two files edited during compilation were forced to rebuild
before the final complete Release and sanitizer reruns.

The private recovery proof avoids copying all retained COW history per step;
normal publication still copies the confirmed immutable interval index. Its
Known/Live/provenance maps remain in memory under the fixed recovery envelope.
The disk visited-path bound and the 520-operation probe do not qualify arbitrary
enlarged history profiles for heap use or recovery latency.

The native dependency prefix uses Clang 18/libc++ and pinned Arrow 25. For the
combined ASan/UBSan profile with that uninstrumented prefix, follow the existing
[documented compatibility setup](BUILDING.md): `_LIBCPP_HAS_NO_ASAN` and static
libc++ linkage. Allocation bounds, use-after-free checks and UBSan remain enabled;
no runtime sanitizer-error suppression is used.
