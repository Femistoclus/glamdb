# Explicit key-value collections

Issue [#101](https://github.com/glamourdb/glamdb/issues/101) adds an immutable
`CollectionMode::KeyValue` alongside the existing append mode. Both use the same
serialized engine writer, attempt-bound durable FileIO, exact-base guard, outer
SQLite transaction, operation registry and recovery fence. Append still preserves
duplicates and retains its original hashes, receipts and one-file profile.

## Operations

A KV schema contains one required UTF-8 string field named `key`, at any schema
position. Keys may be empty and are bounded to 1,024 bytes. Comparison is binary
UTF-8 order. Collection mode is fixed at creation; an existing append collection
cannot be changed in place. Raw append and bulk append reject KV collections.

- `UpsertDurable(AppendRequest)` replaces the complete row. The last input
  occurrence of a key wins, including across bulk chunks. Omitted nullable
  fields become NULL. An identical upsert is a durable no-op.
- `DeleteDurable(DeleteRequest)` accepts up to 10,000 keys. Repeated and missing
  keys are safe. Empty inputs and deletes containing only misses are no-ops.
- `Lookup(LookupRequest)` resolves latest once or selects a retained snapshot.
  Up to 4,096 keys return optional full rows in input order, including repeated
  hits and explicit misses. A present row with NULL values remains a hit.
  The result records the resolved selector even for empty input. Returned values
  own their storage and reservation across engine shutdown. Results are bounded
  to a conservative 16 MiB DTO envelope; split larger requests explicitly.
- `BulkUpsertDurable(BulkAppendRequest)` uses the existing replayable two-pass
  source contract. Every source chunk is independently bounded to 10,000 rows
  and 16 MiB. One operation has at most 25,600,000 input rows and 4 GiB decoded
  input, produces at most 256 files, and publishes once. This is an embedded C++
  API; it does not add a network upload queue.
- `CompactDurable(CompactionRequest)` selects a bounded contiguous key-range
  group and produces sorted output only when at least four small files fit into
  one target file. Otherwise it records a no-op. Independent full typed-row
  digests must agree before publication and on recovery. Compaction changes
  physical layout while preserving logical values.

All mutations use permanent operation IDs and snapshot preconditions. Matching
terminal retries return the original outcome before applying new admission
budgets or preconditions once request identity is established. External bulk
sources must replay canonical validation first. A refusal during source validation
has not established a match to any prior operation. Use `FindAppendOutcome` to
resolve that permanent ID, or replay the unchanged source successfully. Fixed-source
migration can prove its identity from durable ownership and replays terminal
results before source reads or new execution budgets. Reusing an ID across different kinds, input order,
duplicate policy or compaction options is a conflicting request.

## Explicit append migration

`MigrateAppendToKV` requires a distinct target ID/name, identical schema, a fixed
source snapshot (`Id` or `InitialEmpty`), and an explicit `Reject`, `FirstWins`,
or `LastWins` policy. The order is chronological append-file order, preserving
physical row order within each file. Snapshot IDs are opaque and never order
operations. Source identity, fixed selection and duplicate policy participate
in the population request identity.

Migration has **two durable steps**: create the target, then populate it with one
bounded bulk upsert. `MigrationOutcome` contains the creation outcome and an
optional population outcome. If a different publication or recovery barrier
prevents reading the population identity, `resolution_error` describes the blocked
lookup and no population outcome is fabricated; retry after resolving that
barrier. A matching pending population retains its original recovery context.
Population never starts after failed or uncertain creation. Failure after
creation can leave an observable empty target. Creation transactionally records
a framed migration identity covering the fixed
source, policy, operation and target definition. Creation reserves the population
operation ID; ordinary writes cannot bind it. The population hash is linked to
that ownership in the same transaction as its permanent operation binding.
Replay verifies this link before returning an original receipt. Only that
migration can resume the target, including after an uncertain creation or a
pre-binding population refusal. A pre-existing ordinary empty target is
rejected. Population always has a no-snapshot precondition. This API makes no
atomic create-and-populate promise; migration itself does not change source
collections or receipts/history and never reuses source paths in the target.
If a separate append retention operation later expires the selected source,
a terminal migration retry still replays its original receipt. An owned target
whose population never bound remains a committed creation; source expiry then
refuses population with `SNAPSHOT_EXPIRED`. A fresh migration refuses an expired
source before creating its target.

Append snapshot-retention maintenance is a separate opt-in contract. KV
collections reject retention policy reads, policy updates and expiration with
`COLLECTION_MODE_MISMATCH`; all KV snapshot intervals and no-op roots remain
retained. A KV expiration ledger or cleanup queue is corruption, never authority
to delete a KV file. Physical inventory includes retained historical manifests
and exact no-op bound roots.

## Receipt and recovery format

Catalog version 4 is installed transactionally on the first KV collection,
including the version-3 append retention tables. Existing version-1/2 append and
version-3 retention catalogs retain their format until then. The new mode
relation contains only KV rows; absent rows mean append. The immutable Iceberg
property `glamdb.collection-mode=kv1` must match SQLite. Every KV snapshot has
version/kind provenance, so unmarked append operations cannot bypass uniqueness.

Version-3 receipts extend `AppendResult` with `mutation_kind`, `snapshot_created`,
input/affected rows, physical written/removed rows and removed files/bytes.
`rows_appended` is zero for KV. Physical counters include surviving rows copied
from affected files; they are distinct from logical affected rows. Legacy
version-1/2 serialized receipts remain byte-identical.

No-ops create no snapshot or metadata root. Their effective snapshot is the bound
base (zero before the first snapshot), parent is that base, and creation time is
zero. A real SQLite transaction still completes the bound operation permanently.
A lost COMMIT reply is `CommitUnknown` even without a candidate snapshot/root.
Only reconciliation, authoritative outcome lookup and a durability fence resolve
that uncertainty. No-op receipts are excluded from snapshot history, while their
operation outcomes remain queryable after later mutations and restart.

## Layout decision and invariants

The [4/16 GiB component experiment](KV_LAYOUT_MEASUREMENTS.md) selected globally
sorted, disjoint file ranges before the main implementation. No per-key index
was added. Bounds route keys to affected files; each changed route is merged
and split independently so it cannot span an untouched range. Unchanged files
retain their exact descriptors. New file paths are fresh, and removed paths
cannot be resurrected. Closure checks full descriptors, inherited sequences,
key ordering/uniqueness, provenance, counters and durable receipts.

The verified membership index stores each retained file once with a half-open
sequence interval: `introduced <= selected_sequence < removed`. Opaque snapshot
IDs map to sequences. Append continues using its strict prefix representation.
Both pin exact generations with existing scan/store/allocator leases.

Compaction and deletion remove files only from the latest view. Historical
snapshots keep their original files, and aggregate retained physical capacity
never decreases. No file reclamation, snapshot expiration or receipt expiration
is implicit in these operations.

## Bounded execution and measured scope

A temporary SQLite spool orders/deduplicates input using binary keys. It is
bounded scratch, not another catalog or recovery payload journal. Its VFS checks
requested I/O before access, limits the main file to 4 GiB, caps scratch I/O at
16 GiB and disables journals/mmap. Exact owned scratch names are reclaimed on
open only under the validated exclusive store lease.

COW uses an 8 GiB attempt I/O/output ceiling, at most 256 output files, 100,000
rows and 16 MiB input per output file, and a 32 MiB physical file ceiling. The
coordinator reserves bounded memory and device envelopes. Deadline/cancellation
checks occur between chunks, reads, writes and final publication. Scratch I/O is
separate from the COW ceiling. Snapshot-producing closure rereads consume the COW attempt budget. A no-op
seals its data attempt before the final read-only metadata-root proof; that
additional read is separately bounded by the root-size limit.
Lookup has a shared 8 GiB read ceiling and a 60-second cooperative deadline;
migration shares a 16 GiB read ceiling across source replays plus the target's
normal mutation bounds. Failed and repeated requested reads consume budgets.
These are admission/work bounds, not hard preemption or total process-RSS claims.

Localized writes are the intended workload. Randomly scattered changes may need
to rewrite most live files and can be refused. An auxiliary index would reduce
candidate searches, not rewrite bytes. Component timings do not qualify native
publication, engine reopen or cold block-device latency. Native acceptance is
recorded separately; no result establishes physical power-loss durability, a
1 TiB bulk profile, retention/GC, remote storage or general random-write latency.

See [validation and independent review](KV_VALIDATION.md) for executed checks,
corrected findings and remaining limits.
