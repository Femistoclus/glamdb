# Explicit KV collections and bounded copy-on-write

Issue: https://github.com/glamourdb/glamdb/issues/101

## Intent and constraints

Add a separate immutable `KeyValue` collection mode. Existing append collections,
canonical hashes, receipt versions 1/2, native one-file guards and historical
views retain their contracts. One serialized coordinator and one outer SQLite
pointer/receipt transaction remain the write authority. COW never deletes retained
files or expires receipts. All new publications must reopen under the same bounds.
The caller authorized choosing the layout from measurements and continuing all
stages autonomously on 2026-11-12.

## Approaches and decision gate

Compare overlapping independently sorted runs with globally disjoint sorted key
ranges on identical 4/16 GiB decoded Parquet fixtures. Sorting individual batches
cannot provide disjoint ranges. An auxiliary index can reduce candidate searches,
but cannot reduce files rewritten by scattered updates. Prefer disjoint range
routing if the 16 GiB locality workload meets the 8 GiB I/O / 60 second target;
otherwise evaluate a snapshot-aware index before selecting a layout. Record the
measured decision and component-versus-engine limits in KV_MODE.md before main
implementation. Random scatter has a bounded refusal contract, not a throughput
promise. The component experiment does not qualify engine durability.

## Public contracts

`CollectionMode { Append=1, KeyValue=2 }` defaults to Append and is immutable.
KV schemas require the existing required UTF-8 string `key` field (at any schema
position). Full-row upsert uses the last input occurrence of a key; omitted
nullable columns become NULL. Delete preserves input identity but safely ignores
duplicate and absent keys. Lookup resolves one snapshot and returns results in
request order, including repeated keys and explicit misses. Initial-empty remains
empty. Migration creates a new KV collection and requires an explicit Reject,
FirstWins or LastWins duplicate policy over a fixed source snapshot.

Expose UpsertDurable(AppendRequest), DeleteDurable(DeleteRequest),
BulkUpsertDurable(BulkAppendRequest), CompactDurable(CompactionRequest),
Lookup(LookupRequest), and MigrateAppendToKV(MigrationRequest). Bulk reuses the
existing replayable two-pass producer and bounded validation/hash pass. Ordinary
AppendDurable and BulkAppendDurable reject KV mode. Native HTTP and Python client
operations adapt this same facade, with capability-gated UI mode/write controls.

## Publication and receipts

KV operations domain-separate their canonical input hashes by kind and duplicate
policy, retaining input order and duplicates. Matching retries return the exact
stored terminal outcome before checking new budgets or preconditions. Catalog
version 3 is installed transactionally only when KV is first created; versions
1/2 and their bytes remain unchanged for append-only stores. KV mode is also
present in immutable table properties (`glamdb.collection-mode=kv1`).

A version-3 result extends the existing outcome envelope with mutation kind,
input rows, affected rows, written/removed rows/files/bytes and snapshot_created.
No-op success has no pointer change or new snapshot, but durably completes its
bound operation in an exact-base checked transaction. Recovery validates no-op
receipts against their retained prepared base separately from snapshot/receipt
bijection. Uncertain binding, no-op COMMIT, failure recording and publication
remain CommitUnknown until authoritative recovery and a durability fence.

Nonempty changes use Iceberg NewOverwrite (append/delete/overwrite as selected by
the dependency); value-preserving compaction uses NewRewriteFiles (replace).
Explicit KV markers select separate metadata/manifest validation. Strict append
validation is never relaxed. Validate added, existing and deleted entries, inherited
sequences, counters, live paths, no resurrection, exact retained descriptors,
durable receipts for new files and aggregate retained capacity before publication.

## Membership, layout and resources

A KV file is recorded once with an introduction and optional removal sequence;
each opaque snapshot ID maps to its sequence. Active membership is the interval
`introduced <= snapshot < removed`, preserving all history in O(files+snapshots)
space. Append retains its prefix index. Retained generations carry their shared
metadata charge and scan buffers retain the normal owned pool/FileIO/store lease.

Route mutations to the next live file whose upper key bound covers the key (the
last file covers the tail); delete misses outside actual ranges need no rewrite.
Read selected files in bounded batches, merge full rows, and emit sorted, unique
files per route without spanning an untouched range. Preserve untouched descriptors.
A bounded temporary SQLite spool provides key deduplication and external ordering;
it is scratch, never an alternate catalog, durable outcome journal or data authority.
Bound and charge its device space and memory; reclaim interrupted scratch only
under validated exclusive ownership, like the existing recovery workspace.

Keep source chunks <=10,000 rows /16 MiB, output file rows <=100,000 and physical
files <=32 MiB. Use finite input/spool/file/I/O/memory/deadline budgets and check
cancellation between reads, chunks, sort/merge steps and publication. Compaction
selects a bounded contiguous group, preserves row values and improves locality;
a group without a useful size reduction is a durable no-op. Full retained physical
usage can grow after compaction. The 1 TiB extended-load and physical power-loss
qualification remain distinct campaigns and must never be inferred from smoke tests.

## Verification and independent critique

Pin failing tests for immutable mode, append rejection, full-row replacement,
last-input-wins, key outside first column, delete duplicate/missing/all, no-op
receipt/retry/reopen, fixed-view lookup with misses/duplicates, held historical
cursors, stale exact bases, source changes/cancellation, compaction, migration
policies, corrupt metadata and uncertain SQLite/file failure boundaries. Compare
old receipt bytes literally. Run native build/CTest, relevant SDK/UI tests,
format/tidy and sanitizer lifetime/fault checks. An independent reviewer must
challenge invariants, bugs, layout alternatives and measurement claims; fix
material findings and record limitations honestly before the final PR.
