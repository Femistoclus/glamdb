# Bounded streaming bulk append

`Engine::BulkAppendDurable` is an embedded append-only extension for issue #100.
One nonempty caller operation creates bounded immutable Parquet files, one
Iceberg snapshot and one permanent receipt. It preserves every row and duplicate.
Scans remain unordered across files; input order determines the canonical hash.
Independent operations are never combined. Ordinary `AppendDurable` keeps its
16 MiB / 10,000 row / one-file contract.

## Source and identity

Implement `BulkAppendSource::Rewind()` and `Next()`, then supply the source,
declared total `rows`, collection/operation UUIDs and optional snapshot precondition
in `BulkAppendRequest`. Null batch means EOF. Empty batches, incorrect totals,
invalid Arrow/schema/scalar data and oversized input are errors. The source may
rewind a file or regenerate deterministic values; a non-replayable upload needs
caller-owned staging. There is no bulk HTTP upload, Python upload method or UI
import endpoint in this extension. Their existing lookup routes can read bulk
receipts written by an embedded caller.

The synchronous call reads twice. Pass one validates and fingerprints the entire
source before operation binding or file production. Pass two rewinds, normalizes
one chunk at a time, writes files, and verifies the complete canonical fingerprint
and total before staging a snapshot. Chunk boundaries can differ between passes.
Collection Parquet write settings are read under the serialized writer and
applied to every output file; later settings changes preserve committed files
and the original receipt on retry.

Each returned batch must stay immutable until the next source callback, and the
source must remain alive through the call. Callbacks run on the calling thread,
must return promptly and must not reenter the engine writer. The embedding host
owns and budgets source buffers; the engine checks their retained parent capacity
and makes bounded owned copies. Source callbacks cannot be forcibly interrupted.

The canonical encoding is unchanged version 1: collection UUID, fixed-schema
fingerprint, uint64 row count and ordered normalized cells. Bulk extends the
allowed total row count through its own entry point. NULL omission and negative
zero normalize exactly as ordinary append. Matching UUID/hash retries return the
original terminal receipt before new execution budgets or preconditions, including
ordinary/bulk retries of the same small input. A retry still rereads the source
once to establish its hash; another hash conflicts.

## Fixed development profile

| Unit | Bound |
| --- | --- |
| Source batch | 1..10,000 rows, 16 MiB canonical bytes and 16 MiB retained Arrow root-buffer capacity |
| Scalar / row | Existing 1 MiB string, 1,024-byte key and 2 MiB canonical row limits |
| File input | Target 8 MiB, hard 16 MiB Arrow buffers and 100,000 rows |
| Parquet | 8 MiB decoded row-group bound during ingestion; 32 MiB physical file |
| Whole operation | 256 files, 25,600,000 rows, 4 GiB canonical bytes |
| Owned input | 128 MiB reservation for a new bulk operation, plus the existing 256 MiB writer envelope and separately tracked Arrow capacities |
| Metadata | Existing working/root envelopes plus a 32 MiB bulk descriptor envelope retained through staged publication; verified index vectors, file metric maps and variable buffers receive an additional generation-pinned charge |
| Attempt I/O | 8 GiB + 32 MiB new files; four times that amount in requested reads/writes |
| Device admission | Reserve that full new-file envelope and 320 inodes before binding |
| Time | Default and maximum 10 minutes per bulk call; the existing writer wait still applies |

Small batches are coalesced under a bounded descriptor count rather than forcing
small files. Physical buffers, canonical bytes and RSS are different units.
The reservations are conservative admission envelopes; they are not measured RSS
guarantees. The default process device-reservation ceiling is 16 GiB to permit a
bulk envelope alongside store/catalog/recovery reservations. Filesystem headroom,
warehouse capacity and shared-owner availability can reject the operation even
when its input fits. The 4,096 operation-binding and snapshot limits remain;
failed bound operations consume permanent identities. No receipt/history expiry
or 1 TiB bulk qualification is implied.

## Publication, failures and recovery

Bulk keeps the exact prepared base and the existing single writer. Verified data
receipts, manifests, manifest list and metadata root precede one outer SQLite
pointer/receipt transaction. Until that transaction commits, scans see the last
confirmed view. Success is reported only after its durable COMMIT.

Before binding, interruption leaves no new identity. After binding, source errors,
hash changes, cancellation, capacity refusal and proven publication failure use
the permanent failure path. Unpublished files remain capacity-accounted orphans;
no payload spill or new cleanup authority is introduced. Uncertain binding,
failure-recording or publication remains `CommitUnknown` and retains recovery
capacity. Recovery validates all files or none, resolves the operation and fences
SQLite before reopening write admission. No source data is replayed during open.

An optional shared atomic `cancellation` flag is cooperative. Shutdown also stops
production at chunk boundaries. Cancellation cannot revoke a completed COMMIT;
look up the original operation after an uncertain response.

Bulk snapshots carry `glamdb.bulk-version=1`; committed bulk receipts use result
`receipt_version=2`, while catalog outcome-blob encoding remains version 1
(independent of the SQLite catalog schema version). Ordinary
receipt bytes and hashes are preserved. New readers accept both result versions;
older binaries reject the extended bulk state and must not be used to open it.
Recovery and publication both validate actual file rows/bytes/counts against
snapshot summary and receipt, enforce per-file/operation limits and preserve
retained manifests. Unmarked ordinary transitions still require exactly one
data file. Canonical input and decoded row-group byte limits are ingestion checks;
footer-only recovery independently verifies persisted file/row/metadata bounds.

## Reproduction and evidence

Build and run the focused C++ suites and process acceptance:

```sh
cmake --build --preset release --parallel 4
ctest --test-dir build/release --output-on-failure -R 'Bulk|bulk_ingestion'
```

The process harness in `tests/integration/bulk_ingestion_test.py` compares exact
rows/duplicates and initial/historical views, retained receipts, fresh-process
reopen and interrupted production. Its manual large profile compares ordinary
and bulk ingestion with a source larger than the process address-space ceiling.
It measures RSS, files/groups, metadata, external syncs, throughput, final response
latency and cold-engine reopen. See [executed evidence and independent review](BULK_INGESTION_VALIDATION.md).
Process crashes and injected I/O/SQLite errors do not qualify physical power loss.
