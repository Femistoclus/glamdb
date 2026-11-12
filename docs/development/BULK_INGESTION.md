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
rewind a file or regenerate deterministic values. Native HTTP stages uploads in
anonymous transport scratch for replay; the SDK and CLI expose this route as
specified below. UI import and the organization gateway upload adapter remain
outside this extension. Existing lookup routes also read embedded bulk receipts.

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

## Native HTTP, SDK and CLI

`POST /databases/{database_id}/collections/{collection_id}/bulk-append` (or the
legacy unscoped collection route) uses `Content-Type: application/x-ndjson` and
an exact `Content-Length`. HTTP chunked transfer and compressed bodies remain
unsupported. Every record ends with LF, including the last record; empty records
and trailing malformed data are errors. The first record is a JSON object:

```json
{"operation_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","rows":"12000","expected_snapshot":null}
```

`rows` declares the total operation row count; omit `expected_snapshot` to accept
any confirmed base. Subsequent records are `{"rows":[...]}` objects using the
ordinary precise JSON scalar forms. Each batch has 1..10,000 rows, at most
16 MiB canonical bytes and at most 16 MiB wire bytes **including its LF**. The
header is at most 64 KiB including LF; the complete body is at most 4 GiB wire
bytes. The separate embedded row/canonical/file limits still apply. The
executable [route fixture](../../fixtures/http/v0.json) has `request_records` and
`record_schemas` for this framing. Capabilities expose `features.bulk_append`,
`bulk_request_bytes`, `bulk_batch_bytes`, `bulk_uploads` and
`bulk_uploads_per_database`.

The authenticated listener admits at most two uploads globally and one per
database, including completed spools waiting for/executing a publisher and worker cleanup. Refusals
return `SERVER_BUSY` with `execution_started:false` and `Retry-After:1`. Uploads
use ordinary body capacity, preserving the four reserved control readers. A
separate two-worker pool creates, writes, flushes and closes scratch in 64 KiB blocks;
network staging and file reclamation never wait for disk I/O on the listener.
Failed worker enqueue restores ownership; failed completion notification remains
observable through a separate atomic completion watchdog/reaper. Scratch is mode 0600,
unlinked before data is accepted, and reserved through the shared owner on the
actual temporary filesystem (`TMPDIR` or the platform default). The full declared
wire length and one inode are reserved before scratch creation. The Linux profile
rejects tmpfs/ramfs scratch, which would charge upload size to memory rather than
a disk-backed spool; configure a disk-backed `TMPDIR`. It is nondurable
transport scratch, outside the warehouse, with no restart replay or persistent
payload journal. A disconnect, upload deadline or shutdown before the body is
complete never enters the engine or binds an operation. Completed bodies use the
existing fair publisher queue; its configured queue timeout still applies. Once
execution starts, accepted writes drain and only the core decides their outcome.

Upload time is at most ten minutes after authenticated headers, followed by the
configured publisher queue budget and the existing ten-minute bulk execution
budget. Client deadlines cover their own complete call and must allow response
latency; a client timeout never proves rollback. Ordinary requests still use the
configured short body timeout and 16 MiB bound. During staging, each upload owns
128 MiB shared data admission for replay/parsing plus 256 KiB body admission;
execution additionally reserves the existing 128 MiB transport working envelope,
128 MiB core bulk input and 256 MiB writer envelopes, plus separately tracked
Arrow/metadata/device allocations. These conservative reservations and observed
staging RSS are different units; this is not a universal process RSS guarantee.
At maximum sizes, concurrent scratch and core device envelopes can exceed the
default 16 GiB shared reservation ceiling and cause explicit backpressure.

The Python SDK validates, normalizes, hashes and spills batches before POST:

```python
with client.prepare_bulk_append(
    collection_id, batches, rows=12000, operation_id=operation_id, schema=schema
) as request:
    outcome = client.bulk_append(request, deadline=700)
    # If delivery is uncertain, retain request and explicitly retry it or look
    # up client.find_append_outcome(collection_id, operation_id).
```

`batches` is an iterable of bounded iterables of rows. Preparation is local when
`schema` is supplied/cached, accepts a one-shot generator and never submits a
write. A prepared request retains its anonymous file until `close()`/context exit;
its 64 KiB upload blocks replay the same bytes and canonical hash. Concurrent use
of one prepared request is rejected. Direct `client.bulk_append(collection_id,
batches, rows=..., operation_id=...)` prepares and closes a temporary request;
retain a separately prepared request when exact retries are needed. Set the
client's HTTP read timeout high enough for bulk execution (for example
`Client(..., timeout=650)`); the per-call deadline also covers preparation and
upload. Preparation uses bounded batch memory and temporary disk up to the wire
limit. Preserve UUIDs and source content outside the process when cross-process
retry is required; prepared scratch is not a durable client journal.

The standalone CLI streams an existing regular NDJSON file:

```sh
glamdb-cli bulk-append --url https://db.example.test --database-id DATABASE_UUID \
  --collection-id COLLECTION_UUID --body upload.ndjson --timeout 700
```

The header owns the operation ID and row declaration. Preserve this exact file
and UUID for an explicit retry; do not edit it during upload. Stdin is unsupported
because retry requires replayable input. The client retains at most 64 KiB of
file data, verifies HTTPS with the existing CA options, and never retries or
reconciles automatically. Outcome/exit-code handling matches ordinary named
append. `glamdb-admin bulk-append` supports HTTP `--url` only. The generic buffered
`request` command retains its ordinary 16 MiB limit. Reverse proxies must permit
the chosen body size/timeouts and forward this native route without buffering
into memory; the current organization gateway does not expose bulk upload.

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
