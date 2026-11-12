# Snapshot archive tier

An append collection can keep only its newest snapshots in the Iceberg root and
move older ones to an archive in the SQLite catalog, as DuckLake stores
snapshots as catalog rows. Archived snapshots stay readable; only the explicit
retention policy of [snapshot retention](SNAPSHOT_RETENTION.md) makes a
snapshot unreadable. New append collections use the tier by default
(`DefaultHotSnapshots` = 100); a collection created with
`KeepAllSnapshotsInRoot` keeps every retained snapshot in the root, and a store
whose collections all opt out creates no archive table. Existing collections
keep their setting on open and upgrade.

| State | Where the snapshot entry lives | Readable | Manifest list and manifests |
| --- | --- | --- | --- |
| Hot | Iceberg root (the newest `hot_snapshots`) | yes | on disk |
| Archived | `archived_snapshots` catalog row | yes | on disk, never deleted while archived |
| Expired | `expired_snapshots` ledger (explicit retention only) | no, `SNAPSHOT_EXPIRED` | reclaimed by the cleanup queue |

```cpp
CreateCollectionRequest request{collection_id, "events", schema};
// absent: DefaultHotSnapshots (100) for append collections
// request.hot_snapshots = 50;                      // explicit
// request.hot_snapshots = KeepAllSnapshotsInRoot;  // explicit opt-out (0)
engine->CreateCollection(request);
engine->UpdateSnapshotArchive({collection_id, 50});   // later appends apply it
auto status = engine->GetSnapshotArchive(collection_id);
auto moved = engine->ArchiveSnapshots(collection_id); // bounded maintenance
```

The creation request is tri-state: absent means the default (100 for append
collections when the backend supports the archive, none for KV and for
backends without it), `KeepAllSnapshotsInRoot` (0) means no archive, and
1–2^40 is an explicit value. The effective value is stored per collection in
the catalog like an explicit one; `CollectionInfo::hot_snapshots` reports it
(absent: no archive) and `CollectionInfo::hot_snapshots_defaulted` whether it
came from the default and was never set explicitly (catalog table
`collection_hot_defaults`, version 6). A repeated creation compares effective
values (`SNAPSHOT_ARCHIVE_MISMATCH` otherwise): omitting the field matches a
collection that received the default or was created with an explicit 100; an
explicit opt-out matches neither. A creation retry that omits the field no
longer matches a collection created before the default existed.
`UpdateSnapshotArchive` replaces the value with an explicit one under the
writer, is applied once when repeated and moves nothing by itself; an absent
value or `KeepAllSnapshotsInRoot` stops archiving later snapshots. Raising it
does not move archived snapshots back. Retention must keep at least the hot
snapshots: an explicitly set `hot_snapshots` greater than `keep_last` is
`INVALID_ARGUMENT`, whichever of the two is changed; a defaulted value above a
new `keep_last` is lowered to `keep_last` in the expiration's transaction (or
its own when the root does not change) and stays marked as defaulted. KV
collections need their complete root history; the default never applies to
them, an explicit value and archive maintenance return
`COLLECTION_MODE_MISMATCH`, like retention.

## Appends and maintenance

An append whose root would hold more than `hot_snapshots + min(hot_snapshots,
16)` snapshots moves the oldest root snapshots to the archive, leaving exactly
`hot_snapshots`, so one cut serves up to 17 appends. One append moves at most 64
snapshots. The cut and its archive rows are published by the append's own outer
SQLite transaction; ordinary and bulk appends behave the same. An uncertain
COMMIT keeps the existing `RECOVERY_REQUIRED`/`Reconcile` contract.
`ArchiveSnapshots` moves at most 1024 root snapshots above `hot_snapshots` per
call through the same guarded maintenance publication as retention; it archives
a long history after the setting is first applied. `GetSnapshotArchive` reports
root, archived and expired counts and the root backlog without I/O.

The publication guard verifies that the new root is the base root without the
moved prefix (plus the new snapshot for an append) and describes the cut from
the base: it reads each moved snapshot's manifest list and the list of the
oldest snapshot that stays, hashes the moved lists, requires each list's file
counts to equal the snapshot's verified membership boundary, and computes the
manifests each moved snapshot references last. Manifests are referenced by a
contiguous snapshot range in the verified append profile, so these lists are
enough. The verified candidate then drops the moved boundaries from its
membership and erases the retired manifests from its retained path set without
copying the shared file index (see [the scan index](SCAN_INDEX.md)). The root keeps valid Iceberg v2: it lists the hot snapshots, and the
oldest one's parent is archived.

## Catalog format

Catalog version 5 adds two tables after the version 4 KV tables, and version 6
adds the verified checkpoints (see [verified checkpoints](#verified-checkpoints)).
Both are created by the first transaction that stores a setting (creation or
update), together with any missing version 2–4 tables; a version 5 catalog
written before checkpoints existed becomes version 6 at its first publication.
Older binaries reject versions 5 and 6. Stores that never store a setting keep
their version and stay byte-compatible.

```sql
CREATE TABLE collection_snapshot_archive(collection_id TEXT PRIMARY KEY REFERENCES collections(collection_id),
  created_hot_snapshots INTEGER CHECK(created_hot_snapshots IS NULL OR created_hot_snapshots BETWEEN 1 AND 1099511627776),
  hot_snapshots INTEGER CHECK(hot_snapshots IS NULL OR hot_snapshots BETWEEN 1 AND 1099511627776))
CREATE TABLE archived_snapshots(collection_id TEXT NOT NULL REFERENCES collections(collection_id),
  snapshot_id INTEGER NOT NULL, sequence_number INTEGER NOT NULL CHECK(sequence_number>=1),
  parent_snapshot_id INTEGER, timestamp_ms INTEGER NOT NULL, operation_id TEXT NOT NULL,
  manifest_list TEXT NOT NULL CHECK(length(manifest_list)<=2048),
  manifest_list_bytes INTEGER NOT NULL CHECK(manifest_list_bytes>=1),
  manifest_list_sha256 TEXT NOT NULL CHECK(length(manifest_list_sha256)=64),
  data_files INTEGER NOT NULL CHECK(data_files>=1), retired_manifests INTEGER NOT NULL CHECK(retired_manifests>=0),
  retired_manifest_bytes INTEGER NOT NULL CHECK(retired_manifest_bytes>=0),
  snapshot_blob BLOB NOT NULL CHECK(length(snapshot_blob)<=65536), PRIMARY KEY(collection_id,snapshot_id),
  UNIQUE(collection_id,sequence_number), UNIQUE(collection_id,operation_id), UNIQUE(collection_id,manifest_list),
  FOREIGN KEY(collection_id,operation_id) REFERENCES operations(collection_id,operation_id))
```

`snapshot_blob` is a versioned, canonical binary encoding of every field of the
Iceberg snapshot entry (ID, parent, sequence, timestamp, list path, the complete
summary in key order, schema ID, first row ID, added rows). The pinned Iceberg
installation has no public per-snapshot JSON codec; decoding the blob and
writing it with Iceberg's serializer reproduces the root entry. Rows are checked
against their blob whenever they are read. `data_files` is the snapshot's live
data-file count, which is the length of its prefix of the chronological
append-only membership. `retired_manifests`/`retired_manifest_bytes` describe
the manifests whose last reference is this snapshot.

## Reads

Hot reads are unchanged. An ID missing from the pinned root's verified index is
looked up by primary key in `archived_snapshots`; no archive state is kept in
memory. An archived snapshot older than the pinned root is read as follows:

- scans and explain use the prefix of `data_files` files of the verified index,
  exactly the files and order the snapshot had while hot, without reading its
  list; an already planned cursor keeps its immutable plan;
- statistics, file listings and file details read its manifest list, require
  the recorded size and SHA-256 and then decode it as for a hot snapshot;
- `ListSnapshots` and its pages list archived snapshots before the root's, from
  an ordered catalog query bounded by the pinned root;
- an append `Equals` precondition on an archived ID is `PRECONDITION_FAILED`
  like any non-current snapshot.

An ID that was expired after the view was pinned reports `SNAPSHOT_EXPIRED`;
unknown IDs keep `NOT_FOUND`. An altered or missing archived list is
`CORRUPTION` for the reads that need it; scans never depend on it.

## Retention

Retention counts and ages hot and archived snapshots together; archived
snapshots are older and expire first, at most 4096 per call. Expiring an
archived snapshot deletes its row, adds it to the expiration ledger and queues
its list and last-reference manifests for the existing cleanup, all in the
maintenance transaction. Root snapshots expire only after the whole archive.
Last references use the expired lists and the oldest remaining archived list,
whose identity must still match its row. Cleanup refuses a queued path that is
still an archived list.

## Capacity

`history.max_snapshots` and the snapshot-log limit bound the root snapshots.
Archived lists and the manifests only archived snapshots reference stay on disk
and keep counting against the manifest, manifest-byte and list-byte budgets;
reopen adds the row totals to the verified root usage, so publication and
reopen agree and a lower budget is rejected at reopen.

## Recovery and open cost

Open reads the expiration ledger and streams `archived_snapshots` in keyset
pages of 256. Every archived row must decode to its own columns, be bound to
exactly one committed receipt of the collection (identity, parent, time, content
hash and totals), continue the expiration ledger without a gap, form one parent
chain ending at the oldest root snapshot's parent, and carry the cumulative
data-file count of its receipts. The oldest root snapshot is then verified as a
complete checkpoint against the expired and archived ancestry, exactly as after
expiration, and receipts, expired and archived snapshots and root snapshots
form a bijection. A KV collection with archive rows is corruption.

In this stage open therefore costs: one catalog pass over the collection's
operations (existing), one pass over its archive rows (decode and compare per
row, no file I/O), the oldest root snapshot's list, its manifests and every data
file footer (existing checkpoint path), and each later root transition. It reads
no archived list; transient memory holds one parsed entry per expired or
archived snapshot. The archive replaces reading one list per archived snapshot
with one catalog row each. A verified checkpoint (E3) can remove the remaining
per-history work.

Archived list integrity is checked when a read or expiration needs the list.
`manifest_list_bytes` and the retired manifest totals are trusted catalog state
written by a verified publication, like receipt totals.

This full path remains the one used by `Reconcile`, by recovery during
`Shutdown`, by `Engine::VerifyStoreFull`, by `OpenStoreRequest::verify_full`
and whenever a version 6 store's checkpoints are missing or do not match.

## Verified checkpoints

Catalog version 6 stores one checkpoint per append collection and a partial
index of interrupted operations:

```sql
CREATE TABLE collection_checkpoints(collection_id TEXT PRIMARY KEY REFERENCES collections(collection_id),
  snapshot_id INTEGER, sequence_number INTEGER NOT NULL CHECK(sequence_number>=0),
  metadata_location TEXT NOT NULL CHECK(length(metadata_location)<=2048),
  root_bytes INTEGER NOT NULL CHECK(root_bytes>=1), root_sha256 TEXT NOT NULL CHECK(length(root_sha256)=64),
  usage_root_bytes, usage_snapshots, usage_snapshot_log, usage_data_files, usage_manifests,
  usage_manifest_bytes, usage_manifest_list_bytes, usage_footer_bytes INTEGER NOT NULL CHECK(...>=0),
  operations, archived, expired INTEGER NOT NULL CHECK(...>=0),
  receipt_chain TEXT NOT NULL CHECK(length(receipt_chain)=64))
CREATE INDEX operations_bound ON operations(collection_id,operation_id) WHERE state=0
```

Every publication of an append collection (creation, ordinary and bulk append,
archive maintenance, expiration) writes its checkpoint in the same outer
transaction as the pointer and receipt: the verified candidate's root location,
size and SHA-256, current snapshot and sequence, and cumulative storage usage.
The catalog maintains the operation count (every binding increments it), the
archive and expiration row counts, and a SHA-256 chain over the publications
since the row was established (kind, root, digest and the encoded receipt). KV
collections have no checkpoint.

A fresh `OpenExisting` of a version 6 store opens from checkpoints when every
collection has one and all of them match: the catalog validates its header and
registry (O(collections), no `integrity_check` and no walk over operation
records), requires each checkpoint to equal the catalog pointer and current
snapshot, the archive and ledger row counts, and the operation counts to sum to
the exact operation count. Each collection is then rebuilt from its root (size
and digest must match), the manifest lists of its root snapshots and its
current manifests: the scan index groups the live files by origin sequence in
list order, which every append and merge preserves, and is identical to the one
full verification builds; usage comes from the checkpoint and must agree with
the root and the live files and fit the configured budgets. Open reads no
data-file footer, no archived or expired list, no superseded manifest and no
archive row except the newest; interrupted operations are found through the
partial index. Any mismatch or error discards the attempt and runs the full
path, which then re-establishes every checkpoint in its durability fence.

What the checkpoint path trusts instead of re-proving: the history before the
current root (each transition was verified by the publication guard before
its COMMIT), the cumulative byte totals, archive rows until they are used, and
the SQLite file itself. Archive rows are therefore checked on use: an archived
read binds the row to its committed receipt (snapshot, parent, time) and
requires its `data_files` prefix to end exactly at the files whose origin
sequence is the row's; a tampered row is `CORRUPTION`, never other data.
Scrubbing remains explicit: `VerifyStoreFull`, `Reconcile` or `verify_full`.
Detected `CORRUPTION` (during recovery or by any read) deletes every
checkpoint, so the next open verifies fully and finds it again instead of
reopening ready; a successful full verification re-establishes them.

The physical inventory (`GetStorageFootprint`) reads each archived list with
its recorded size and SHA-256 and counts it and its manifests as referenced
metadata, so archiving does not turn retained metadata into "unreferenced"
warehouse bytes.

## Validation

`glamdb_vertical --gtest_filter='*SnapshotArchive*:*ArchiveDifferential*'` runs
the focused tests: exact answers of archived snapshots versus their hot
answers, a randomized two-store differential with ordinary, bulk and failed
appends, two collections, a setting change, retention and retries, catalog
tampering, altered or missing lists, process exits around the archiving COMMIT
with fresh-process verification, lost COMMIT replies, lower-limit reopen,
compatibility without the setting and KV rejection.
`SnapshotArchiveCost.*` measures root bytes and catalog work per append at two
history lengths; `GLAMDB_ARCHIVE_COST=1008,5004` selects the campaign lengths.

Measured on 2026-11-12 (Release, Linux arm64 container, tmpfs store, one run,
`hot_snapshots=8`): after 1,008 and 5,004 three-row appends the root held 9
snapshots in 9,389 and 9,398 bytes, one 9-append archive cycle executed the same
743 catalog statements (45 rows changed, 500 returned, 9,990 SQLite VM steps),
and appends averaged 3.04 and 2.85 ms. The same history without the setting
grew the root to 929,886 and 4,565,674 bytes with 7.40 and 43.38 ms appends
(after lifting `max_snapshots`, which otherwise refuses appends at 4,096).
Reopen took 865 ms with 5,004 archived snapshots versus 5,920 ms with all of
them in the root. These runs do not qualify durability or power loss.
