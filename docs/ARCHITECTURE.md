# Architecture

This document describes the intended engine and its correctness boundaries.
The current checkout implements the catalog, coordinator, production file
backend and fixed-view scanner. A native HTTP adapter exposes their public API, including fixed-view metadata,
verified file details and explain. See the
[coordinator guide](development/COORDINATOR.md) for tested behavior and the
[roadmap](ROADMAP.md) for acceptance gates. The SDK and workbench can use the [native HTTP server](development/HTTP_SERVER.md)
or the separate [disposable mock](development/HTTP_MOCK_CONTRACT.md). Neither
client integration nor mock tests qualify physical storage durability. See the [build guide](development/BUILDING.md) and
[workbench guide](development/WORKBENCH.md) for runnable commands.

## Model and scope

GlamDB is an embedded C++23 engine. Arrow represents typed batches in memory,
Parquet holds immutable data, Iceberg v2 defines tables and snapshots, and SQLite
publishes the authoritative metadata location. The core coordinates writes and
recovery and exposes snapshot scans. HTTP, SDK, and UI layers use this
same authority rather than maintaining their own storage writers.

| Term | Meaning |
| --- | --- |
| Store | A persistent root with an immutable identity, catalog, warehouse, and one process owner. |
| Collection | A named, fixed-schema, unpartitioned Iceberg table with an immutable ID and mode. |
| Snapshot | An immutable view of the files belonging to a table; its ID is opaque, not a timestamp or sequence. |
| Initial empty view | A schema-bearing view before the first snapshot; it is distinct from latest and from an empty snapshot. |
| Operation | A collection-scoped identity bound to a canonical input hash and a durable terminal outcome. |
| Orphan | A file not referenced by a published snapshot; it consumes space but is not visible to scans. |

The initial mode is append: every input row is preserved, including duplicate
keys. A nonempty successful append publishes one snapshot; independent requests
are not coalesced, and empty input is rejected before writing. One process owns
the store, writes are serialized, and independent cursors can read concurrently.

Upsert, delete and compaction belong to the later explicit KV mode.
The [bounded bulk append extension](development/BULK_INGESTION.md) adds replayable
streaming ingestion to append collections before KV, with one atomic publication. Initial append has no payload journal, asynchronous queue, automatic file
deletion, snapshot expiration, or rollback to an older snapshot. Other later
features and their prerequisites are listed in the roadmap.

## Store identity and file ownership

Creation is explicit. The caller retains the store UUID before creation so a
lost response can be resolved by opening that same identity. Opening requires
the existing identity, readiness marker, and catalog; it does not create missing
directories, database files, tables, or a replacement empty store. Interrupted
initialization and incompatible formats are reported as errors.

The intended persistent layout includes `store.identity`, `store.ready`,
`catalog.sqlite`, and `warehouse/`. Metadata uses logical
`store://<store_id>/warehouse/...` locations resolved against the configured
physical root. This permits a verified restore at another root without reading
the original volume. Validate the identity and path components, keep access
inside the owned root, and reject path traversal and symlink escapes.

New data and metadata use unique names and exclusive creation. A published file
is immutable; overwrite is disabled. The only file the engine removes is a
metadata root superseded by a confirmed append: the current root carries the
whole retained history, so recovery and scans never read an older root, and a
scan that already resolved the old root keeps it until its planning finishes.
Other automatic cleanup, including orphan removal, is outside the initial
engine. While the store is open, warehouse mutations belong exclusively to the
engine. Linux incremental physical-capacity admission detects ordinary notified
local mutations but does not promise detection of arbitrary external mmap, alias
or mount changes; [the inventory contract](development/WAREHOUSE_INVENTORY.md)
defines strict full-traversal mode and safe fallback. Store ownership spans creation, recovery, writes, cursors, retained
buffers, and maintenance, including concurrent opens in the same process.

## Durable publication

The write coordinator owns validation, operation binding, the prepared commit
base, durable file creation, and final publication. A prepared base includes
the table UUID, metadata location, and optional current snapshot. The catalog
must match that exact base at publication, including an initial empty table or
a metadata-only change with an unchanged snapshot. A user snapshot precondition
does not replace this check. Automatic refresh/rebase/replay of a prepared
write is disabled.

All new Parquet, manifest, manifest-list, and metadata JSON files use the same
durable file-I/O boundary. A file is eligible for publication only after format
finalization, user-buffer flush, file synchronization, checked close, and
synchronization of its directory entries through the durable root. If a rename
is used, affected directory entries also require synchronization. The first
failure remains recorded even if a later close or cleanup succeeds.

The catalog integration uses Iceberg `SqlCatalog` with a narrow GlamDB
`DurableSqliteCatalogStore`. SQLite owns its database and rollback-journal
durability; the file-I/O layer owns external files. The working connection uses
`journal_mode=DELETE` and `synchronous=EXTRA`, set and read back before serving
operations. Opening existing state does not enable SQLite's create flag or
silently convert an unexpected journal mode.

Before the authoritative pointer changes, a publication guard verifies that
every new reachable file is durable and the candidate state can be reopened
within the configured limits. The pointer and committed operation receipt are
updated on the same connection in one outer SQLite transaction. Only successful
completion of that outer commit authorizes a committed response. An internal
Iceberg commit, a tentative SQL update, or a closed file is insufficient.

## Outcomes, retries, and recovery

| Outcome | Meaning |
| --- | --- |
| `Committed` | The snapshot and original result are durably published. |
| `DefinitelyNotCommitted` | Publication is proven absent; a registered operation's failed outcome is durably recorded. |
| `CommitUnknown` | The publication or operation-record outcome is unresolved; it is not a terminal failure. |

For new valid input, persist the operation ID, canonical hash, and prepared base
before file I/O, without storing input rows in a separate journal. Validation
failures before registration do not bind an ID. Registered operations retain
their successful or failed terminal outcome permanently. Matching ID/hash
retries return the original outcome before checking a new precondition or
execution budget; another hash conflicts. A deliberate new attempt after a
registered failure needs a new ID.

Canonical hashes preserve input row order and duplicates and are independent of
Arrow chunk boundaries or transport encoding. The canonical representation is
versioned and tested with byte fixtures. A committed receipt is stored with the
catalog pointer and cross-checked against operation information in the snapshot
summary. Retrying A after a later B returns A's receipt without modifying B.

An uncertain write closes readiness and holds the writer barrier. A retry whose
outcome lookup is blocked by writer contention, admission limits or shutdown may
return `CommitUnknown` without replacing the unresolved write. Resource pressure
alone does not close readiness.
Resource refusal alone never proves that an earlier operation did not commit.
Resolve outstanding SQLite transactions, reopen the authoritative catalog,
validate identities and retained history, and check reachable files before
releasing it. Recovery includes a
**durability fence**: a real increment of the store's fence counter committed
through SQLite's configured durability mode. A SELECT or no-op update cannot
substitute for that transaction. The fence creates no Iceberg snapshot and does
not change the original receipt.

New-process open performs recovery before readiness. Unpublished registered
operations interrupted by a crash become durable failed outcomes after
reconciliation; input rows are not replayed. Until resolution, reads use only
the last confirmed view. An unavailable catalog, inconsistent history/receipt,
or failed fence leaves writes blocked. Timeout, cancellation, and shutdown do
not turn uncertainty into a definite failure.

## Reads, schemas, and statistics

A scan fixes latest, a historical snapshot, or the initial empty view once.
Its schema and resolved identity are available before the first batch, including
empty scans and a zero row limit. Files come from that view's manifests, never
from a warehouse directory scan. File/history pagination retains the selected
view; collection pagination retains a creation high-water mark rather than
changing membership between pages.

Projection and typed predicates use stable field IDs. File and row-group
pruning only exclude impossible matches; missing or truncated statistics remain
conservative. Exact residual filtering uses SQL three-valued NULL logic and
keeps only TRUE. Unsupported table features and active delete files are rejected.
Page/Bloom index presence does not claim execution support; those optimizations
are outside initial scan execution.

The schema is flat and supports UTF-8 strings, booleans, int64, finite float64,
UTC timestamps in microseconds, decimal128(p,s) and calendar dates. Decimal
precision is 1..38 and scale is 0..precision; dates span years 0001..9999.
See [scalar forms and version compatibility](development/SCALAR_TYPES.md).
Field IDs persist in Iceberg and
Parquet. A `key` field, when present, is a required string but is not unique in
append mode. Omitted nullable fields become NULL; missing required fields,
unknown fields, and incompatible types are rejected. NaN and infinity are
rejected, and negative floating-point zero canonicalizes to positive zero.

Timestamps span years 0001 through 9999. Text input needs a timezone, is
normalized to UTC, and rejects leap seconds and more than six fractional digits.
Integer and timestamp precision survives the future wire/SDK boundary without
passing through JavaScript Number or Date.

The `key` column uses full Iceberg bounds; other applicable metrics use explicit
`truncate(16)`. Validate counts and bounds against the written rows and stable
field IDs. Full key bounds cannot be inferred from truncated footer statistics.
Missing statistics affect optimization, not query results.

## Capacity and lifetime

Data, metadata, and control memory have separate shared budgets. Arrow's pool
does not account for every STL, Avro, JSON, SQLite, codec, or upstream writer
allocation. Lazy file planning does not bound materialized table metadata or
retained snapshot history. Account for buffers held by callers as well as work
still executing inside the engine. The [shared resource budget contract](development/RESOURCE_BUDGETS.md)
defines units, owners, reservation/release lifetimes, completion/recovery reserves
and limit errors. Its matrices distinguish shared reservation envelopes from remaining RSS and
caller-owned metadata qualification. See the [resource owner](development/SHARED_RESOURCES.md)
for the process parent, protected completion/recovery and atomic device admission.

The synchronous append profile caps ordinary append input at 16 MiB of canonical
decoded data and 10,000 rows, with one new Parquet file and one snapshot. Current
default store-wide checks include 4,096 retained snapshots and 4,096 operation
bindings/outcomes, including failed operations. The opt-in [retained-history
profile](development/SCALABLE_HISTORY.md) exposes coordinated count, byte and
recovery budgets and bounded catalog iteration beyond those defaults. These
development limits do not qualify combined memory usage or storage capacity. Canonical bytes, owned-input
estimates, physical Arrow/writer allocations and HTTP payload bytes have separate
checks; no one of them bounds process memory across stores.

Small appends therefore do not imply large-scale ingestion: 1 TiB at 16 MiB per
append needs at least 65,536 operations, exceeding the candidate history cap.
A sufficient explicit history profile is required beyond the default horizon.
The bounded bulk append extension amortizes publication over multiple files;
its finite development profile and measured limits are documented separately.
Larger bulk/compaction qualification remains future work. Initial history
and receipts do not expire, and orphan files count toward physical capacity.
Reject new work before publication when a bound is reached while retaining old
reads and retries. A published state must reopen with the same limits.

Cursors and every returned buffer retain the allocator, accounting state,
executor, file I/O, and store lock they depend on. Closing a cursor does not
release the budget of buffers still held by its caller. Slices and exported
Arrow data must preserve the same lifetime. Calls to one cursor are serialized;
different cursors are independent. Shutdown closes admission and cancels scans,
but an in-progress publication must reach a known result or remain explicitly
recovery-required before dependent resources can be released.

## Corruption, compatibility, and backup

SQLite handles its rollback journal. GlamDB does not truncate suspicious tails,
delete a hot journal, create a replacement database, or silently reconstruct
canonical outcomes from snapshots. Snapshot summaries cannot recover failed
operations and their permanent ID/hash bindings. Detected corruption or a
missing published file closes readiness. Opening does not verify every data
byte; some latent corruption is detected only during scanning or scrubbing.

Unknown store/catalog/hash/receipt formats are rejected before writes. An
upgrade is an explicit offline operation with a verified backup and comparison
of retained snapshots and outcomes, rather than an implicit change during open.

The initial backup model is quiesced: stop new calls, finish writes and recovery,
wait for cursor/buffer leases, and close SQLite connections while retaining an
independent store-ownership lease. Copy identities, the catalog, and all retained
reachable files; check checksums and synchronize the destination. Hold source
ownership through copy and verification. Test reopening at a different physical
root with the original volume unavailable and the same logical store identity.
The tested offline helper uses `glamdb-admin quiesce-store`: SQLite is closed,
while the retained engine owns its independent directory lease until the copying
process releases stdin. Copying includes all regular store files (including
orphans), rejects links/special files, verifies SHA-256, synchronizes files and
directories and publishes `store.ready` last. A competing process is refused
during copying. The [system recipe](development/NATIVE_ACCEPTANCE.md) verifies
successful and failed outcomes and current/historical/initial reads at another
root while the original root is unavailable. This is a local quiesced procedure,
not online backup or physical power-loss qualification.
A live copy of only `catalog.sqlite` is not a backup of the store.

Durability claims apply only to the
[qualified environment](ROADMAP.md#g4-local-release-qualification). Disk loss,
devices that falsely acknowledge flushes, and later media corruption are not
made recoverable by `fsync`; backup and replication are separate capabilities.
