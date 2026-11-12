# Snapshot scans

`Engine::Scan(ScanRequest)` returns a cursor over one confirmed collection view.
Select `Latest`, an explicit retained snapshot ID, or `InitialEmpty`. Latest
resolves once when opening the scan; later writes never enter it. Snapshot IDs
are opaque and do not indicate time, order or row count.

`ScanCursor::Info()` reports the output schema and resolved view before the
first batch, including empty results. `Next()` returns an Arrow batch, a null
batch at EOF, or an error. `Cancel()` cooperatively stops reading. Calls on one
cursor are serialized; independent cursors can read concurrently.

## Projection and predicates

An omitted projection selects all columns; an empty projection preserves
matching row cardinality with zero columns. Projection order determines output
order. Stable field IDs come from the original collection schema, not projected
ordinals. Limits apply after exact predicate evaluation.

Predicates support comparisons, IN, IS NULL, IS NOT NULL, AND, OR and NOT.
Types must match the fixed schema. SQL three-valued NULL logic applies and only
TRUE matches survive. Limits are 64 predicate levels, 1,024 nodes and 4,096
literals. Scan batch size is 1–65,536 rows; HTTP page limits are separate.
See [scalar types](SCALAR_TYPES.md) for exact decimal, date and timestamp rules.

Local scans conservatively prune files using manifest statistics and row groups
using Parquet footer statistics. They read only column ranges needed for
projection and predicates in surviving row groups. Missing/truncated bounds
remain conservative. Decimal/date value bounds are treated as unknown; valid
null counts can still prune. Page indexes and Bloom filters are not used for
pruning. Exact residual predicates determine results after decoding.

Files come from verified membership of the selected Iceberg snapshot. Orphan
files and uncommitted writes never become visible through directory enumeration.
KV membership uses retained file intervals; append membership preserves every
input row, including duplicates. An expired append snapshot returns
`SNAPSHOT_EXPIRED`; already planned cursors keep their fixed data view.

## Ownership and resources

Cursors and returned Arrow buffers retain their allocator, resource accounting,
read-only file I/O and store ownership. Slices and exported Arrow buffers preserve
that lifetime after the engine or cursor closes. Release all dependent handles
before expecting another process to acquire the store.

Verified-index planning uses a 16 MiB temporary schema/projection workspace.
Uncached planning uses the configured history-derived metadata envelope.
Cursor, manifest, footer/decoder and output allocations have separate
[shared reservations](SHARED_RESOURCES.md). Reservations are admission bounds,
not measured process RSS.

`ScanCursor::Profile()` separates completed reads, filtered/limited rows and
file/row-group pruning. A limit or cancellation does not count unread files as
pruned. EOF can mean the requested limit was reached; it does not imply every
candidate was read. [Metadata and profiles](METADATA.md) describe the counters.

The [HTTP API](HTTP_SERVER.md) exposes fixed-view JSON/Arrow pages with replay
and cancellation. The [Python SDK](../sdk/python/README.md) drains those pages
through context-managed iterators. [Scan session policy](SCAN_SESSIONS.md)
describes quotas, expiry and retained replay bytes.

## Native read views and scan splits

Contract 1.1 [read views](READ_VIEWS.md) capture an ordered snapshot vector from
one store for repeated or delayed scans. They retain original verified state
across publication and explicit append retention. Trusted adapters supply the
same authorization scope on every use; view handles remain process-local.

Contract 1.2 [scan splits](SCAN_SPLITS.md) page bounded whole-file descriptors
from one captured entry. Different splits cover disjoint physical membership;
replaying a descriptor reproduces its selected row multiset. Compute owns
scheduling and attempt deduplication. A split request rejects any global LIMIT,
including zero; ordinary serial scans retain their existing limit semantics.
The view lifetime governs future split admissions, while admitted cursors and
returned buffers retain independent leases. These methods have no HTTP routes.

## Native scan negotiation

Contract 1.3 [scan negotiation](SCAN_NEGOTIATION.md) classifies translated
conjuncts as exact, necessary conditions or unsupported residuals. A necessary
condition can eliminate rows and files while preserving the original expression
for compute evaluation. Residual and operator inputs remain in the storage
projection until compute finishes. Global limits stay in compute for split
execution, residual filters or ordered results. Storage does not execute sorting,
casts, functions or locale collations through this API.
