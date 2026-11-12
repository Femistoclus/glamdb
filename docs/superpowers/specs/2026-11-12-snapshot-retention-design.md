# Explicit snapshot retention

Issue #168 requires an opt-in maintenance operation that bounds retained root
history while preserving every append receipt and all current data.

The public C++ facade exposes `ExpireSnapshots` with an optional policy. An
omitted policy uses the policy already persisted in the current Iceberg root;
an empty policy retains everything. `keep_last` protects at least that many
snapshots, and `min_age` protects younger snapshots. Their union is retained,
including the current snapshot. Only an oldest contiguous prefix can expire.
Maintenance is explicit; applications choose when to invoke it.

## Approaches

Silently expiring during append changes existing admission and retry semantics.
Keeping every old manifest list in a side history does not reclaim metadata.
The selected approach publishes maintenance separately through the existing
outer SQLite authority, with a durable expiration ledger and cleanup queue.

Catalog version 3 is created lazily on the first maintenance publication.
Versions 1 and 2 remain unchanged until opt-in. Expiration records bind opaque
snapshot IDs to their original sequence and operation receipt. Recovery verifies
the oldest retained inventory against original file provenance and permanent
receipt totals, then validates later exact manifest rewrites without reading
expired lists or obsolete manifests. The current root carries only a sequence
watermark and the retained suffix; snapshot IDs are never compared numerically.

Cleanup follows confirmed publication, waits for collection metadata planners,
unlinks only ledgered metadata and synchronizes the directory before acknowledging
the queue. Interrupted cleanup resumes in bounded batches after authoritative
recovery. A manifest is reclaimed only after its last retained snapshot reference
disappears. Current manifests and Parquet files remain referenced and are never
removed. Already
planned cursors continue reading their immutable membership and data; new reads
and preconditions on expired IDs return `SNAPSHOT_EXPIRED`.

The default operation capacity remains 4,096. The configured operation, catalog,
history and recovery budgets stay authoritative before and after expiration.
Expiration inventories use bounded keyset pages. Data and metadata remain finite;
expiration is not compaction. No HTTP/SDK/UI maintenance interface is added.

## Invariants and evidence

The current snapshot, schema, table identity, data membership and receipts never
change during maintenance. Exact-base CAS and durable root verification precede
the outer COMMIT. Uncertain COMMIT closes readiness until reconciliation. Failed
or interrupted maintenance cannot authorize file deletion. Tests cover default
compatibility, policy unions, expired errors, cursor lifetime, receipt replay,
reopen, corruption, publication faults, resumable cleanup and a >4,096 campaign.
Process crashes and simulated faults do not qualify physical power loss.
