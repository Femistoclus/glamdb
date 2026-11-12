# Explicit snapshot retention

This maintenance API supports append collections. KV collections preserve their
complete snapshot intervals and no-op bound roots; policy reads, updates and
expiration refuse with `COLLECTION_MODE_MISMATCH`. A KV expiration ledger or
metadata cleanup queue is corruption. See [KV mode](KV_MODE.md).

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

## Pinned query views

A native [read view](READ_VIEWS.md) acquired before maintenance retains the
verified generation and a root reader for each captured entry. Its first scan
can be planned after expiration and still select the captured history. Expired
snapshot IDs remain unavailable to new acquisitions. Root retirement and pending
metadata cleanup wait for these pins just as for existing planners; view
expiration is swept on writer cleanup. Explicit release, cancellation, final
handle destruction and shutdown release the pins. Physical unlink remains on
the writer, and current data files remain retained by the existing profile.
