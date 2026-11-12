# Bounded streaming append ingestion

Implements issue #100 for append collections before KV. Each caller operation
preserves input order for hashing and keeps all duplicate rows and publishes one snapshot and permanent receipt.
Ordinary append retains its 16 MiB / 10,000 row / one-file contract.

## Approaches

A one-pass upload needs a caller-supplied canonical digest or a bounded on-disk
spool before permanent identity binding. A replayable source instead permits a
bounded validation/hash pass, binding, and a checked production pass. Select the
replayable source: it adds a second source read but needs no payload journal,
spill recovery protocol, early acknowledgement or additional cleanup authority.
Sources may regenerate data or rewind an input file. Network adapters are outside
this embedded API change.

## Contract

`BulkAppendRequest` supplies collection/operation identity, snapshot precondition,
declared total rows, a caller-owned `BulkAppendSource` (`Rewind`, `Next`) and a
cooperative shared atomic cancellation flag. Calls and returned Arrow buffers must remain immutable
until the synchronous call returns; source callbacks must return promptly and
must not reenter the writer. Null batch means EOF; empty batches are invalid.
Both passes must deliver exactly the declared rows and identical canonical
content; chunk boundaries may differ. Canonical version 1 remains byte-compatible:
its uint64 row count is extended only by the bulk API. Ordinary validation keeps
the old row ceiling. Receipt version 2 identifies bulk results; old receipt bytes
and catalog schema remain unchanged.

Each source batch is at most 10,000 rows and 16 MiB decoded and retained Arrow
buffers. Per-file accumulation targets 8 MiB, caps buffers at 16 MiB, rows at
100,000 and coalesces batch descriptors at a fixed bound. Parquet groups remain at most 8 MiB and
files at most 32 MiB. A bulk operation is at most 256 files, 25,600,000 rows and
4 GiB canonical input. Limits are finite development bounds, not 1 TiB claims.
New-call owned-input reservation is 128 MiB plus the existing writer envelope; a separate 32 MiB metadata envelope
covers the bounded file/attempt descriptors. Existing shared allocator tracking,
catalog/history limits and publication/recovery reserves also apply. Bulk reserves 8 GiB + 32 MiB of device growth and 320 inodes before binding;
the process device ceiling is 16 GiB. Scans have no cross-file ordering guarantee.
No payload
spill is used; unpublished files are ordinary capacity-accounted orphans.

Validation interruption before binding leaves the identity unbound. After binding,
errors, changed input, deadline and cancellation use the coordinator's durable
failure path; lost publication replies remain CommitUnknown. Old matching retries
return their original result before execution admission/preconditions. The source
must still be fingerprinted to establish matching content. Only a complete source,
durable bounded files and a verified candidate can reach the outer pointer/receipt
transaction. Cancellation is cooperative and does not revoke a completed COMMIT.

The snapshot carries an explicit bulk marker. Closure/recovery permits multiple
verified added entries only for that marker; unmarked ordinary transitions still
require exactly one data file. Recovery checks actual rows/files/bytes against
the receipt and summary, exact retained manifests and the same capacity profile.

## Evidence required

Test changed chunking, duplicate rows, omission/null normalization, latest/history/
initial views, old retries, same-profile fresh-process reopen, source interruption,
cancellation, changed second pass, file/I/O/outer-COMMIT failures and each bound.
Compare ordinary/bulk on identical generated input with process RSS, groups/files,
metadata bytes, external sync counts, throughput, final publication latency and
cold reopen. Record actual measurements and explicitly leave physical power-loss
and 1 TiB qualification unclaimed. An independent reviewer will challenge these
invariants and the selected two-pass tradeoff after implementation.
