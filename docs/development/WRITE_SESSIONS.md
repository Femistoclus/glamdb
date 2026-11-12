# Native durable write sessions

Contract 1.4 adds `TWriteSessions` in installed `storage/write_session.h`. It
stages one caller-owned append or last-wins KV upsert for one collection. All
publication uses `Engine::BulkAppendDurable` / `BulkUpsertDurable`, their existing
coordinator, original canonical operation hash, exact prepared base and permanent
receipts. Staging is not publication. There is no HTTP endpoint, external Parquet
commit, remote descriptor, overwrite, merge, delete or compaction session.
Capability discovery does not create a staging owner or promise readiness.

## Ownership and identity

Open a facade with a **shared existing Engine**, a canonical absolute staging
directory, an exact nonempty authorization scope (up to 256 UTF-8 bytes), and
optional reduced limits. The directory must be outside, and not an ancestor of,
the engine's managed store tree, with no symlink components. Its parent must
exist. It gets a separate nonblocking process lock and a SQLite journal bound
permanently to the store UUID, scope and chosen limits. Access to the facade
conveys that scope: the embedding host controls which callers receive it.
The facade retains that exact engine and its ResourceOwner; it never opens a
second store owner. Closing the facade releases its lock/reservations, not staged
data. Hosts must not modify the staging directory or use a session's operation ID
for unrelated direct Engine writes. Identity-aware lookups still reject conflicts.

A `TWriteIdentity` is the caller-known collection UUID and operation UUID. There
is no random token that could be lost with the Begin reply. Retain both before
calling Begin. The directory's scope and limits cannot change on reopen.

1. `BeginWrite` persists schema, mutation, original snapshot precondition and TTL.
   Retrying must repeat the same mutation, precondition and requested lifetime.
   Unsupported mutations/modes fail before staging or engine operation binding.
2. `PutPart` takes task/attempt/part integers and a **one-pass Next callback**,
   returning null at EOF. It never rewinds the caller. It normalizes and stages
   one bounded Arrow IPC batch at a time in one SQLite transaction per part.
   Partial, cancelled, malformed or quota-rejected uploads have no accepted part.
   Acknowledgment means durable staging, not visible rows. Each part is immutable.
   Retrying the same tuple checks canonical typed values and row order; changing
   Arrow buffer layout or batch boundaries does not change its identity.
   Replay copies decoded column buffers into bounded owned buffers, so IPC schema
   and framing bytes do not inflate the core retained-input accounting.
3. `GetPartReceipt` resolves a lost PutPart reply using the known tuple without
   reexecuting a one-shot producer. Missing receipt means no acknowledged part;
   an expired/cleaned session's status distinguishes reclamation.
4. `SealWrite` accepts an explicit selection. It rejects duplicates and multiple
   attempts of the same task, sorts numerically by task then part, and preserves
   row order within each part. Arrival order does not affect the final input.
   It verifies acknowledged part digests and freezes selected tuples, total rows
   and the **existing** bulk/KV canonical payload hash. Repeating the selection in
   another list order succeeds; changing it fails. KV duplicates resolve by the
   existing last-wins policy across that complete deterministic row sequence.
5. `CommitWrite` verifies the seal, persists CommitStarted, then invokes the
   existing synchronous Engine bulk operation with the original precondition.
   The engine alone chooses/binds the exact prepared base and publishes one
   atomic result. A stale precondition becomes its existing permanent failure.
   Admission/cancellation before core binding may have no authoritative receipt;
   a returned attempt outcome must not be mistaken for a permanent bound receipt.
6. `GetWriteStatus` reads persisted staging state, expiry, selected parts, rows
   and hash. `GetWriteOutcome` consults the existing Engine catalog/recovery state
   and checks the original binding hash, including permanent failed outcomes that
   have no hash in their public DTO. An unsealed session returns a Result error,
   not a fabricated AppendOutcome. A missing outcome is distinct from a failure.
   Commit replay remains available after payload reclamation because the seal and
   core receipt remain. No receipt schema/version or original bytes are changed.

Callbacks must return promptly, keep returned input immutable during the call,
and cooperate with cancellation. There is one active facade call (including
upload/commit). Competing calls immediately return `OPERATION_IN_PROGRESS` rather
than waiting on the producer, SQLite transaction or engine writer. Uploads have a
10-minute cooperative deadline; no timeout can interrupt a blocked OS call or
prove publication failure. Commit cancellation follows the Engine's three-way
outcome semantics. Reconcile the supplied Engine when instructed, then resolve or
retry the original identity.

## Persistence and cleanup

The directory contains only `owner.lock`, `staging.sqlite`, its persistent rollback
journal and `completion.reserve`. SQLite uses 4096-byte pages, PERSIST journaling,
EXTRA synchronous mode, a 1 MiB page cache, no mmap and no busy waiting. Initial
directory creation syncs the parent; lock/database/reserve setup syncs the staging
directory. SQLite owns checked journal/database write and sync ordering. Opening
checks bounded artifact sizes, binding, inventory, page size and quick integrity;
SQLite recovers interrupted transactions before any upload admission.

`AbortWrite` checks the Engine outcome. It refuses committed or unresolved writes
and refuses CommitStarted with no authoritative terminal outcome. Neither TTL,
cancellation nor abort turns uncertainty into DefinitelyNotCommitted. After any
commit-started marker, the only replayable sealed input is retained until matching
terminal resolution. A failed prebinding attempt can be retried with CommitWrite.

TTL defaults to one hour, can be positive up to 24 hours, uses persisted wall-clock
seconds and is not renewable. There is no background thread. `Cleanup` explicitly
expires unstarted uploads/seals by safe abort, and reclaims payload for aborted
sessions or matching authoritative terminal outcomes. Unknown started sessions
are retained regardless of expiry. Cleanup deletes at most one bounded batch per
transaction and is restartable; freed SQLite pages are reusable. SQLite file
length does not shrink. It preserves session tombstones/seals and permanent core
receipts, so an old operation cannot silently become a new session. Once the
finite lifetime session inventory is full, use a new explicit directory; retain
old directories while their status/replay handles are needed. Never remove a
sealed unresolved directory as a capacity workaround.

## Resource bounds

| Resource | Bound/ownership |
| --- | --- |
| Lifetime session descriptors, including tombstones | 128 per directory, configurable down |
| Accepted parts across all sessions | 4096 per directory, configurable down |
| Staged batches across all sessions | 8192 per directory, configurable down |
| Aggregate IPC payload | 256 MiB per directory, configurable down |
| Batch | Existing 10,000-row / 16 MiB canonical input bounds; IPC at most 24 MiB |
| Selected operation | Existing 25,600,000-row / 4 GiB canonical bulk bounds |
| Concurrent facade calls/uploads | One; conflicting admission is nonblocking |
| Manager bookkeeping | 16 MiB shared Metadata envelope; 1 MiB protected Completion |
| Original returned status / part receipt | Retained 128 KiB Metadata / 512-byte Control reservation per result; releases with its final lease |
| Upload/seal/replay scratch | 256 MiB shared Data envelope during the call; caller input is host-owned |
| SQLite database | Payload cap + 64 MiB, enforced with max_page_count; separate from upload quota |
| Device reservation | Twice the database cap, 2 MiB journal overhead, 64 MiB emergency file, eight inodes |
| Execution headroom | Atomic shared-device staging check leaves 12 GiB + 64 MiB and 512 inodes available for existing KV/bulk execution |

The execution headroom check is not a second execution reservation: Commit uses
ordinary Engine reservations once. Multiple staging managers compete under the
same device mutex; their ceilings do not each spend the same headroom. Reopening
an existing journal restores recovery accounting without demanding execution
headroom; new Begin/Put calls recheck it. Other engines/hosts may consume resources
later, so accepted staging does not guarantee immediate commit admission. Retain
and retry the seal on temporary pressure. Shared envelopes are conservative
admission bounds, not measured RSS guarantees. Original status/part DTOs carry independent retained reservations; independently
deep-copied host DTOs belong to the embedding host.

The separate **physically allocated 64 MiB completion file** is never upload
payload. On an actual SQLite FULL error during a control/cleanup transaction,
rollback, unlink/sync that reserve, and retry the bounded transaction once. Then
new Begin/Put are refused until reopen can recreate the reserve. Upload failures
do not spend it. Payload quotas leave ample separate database pages for bounded
session/part/selection descriptors; the emergency file also supplies physical
journal/bookkeeping room after external disk pressure. I/O failure or exhausted
external media can still refuse progress; the facade does not invent a receipt or
delete unresolved payload to force success. Database/journal reservations include
cleanup/recovery overlap. Existing data pages may stay allocated after cleanup.

## Executed evidence and limitations

The focused native tests cover typed append duplicates and canonical retry across
batch boundaries; deterministic selected attempts and cross-part KV last-wins;
cancellation/malformed rollback; precondition receipts and conflicting original
bindings; quota/ownership/scope/resource lifetime; lost replies across fresh
processes; interrupted upload transactions; an abrupt exit after a real staging
journal fsync; and catalog COMMIT uncertainty before/after real journal unlink.
A near-limit acknowledged batch is sealed and committed in fresh processes, with
exact row and saved-receipt comparison after reopen and replay after cleanup.
A test-only native SQLite VFS injects SQLITE_FULL at xWrite to exercise actual
rollback/control cleanup, distinct from an admission-quota refusal. These tests
model I/O faults and process crashes, **not physical power loss or native ENOSPC
from a filled dedicated volume**. Physical durability qualification remains NOT
RUN. Run the WriteSession/WriteSessions/WriteSessionProcess native suites plus
StorageContract/ResourceOwner and existing canonical/outcome compatibility tests.
