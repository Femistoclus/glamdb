# Managed database deletion

`POST /databases/{database_id}/drop` accepts exactly
`{"store_id":"<original store UUID>"}`. The UUID must match the database ID.
The native server requires its configured Bearer credential, as for every
database lifecycle operation. The organization gateway continues to reject
backend lifecycle routes; native cleanup is an operator operation, not a new
organization member permission.

The successful result is `{"database_id":"…","store_id":"…","dropped":true}`.
Save the original IDs before sending the request. A timeout or lost reply is
ambiguous: retry the same route and body, never resolve a database by name for
cleanup. Completed deletion receipts remain in the service registry, prohibit
reuse of a deleted UUID, and do not consume live database slots or names.
Receipts remain subject to the registry's bounded storage budget; exhaustion
refuses a new deletion before data removal instead of expiring retry protection.
Unknown IDs return 404; invalid requests return 400; mismatched identities,
unexpected files, replaced directories and unsafe paths return a precondition
error (412). An owned directory returns `STATE_IN_USE` and cannot be removed.
The `dropping` descriptor retains the cleanup error and its retry hint.
An uncertain registry result requires restart (`RECOVERY_REQUIRED`), never an
automatic destructive retry under a new identity. Failed or interrupted cleanup
remains visible and fenced; it must not be reported as successful.

## Ordering and recovery

1. Serialize lifecycle changes for this ID, fence new admissions and drain
   already admitted calls. Writes that were admitted retain their normal core
   outcomes. Cancel the database's cursors and metadata inventories, shut down
   its engine, and release all server-owned engine handles. Other databases
   retain their admission and engines.
2. Acquire the independent store directory lease and validate the exact store
   markers, directory identity and complete owned filesystem layout without
   following symlinks. Verify the lifetime service/parent descriptors and the
   directory identity captured when the live engine opened. Reject unknown
   contents before deleting anything. Prepare and synchronize a unique
   `store.deleting` seal; an interrupted preparation leaves the data intact.
3. Commit a deletion receipt with the root device/inode and seal in the registry under
   SQLite DELETE/EXTRA. No physical removal precedes this confirmed commit.
   An uncertain registry result requires restart.
4. Remove only validated owned entries using directory-relative descriptors,
   retaining the directory lease. Synchronize changed directories and the
   database parent after removing the root. Keep the seal until all other
   contents have been removed. An interruption leaves the receipt pending and
   the name reserved; startup resumes only the recorded inode with its exact
   seal. Device/inode numbers alone are reusable and never authorize recovery.
5. Atomically retire the database reservation and complete its receipt. Only
   this confirmed final registry commit permits success. A recreated or
   redirected directory is never removed by an old request.

If interruption occurs after removing the seal but before removing the root,
an existing markerless root is reported as incomplete cleanup requiring operator
inspection. It is never automatically deleted, even if its inode matches. With
the service stopped, an operator may remove an independently verified empty
leftover root and synchronize its parent; restart then completes the receipt.
Never fabricate a seal to bypass identity verification. If the root is already
absent, startup synchronizes the parent and completes the original receipt.

Registry version 3 adds deletion receipts; version 2 is migrated transactionally.
Core store formats and append receipts are unchanged. Startup attempts pending
deletions before reopening stores, and reports a per-database error when cleanup
cannot safely finish. Closed and errored initialized stores can be deliberately
dropped under the same identity and ownership checks. `discard` retains its
protection for initialized and ready stores.

This operation drops one entire dedicated benchmark database, including all
collections and history. It does not implement row deletion, TPC-H RF2,
`TRUNCATE`, collection deletion, or automatic garbage collection. It reclaims
directory entries and allocated file blocks; it does not promise secure erasure
or qualify physical power-loss durability.
