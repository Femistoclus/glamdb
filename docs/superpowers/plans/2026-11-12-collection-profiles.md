# Collection profiles implementation plan

**Goal:** Resolve [issue #155](https://github.com/glamourdb/glamdb/issues/155) with
finite, resource-backed collection profiles, complete inventories and persisted
managed configuration.

**Architecture:** Keep eight collections by default; support an explicit limit
of 1–64. Admit additional catalog/descriptor, control and recovery memory before
store creation/open. Retained roots continue to charge their measured size.
Reuse the engine's fixed-view collection pagination in HTTP and its consumers.

**Tech stack:** Existing C++23 engine, SQLite, Arrow/Iceberg, HTTP, Python SDK and CLI.

**Spec:** Issue #155 and `docs/development/RESOURCE_BUDGETS.md`.

## Constraints and alternatives

Removing the count bound leaves unbounded descriptors; scaling all fixed engine
envelopes multiplies unrelated write/history limits. Instead, add conservative
per-collection reservations above the default profile. Existing operation,
history, warehouse and catalog byte caps remain finite. The recovery workspace
continues to use its global file/history bounds. No persistent format change.
Managed limits remain in the service registry; embedded/offline callers supply
the same limits when opening an existing store.

## Review focus

- Exhaustion must refuse before publication and preserve idempotent creation.
- A smaller reopen limit must report CONFIG_TOO_SMALL and preserve all data.
- Continuations must retain exact identities across concurrent collection creation.
- Collection-profile memory admission must precede filesystem mutation and
  release on failed open; separately admitted native caches retain their own
  lifecycle and failure behavior.
- Advertised 64-collection behavior needs measured native concurrent/restart evidence.

## Tasks

- [x] Add failing engine and native HTTP regressions for 16 collections and scaled admission.
- [x] Share the supported bound across core/catalog/HTTP; admit profile resources;
  bound catalog enumeration before materialization and report effective capabilities.
- [x] Expose core collection pages through HTTP; preserve continuation binding;
  configure network/offline CLI and document existing SDK limit/page controls.
- [x] Exercise 8/16/64 native profiles: exact schemas/rows/receipts, exhaustion,
  fixed inventories, concurrent reads/writes, too-small reopen and fresh-process recovery.
  Record RSS, reservation peaks, catalog bytes and startup/recovery timing.
- [x] Update architecture/status/usage guidance with measured evidence.
- [x] Run focused and appropriate broader checks, obtain an independent critique
  of invariants and alternatives, resolve findings, and prepare the commit and
  upstream PR.

Execution: implement inline in `2glamdb` on a dedicated branch, followed by a
fresh reviewer. The issue supplies the acceptance contract and the user's request
authorizes implementation, commit and PR creation.
