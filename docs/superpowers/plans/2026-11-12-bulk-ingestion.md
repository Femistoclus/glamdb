# Bounded Bulk Ingestion Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement and verify the tasks.

**Goal:** One bounded streaming append operation publishes multiple files atomically.

**Architecture:** Replayable Arrow source, bounded canonical validation pass,
existing operation binding and coordinator, bounded file rollover, one verified
Iceberg append and outer SQLite pointer/receipt transaction.

**Tech Stack:** Existing C++23, Arrow, Parquet, Iceberg and SQLite dependencies.

**Spec:** `docs/superpowers/specs/2026-11-12-bulk-ingestion-design.md`.

## Global Constraints

- Keep ordinary append validation, permanent receipts and historical views.
- Source: 10,000 rows / 16 MiB per chunk; files: 100,000 rows / 16 MiB input /
  32 MiB output / 8 MiB groups; bulk: 256 files / 25,600,000 rows / 4 GiB input.
- Retain exact-base publication, three outcomes, independent recovery and bounded
  shared reservations. No payload WAL, automatic orphan deletion or KV semantics.
- Work in 4glamdb on a fork branch; preserve untracked server-data; English commit
  and PR with no agent attribution. Open the upstream PR, without merging.

## Review Focus

- Changed source contents between passes must never publish a partial operation.
- Cancellation, shutdown and corrupt-read races must preserve publication outcomes.
- NULL-heavy wide rows and slices must fit the declared memory envelope.
- Ordinary manifests and receipt validation must retain their strict old bounds.
- Bulk metadata and file caps must be identical at publication and reopen.

### Task 1: Public source and coordinator

- [x] Add a failing public-API test for >10,000 rows, one receipt and multiple files.
- [x] Define BulkAppendSource::Rewind/Next and BulkAppendRequest; add
  Engine::BulkAppendDurable and share existing append coordination internally.
- [x] Add bounded incremental hashing without changing old canonical bytes.
- [x] Wire two-pass validation, reservations, cancellation and failure recording.
- [x] Test changed source/chunking, incorrect totals, interruption, ordinary caps
  and matching/conflicting retries.

### Task 2: Storage and recovery

- [x] Extend staged append to verified multiple-file input and bounded rollover.
- [x] Mark bulk snapshots/receipts explicitly; retain ordinary one-file guards.
- [x] Extend closure/history validation with overflow-safe actual aggregate
  row/file/byte checks and equal publication/recovery caps.
- [x] Verify file/group bounds, exact historical/initial scans, source failure,
  durable I/O failure and uncertain COMMIT with no partial visibility.

### Task 3: Reopen, measurements and documentation

- [x] Add a fresh-process generation/reopen benchmark and bounded-memory campaign.
- [x] Measure bulk and ordinary input under the same profile; record all requested
  metrics, distinguishing source validation time and final publication latency.
- [x] Update architecture, resource contract, coordinator, README, AGENTS and roadmap.
- [x] Build; run relevant CTest suites, sanitizers, format and tidy checks.

### Task 4: Independent review and publication

- [x] Request a fresh independent review and critique, including alternate designs
  and every Review Focus invariant; address material findings with regression tests.
- [x] Verify the complete diff, commit in English, push the fork branch and open
  an English upstream PR using its template; attach the created PR.
