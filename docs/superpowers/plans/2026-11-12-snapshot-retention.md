# Snapshot Retention Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task.

**Goal:** Implement issue #168 without changing default retained history.

**Architecture:** Publish explicit retention maintenance with the existing outer
SQLite transaction. Persist expiration identities and cleanup work atomically;
recover current files from retained snapshots and permanent append receipts.

**Tech Stack:** C++23, SQLite, Arrow, Iceberg, GoogleTest.

**Spec:** [design](../specs/2026-11-12-snapshot-retention-design.md).

## Global Constraints

- Retain everything by default; preserve catalog versions 1 and 2 until opt-in.
- Never delete Parquet or a manifest referenced by a retained snapshot.
- Keep all original append receipts and exact retries.
- Use snapshot sequence numbers for prefix ordering, never opaque IDs.
- Reject a new snapshot identity already reserved by the expiration ledger.
- Public errors distinguish `SNAPSHOT_EXPIRED` from unknown snapshots.

## Review Focus

- Unknown COMMIT must prevent cleanup until authoritative recovery (task 3).
- Planning against any older root must delay cleanup (task 3).
- Merged manifest inputs remain until their last retained reference expires.
- Checkpoint recovery checks every original file against its receipt profile.
- Cleanup parsing and queue materialization retain finite preallocation bounds.
- A larger age guarantee must protect history even when keep-last is small (task 1).
- An expired parent must not weaken receipt/provenance checks (task 2).
- Larger profiles must keep bounded inventory reads and shared admission (task 4).

### Task 1: Retention policy and public behavior

Files: `public/storage/{types,engine}.h`, `core/storage/snapshot_retention.*`,
`tests/vertical/snapshot_retention_test.cpp`.

- [x] Add failing tests for default retention, keep-last, minimum age, expired
  reads/preconditions, active cursor continuation and receipt replay.
- [x] Run `cmake --build build/local --target glamdb_vertical --parallel 2`;
  confirm the missing API failure.
- [x] Add `SnapshotRetentionPolicy`, `SnapshotExpirationRequest`,
  `SnapshotExpirationResult`, `Engine::ExpireSnapshots` and policy inspection.
  Select only an unprotected oldest prefix and preserve the current snapshot.

### Task 2: Catalog and recovery

Files: `core/catalog/durable_sqlite_catalog.*`,
`core/{backend,storage}/*`, `core/engine/storage_backend.h`.

- [x] Persist version-3 expiration records and cleanup paths with exact-base CAS.
- [x] Verify retained/expired receipts bidirectionally and rebuild membership
  from the oldest retained inventory without expired lists or obsolete manifests. Test missing/modified ledger
  and manifest provenance, reopen and original receipt verification.

### Task 3: Publication, cleanup and concurrency

Files: `core/engine/engine.cpp`, `core/io/store_dirs.*`, vertical tests.

- [x] Publish maintenance under the writer and commit gate. Promote only a
  verified candidate after outer COMMIT, and close readiness on uncertainty.
- [x] Reclaim queued metadata after planners release; sync before deleting queue
  records. Test cleanup faults, interrupted publication and independent reopen.
- [x] Run `build/local/tests/glamdb_vertical --gtest_filter=SnapshotRetention.*`;
  require all cases PASS.

### Task 4: Capacity campaign, documentation and independent review

Files: `public/storage/types.h`, resource envelopes, development documentation.

- [x] Preserve configured finite operation/history budgets and defaults, and
  run >4,096 real appends with periodic explicit maintenance and fresh reopen.
- [x] Record measured root sizes and reopen time; verify that expired manifest-list
  bytes and last-reference manifests are reclaimed.
- [x] Run relevant native suites, formatting and quality checks; inspect outputs.
- [x] Obtain a fresh independent whole-change review, critique the design and
  resolve actionable findings with regression tests.
- [x] Prepare the verified change for an English commit and a fork PR targeting
  official `main`.
