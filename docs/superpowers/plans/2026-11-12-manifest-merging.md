# Manifest merging implementation plan

> Execute inline with the executing-plans and test-driven-development skills;
> request a fresh independent whole-branch review after verification.

**Goal:** Fix official issue #169 without weakening append closure or durability.
**Architecture:** Use Iceberg merge append and verify unchanged and rewritten
membership in the shared publication/recovery path.
**Tech stack:** C++23, pinned Iceberg/Arrow/Parquet, SQLite, GoogleTest, CTest.
**Spec:** ../specs/2026-11-12-manifest-merging-design.md

## Global constraints

- Eight new reachable manifests, 8 MiB per manifest and 32 MiB commit verification.
- Ordinary append adds one file; bulk append adds at most 256 files.
- Retain all snapshots and permanent exact outcomes; never delete old manifests.
- Use a fork branch and an English commit and pull request.

## Review focus

- Merged EXISTING entries must preserve every original descriptor and sequence.
- Rewrites cannot omit or duplicate parent files, including retained manifests.
- Publication and fresh-process recovery must enforce identical byte/count guards.
- Failures at the merge boundary must preserve confirmed visibility and retries.
- Existing FastAppend histories and bulk-to-ordinary transitions remain readable.

## Task 1: Merge append and strict closure verification

Files: `core/storage/append_stager.cpp`, `core/storage/base_state.cpp`, focused
tests in `tests/unit/storage/` and `tests/vertical/`.

- [x] Add real-storage tests that append across multiple 100-manifest boundaries,
  assert the latest manifest count is below 100, and verify rows, old/initial
  views and exact retries after reopen. Run and observe the manifest-count failure.
- [x] Add malformed-rewrite and bounded-resource cases to publication tests.
- [x] Use `NewMergeAppend`; compare rewritten entries exactly and retain existing
  count, byte, receipt, profile and membership invariants.
- [x] Exercise file and SQL faults at a merge boundary, including uncertain COMMIT.
- [x] Run focused tests, the native CTest suite and relevant C++ quality checks.
- [x] Commit the tested implementation and regression tests.

## Task 2: Measurements and documentation

Files: `benchmarks/capacity.cpp`, `docs/development/CAPACITY_CAMPAIGN.md`, compact
JSON evidence, storage/scan/resource guides and `AGENTS.md` where affected.

- [x] Record the latest manifest count in campaign evidence.
- [x] Run the unchanged default campaign on the baseline and fixed Release builds;
  require 4,096 successful appends and all reopen/history/retry checks.
- [x] Publish comparable I/O and retained metadata measurements with growth laws,
  limits and explicit qualification boundaries.
- [x] Run campaign integration and relevant quality checks; commit the evidence.

## Task 3: Independent review and pull request

- [x] Dispatch a reviewer with the requirements, base revision and final diff;
  request critique, alternative approaches and invariant/edge-case analysis.
- [x] Reproduce and fix any material findings and rerun affected checks.
- [ ] Push the working branch to the existing fork and create a pull request
  targeting `glamourdb/glamdb:main`, using its template and `Fixes #169`.
