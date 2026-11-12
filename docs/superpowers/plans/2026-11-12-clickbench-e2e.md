# ClickBench E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship a measured network ClickBench lifecycle with append and KV targets.

**Architecture:** Extend the existing ClickBench adapter using the same SDK,
durable SQLite ledger, bounded external verification and Locust as TPC-H. Freeze
the source-to-request mapping and retain target mode and original requests.

**Tech Stack:** C++23 CLI11/reproc, Python, PyArrow, SQLite, Locust, DuckDB/SQLGlot.

**Spec:** ../specs/2026-11-12-clickbench-e2e-design.md

## Global Constraints

- Preserve all 105 logical columns, duplicate rows and exact signed integers.
- Append is the default; KV uses a derived ordinal key, never a source ID.
- Unknown writes require explicit original-ID resolution and exact-byte retry.
- Keep source/request/metadata/memory/disk bounds and server resource admission.
- Use real native network tests; mocks are protocol tests only.
- Commit messages and pull request text are English.

## Review Focus

- Changed resume mode, source bytes or chunk assignment must fail before writes.
- KV duplicate IDs and duplicate full rows must retain separate ordinal keys.
- Unbound refusals and dropped replies must never be silently retried or counted.
- Large operation profiles must check retained history and journal budgets.
- Finite concurrent runs must consume all pages and retain partial/failure counts.

### Task 1: Faithful preparation and immutable append/KV loading

Files: clickbench/dataset.py, schema.py, new target.py and plan.py, ingest.py;
tests/test_clickbench_dataset.py and test_clickbench_ingest.py.

Interfaces: dataset.schema remains105; target_schema(source,mode),
target_rows(rows,start,mode), initialize(...,mode=None),
load(...,batch_rows=None,batch_bytes=None,workers=None,report=None).

- [x] Add failing tests for duplicate-safe KV keys, mode-preserving init/resume,
      byte splitting, extended history capacity, foreign writes and recovery.
- [x] Freeze bounded chunks in SQLite and validate plan/journal identities.
- [x] Implement mode-aware requests/receipts, Locust worker assignments and
      independent source/request/remote verification before complete status.
- [x] Run focused dataset/load/size tests, then SDK/workload regressions.

### Task 2: Complete measured query campaigns

Files: clickbench/run.py, query worker module, campaign tests.
Consumes: saved collection_mode, logical dataset schema and pinned snapshot.
Produces: campaign(...,users=1,processes=1) preserving existing report stems.

- [x] Add failing tests for complete finite rounds, distributed merged statistics,
      refusal/failure counts and physical KV-key exclusion from SELECT *.
- [x] Use Locust for query scheduling and percentiles; retain bounded reducers,
      independent oracle checks and all43 coverage entries.
- [x] Run focused campaigns and real native concurrent scan/reduce acceptance.

### Task 3: CLI integration and native acceptance

Files: apps/cli/workload.cpp, clickbench/commands.py, bridge/parser/native tests.
Consumes: Task1 initialize/load and Task2 campaign interfaces.

- [x] Add failing bridge/parser tests for init mode, load bounds/workers/report,
      scan/query aliases and users/processes/repeat options.
- [x] Extend real native lifecycle tests for both modes, resume/restart,
      JSON/Arrow verification, all43 queries, size, exact clean and fresh reload.
- [x] Build server and standalone CLI; run bridge and native TLS/HTTP campaigns.

### Task 4: Documentation, review and publication

Files: tools/workload/CLICKBENCH.md, CLI/workload guides, AGENTS.md,
docs/development/CLICKBENCH_VALIDATION.md and compact measured evidence.

- [x] Document complete append/KV recipes, explicit larger capacity profiles,
      reproducibility, client/server measurement boundaries and actual evidence.
- [x] Run relevant Python, C++ formatting/build, CLI and native E2E checks.
- [x] Obtain independent whole-branch review and address actionable findings.
- [x] Commit, push to the user's fork and create an English pull request.
      Published as [PR #198](https://github.com/glamourdb/glamdb/pull/198).
