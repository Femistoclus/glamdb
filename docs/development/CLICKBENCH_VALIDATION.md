# ClickBench validation

The CLI supports the complete network lifecycle in append and KV modes, all 105
logical fields and all 43 scan/reduce workloads. Executed evidence below qualifies
a small reproducible profile; the full 99,997,497-row reference load is NOT RUN.
See [the command recipes](../../tools/workload/CLICKBENCH.md) and
[compact machine-readable evidence](evidence/clickbench-e2e-2026-11-12.json).

## Automated checks

Python 3.13 SDK/independent-verifier tests: **1176 passed**. Workload tests:
**420 passed**, including the standalone CLI process-group signal checks (no
skips). The C++/Python bridge passed **13 tests**. LLVM 18 formatting and tidy
checks passed for the changed CLI translation unit; documentation links and
JSON evidence were checked. Independent reviews report no remaining blocker.

A fresh independent pre-merge review found legacy `resolve`/`verify` compatibility
and load-report sidecar collisions with saved state, dataset and journal paths.
Both are fixed and independently rechecked. Eight new regressions failed before
their fixes; the focused ingestion/plan suite passed 30 tests. The complete
workload suite (420 tests, 117.79 seconds) and native suite (8 tests, 129.073
seconds) were rerun locally after these fixes and passed.

## Native acceptance

Both the full Release server/CLI and the independently configured standalone CLI
were built with Clang 18. Standalone client acceptance executes the real CLI over
native HTTP/verified HTTPS, with no mock backend. The complete native workload
suite passed 8 tests, including three ClickBench profiles:

| Logical profile | Collection mode | Source fields | Physical fields | Rows |
|---|---|---:|---:|---:|
| Legacy DATE as explicit UTC midnight | append |105|105|31|
| Native DATE | append |105|105|31|
| Native DATE | kv |105|106|31|

Each profile verifies exact integers above 2^53, near-int64-limit late-column values,
Unicode and duplicate full rows. Five source parts become 13 frozen requests
under a 3-row limit and 3 disjoint workers. The TLS proxy loses a reply after a
real append/upsert commits. A server restart, original-ID lookup, resume with
omitted tuning options, every exact prepared body/hash/receipt and a no-op second
resume are checked. Independent JSON/Arrow source multiset and ordinal-key checks,
all 43 scan/reduce queries, 2-user/2-process scans, footer/physical sizes, exact
cleanup and fresh reload are exercised. The broader native suite also covers
maximum-width decimal restart/retry, distributed ordinary workloads and signal
cleanup of owned workers.

## Authentic source prefix

An exact bounded Range reader retrieved the original footer and first row-group
column chunks from the official hits Parquet. PyArrow retained the original schema
and first 1000 rows; no IDs or values were fabricated. Provenance records full
file length 14,779,976,446, footer hash, 99,997,497 declared source rows, source ETag,
54,514,582 downloaded bytes and the derived prefix SHA-256. The tiny derived
Parquet is explicitly a prefix, not the full official file.

All 105 converted values in both DATE profiles independently match the pinned
upstream DuckDB schema with UTC explicitly selected. In particular the real
EventDate is UInt16 epoch days, not YYYYMMDD; signed UserID/hash values are exact.
The network CLI then loaded 1000 rows into each fresh append/KV target, verified
them in JSON/Arrow, measured sizes and all 43 queries in both modes, executed 10
selected concurrent scans, restarted/resumed without new writes, cleaned and
reloaded fresh targets. Every query and scan passed. Full per-attempt evidence
remains in ignored `artifacts/clickbench-e2e/` run directories; the committed
summary includes provenance, snapshots, counts, bytes and measured ingestion time.

## Reproduction

Use the same-checkout SDK/workload/verifier on Python 3.13 with hash-pinned PyArrow,
Locust, DuckDB and SQLGlot. Build the server and standalone client as in the CLI
workload guide. Fetch the pinned query cache, then run:

```sh
export GLAMDB_CLICKBENCH_CATALOG=/absolute/path/to/clickbench-reference
export PYTHONPATH="$PWD/sdk/python/src:$PWD/tools/workload/src:$PWD/tools/verification/src:$PWD/apps/mock_server/src:$PWD/tools/logging/src"
python -m pytest -q sdk/python/tests tests/verification
python -m pytest -q tools/workload/tests
python tools/workload/tests/bridge_test.py build/client-standalone/glamdb-cli
python tools/workload/tests/native_e2e_test.py \
  build/client-standalone/glamdb-cli build/release/apps/server/glamdb \
  --report-root artifacts/clickbench-native
```

Native tests require localhost sockets, OpenSSL and sufficient disposable device
space. The native suite starts only owned temporary services/stores. The official
source acceptance is additionally reproducible with the published CLI recipes
against a local hits Parquet and `prepare --rows 1000`.

## Review and measurement limits

Independent reviews exercised failed-verification invalidation, changed unloaded
parts, altered snapshot preconditions, dropped replies/unbound refusals, active
SQL deadlines and failed-comparison accounting. Regressions accompany fixes.
Queries use Locust histograms/merged percentiles and include fresh candidate
staging in SQL reduction latency; source-oracle work is separate and remains
subject to the absolute operation deadline. Successful throughput excludes
refused/failed/cancelled work, whose consumed bytes/rows are preserved separately.

These runs used a shared Linux host and uncontrolled OS/device caches. Tiny
prefix throughput is not full-scale capacity or an official ClickBench score.
Full 1M/10M/reference campaigns, power-loss qualification, hard process RSS bounds
and server-native SQL are not established by this adapter. Larger ingestion uses
explicit persisted operation/history budgets and finite conversion/journal/disk
space; the removed adapter 4096 ceiling is not permission to bypass admission.
