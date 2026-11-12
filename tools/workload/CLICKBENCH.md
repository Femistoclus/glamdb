# ClickBench network benchmark

`glamdb-cli bench clickbench` manages a dedicated database containing the complete
105-column `hits` collection. Like `bench tpch`, it prepares a reproducible source,
creates a target, loads disjoint immutable requests, verifies rows and receipts,
runs timed network workloads, resumes original work and cleans only its own target.

New targets default to **append**. `init --mode kv` creates a KV target with the
same 105 source fields plus a physical string `key`. The key is the zero-based
source row ordinal, encoded as 20 decimal digits. Duplicate WatchID/UserID values
and duplicate complete source rows remain separate rows. Query projections use
the original 105 logical columns, including Q24 `SELECT *`.

## Install and connect

Build the standalone network CLI and install the same-checkout Python 3.13 SDK
and workload package as described in [the workload guide](README.md#install).
PyArrow 25 is required. Ingestion, verification and sizes do not import DuckDB or
SQLGlot; those are optional query-planning/reduction dependencies:

```sh
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps \
  -r tools/workload/requirements-clickbench.txt
export GLAMDB_PYTHON="$PWD/.venv/bin/python"
export PATH="$PWD/build/client:$PATH"
```

Set `GLAMDB_TOKEN` through your normal secret mechanism. Credentials never go in
command arguments, state, journals or reports. Remote connections require verified
HTTPS; loopback HTTP is supported. Pass `--ca-file CA.pem` to online commands for
a private CA. The real native backend is required by default; `--test-backend mock`
is an explicit protocol-test profile and never qualifies performance.

The benchmark currently supports POSIX hosts and local advisory run locks. One
coordinator owns a saved state; concurrent writers must use separate fresh targets.

## Complete append or KV lifecycle

Use the official [hits Parquet](https://datasets.clickhouse.com/hits_compatible/hits.parquet)
or a local copy. A deterministic small prefix retains every column:

```sh
mkdir -p ./clickbench
glamdb-cli bench clickbench prepare \
  --source /data/hits.parquet --output ./clickbench/dataset \
  --type-profile native --rows 10000 --part-rows 1000

glamdb-cli bench clickbench init \
  --url http://127.0.0.1:8080 --dataset ./clickbench/dataset \
  --name clickbench-append --state ./clickbench/append.json

glamdb-cli bench clickbench load --state ./clickbench/append.json \
  --workers 2 --batch-rows 1000 --batch-bytes 8388608 \
  --report ./clickbench/append-load.json

glamdb-cli bench clickbench status --state ./clickbench/append.json
glamdb-cli bench clickbench verify --state ./clickbench/append.json --format arrow
glamdb-cli bench clickbench verify --state ./clickbench/append.json --format json
glamdb-cli bench clickbench size --state ./clickbench/append.json \
  --report ./clickbench/append-size.json --include-footers
```

To compare KV, reuse the immutable prepared dataset and create a different target:

```sh
glamdb-cli bench clickbench init \
  --url http://127.0.0.1:8080 --dataset ./clickbench/dataset \
  --name clickbench-kv --state ./clickbench/kv.json --mode kv
glamdb-cli bench clickbench load --state ./clickbench/kv.json \
  --workers 2 --batch-rows 1000 --report ./clickbench/kv-load.json
```

The server must advertise 105 schema fields for append or 106 for KV, the selected
DATE/timestamp types, managed database creation, paged scans, and append or KV
upsert capabilities. A repeated `init` preserves the saved mode when `--mode` is
omitted. A different explicit mode requires a new state and target.

`load` freezes source ranges, actual request row/wire bounds, content hashes,
operation UUIDs and worker assignments in SQLite before sending. Each source
range is published once globally; extra workers never multiply the dataset.
Locust owns scheduling and latency statistics. One publication gate matches the
engine's serialized writer; increasing workers does not claim parallel commits.
Each request includes the preceding snapshot precondition. Foreign writes,
changed source/plan/schema/mode or edited bodies invalidate the run.

A complete load independently checks every source row, prepared request and
persisted authoritative receipt, then scans the entire pinned result into a
bounded disk-backed duplicate-aware verifier. KV additionally checks every
ordinal key and insert-only live-row delta. `load_complete` is set only after
that verification succeeds. Explicit verification failures revoke completion.

Load reports use one JSON/CSV/Markdown stem. They include newly confirmed rows,
actual journaled request bytes, ingestion wall time, rows/s, Locust latency,
worker/publication settings, journal size and separate planning/verification time.
KV reports retain input/affected/written/removed row and file counters, so COW
work is visible. Source preparation and verification are excluded from ingestion
latency; source reads, encoding, durable journal updates and HTTP writes are included.
A no-op resume reports zero newly confirmed rows and zero rows/s.

## Recovery and cleanup

A lost response stops dependent publication. After the database is authoritatively
ready again, look up original IDs and resume the saved plan:

```sh
glamdb-cli bench clickbench resolve --state ./clickbench/kv.json
glamdb-cli bench clickbench load --state ./clickbench/kv.json --resume
```

Omit load tuning options on resume to retain the original row/byte/worker settings.
An explicit different setting is rejected. Only an authoritative not-found lookup
permits the explicit `resolve --retry` command to resend verified original bytes
with the original operation ID. Admission refusals remain resolvable; permanent
bound failures remain failures. There is no automatic retry or reconciliation.
Keep source parts, state and the adjacent `*-artifacts` SQLite journal directory.
Older append run identities and original request bodies remain usable.

```sh
glamdb-cli bench clickbench clean --state ./clickbench/kv.json
```

Cleanup uses the original database/store identity, supports exact repetition and
retains local source, journals and reports. To reload, create a new target/state;
never select an existing target only by a familiar name.

## Timed Q1..Q43 workloads

Fetch the pinned upstream queries and their original license into a separate cache:

```sh
glamdb-cli bench clickbench fetch-queries --output ./clickbench/reference

glamdb-cli bench clickbench scan --state ./clickbench/append.json \
  --catalog ./clickbench/reference --queries all --mode scan \
  --users 1 --processes 1 --warmup 1 --repeat 3 \
  --report ./clickbench/append-scan.json

glamdb-cli bench clickbench query --state ./clickbench/kv.json \
  --catalog ./clickbench/reference --source /data/hits.parquet \
  --ids all --mode both --users 1 --processes 1 --warmup 1 --repeats 3 \
  --report ./clickbench/kv-query.json
```

`scan` and `query` share a campaign implementation. `scan` defaults to native scan
workloads; `query` defaults to scan plus reduce. Both accept `--queries`/`--ids`
and `--repeat`/`--repeats`. Select a subset such as `--queries 1,20,24,30,43` for
concurrent or long campaigns; add users/processes within declared client/server
budgets. Locust schedules finite complete query rounds and merges its native
histograms/percentiles. Successful, refused, failed, cancelled and partial work
remain separate. Reports identify user/process counts, source/profile, mode,
snapshot, hardware/dependencies, coverage and client resource usage.

Native scans push safe typed predicates and the minimum sufficient projection,
consume every page and report candidate rows/bytes. They are scan workloads,
not SQL answers. Q20 consumes every native UserID match; Q24 reads all 105 source
columns and Q30 reads one input column. Partial OR/NOT and final LIMIT before
residual sorting are never pushed.

Reductions are explicitly **GlamDB + DuckDB**. Each query streams fresh network
batches into a bounded temporary DuckDB relation, executes the residual SQL,
consumes the result and releases that relation. Staging is included in latency;
there is no permanently preloaded client copy. This also keeps SDK I/O outside
native DuckDB Arrow callbacks during concurrent execution. An independent PyArrow
reader over the original hash-verified source prefix supplies the oracle. Exact
integers/dates/strings, float tolerances (rtol=1e-10, atol=1e-12), legal top-N ties,
unordered LIMIT and UTF-8 byte-length STRLEN semantics are checked outside the
per-query timed path. Campaign wall time includes that between-query validation.

Default per-query bounds are 512 MiB DuckDB memory, 2 GiB spill, 8 GiB candidates,
100,000 result rows and 64 MiB results. These are finite engine/input budgets,
not a hard Python RSS limit. Per-query CPU deltas are process measurements and
overlap across concurrent users; additive worker CPU totals are reported separately. Explicit resource errors retain unsuccessful status
and never become valid timings. Timeouts/cancellation unwind cursor and owned
worker processes. Warmups do not claim cold OS/device caches.

## Source fidelity and full-scale capacity

The pinned [ClickBench revision](https://github.com/ClickHouse/ClickBench/tree/7dd9941f45f5f0af088cdaf8b9da3a6156a18bdc)
has 99,997,497 rows and 105 columns. The observed official Parquet is 14,779,976,446
bytes; manifests record the actual hash, footer schema/counts and dependency
versions, since the URL alone does not pin bytes. `--rows N` is always labelled
a prefix. Omitting it converts the complete source. Downloading by URL still
retrieves and hashes the entire source even for a small prefix.

The official EventDate UInt16 is **days since 1970-01-01**; timestamp integers
are epoch seconds. Native DATE and explicit `legacy-date` UTC-midnight profiles
are supported. A separately typed signed-int64 YYYYMMDD input remains supported
for older fixtures. Physical schema determines the conversion; values never
trigger a unit-magnitude guess. Signed IDs/hashes preserve all int64 bits, strings
require valid UTF-8 and required NULLs are rejected.

Defaults remain finite: source 32 GiB, converted JSONL 8 GiB, journal request
payload 1 GiB, prepared part 4 MiB/10,000 rows (up to 8 MiB), and 100,000 parts.
The complete source needs much larger explicit converted/journal/disk budgets;
small defaults are not evidence that the full source fits. Source, converted
parts, journals and verification scratch may reside on different caller-owned
devices. Account for SQLite/index overhead and retained server files separately.

`init --limits limits.json` forwards supported database operation/history limits.
There is no adapter-only 4096-operation ceiling. Before writes the actual frozen
plan checks permanent operation count (including creation), retained snapshot,
data-file/manifest and recovery-workspace lower bounds plus journal payload bytes
against effective persisted limits. A larger run must raise the coordinated
budgets described in [scalable history](../../docs/SCALABLE_HISTORY.md),
not only `max_operations`. Byte-dependent server storage/device/shared-resource
admission remains authoritative; no receipts/history are silently discarded and
there is no automatic switch to bulk or sharding.

The full source needs at least 10,000 ordinary writes; row/wire splits can require
many more. Select finite budgets for the **actual plan**, with capacity headroom,
and retain failed bound operations. This benchmark exposes engine limits rather
than hiding them. Small authentic-source prefix runs have been exercised; the
full 99,997,497-row source and 1M/10M campaigns have not been run. Full-data
acceptance and resource measurements must be reported separately.

## Sizes and provenance

Size output separates source/converted/journal bytes, selected-snapshot Parquet
file lengths, file/history counts and optional compressed/uncompressed chunks.
Parquet-only bytes are never labelled complete database size. Servers advertising
`storage_footprint` also return the complete current physical owned-store inventory
separately from the selected snapshot. [Inventory semantics](../../docs/STORAGE_FOOTPRINT.md)
cover historical/unreferenced files, catalog and allocated blocks. An explicit
`size --owned-store-dir /srv/glamdb/databases/STORE_UUID` can independently inspect
caller-owned same-host files; the walk is not atomic under concurrent writes.
The shared registry remains service-scoped and is excluded from store totals.

Keep codec comparisons in separate fresh append/KV targets; changing settings on
existing files does not produce a uniform fresh variant. Fetched upstream SQL
retains its CC BY-NC-SA 4.0 attribution/license and checksums. No official native
SQL ClickBench score or interchangeable engine-only timing is claimed.

Exit codes: PASS 0; FAIL 1; configuration 2; BLOCKED/INCOMPLETE 3; missing dependency 4.
