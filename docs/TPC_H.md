# TPC-H scan and client-reduction workloads

`glamdb-cli bench tpch` generates a reusable reference dataset, explicitly
creates a dedicated managed target, loads and verifies every table, executes
fixed-view scans, resolves uncertain writes and deletes the exact target.
It uses the existing Python SDK through the standalone CLI's reproc bridge.
The client executable still builds without Arrow, Iceberg or engine linkage.

This is a TPC-H-derived scan/reduction workload. It does not implement an SQL
engine, general joins, refresh functions or official TPC-H compliance, and its
reports are not QphH results. `scan` measures query source access patterns;
`reduce` completes Q1/Q6 with explicitly reported client computation.

## Installation and generator provenance

Install the SDK and workload package from the same checkout as described in
[the workload guide](../tools/workload/README.md), then select that interpreter:

```sh
export GLAMDB_PYTHON="$PWD/.venv/bin/python"
export GLAMDB_TOKEN='your-server-token'
```

Generation uses the pinned TPC-H Tools 3.0.1 source package from the
[Cloudberry mirror at commit `392ba0c`](https://raw.githubusercontent.com/cloudberry-contrib/TPC-H-Toolkit/392ba0c26dae888fcbc27b68b5626182e4d744b5/00_compile_tpch/TPCH-software-code-3.0.1.zip).
The official download requires registration; these mirrored archive bytes have
not been independently compared with an official ZIP. The package's `release.h`
reports internal version 3.0.0 build 0. Manifests retain both package and reported
versions and explicitly identify the mirror origin.

| Pinned artifact | SHA-256 |
| --- | --- |
| Source archive | `23fd9a525cd19ffe81089f0c3cc5ba4deedfcd203adfbc5b0135d2005f4f2d0a` |
| Aggregate source-file inventory | `6876073d91b143511c280893da863eeb6a28162e66c2adfbcc00ac82cfb3adb7` |
| `dists.dss` | `4dc56300de1e5ac5e1900519794538e2b5d1b6caf5cd55703bf53000dd748876` |

Download and hash-check that archive separately, extract it and build from the
unmodified package's `dbgen` directory:

```sh
make -f makefile.suite CC=gcc DATABASE=ORACLE MACHINE=LINUX WORKLOAD=TPCH dbgen
```

Keep the pinned `.c`/`.h` files and `makefile.suite` beside the executable;
copying only a binary is refused. `--distributions` defaults to sibling
`dists.dss`; an explicitly supplied file must match the pinned digest.
`--dbgen-sha256 HEX` additionally checks the executable's build-specific digest.
The adapter records executable/source/distribution digests, exact scale,
partition configuration, schemas, conversion rules, dependency versions and
every part's row count, size and SHA-256. It does not substitute Mimesis columns.
Retain these generator inputs with the manifest when reproducing a run.

```sh
glamdb-cli bench tpch generate --scale-factor 0.01 \
  --dbgen /path/to/tpch-3.0.1/dbgen \
  --distributions /path/to/tpch-3.0.1/dists.dss \
  --type-profile native --generator-parts 1 --output /tmp/tpch-small
```

`--scale-factor` remains exact decimal text; it is not rounded through a float.
Recognized sizes are 0.01, 0.1 and integral values from 1 through 100,000,
subject to the finite metadata profile below.
Fractional and other nonstandard sizes are labelled development profiles;
accepting a scale does not qualify its capacity. `--generator-parts N` partitions
one global scale using reference DBGEN partition rules. It never invokes
independent full datasets per worker.
REGION/NATION remain fixed, sparse order keys and cross-table relationships
come from DBGEN, and LINEITEM retains the actual generated count.
Scale 0.001 is refused: the pinned generator produces duplicate PARTSUPP
composite keys at that size, so it cannot satisfy the workload's logical-key
verification contract.

Generation streams source rows into bounded JSONL parts. Defaults are 10,000
rows and 8 MiB per part, configurable through `--part-rows`/`--part-bytes`.
Part rows must be 1–10,000 and part bytes 1 KiB–8 MiB. Generator partitions must
be positive and no greater than the global supplier count or 100,000.
The output directory must be new. A failed generation leaves incomplete
artifacts and a top-level `manifest.json` with `complete:false` for inspection.
Only successful conversion of all eight tables publishes `complete:true`.
Each table has its own `TABLE/manifest.json`; `generator.log` records generator
output. Generation does not resume or overwrite an existing output directory.
Loading validates the completed dataset, schemas, part sizes and checksums before
creating an immutable load plan.

The initial metadata profile permits 65,536 parts overall, a 1 MiB top manifest,
32 MiB per-table manifests and 64 MiB of table manifests overall. These limits
bound descriptors as well as row buffers. Obvious oversized plans, including
SF=1 with one-row parts, fail before DBGEN starts; byte-driven overflow leaves an
incomplete dataset. Readers bound bytes before parsing. SF=1/SF=10 with default
parts fit the descriptor lower bounds; this does not qualify their complete
campaigns. Larger layouts need a separately declared metadata profile or a
streaming part index. Generation never truncates a dataset to fit.

## Schemas and scale

All 61 source fields, their canonical names/order, required values, comments,
addresses and other strings are retained. The schema/conversion authority is
[`tpch_schema.py`](../tools/workload/src/glamdb_workload/tpch_schema.py).
PARTSUPP uses `(part, supplier)` and LINEITEM uses `(order, line number)` as
composite logical keys. Generated datasets retain exactly those 61 fields.
`init --mode kv` adds a required string `key` to each target schema without
rewriting the source dataset. All source field IDs, types and values remain
unchanged; `key` has the next field ID. Append targets retain the source schema.

KV keys encode the natural primary key: add 2^63 to each signed int64 component,
format it as exactly 20 decimal digits, then join components with `:`. For
example, LINEITEM `(order=1, line=23)` becomes
`09223372036854775809:09223372036854775831`. This encoding is unambiguous and
preserves numeric tuple order as bytewise key order, including sparse order
keys. Both type profiles use the same encoding (`int64-offset-decimal-v1`).

| Table | Columns | SF=1 rows | Scaling |
| --- | ---: | ---: | --- |
| REGION | 3 | 5 | Fixed |
| NATION | 4 | 25 | Fixed |
| SUPPLIER | 7 | 10,000 | 10,000 × SF |
| CUSTOMER | 8 | 150,000 | 150,000 × SF |
| PART | 9 | 200,000 | 200,000 × SF |
| PARTSUPP | 5 | 800,000 | 800,000 × SF |
| ORDERS | 9 | 1,500,000 | 1,500,000 × SF |
| LINEITEM | 16 | 6,001,215 | Actual generator count, approximately 6,000,000 × SF |

SF=1 has 8,661,245 rows overall. Reference SF=10 has 59,986,052 LINEITEM rows
and 86,586,082 rows overall. Scale factor does not describe JSON, Parquet,
journal or physical filesystem bytes; record those measurements separately.

Explicit `--type-profile` selection is required:

| Profile | Decimal fields, including quantity/discount/tax | Date fields |
| --- | --- | --- |
| `native` | DECIMAL(15,2), parsed with exact coefficients | Native DATE |
| `legacy-scaled` | Int64 coefficient ×100: 123.45 → 12345, 0.06 → 6, 17.00 → 1700 | UTC-midnight `timestamp_us_utc` |

Money, quantities and rates never pass through binary floating point. The
manifest retains logical types/scales for both profiles, and query literals
and reducers follow the selected profile. Native decimal/date support is
described in [scalar types](SCALAR_TYPES.md).

## Managed target, capacity and loading

Start an authenticated [native server](HTTP_SERVER.md). Tokens remain in
`GLAMDB_TOKEN`, outside arguments, manifests, state and reports. Only loopback
HTTP is permitted; other endpoints require verified HTTPS. Network commands
accept `--ca-file PEM` and a positive finite `--timeout`: 30 seconds by default,
or 300 for verification and scans. There is no TLS-verification bypass.

```sh
glamdb-cli bench tpch init --url http://127.0.0.1:8080 \
  --dataset /tmp/tpch-small --name tpch-small-run1 --state /tmp/tpch-run.json --mode kv
glamdb-cli bench tpch load --state /tmp/tpch-run.json \
  --batch-rows 10000 --batch-bytes 8388608 --workers 2
glamdb-cli bench tpch status --state /tmp/tpch-run.json
```

`init` explicitly creates one managed database and eight fixed-schema
collections, checks capabilities/readiness and saves original endpoint and
database/store/collection identities. Creation is distinct from reopening.
An uncertain creation reply returns INCOMPLETE; repeating explicit `init` uses
the saved identities to finish creation rather than allocate another target.
`--mode append|kv` selects the immutable collection mode. New targets default
to `append`; use `--mode kv` to measure full-row upserts. An omitted mode on a
repeated init keeps the saved mode. Legacy states and plans without a mode
remain append; an explicit conflicting mode is refused before lifecycle writes.
Load, resolve, verify and scan always use the saved mode. The scan subcommand's
`--mode scan|reduce` independently selects query behavior.

`--table-prefix PREFIX` labels collection names; benchmark metadata stays in
local state/journals and does not consume a ninth collection.

`--limits FILE` supplies an explicit JSON object using the server's public
limits fields. It is read by the Python adapter before target creation.
The initial eight-table layout fits the default collection count; larger
collection profiles are documented in [collection profiles](COLLECTION_PROFILES.md).
The [retained-history guide](SCALABLE_HISTORY.md) describes the defaults, which
no longer bind SF=10 by operation or history counts; the explicit
[`benchmarks/profiles/tpch-sf10.json`](../benchmarks/profiles/tpch-sf10.json)
profile (lower than the current defaults for most counts) is kept to reproduce
the recorded SF=10 measurements. Configured limits are not universal capacity
guarantees.

The lifecycle depends on managed deletion (#153), native decimal/date support
for the native profile (#154), and sufficient configurable operation/history
budgets for larger scales through the [retained-history profile](SCALABLE_HISTORY.md). Optional extra eight-table variants require
larger collection capacity (#155); the canonical dataset needs only eight.

Ordinary append and upsert retain the 10,000-row / 16 MiB input ceiling. Load defaults
to 10,000 rows / 8 MiB per request and can split source parts under both caps.
Actual source part boundaries, byte limits and existing bound failures matter:
at 10,000 rows/request the row-count lower bounds are 869 operations for SF=1
and 8,661 for SF=10; 1,000-row source parts already require at least 8,664 for
SF=1. The load preflight checks planned counts against advertised operation and
history capacity, including retained manifest counts and the native recovery
workspace envelope `8192 * (1 + snapshots + manifests + data_files)`.
These are lower-bound checks; manifest merging and KV COW rewrites can add retained metadata/data files.
Physical byte, device, catalog, shared-memory and free-space
admission still occur on the server. Insufficient capacity produces a refusal;
the workload never silently raises limits, shards a target or expires history.

`--workers N` assigns disjoint immutable chunks through the load scheduler.
There is one write authority and effective publication concurrency is one.
This differs from generic `bench write`, whose workers each consume a full
dataset copy. Additional load workers do not multiply rows or promise parallel
engine commits. Both write modes use JSON even when scans use Arrow.

Before sending, load freezes each chunk's table, part ordinal/range, worker
assignment and operation UUID. The UUID derives from the original load and
collection identities and chunk boundaries, independent of worker execution.
The local SQLite journal commits the exact prepared SDK body, canonical hash
and request-body checksum before sending, then records its outcome. The journal
also preserves the mutation kind; old journals migrate as append without
changing any original request bytes. The frozen plan binds the collection mode
and, for KV, the key encoding. Identities
and plans remain immutable; progress and receipts are updated durably.

Keep the generated dataset, state file and adjacent journal throughout recovery.
Do not edit their identities, boundaries or hashes, or run other writers against
the target. Resume rejects changed inputs/targets and unexpected foreign writes.
No load command reconciles the database implicitly.

## Recovery, verification and fixed views

```sh
# Inspect original operations without resending writes.
glamdb-cli bench tpch resolve --state /tmp/tpch-run.json

# Explicitly permit the original verified request after authoritative not-found.
glamdb-cli bench tpch resolve --state /tmp/tpch-run.json --retry

# Retain the same original batch/worker configuration when resuming.
glamdb-cli bench tpch load --state /tmp/tpch-run.json \
  --batch-rows 10000 --batch-bytes 8388608 --workers 2 --resume

glamdb-cli bench tpch verify --state /tmp/tpch-run.json --format arrow --level full
```

A timeout, disconnect or malformed reply does not prove rollback. Uncertainty
stops dependent progress. Lookup uses the original operation UUID; only explicit
`--retry` may resend an authoritative not-found request with its original body.
Committed and definitely-not-committed outcomes retain their original meanings.
An admission refusal can occur before the server binds an operation identity.
Such a POST enters `failure_lookup_required`: authoritative lookup must establish
whether the failure was persisted or the ID remains absent. Restoring capacity
does not authorize an automatic resend. The journal retains each attempt's
outcome, phase, error code and retry hint; authoritative bound failures remain
terminal.
Unresolved work yields INCOMPLETE. Required server recovery remains an explicit
operator action; the workload never creates a new request identity to hide it.

After all chunks are confirmed, load performs full verification before setting
`load_complete`. Verification compares every table's complete row multiset,
including duplicate multiplicities, with the local dataset and checks receipts.
KV verification also compares every derived key. Since source primary keys are
unique and targets begin empty, each upsert must create a snapshot and its
`rows_written - rows_removed` must equal `input_rows`. A replacement or no-op
cannot be counted as newly loaded data.
Load, resolve and verify validate the complete frozen plan checksum and its
state/journal inventory. Verification checks every original receipt and the
initial empty, first committed and final table snapshots. Removing an earlier
chunk or its receipt cannot turn a partial journal into a successful result.
`--level rows` performs these content/receipt checks; `--level full` additionally
checks logical primary/composite keys and foreign keys with disk-backed bounded
working memory. `--format json|arrow` chooses the SDK scan encoding.

Completed state retains each table's final snapshot. All timed scans and later
verification bind this vector of eight fixed views, drain every page to EOS and
close/cancel owned cursors. There is no assumed global cross-table transaction.
Historical snapshot and operation retention remain server responsibilities.
`status` exposes original identities, completeness, unresolved work, selected
snapshots, capabilities and limits without rebinding the run to another server.
Its report includes `collection_mode`. Load metrics and scan reports carry the
same mode so append and KV measurements remain distinguishable.
Its journal connection is read-only and can inspect an active run. Mutating
lifecycle commands and cleanup hold the same exclusive local run lock.

## Query coverage and measurements

```sh
glamdb-cli bench tpch scan --state /tmp/tpch-run.json \
  --queries all --mode scan --format arrow --consume batches \
  --users 1 --processes 1 --warmup 1 --repeat 5 --report /tmp/tpch-scans

glamdb-cli bench tpch scan --state /tmp/tpch-run.json \
  --queries 1,6 --mode reduce --format arrow --consume batches \
  --users 1 --warmup 1 --repeat 5 --report /tmp/tpch-reduce
```

Versioned query descriptors record sources/aliases, projections, safely pushed
predicates, fixed qualification substitution parameters, residual operators and
snapshot bindings. Q11's threshold retains its scale-dependent fraction.
Predicates include typed comparison, IN, NULL checks and compound AND/OR/NOT.
Unsupported patterns and column-to-column expressions remain explicit residual
work; filters must retain a conservative superset of required source rows.

| Queries | Engine scan coverage | Result coverage |
| --- | --- | --- |
| Q1 | LINEITEM ship-date bound and aggregate/grouping inputs | Complete client grouping, exact sums/averages/counts and ordering in `reduce`. |
| Q6 | LINEITEM date, discount and quantity filters; revenue inputs | Complete exact client revenue sum in `reduce`. |
| Q2–Q5, Q7–Q22 | Every required source table, projected columns and safely expressible local predicates | Access patterns in `scan`; joins, residual expressions, grouping, sorting and other SQL work are reported but not evaluated. |

`scan` reports returned candidates by table/stage, pushed filters and residual
work, with `result_verified=false` for the SQL answer. `reduce` labels grouping
and arithmetic as client work and compares Q1/Q6 results with an independent
evaluation of the same local source and parameters. Decimal arithmetic remains
exact, including legacy coefficients and SQL empty-input behavior. Unsupported
reductions are explicit; `--mode reduce --queries all` never silently skips or
downgrades Q2–Q5/Q7–Q22.

`--queries` accepts `all` or comma-separated Q1–Q22 numbers. Defaults are one
Locust user, one client process, one warm-up and five measured repetitions.
`--warmup 0` is allowed; other counts must be positive. JSON consumes decoded
rows; `--consume batches` requires Arrow. Locust owns scheduling and latency
statistics. Each user runs complete warm-up query rounds followed by complete
measured rounds. `--users` is the total across fresh worker interpreters; multiple
processes use a local Locust master and workers. Warm-ups do not enter measured
latency histograms, but their failures remain visible. Cache state is uncontrolled;
warm-up does not establish a cold-cache or physical-I/O measurement.

`run.json` retains immutable settings, exact plans/parameters, dataset identity,
fixed snapshots and advertised server capabilities/effective configuration.
Record server build identity separately; the native acceptance supervisor records
the executable SHA-256. `summary.json`
contains status, per-query/source-stage rows/pages, Arrow wire/batch bytes,
client computation/validation time and merged Locust latency.
`worker-*.json`, `locust_stats.csv`, `locust_failures.csv`,
`locust_exceptions.csv` and `locust.html` retain worker and Locust evidence.
Warm-up and source-result validation are separate from measured repetitions.
Results include SDK/network/Python costs and do not isolate storage-engine
throughput.

Load reports separate plan preparation, preflight, append publication and final
verification durations. Preflight time includes planning and source validation;
it is not an additional disjoint duration. Byte counters distinguish source
`.tbl`, converted JSONL, unique committed request bodies, actual compressed data
bytes from receipts, and the local journal. A missing receipt byte field is
reported as unavailable. Resume throughput counts only new committed work in
that invocation. Physical store allocation and server build/resources require
separate supervisor measurements, as in the native acceptance evidence.

The controller owns a private process group for Locust. CLI SIGINT/SIGTERM
unwinds the controller and stops users immediately so SDK cursor cancellation
runs before the process group's two-second termination deadline. It
reaps the child before the native bridge's five-second allowance. Repeated
signals cannot bypass that cleanup.
Normal distributed completion has a separate five-second terminal-report drain
and a six-second outer stop bound. This keeps the master's receiver alive for
worker statistics without delaying user cancellation.

## Exact cleanup

```sh
glamdb-cli bench tpch clean --state /tmp/tpch-run.json
```

Cleanup validates the saved original URL, `database_id` and matching `store_id`,
then sends one authenticated managed drop request. It reports success only after
the returned identities and `dropped: true` match. A lost reply is ambiguous;
rerun the same command with the same state to retrieve/finish the original
deletion. Name reuse cannot redirect cleanup. There is no automatic retry,
force option or client filesystem removal.

The server removes the entire dedicated database, all eight tables and history.
Generated data, local state, journals and reports remain. Cleanup is neither RF2
nor row deletion. See [managed deletion](DATABASE_DROP.md) and the
[minimal cleanup state format](../tools/workload/README.md#exact-tpc-h-database-cleanup).

Workload exits are 0 PASS, 1 FAIL, 2 invalid configuration/usage, 3 INCOMPLETE,
4 missing interpreter/dependency, 130 interrupted by SIGINT and 143 by SIGTERM.
Generation returns 0 only
for a completed manifest; cleanup returns 0 only for a validated drop response.

## Comparing append and KV

Use the same generated dataset, explicit row/byte/worker settings, server limits,
transport and scan parameters for two separate states initialized with
`--mode append` and `--mode kv`. Alternate run order, use fresh targets, retain
all raw reports and compare several repetitions. Do not compare different
source/type profiles or interpret warmup as a controlled cold-cache experiment.

`elapsed_seconds`/`rows_per_second` cover the publishing loop; planning,
preflight and full verification are separate, and `command_elapsed_seconds`
includes them. `committed_parquet_bytes` sums uniquely committed files written,
including retained COW history. It is not the size of only the final live view.
KV metrics additionally expose `input_rows`, `rows_affected`, `rows_written`,
`rows_removed`, added/removed file counts and bytes removed from the live view,
plus `row_write_amplification = rows_written / input_rows`. Removing a file from
the live view does not reclaim its historical bytes.

Scan projections continue to select original TPC-H fields. Compare per-query
latency for the same scan/reduction mode; scan candidates and exact Q1/Q6
reductions are different workloads. Neither result is an official TPC-H score.
Append remains the default; select KV explicitly for a separate comparison.

## Benchmark scope

Q1–Q22 scan workloads measure source access patterns; only Q1/Q6 provide complete
client reductions. Small SF=0.01 append/KV lifecycles have been exercised.
Full SF=1/SF=10 CLI HTTP campaigns and physical power-loss qualification have
not been run. Earlier embedded retained-history measurements are a different
workload and do not qualify this network command family. No official TPC-H
compliance or QphH result is provided.
