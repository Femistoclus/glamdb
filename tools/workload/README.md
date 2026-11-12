# GlamDB CLI workloads

For the complete-width hits ingestion and size adapter, see
[ClickBench ingestion and storage reports](CLICKBENCH.md).

`glamdb-cli generate` creates reproducible bounded datasets. `bench write` and
`bench read` run the existing Python SDK through Locust. `bench verify` checks
the rows actually committed; `bench resolve` looks up uncertain operations.
These commands use the running native server. They never create or open a
database implicitly, reconcile it, or switch to a mock backend.
`bench tpch clean` drops the dedicated database identified by an explicit saved
run-state file; TPC-H generation and loading commands are not implemented.

## Install

The network client builds independently of Arrow, Iceberg and the engine:

```sh
git submodule update --init third_party/cli11 third_party/asio third_party/json third_party/reproc
cmake -S apps/cli -B build/client -G Ninja
cmake --build build/client --parallel 2
```

New workload commands additionally require Python 3.13 and the local SDK from
the same checkout. A small installation with resolved transitive dependencies:

```sh
python3.13 -m venv .venv
.venv/bin/python -m pip install -e sdk/python -e 'tools/workload[generate,bench,arrow]'
export GLAMDB_PYTHON="$PWD/.venv/bin/python"
export PATH="$PWD/build/client:$PATH"
```

For a hash-pinned development/test installation use:

```sh
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps -r tools/workload/requirements.txt
.venv/bin/python -m pip install --no-deps --no-build-isolation -e sdk/python -e tools/workload
```

Extras select Mimesis (`generate`), Locust/gevent (`bench`), and PyArrow (`arrow`).
The current SDK itself requires PyArrow 25; selecting a smaller workload extra
does not remove that SDK dependency. `request` and `create-store` continue to
work without Python. The CLI never downloads packages at runtime. Its Python
executable is `GLAMDB_PYTHON`, defaulting to `python3`.

## Prepare the target

Start a [native server](../../docs/development/HTTP_SERVER.md), set its bearer
token in `GLAMDB_TOKEN`, and create an open database and fixed-schema collection
using the SDK or existing `request` command. The server starts with no databases.
For example, the following SDK snippet creates a dedicated benchmark target:

```python
import os
from uuid import uuid4
from glamdb_sdk import Client, Schema, Field

url = "http://127.0.0.1:8080"
schema = Schema([Field("key", "string"), Field("value", "int64", True)])
with Client(url, os.environ["GLAMDB_TOKEN"]) as root:
    db = root.create_database("benchmark")["database_id"]
cid = str(uuid4())
with Client(url, os.environ["GLAMDB_TOKEN"], database_id=db) as client:
    assert client.create_collection("events", schema, collection_id=cid).outcome == "committed"
print(db, cid)
```

Write workloads require a dedicated **initially empty** collection with the
same schema as the dataset. Other writers must leave it alone through verify.
This makes a complete duplicate-aware content check possible without a missing
delete API or an assumed metadata/snapshot API. Native database operation and
storage limits still apply; the default operation budget is finite.

## Generate

Save `schema.json`:

```json
{
  "schema": {
    "fields": [
      {"name": "key", "type": "string", "nullable": false},
      {"name": "value", "type": "int64", "nullable": true}
    ]
  },
  "generation": {
    "key": {"profile": "unique", "prefix": "event-"},
    "fields": {
      "value": {"min": 9007199254740993, "max": 9007199254741993, "null_rate": 0.1}
    }
  }
}
```

```sh
glamdb-cli generate --schema schema.json --rows 1000000 --seed 42 --output dataset/
```

A plain SDK schema is also accepted, with deterministic defaults. Supported
types are `string`, `bool`, `int64`, `float64`, `timestamp_us_utc`, `decimal`, and
`date`. Numeric fields
accept `min`/`max`; timestamps accept exact microsecond integers or timezone-aware
RFC3339 values. Default dates cover 2020-01-01 through 2021-01-01, independent of
the clock. Floats must stay finite. Strings accept `length` (0–4096) and an
explicit Unicode `alphabet`. `null_rate` is allowed only on nullable fields.
Decimals require schema `precision`/`scale` and exact plain-text `min`/`max`
bounds. Generation samples integer coefficients and formats fixed-scale text,
including precisions above 2^53, without binary float conversion. Default
decimal bounds span the full declared precision. Date bounds use strict ISO
calendar text; generation samples whole days. Normalized manifest generation
bounds store decimal coefficients and date epoch-day counts as integers.
This is the existing scalar workload generator; no TPC-H adapter is provided.
For duplicate keys use `{"profile":"duplicates","distinct":100,"prefix":"key-"}`;
all duplicates are intended append rows, not replacements.

`--locale en`, `--part-rows 1000` and `--part-bytes 4194304` are the defaults.
Parts are bounded by both limits; at most 10000 rows and 8 MiB per part are
allowed. A million rows is not retained in memory. The output directory must
not exist. A failed generation leaves its partial directory for inspection;
do not use it without a completed `manifest.json`.

The ordered manifest records the schema, normalized settings, locale, seed,
Python/Mimesis/SDK versions, row counts, sizes and SHA-256 for every JSONL part.
Integers and timestamps use the SDK's exact wire codecs. Dataset readers verify
sizes and hashes. Reproduction requires the recorded versions as well as seed
and settings; retries use saved payloads, never regenerated random data.

## Write and read

```sh
glamdb-cli bench write --url http://127.0.0.1:8080 --database DATABASE_UUID --collection COLLECTION_UUID --dataset dataset/ --users 4 --spawn-rate 4 --duration 60s --report run/write/

# query.json contains SDK scan parameters; {} reads the whole fixed view.
glamdb-cli bench read --url http://127.0.0.1:8080 --database DATABASE_UUID --collection COLLECTION_UUID --query query.json --format arrow --users 2 --duration 60s --report run/read/

# Batch-native Arrow from four independent client processes, one merged report.
glamdb-cli bench read --url http://127.0.0.1:8080 --database DATABASE_UUID --collection COLLECTION_UUID --query query.json --format arrow --consume batches --processes 4 --users 32 --spawn-rate 32 --duration 60s --report run/read-batches/
```

`--users` is concurrency; `--spawn-rate` is new users per second, **not RPS**.
`--duration` accepts positive whole `s`, `m`, `h` values and is a maximum for
write runs: they finish earlier when every dataset part is consumed or a
write becomes uncertain. Each worker consumes its dataset once. The summary
states whether it consumed every part. Data generation, hash validation and
target/capability checks occur before measurement. Measured run throughput
includes file reads, SDK encoding, SQLite transactions, user startup and drain.
Locust append latency surrounds the SDK append call (HTTP send/receive and
response validation), excluding the pre-send journal transactions and the
outcome update. This measures the network SDK scenario. `summary.json`
additionally splits every attempted append into client time buckets, each with
count, total, mean, max and share of the client end-to-end time:
`input_load` (dataset part read, hash check and decode), `sdk_prepare`
(`prepare_append`: encoding, canonical hash, body), `journal` (pre-send
prepare/attempt SQLite transactions, row counting and the outcome update),
`http_send` (the same interval as Locust's `append` latency) and `end_to_end`
(from loading the part through the recorded outcome). The buckets do not
overlap; end-to-end also contains small bookkeeping outside them.

`--timeout 30` bounds each SDK operation/full scan. `--stop-timeout 3` permits
up to three seconds to finish in-flight calls before Locust cancels them. Ctrl-C
stops the Python process and returns 130. The bridge escalates termination
after five seconds and reaps the child. A forced kill can leave an unfinished
report; its durable pre-send operations remain available for resolve.

The current scan query supports `projection`, one typed `predicate`, `limit`,
and `snapshot_id`, following the SDK contract and advertised capabilities.
Arbitrary SQL/AND/OR and unavailable stats/files/snapshots/explain scenarios are
not offered. Every read mode consumes **all** pages of every scan. Each Arrow
page is a complete independent IPC stream; the SDK/PyArrow decode each page and
validate its identity and integrity. No byte-concatenated IPC export is used.
`--consume` selects how the client consumes the data, recorded as
`read_mode` in `summary.json` and in the Locust entry name:

| Options | `read_mode` | Locust name | Client work per scan |
| --- | --- | --- | --- |
| `--format json` (default) | `json-decoded-rows` | `scan/json/decoded` | SDK JSON decoding to Python rows |
| `--format arrow` | `arrow-python-rows` | `scan/arrow/decoded` | Arrow IPC decode plus `to_pylist()` per batch |
| `--format arrow --consume batches` | `arrow-batches` | `scan/arrow/batches` | Arrow IPC decode; counts rows and batch buffer bytes only |

`--consume rows` is the default and keeps the historical behaviour.
`--consume batches` requires `--format arrow`; it is the batch-native client
and never materializes Python values. None of these modes isolates engine
capacity: all include the SDK, HTTP and Python client costs. JSON response byte
counts are not invented; Arrow byte counts are the SDK's consumed stream bytes,
and batch mode adds in-memory `arrow_batch_bytes`.

`GLAMDB_TOKEN` stays in the environment, outside argv, manifests and reports.
Only loopback HTTP is permitted. Other endpoints require verified HTTPS.
`--ca-file CA.pem` adds the test/private CA; certificate verification cannot be
disabled. Redirects follow the SDK's rejection policy.

## TPC-H database cleanup

Save the server URL and both original UUIDs when creating a dedicated benchmark
database. The minimal run-state JSON format is:

```json
{
  "version": 1,
  "url": "http://127.0.0.1:8080",
  "database_id": "11111111-2222-4333-8444-555555555555",
  "store_id": "11111111-2222-4333-8444-555555555555"
}
```

Additional benchmark fields such as table identities may be retained in this
file. Keep credentials out of it; the command reads `GLAMDB_TOKEN` from the
environment. `database_id` and `store_id` must be the same original UUID.
Only loopback HTTP is allowed; other saved endpoints require verified HTTPS.

```sh
glamdb-cli bench tpch clean --state run/tpch.json
```

`--ca-file CA.pem` and `--timeout 30` are optional. This command needs the SDK
and workload package; it does not need Mimesis, Locust, or a new dependency.
It validates the saved state before sending one authenticated
`POST /databases/{database_id}/drop` with only `store_id` in its body. It prints
`dropped: true` only after the response contains the same saved identities and
confirms completed deletion. The entire database, including all eight TPC-H
tables and history, is removed. It does not perform row deletion, RF2,
`TRUNCATE`, or benchmark generation/loading.

A timeout or lost reply is ambiguous. The command leaves the state file
unchanged after every outcome, and sends no automatic retry. Run the same
command again with the same saved state to retrieve or finish the original
deletion. No name lookup is performed, so reusing the old database name cannot
redirect cleanup. Failed HTTP or protocol checks return a nonzero exit without
claiming cleanup. There is no force option or client filesystem removal.

## Reports and recovery

Locust produces `locust.html`, `locust.json`, and its CSV reports/history.
Locust owns latency histograms and combined worker percentiles. `run.json`
records target capabilities/limits, SDK source hash, dependency/client versions,
machine details and scenario parameters. Unavailable server build version is
`null`; client timings do not claim server-stage measurements.

`summary.json` supplements Locust with attempts, unique operation IDs, terminal
and unknown outcomes, confirmed rows/request-body bytes and their rates, full
scans/pages/rows, unresolved operations, per-worker CPU/RSS and PASS/FAIL/INCOMPLETE.

Admission refusals (`SERVER_BUSY`, `DATABASE_BUSY`, `RESOURCE_EXHAUSTED`,
`REQUEST_TIMEOUT`) and other failures are fired to Locust under separate request
types (`REFUSED`, `ERROR`); successes use `SDK`. Each Locust entry therefore holds
one population, and `latency_ms` in `summary.json` copies Locust's own
p50/p95/p99/avg/max for `success`, `refused` and `error` separately. Locust's
aggregated row still mixes them. `throughput` reports `offered_attempts`
(scans started or append sends), `successful_operations` (completed scans or
unique committed appends), `rejected_attempts`, the remaining unsuccessful
attempts and per-second rates; refusals and failures never count as successful
throughput. `rejections_by_code` and `errors_by_code` count failures by error
code. Refusals and errors still make the run FAIL as before; the separation only
changes how they are measured. A refused append keeps the conservative journal
handling below (it is not reinterpreted as definitely not committed).

`client_cpu` lists user+system CPU seconds and the share of one core for every
worker process (`controller` is a distributed master's own CPU). A process at or
above 85% of one core is marked `saturated`, and `generator_saturated` is true
when any is: the Python client, not the server, may then bound the result. Use
more `--processes` or a separate load host before interpreting such a run.
`pending.jsonl` lists unresolved IDs/states without copying row payloads into the report.
Only unique committed operations count toward confirmed throughput. Reading is
timed from opening a view through consuming/decoding its last page. A partial
scan contributes no completed-scan rows. Normal stop-timeout cancellations are
counted separately from failed scans.

Each worker owns `worker-*.sqlite` and a `worker-*.json` report. Before append,
SQLite DELETE/EXTRA commits the operation UUID, collection, canonical payload
hash, exact immutable SDK request bytes, a body SHA-256 and row count. Outcomes
are committed separately. The default per-worker request-payload budget is 1 GiB
(`--max-journal-bytes`); SQLite file/metadata overhead is additional. The journal
is local recovery evidence, not a server power-loss durability qualification.

```sh
# Look up every pending UUID. No writes or reconciliation are performed.
glamdb-cli bench resolve --url http://127.0.0.1:8080 --run run/write/

# Explicitly allow resending only IDs whose authoritative lookup is OPERATION_NOT_FOUND.
# The exact original UUID and validated bytes are retained.
glamdb-cli bench resolve --url http://127.0.0.1:8080 --run run/write/ --retry

# Full collection comparison, unordered and counting every duplicate.
glamdb-cli bench verify --url http://127.0.0.1:8080 --run run/write/ --format arrow
```

Committed and definitely-not-committed outcomes are terminal. Transport failures,
unknown outcomes, in-progress IDs and conflicts remain unresolved. A conflict
does not describe the original UUID's outcome. No operation is automatically
retried with a new ID. `commit_unknown` requires server/operator recovery before
lookup can resolve it; these commands never reconcile the database.

Resolve works after a process crash even without a final worker report. Keep
every worker journal. Verify uses a temporary bounded-cache SQLite multiset,
then removes it; it checks the committed prefix, not unsent dataset parts. A
missing worker journal or unresolved operation yields INCOMPLETE. Local reports
are bound to their original endpoint; resolving on another URL is rejected.

Exit codes: 0 PASS, 1 FAIL, 2 invalid configuration/usage, 3 INCOMPLETE,
4 missing interpreter/dependency, 130 interrupted. Individual `request` and
`create-store` commands retain their existing exit codes.

## Multiple client processes

One Python process is limited to about one CPU core. `--processes N` starts a
local Locust master and N fresh worker interpreters in one command; `--users`
are spread across them by Locust and the master writes one merged Locust and
`summary.json` report. Workers are ordinary distributed workers (below): each
owns a journal and, for writes, appends one full dataset copy. The control
socket uses `--master-host`/`--master-port` (default `127.0.0.1:5557`; choose
another free port for parallel runs). `--processes` excludes `--master` and
`--worker`. Fresh interpreters are used instead of Locust's fork-based
`--processes`, which is not safe on macOS. If the master is killed, workers stop
when they lose the master heartbeat.

## Distributed Locust workers

Use a shared filesystem visible at the same report path on all processes. Start
the command above with `--master --expect-workers 2 --master-host 127.0.0.1`, then
start two otherwise identical commands with `--worker --master-host 127.0.0.1`.
The master must have created `run.json` before workers start. `--master-port`
defaults to 5557; the master waits at most 30 seconds for the expected workers.
Locust's control connection is not an authenticated public service: use loopback
or a trusted tunnel/network. Workload API traffic still uses verified HTTPS.

Each worker gets a separate SQLite journal and appends one full dataset copy;
the metadata states this replication, and verify counts all copies. The master
alone writes shared Locust/summary output. Keep the master and workers alive
through the final report. Replacing/restarting a worker mid-run or losing its
journal makes the run INCOMPLETE; automatic job recovery is not implemented.

## Tests

Long-running capacity and concurrent-session campaigns are separate, opt-in
tests in [tests/stress](../../tests/stress/README.md). Run them explicitly with
`python tests/stress/run.py --profile capacity --output artifacts/stress-run-01`
from the repository root. They are excluded from default pytest/CTest discovery
and CI; the short checks below retain their current CI behavior.

```sh
bash scripts/check-workload.sh unit
GLAMDB_CLI="$PWD/build/client/glamdb-cli" GLAMDB_NATIVE_SERVER="$PWD/build/local/apps/server/glamdb" bash scripts/check-workload.sh
```

The default script requires the actual server; there is no mock fallback. E2E
creates a disposable data directory, database and collection, writes data larger
than one HTTP response, consumes JSON/Arrow pages over verified TLS, verifies
duplicate multiplicity, checks restart lookup and loses a response **after an
actual native commit** before resolving it without a duplicate write. Reports
remain under `artifacts/workload-native`. Explicit SDK transport doubles cover
unknown/conflict/timeout/retry failures in unit tests. Bridge tests exercise
argv paths, streams, missing dependencies, exit codes and Ctrl-C child cleanup.
Native normal CI runs this E2E after building the real server and standalone CLI;
the existing native sanitizer suites remain separate.

Dependencies: reproc/reproc++ 14.2.7, Mimesis 22.2.0 and Locust 2.47.0 use MIT;
gevent uses MIT, PyArrow uses Apache-2.0, and Python's sqlite3 follows the Python
distribution license (SQLite is public domain). Upstream notices are retained in
the submodule and installed distributions. Direct and transitive Python pins
and artifact hashes are in `requirements.txt`; reproc's commit is in
[`third_party/lock.json`](../../third_party/lock.json).
