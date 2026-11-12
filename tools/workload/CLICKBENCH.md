# ClickBench ingestion and storage reports

This adapter preserves all 105 columns of the pinned ClickBench hits schema.
It uses the existing SDK, ordinary append requests, PyArrow, and durable SQLite
journals. It does not write Parquet or Iceberg files behind the server.

Implemented commands: prepare, init, load, status, resolve, verify, size, clean.
All 43 pinned query scan/reduce adapters are implemented. Ingestion uses one coordinator. Generic Locust write workers each replay their
whole dataset; do not use that replication policy for this single hits import.

## Prerequisites and scope

Build the network CLI and install the same-checkout SDK/workload package as in
[README.md](README.md#install). Python 3.13 and PyArrow 25 are required. This
adapter currently supports POSIX hosts (Linux/macOS), including advisory local
file locks. There is one loading coordinator per run.

The server must advertise a real local backend and at least 105 schema fields
([#161](https://github.com/glamourdb/glamdb/issues/161)). Mocks require the explicit
init flag `--test-backend mock`; that choice is persisted and every result is
excluded from native performance qualification. There is no fallback to mocks.
Credentials come from `GLAMDB_TOKEN`, never from saved state or command options.
Remote connections require verified HTTPS; HTTP is allowed only on loopback.

The current HTTP database profile accepts at most 4096 operations, including
collection creation. The complete 99,997,497-row source requires at least 10,000
ordinary appends and is blocked by [#156](https://github.com/glamourdb/glamdb/issues/156).
This adapter rejects an oversized profile before creating a database. Raising
only an operation count is not a validated history/catalog capacity solution.

Choose the `native` profile to retain EventDate as native DATE, or the explicit
`legacy-date` profile to convert it to a UTC midnight timestamp. All other logical
timestamp fields use UTC. Source integer
timestamps are epoch seconds; integer EventDate values are YYYYMMDD. Dates and
logical timestamps are converted without a magnitude-based unit heuristic.
Numeric identifiers, including WatchID and UserID, remain exact signed int64;
overflow is an error. No synthetic `key` is added. Strings require valid UTF-8,
required NULLs are rejected and duplicate rows are retained. The chosen type
profile is stored in the immutable dataset manifest and cannot change on resume.

## Prepare a small complete-width dataset

Obtain the real source from
[the ClickHouse dataset host](https://datasets.clickhouse.com/hits_compatible/hits.parquet).
Preparation accepts that HTTPS URL or an already downloaded Parquet file. URL
redirects, URL credentials, fragments, and query strings are rejected. Record
and retain the actual source SHA-256: the URL itself is not immutable content.

```sh
glamdb-cli bench clickbench prepare \
  --source /data/hits.parquet --output ./hits-10k \
  --type-profile legacy-date --rows 10000 --part-rows 1000
```

This is a deterministic prefix with every column, not the official full scale.
Omitting `--rows` converts the entire source; only the official row count is
marked full. Defaults are bounded: source 32 GiB, converted parts 8 GiB, at most
10,000 rows and 4 MiB per part (configurable up to 8 MiB), at most 100,000 parts.
A requested prefix currently hashes the whole source for content identity.
Conversion can take time and consume client disk; it is outside ingest timing.
A failed preparation retains its partial output without a complete manifest.
Use a fresh output directory rather than appending to a partial dataset.

The manifest records source hash/bytes/rows, complete ordered schema, type
profile, part boundaries/hashes/bytes, converted bytes, and preparation time.
Changing any manifest byte makes it a different run identity.

## Create, load, inspect and recover

Set `GLAMDB_TOKEN` using your deployment's normal secret mechanism. Then:

```sh
glamdb-cli bench clickbench init --dataset ./hits-10k \
  --url http://127.0.0.1:8080 --name clickbench-10k --state ./hits-run.json
glamdb-cli bench clickbench load --state ./hits-run.json
glamdb-cli bench clickbench status --state ./hits-run.json
```

Init validates every part against advertised row/request budgets, checks the
requested operation and journal payload budgets, persists UUIDs before the
first mutation, then explicitly creates a dedicated database and hits collection.
Repeated init confirms the original identities. State commands cannot override
the endpoint, database, collection, or part boundaries. Treat the state as an
owned file; do not edit it or share the target with another writer.

Each append UUID derives from load ID, collection ID, manifest hash, and part
ordinal. Exact prepared bytes are journaled before send. Unknown responses stop
the load. Resume never automatically replays an ambiguous attempted append:

```sh
glamdb-cli bench clickbench resolve --state ./hits-run.json
glamdb-cli bench clickbench load --state ./hits-run.json --resume
```

Only after authoritative absence may an operator explicitly request
`resolve --retry`; it uses the original UUID and bytes. A refusal or definite
failed write cannot be counted as loaded. Journals and state must be retained.
Default journal request-body budget is 1 GiB; SQLite/index overhead needs extra
disk space. `--limits limits.json` supplies finite supported database limits.

Load timing covers this invocation's part reads, encoding, journal syncs, and
SDK HTTP outcomes, ending after the last append confirmation. Hash/capability
preflight and final statistics validation are excluded. Counts are cumulative;
rows/s uses only newly confirmed rows in this invocation, so resume does not
inflate throughput. This is a sequential client ingestion measurement, not
maximum server throughput or a breakdown of internal server stages.

## Verify and measure

```sh
glamdb-cli bench clickbench verify --state ./hits-run.json --format arrow
glamdb-cli bench clickbench size --state ./hits-run.json \
  --report ./sizes.json --include-footers
```

Verification consumes the complete pinned snapshot and compares duplicate-aware
row multisets against committed prepared requests using bounded SQLite working
memory. It verifies ingestion of the converted dataset, not an independent
SQL oracle over the source Parquet. Both JSON and Arrow paths are available.
Size-report qualification requires a PASS bound to the same load ID, manifest,
and snapshot. Missing/stale verification yields INCOMPLETE, never a valid timing
result. Results from explicit test backends stay unqualified even after PASS.

Reports include JSON, CSV and Markdown with one common stem. Complete paged file
membership is checked against statistics. Parquet file lengths, per-row bytes,
snapshot history count and optional compressed/uncompressed column-chunk bytes
are separate metrics. A column-chunk ratio never divides uncompressed chunks
by the entire Parquet file including its footer.

Servers advertising storage_footprint return a complete current physical store inventory in server_storage. This is separate from the selected snapshot; older servers retain explicit unavailable values. See docs/development/STORAGE_FOOTPRINT.md. On the server host only,
an explicit caller-owned store path can additionally be inspected:

```sh
glamdb-cli bench clickbench size --state ./hits-run.json --report ./sizes-local.json \
  --owned-store-dir /srv/glamdb/databases/STORE_UUID
```

The last path component must equal the original store UUID. The bounded read-only
walk rejects symlinks, special files and mount crossings, deduplicates hardlinks,
and separates Parquet data, Iceberg metadata, catalog and other/shared files.
It counts all physically present historical/staging files, separately from
selected-snapshot committed membership. File lengths and allocated blocks have
different fields. The measurement is not atomic under concurrent writes, and
the caller-supplied path mapping is not a native store identity attestation.
The shared service registry, client dataset, journal and report directories are
outside this store total. Journals are reported separately.

## Cleanup and status

```sh
glamdb-cli bench clickbench clean --state ./hits-run.json
```

Cleanup drops only the exact original database/store pair and retains local
state, converted data, journals and reports. It never deletes an arbitrary
filesystem path or selects a database by name. Repeating the same cleanup uses
the same identity. Drop requires the native endpoint added for
[#153](https://github.com/glamourdb/glamdb/issues/153).

Exit codes: PASS 0; FAIL 1; invalid configuration 2; BLOCKED/INCOMPLETE 3;
missing dependencies 4. PASS for init/resolve is a command-specific outcome;
check `load_complete`, row verification and performance eligibility before
publishing benchmark results.

Upstream schema reference:
[ClickBench 7dd9941](https://github.com/ClickHouse/ClickBench/tree/7dd9941f45f5f0af088cdaf8b9da3a6156a18bdc).
Fetched benchmark material retains its upstream CC BY-NC-SA 4.0 attribution and
terms; this patch does not vendor or relicense the complete upstream SQL suite.

## Query campaigns

Fetch pinned upstream SQL and its original license into a separate cache:

```sh
glamdb-cli bench clickbench fetch-queries --output ./clickbench-reference
glamdb-cli bench clickbench query --state ./hits-run.json --catalog ./clickbench-reference --source /data/hits.parquet --ids all --mode both --warmup 1 --repeats 3 --report ./queries.json
```

Scan consumes all Arrow batches with conservative native predicates/projection;
its candidates are not SQL answers. Reduce is explicitly GlamDB + DuckDB and
streams fresh data rather than timing a cached local table. Q20 is a complete
native filter/projection; Q24 retains all 105 columns; Q30 reads one input column.
Partial AND pushdown is safe; partial OR/NOT remains residual. LIMIT is never
pushed before residual processing. Original WHERE conditions remain in DuckDB.

Validation independently rereads the source Parquet prefix using PyArrow, without
reusing the ingest codec or scan planner. It checks row multiplicities, order
keys and legal top-N boundary ties/unordered LIMIT. Integers are exact; float
tolerances are rtol=1e-10, atol=1e-12. STRLEN preserves UTF-8 byte semantics.

Defaults: DuckDB memory 512 MiB, spill 2 GiB, candidates 8 GiB/query, result
100,000 rows/64 MiB. These are engine/input budgets, not a hard Python RSS cap.
SDK deadlines and DuckDB interruption bound execution. Timing includes fresh
scan, reduction, result consumption and draining input after LIMIT, but excludes
source-oracle work. Resource errors retain a status and null missing timing.
Reports include JSON/CSV/Markdown and coverage with hashes, versions, snapshot,
rows/columns and budgets. Warm runs do not claim cold OS/device cache.

For concurrency, export projection/predicate/snapshot_id from a scan coverage
descriptor as SDK query JSON (omit predicate when null), then reuse Locust:

```sh
glamdb-cli bench read --url https://YOUR_SERVER --database DATABASE_UUID --collection COLLECTION_UUID --query scan-query.json --format arrow --consume batches --users 8 --processes 2 --duration 60s --report ./locust-scan
```

This concurrency profile measures scans. Query campaigns measure client SQL
reduction separately; generic write workers intentionally duplicate their data.
