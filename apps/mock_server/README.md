# Disposable mock adapter

This package supplies the missing server seam for workbench and SDK development.
It serves real HTTP, JSON and Arrow IPC, but keeps all collections, snapshots and
operation outcomes in process memory. It never opens a warehouse, writes Parquet,
creates an Iceberg catalog or calls itself a production storage engine. Restart
discards all state. `mock=true`, `backend=mock`, and
`durability_validation=designed` identify the backend; committed mock outcomes
also contain `durable=false`.

From the repository root, install the independent verifier and this package into
the development virtual environment, then run:

```sh
.venv/bin/python -m pip install -e tools/verification -e apps/mock_server
export GLAMDB_TOKEN='your-development-token'
.venv/bin/python -m glamdb_mock --ui-dir ui --seed-demo
```

The default address is `http://127.0.0.1:8765`. `GLAMDB_TOKEN` is mandatory; it
is read from the environment and never printed. Access logs are disabled. An
explicit `--allow-non-loopback` is required to bind another address. The adapter
provides no TLS termination; the supported default is local development.

`--theme-file PATH` overrides `ui/config/theme.json`. Every
`GET /workbench/config` rereads and validates that file, with `Cache-Control:
no-store`. Missing configuration returns 404, malformed or unsafe configuration
returns 422. The browser keeps its last valid theme. Configuration writes have
no API. Static `/` and `/assets/*` are confined to the selected UI directory,
including symlink checks.

`create_app(token=..., ui_dir=..., theme_file=None, seed_demo=False)` is importable
for ASGI integration. Optional development configuration includes
`enable_development_faults`, `max_body_bytes`, `max_response_bytes`, `max_rows`, `max_state_bytes`,
`clock`, and `cursor_ttl`. Health is public; every `/v0/` API requires a bearer
token. No token is placed in URLs or response data.

## Adapter behavior

- Collections use caller UUIDs, exact names and immutable fixed schemas. The
  same UUID/name/schema create returns its original collection identity.
- Append preserves duplicates and row order. Canonical validation and hashing
  use `glamdb_verify`, not another production writer. Operation IDs are scoped
  to collections. The same ID/hash returns its original outcome before a new
  precondition; a different hash returns 409. Outcomes survive only within this
  process. Omitted `expected_snapshot` means Any; JSON null means NoSnapshot.
- Omitted scan `snapshot_id` resolves latest once; JSON null or GET query
  `snapshot_id=initial` selects the initial empty view. Historical views remain
  available until the mock process exits. The JSON schema retains stable field
  IDs and the requested projection order. Int64 values are decimal strings;
  timestamps use UTC RFC3339 with six fractional digits.
- `Accept: application/vnd.apache.arrow.stream` returns a real bounded IPC
  stream with `Content-Length`, `X-Content-SHA256` and
  `X-GlamDB-Snapshot-Id` (`initial` for the initial view). Empty scans include
  schema and EOS; zero-column projections preserve row cardinality.
- Collection, snapshot and file pages use authenticated opaque cursors with
  a 15-minute default TTL. Tokens bind endpoint, collection, parameters, page
  size and fixed view or creation high-water mark. Restart invalidates them.
- Stats count actual mock rows. File descriptors and row groups are marked
  synthetic; `mock://` URIs do not refer to physical Parquet. Unknown byte and
  I/O counters are null. Explain describes conservative in-memory execution;
  it reports no fabricated I/O or speedup.
- KV mode, uniqueness, deletes, compaction, asynchronous admission, page/Bloom
  pruning and file/row-group pruning are unavailable. Requests are synchronous.

Default request bodies are bounded to 16 MiB based on received bytes, including
chunked requests. The verifier caps individual appends to 10,000 rows and 16 MiB
of canonical values; the disposable state has a configurable row ceiling and
retains at most 4,096 snapshots/operations per collection. Conservative admission
estimates cap retained row values, Python row containers, view pointer arrays
and operation reserves at 64 MiB by default. This estimate is not an RSS claim.
IPC/JSON scan results
have a 32 MiB ceiling. These are mock development limits, not qualified core
memory or disk profiles.

## Explicit development fault scenarios

These scenarios are disabled unless `--enable-development-faults` or
`create_app(..., enable_development_faults=True)` is supplied. They are synthetic
control-flow fixtures, not fault evidence about a storage engine.

On append, `X-GlamDB-Mock-Scenario` accepts:

- `definitely_not_committed`: bind a terminal simulated failure, without rows.
- `commit_unknown`: bind an unknown outcome, keep the previous readable view,
  and block subsequent writes until explicit development resolution.
- `lost_response`: commit once, return a simulated unknown response, and expose
  the original committed receipt through operation lookup or an identical retry.

An unknown scenario can be resolved only in enabled development mode with
`POST /v0/development/collections/{id}/operations/{operation_id}:resolve` and
`{"outcome":"committed"}` or `{"outcome":"definitely_not_committed"}`.
This route also requires authentication. The outcome then remains immutable.
An absent operation is 404 only when no unresolved mock operation blocks
authoritative absence.

## Tests and qualification boundary

```sh
.venv/bin/python -m pytest apps/mock_server/tests -q
```

Tests cover authentication, exact resolution, schema IDs, duplicate-preserving
append, original retries and failures, initial/historical views, SQL NULL
predicates, precise JSON/IPC, real IPC integrity metadata, cursor binding,
resource limits, metadata honesty, config reload/security, static confinement
and CLI address validation. The shared verifier has its own independent golden
expectations; adapter tests use literal expected rows.

C++ append behavior, process recovery, restart durability, real Parquet/Iceberg
interoperability and G4 power-loss qualification remain **NOT RUN** here.
