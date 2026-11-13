# Native HTTP service

`apps/server` builds `glamdb`; `apps/cli` builds `glamdb-cli`. The server manages named databases through the public C++ facade. Start with an
empty data directory; create and administer databases through HTTP or the UI:

```sh
cmake --preset local
cmake --build --preset local
export GLAMDB_TOKEN='replace-with-a-random-secret'
./build/local/apps/server/glamdb --data-dir /tmp/my-glamdb --ui-root "$PWD/ui"
```

`scripts/glamdb-local.sh up` performs these steps (natively on Linux, in Docker
elsewhere) and runs the server in the background; see the README.
Open [the workbench](http://127.0.0.1:8080), enter the token, create a database,
then create collections, append rows and query them. The database screen also
opens, closes and reconciles databases. Restart with the same data directory. No mock
fallback exists. `--port 0` selects a port; the first stdout line reports it as
JSON. Installation places the binaries in `bin` and the UI in `share/glamdb/ui`;
the installed server discovers that UI relative to its executable on Linux.

Existing standalone stores can still be served with `--root ABSOLUTE_PATH
--store-id UUID`, mutually exclusive with `--data-dir`. This compatibility mode
uses unscoped collection routes and does not offer database management.

`--metrics-listen-address 127.0.0.1:9094` enables a separate private Prometheus
listener backed by the actual native process metrics. It is disabled by default;
the API/UI listener does not serve `/metrics`. Its health gauge observes the
owned engines rather than HTTP reachability. The metrics listener does not use
the API bearer token; keep its bind address and network access private. See
[monitoring](MONITORING.md#application-integration) for metric semantics.
The same private listener includes finite HTTP request/status counters,
duration histograms and in-flight gauges through response completion or
cancellation. `--log-directory /path/to/private/logs` separately opts into Quill
schema-1 JSONL operation/lifecycle records with bounded queues and file rotation.

`glamdb-cli` is a standalone network client with DNS and verified HTTPS; it does
not link the engine or storage libraries. `create-store --name analytics` calls
the managed database API. The optional `glamdb-admin` contains offline
`create-store`/`inspect-store`, bounded snapshot `read`, `quiesce-store`, and named
collection/schema/append/scan/operation/snapshot/file commands. Offline commands acquire core ownership and fail with
`STATE_IN_USE` while the server owns the store. See the [client guide](../apps/cli/README.md)
for its independent build and TLS configuration. Online:

```sh
glamdb-cli request --path /databases
# database.json: {"name":"analytics"}; response includes database_id
# Retrying the same name returns the same database.
glamdb-cli request --path /databases --body database.json
glamdb-cli request --path /databases/DATABASE_UUID/collections --body collection.json
glamdb-cli request --path /databases/DATABASE_UUID/collections/COLLECTION_UUID/append --body append.json
glamdb-cli request --path /databases/DATABASE_UUID/collections/COLLECTION_UUID/scan --body query.json
```

`request` defaults to GET, or POST with `--body FILE` (`-` reads stdin).
`--method`, `--url`, `--host` and `--port` are explicit. It writes the exact response
bytes and never retries. Exit 1 denotes HTTP or pre-send transport failure, 2 CLI
usage error, 3 an uncertain POST delivery, and 4 output failure after a complete
HTTP response. Preserve the original operation ID when delivery is uncertain.
`--accept application/vnd.apache.arrow.stream --output result.arrow --headers headers.json`
retains binary Arrow and cursor/checksum metadata. HTTP is loopback-only; remote
CLI and SDK access use verified HTTPS through a TLS reverse proxy. The server uses
system OpenSSL Crypto for page SHA-256 checksums.

## Database lifecycle

| HTTP route | Behavior |
| --- | --- |
| `GET /databases` | List registered identities, names, states and errors |
| `POST /databases` | Create/open `{name,database_id?:UUID,limits?:object}`; returns database info |
| `GET /databases/{id}` | Database info and store profile while open |
| `POST /databases/{id}/open` | Open existing data and run core recovery |
| `POST /databases/{id}/close` | Stop new calls, drain admitted calls and shut down |
| `POST /databases/{id}/reconcile` | Run core reconciliation for an open engine |
| `POST /databases/{id}/discard` | Remove an uninitialized reservation and verified disposable initialization files |
| `POST /databases/{id}/drop` | Remove an initialized store using its recorded `store_id`, durable intent and a retained retry receipt |

Close/reconcile/discard take `{}`; open accepts `{limits?:object}`; drop requires `{store_id:UUID}` matching the original database identity. All routes require the bearer token. Collection
routes in the next table are relative to `/databases/{id}` in managed mode;
`GET /capabilities` at the server root advertises `features.database_management`.
A database response contains `{database_id,name,state,limits,store?,error?}`; states are
`open`, `closed`, `opening`, `closing`, `reconciling`, `dropping`, `error`. Lists contain `{items,next_cursor:null}`.
Names are unique, case-sensitive UTF-8 (1–128 bytes, no NUL or path separators).
At most 64 databases are registered. Paths are generated from UUIDs under
`--data-dir/databases`; clients cannot provide filesystem paths. Database and
collection identities are separate, and collection/operation IDs are database scoped.

Creation reserves the name and UUID durably before calling `CreateStore`.
Repeated creation with the same name and optional matching UUID returns or
reopens that database; a conflicting name/UUID returns 409. A lost response can
be resolved by listing databases or repeating the same request. There are no
automatic client retries. An unfinished reservation without a store can be
completed only by explicit creation. `open` never creates anything. An incomplete
store, or a missing initialized store, remains an error and is never replaced.
Discard returns `{database_id,discarded:true}` and releases the name, UUID and registry slot.
It requires an uninitialized reservation with no owned engine or valid readiness
marker. Empty or mismatched markers are disposable only with a known initialization
layout and an empty warehouse. Cleanup holds the directory ownership lock, rejects
symlinks and unknown contents, and never follows directory links. Valid ready stores
remain protected even if their identity differs from the reservation. Unknown
contents require inspection; existing unregistered `incomplete-...` siblings are
not swept. A registry write with an uncertain outcome requires restart before reuse;
after restart, inspect the original ID (404 means the reservation was removed).
Discard never removes an initialized or ready store. Explicit initialized-store
deletion uses [the managed drop protocol](DATABASE_DROP.md), including admission
draining, crash recovery, filesystem safety and exact-identity retries.
`features.database_drop` advertises this operator capability in managed mode.
Deleted UUIDs remain retired; names and live registry capacity are reclaimed.
Import and rename remain unsupported.

Optional `limits` configure the public engine limits. Creation merges omitted
fields with defaults; an omitted limits object on retry preserves the existing
configuration. An explicit conflicting creation configuration returns 409.
Open merges partial settings with persisted values; changing values requires a
closed database. Configuration and open intent are durable before opening the
engine. If a lower limit prevents reopening existing data, close the errored
entry and open again with sufficient limits; data is never discarded.

| Limit | Range | Default |
| --- | --- | --- |
| `max_operations` | 1–9223372036854775807 | 1099511627776 |
| `max_collections` | 1–64 | 8 |
| `max_catalog_bytes` | 65536–1099511627776 | 68719476736 |
| `max_pending_calls` | 1–16 | 16 |
| `max_owned_input_bytes` | 1–134217728 | 134217728 |
| `writer_wait_ms` | 0–5000 | 5000 |
| `append_deadline_ms` | 1–60000 | 60000 |

`limits.history` configures aggregate snapshot, manifest, data-file, root,
manifest-list and footer budgets, individual root/commit bytes and recovery
workspace bytes. See the [field ranges, defaults and SF=10 profile](SCALABLE_HISTORY.md).
New databases get defaults under which byte budgets (catalog, retained metadata,
warehouse) bind rather than counts; normalized descriptors include every history
field. Persisted descriptors keep their values when the defaults change, and
complete legacy persisted limits objects without `history` receive the former
history defaults (`LegacyHistoryLimits`), not the current ones.

Values are JSON integers; booleans, fractions, unknown fields and values outside
these bounds are rejected before reserving a database. Every database response
includes normalized limits, including closed/error states. The service registry is created directly in version 3; version 2 is migrated transactionally to add deletion receipts. Core store formats are unchanged.

Profiles above eight reserve additional shared metadata, control and recovery
capacity before opening the engine. Capabilities report the selected
`limits.max_collections` and `max_supported_collections=64`. See
[collection profiles](COLLECTION_PROFILES.md) for configuration, admission and inventory rules.
Legacy `--root/--store-id` servers accept `--max-collections`; managed services
configure this through database create/open requests instead.

A separate `registry.sqlite` records identities and intended open state, using
verified SQLite DELETE/EXTRA settings. The registry does not contain collection
catalogs or data. A definitely rolled-back registry write can be retried. An uncertain registry write
blocks further lifecycle mutations until restart; outcome lookup and core reconciliation
remain available for owned engines. Service initialization builds and syncs the complete
registry before publishing it. Explicit create can retry an incomplete, uninitialized
store reservation by reclaiming only those verified disposable files. It never
replaces an initialized store.
A missing registry in a nonempty directory fails startup. Each service directory
has one process owner; each core store retains its own identity and owner lock.
Use an absolute canonical data path with an existing parent on trusted local disk.

Open databases reopen on restart; explicitly closed databases stay closed. A
failed store open is listed as an error while other databases remain usable.
Close persists the intent to remain closed before shutdown. A shutdown deadline retains ownership in `closing`; retry close after calls drain.
A terminal readiness error releases the drained engine so close/reopen can recover. Reconciliation does not undo shutdown. A client
timeout does not roll back lifecycle changes: inspect state or retry the same
action. Per-database leases drain accepted calls and reject new calls during
close/reconcile; unrelated databases remain usable within the fixed worker pool.
Database info/list requests read immutable descriptors without waiting for lifecycle changes.
An unresolved registry fence makes reconcile fail with `OperatorAction`; it cannot
return success while the database remains fenced in `error`.
Repeated open/create of an already healthy database does not drain active calls. Failed core
reconciliation preserves outcome lookup while core readiness rejects unsafe work.
The workbench clears query results and schema caches on database switch and ignores
stale replies. It retains unresolved operation IDs and payloads per database until
resolved or disconnected, and offers reconciliation in the active workspace.

## Contract and ownership

Versioned [JSON Schema wire types](../fixtures/http/v0.schema.json) define
requests, responses, errors and recovery envelopes. Fixed-schema row types,
UUID semantics and int64 range are additionally checked by the native core.
The executable [v0 route/shape fixtures](../fixtures/http/v0.json) inventory
every SDK/workbench call, its core operation, request example, status and required
response-field types. `glamdb.native_http` executes them against real storage.
The fixtures are a bounded response-shape vocabulary, not a full JSON Schema
validator. Native `outcome_json_test` separately pins complete recovery/receipt
serialization. [Shared literal outcome/scalar fixtures](../fixtures/http/README.md)
also exercise native serialization, SDK/browser parsing, incomplete recovery
rejection, and A/B/retry-A through native storage and fresh-process reopen.
Nullable recovery fields must be present; SDK/browser responses retain extension
fields. Fixture, mock and native integration evidence remain distinct.
The existing [mock contract](../apps/mock_server/README.md) remains a
separate, broader development profile.

| HTTP route | Public core operation | Availability |
| --- | --- | --- |
| `GET /capabilities` | Adapter inventory / `Engine::GetHealth` when scoped | Native features, predicates, transport limits; store health when selected |
| `GET /databases/{id}/readyz` (legacy `/readyz`) | `Engine::GetHealth` plus lifecycle admission | Authenticated readiness snapshot; 200 when write-ready, otherwise 503 |
| `GET /collections` | `ListCollectionsPage` | Creation-ordered fixed inventory; `page_size` and `cursor` |
| `POST /collections` | `CreateCollection` | Fixed-schema append or KV collection |
| `GET /collections:resolve?name=...` | `ResolveCollection` | Exact name |
| `GET /collections/{id}` | `GetCollection` | Identity and current schema |
| `GET /collections/{id}/schema` | `GetCollection` / `Schema` | Stable field IDs |
| `POST /collections/{id}/append` | `AppendDurable` | Synchronous native receipt |
| `POST /collections/{id}/bulk-append` | `BulkAppendDurable` | Bounded NDJSON, anonymous disk replay, one atomic receipt |
| `POST /collections/{id}/write-settings` | `UpdateWriteSettings` | Compare-and-set of Parquet settings for later appends |
| `GET /collections/{id}/snapshot-archive` | `GetSnapshotArchive` | `hot_snapshots` and root/archived/expired counts, no I/O |
| `POST /collections/{id}/snapshot-archive` | `UpdateSnapshotArchive` | Replace `hot_snapshots` for later appends; idempotent |
| `POST /collections/{id}/snapshot-archive/maintenance` | `ArchiveSnapshots` | Archive at most 1024 root snapshots above `hot_snapshots` |
| `GET /collections/{id}/operations/{op}` | `FindAppendOutcome` | Original result or authoritative absence |
| `POST /collections/{id}/scan/open` | `Scan` | Resolve a fixed view and return a cursor without consuming rows |
| `POST /collections/{id}/scan` | `Scan` / `ScanCursor::Next` | First page or cursor continuation, JSON or Arrow IPC |
| `POST /collections/{id}/scan/cancel` | `ScanCursor::Cancel` | Release a cursor and its retained resources |
| `GET .../stats` | `GetStatistics` / `ListSnapshots` | Selected-view counts with metric kinds and null unknowns |
| `GET .../snapshots` | `ListSnapshots` | Receipt-backed history, bounded fixed-view pages |
| `GET .../files` | `ListFiles` | Manifest membership and opaque file IDs, bounded pages |
| `GET .../files/{file}` | `GetFileDetails` | Selected-view membership plus actual Parquet footer/row groups |
| `POST .../explain` | `ExplainScan` | Fixed-view plan; advertises file/row-group pruning on local storage while page/Bloom pruning remains false |

CLI mapping: `create-store` → `POST /databases`; `request` exposes every route
in the tables above. Offline `glamdb-admin` uses `CreateStore` / `OpenExisting`.
Both CLIs also expose named data/inspection commands; admin `--url` mode uses
HTTP exclusively. Named scans and snapshot/file inventories follow fixed-view
pages within their result bounds. Named commands return `3` for complete native
unknown or recovery-blocked outcomes, while `request` retains HTTP-level exits.
See the [CLI guide](../apps/cli/README.md). `glamdb serve` is an alias for
server startup and preserves explicit creation and ownership rules.

The transport owns HTTP parsing, codecs, authentication, connection admission
and static UI assets. Core owns store identity/locking, schema validation,
canonical hashes, serialized writes, durable publication, receipts, recovery and
snapshot selection. The adapter never reads core catalog tables or enumerates the
warehouse; its separate registry stores only database lifecycle metadata. Metadata/explain must be added through public core methods, not
synthetic values. The UI respects `features` and reports unavailable sections.

`GET /healthz` is public process liveness, **not write readiness**. It continues
to return 200 while a store is recovering, closed or failed. Readiness is
store-specific: authenticated `GET /databases/{id}/readyz`, or `/readyz` in
legacy single-store mode. A managed root `/readyz` requires database selection;
there is no aggregate assertion that every registered store is writable.
Unknown database IDs retain the normal 404 response.

The readiness body contains `state`, `read_available`, `write_ready`,
`storage_failure_latched`, `durability_qualified`, and the existing `backend`,
`mock`, `durability_validation` identity fields. The same object appears as
`health` in scoped `/databases/{id}/capabilities` and legacy `/capabilities`.
Managed root `/capabilities` has `health: null`; it advertises installed features
(including `file_pruning` and `row_group_pruning` when built with local storage),
not current store admission. Scoped capabilities return 200 even when the store
is unavailable, so clients can inspect health without attempting a write.

| State | New reads | New writes | Readiness HTTP status |
| --- | --- | --- | --- |
| `recovering` | No | No | 503 |
| `ready` | Yes with a storage reader | Yes | 200 |
| `recovery_required` | No | No | 503 |
| `storage_failed` | Yes with a storage reader | No | 503 |
| `unavailable` | Only if a reader is installed | No | 503 |
| `stopping` | No | No | 503 |
| `closed` | No | No | 503 |

Opening and reconciliation report `recovering`. An unresolved commit,
corruption, failed recovery, or registry admission fence reports
`recovery_required`. A durable file-I/O write latch reports `storage_failed`
when recovery has otherwise succeeded: confirmed reads remain available, but
new writes need shutdown and reopen. Reconciliation does **not** clear the
latch. The latch flag remains independently visible on an engine during
recovery and shutdown. Closed managed databases no longer retain an engine,
so their snapshot has no in-memory latch. Lifecycle draining reports `stopping`
before waiting for active calls. Health observation never performs catalog/file
I/O or waits for a writer or lifecycle operation.

Availability is a point-in-time observation, not a reservation. A later call
still checks admission and can fail due to resource limits, contention, or a
newly detected fault. `read_available` describes new scans, not all metadata
lookups or previously retained cursors; recovery outcome lookup keeps its
existing contract. Synchronous C++ factories return an engine only after
startup recovery succeeds; the managed HTTP lifecycle exposes runtime database
opening progress. At initial process startup, registered databases are opened
before the HTTP listener starts, so probes cannot connect during that phase.

`backend=local` and `mock=false` do not imply power-loss qualification.
`durability_validation=designed` and `durability_qualified=false` remain explicit
for both local and mock backends. Neither a successful build, backend selection,
recovery, nor a passing test changes qualification. No evidence-backed qualified
environment is currently advertised. The disposable mock exposes `/readyz` for
its in-memory operations with `backend=mock`, `mock=true`; its readiness does
not promise persistent storage. Executable examples for every state are in
`fixtures/http/health-states.json`, with the `storeHealth` schema in
`fixtures/http/v0.schema.json`.

## Requests and results

Requests use UTF-8 JSON objects with unknown/duplicate fields rejected. Create:
`{collection_id,name,schema:{fields:[{name,type,nullable,...}]},mode?:"append"|"kv"}`.
Types: `string`, `bool`, `int64`, `float64`, `timestamp_us_utc`, `decimal` and
`date`. Decimal fields require integer `precision` (1–38) and `scale`
(0–precision); other types reject those parameters. See [scalar types](SCALAR_TYPES.md)
for exact JSON values and typed predicate literals. Core assigns IDs
1..N; supplied field IDs must match. Schemas contain 1..128 fields; `limits.schema_fields` advertises this finite
limit. Row, request-byte and shared-resource limits still apply independently.
The canonical encoding version and existing hashes are unchanged; older binaries
limited to 64 fields cannot open collections created with wider schemas.
A field named `key`, if supplied, must be a
non-nullable string. Append preserves duplicate keys; KV requires this field
and maintains one full row per key.
Schema evolution and SQL are unsupported. Explicit KV collections support the
mutation routes described below; append collections preserve their old behavior.

Create also accepts optional `write_settings`, part of the collection definition
(a repeated creation with different settings returns 409 `WRITE_SETTINGS_MISMATCH`):
`{parquet:{compression?:{codec,level?}|null,columns?:[{field,compression?}]}}`.
They choose the **Parquet value compression** of data files written by later
appends; HTTP bodies stay uncompressed (`Content-Encoding` other than `identity`
is still rejected). Codecs are `uncompressed`, `snappy`, `gzip`, `lz4_raw` and
`zstd`; `parquet.codecs` in capabilities lists those of the running build with
their level ranges (`null` for codecs without levels) and default levels, and
`parquet.default_compression` is used when nothing is configured. A column entry
overrides the collection default for that field; a level is never inherited
from another codec. Unknown fields, codecs, duplicate columns, empty column
entries, levels outside the range or for codecs without levels are rejected
before anything is bound. Collections return the canonical (schema-ordered)
`write_settings` and `write_settings_version` (decimal string; `"0"` = never set).

The same object chooses **Parquet value encodings**, independent of both
compression codecs and HTTP coding: `parquet.dictionary` and a column's
`dictionary` are `auto` (default: dictionary pages for every type but `bool`),
`enabled` (the same, but rejected for `bool` columns) or `disabled`;
`parquet.dictionary_page_size_limit` (decimal string, 4 KiB–8 MiB, default
1 MiB) bounds each column chunk's dictionary, checked after every 1,024 values.
A dictionary that outgrows it is written as the dictionary page and the
remaining data pages fall back to `plain`, so one column chunk can then hold
both `rle_dictionary` and `plain` data pages. A column's `encoding` selects its
non-dictionary value encoding: `plain` for every type; `delta_binary_packed`
for `int64`/`timestamp_us_utc`/`date`; `delta_length_byte_array` and
`delta_byte_array` for `string`; `byte_stream_split` for `int64`, `float64`
`timestamp_us_utc` and `date`; `rle` for `bool`. Decimal supports explicit
`plain` and dictionary policies. An explicit encoding turns that
column's dictionary off and overrides the collection policy; combining it with
`enabled` (or with `auto` on a non-`bool` column) is rejected, because the
pinned writer would ignore it. `parquet.encodings` and `parquet.dictionary` in
capabilities publish these combinations. File details report the actual
`encodings` of every column chunk and `encoding_stats` (pages per page type
and encoding), lower-case like the settings.

`POST /collections/{id}/write-settings` takes
`{expected_version:"N",write_settings:{...}}` and returns the collection. It
applies only when `N` is the current version and yields `N+1`; repeating an
applied request (same settings, current version `N+1`) succeeds unchanged, so a
lost response can be retried. Otherwise 409 `WRITE_SETTINGS_CONFLICT`. It waits
for the single writer: an append already bound keeps the settings it read, and
existing files keep the codecs recorded in their footers (`compression` in file
details). Settings are never part of an append's operation identity.

Append: `{operation_id,rows:[{field:value,...}],expected_snapshot?}`. Omitted
precondition means any snapshot; null means no snapshot; a decimal string means
an exact snapshot. Missing nullable row values become null. Int64 and snapshot
IDs **must be decimal strings**, never JSON numbers; floats must be finite and
booleans remain booleans. Timestamps accept RFC3339 offsets and at most six
fractional digits, normalized to UTC with six digits, years 0001–9999.

Scan: `{projection?,predicate?,snapshot_id?,limit?,batch_rows?}`. Omission selects
latest once; null or `"initial"` selects the initial empty view. Historical IDs
are decimal strings. Omitted/null projection defaults to all columns; `[]`
returns zero columns with matching row cardinality in JSON and Arrow pages.
JSON rows are empty objects. Predicates are `null`/`{op:"true"}`, comparisons
`{op:"eq|ne|lt|le|gt|ge",field,value}` (where `value` is a correctly typed
literal or `null`), `{op:"in",field,values:[...]}` (typed literals and/or
`null`), `{op:"is_null|is_not_null",field}`, or recursive boolean trees
`{op:"and|or",args:[...]}` (non-empty `args`) and `{op:"not",arg:{...}}` within
`predicate_depth <= 64`, `predicate_nodes <= 1024`, and `predicate_literals <=
4096` (exceeding limits or malformed shapes/types returns 400; unknown operators
within the depth limit return 501). Limit is a nonnegative int64 integer;
batch_rows is 1..65536. SQL three-valued NULL logic applies across the tree and
only TRUE at the root survives. No SQL parser is implied.

JSON scan returns `{schema,snapshot_id,rows,identity,profile,next_cursor}`. The
snapshot is the resolved selected view, including null for the initial empty
view; `identity` is the core catalog identity captured when planning the scan.
Profile contains only the actual page `rows_returned` decimal string, without
pruning or physical IO claims. `Accept: application/vnd.apache.arrow.stream`
returns a schema-bearing Arrow IPC page, including empty results, with
`Content-Length`, `X-Content-SHA256`, `X-GlamDB-Snapshot-Id`, and an optional
`X-GlamDB-Next-Cursor`. Each page is fully prepared before success headers, so a
read/encoding failure cannot become a successful partial page.
The shared `apps/transport/arrow_ipc` codec measures schema, batch and EOS before
allocating one fixed-size output buffer. The session retains its working,
outgoing and replay reservations through encoding. Size refusal returns no page;
the checksum is computed only after the writer closes successfully.

For complete reads, continue with `POST .../scan {"cursor":"TOKEN"}` and the same
Accept header until JSON `next_cursor` is null or the Arrow cursor header is
absent. Each page has at most 10000 rows and targets 4 MiB; a single larger row is
allowed if its complete response fits 16 MiB. Total rows are not capped by the
page size. Writes after scan creation never enter that scan's fixed view.

For retry-safe reading from the first page, first call `POST .../scan/open` with
the ordinary scan body and chosen Accept header. Its JSON response contains
`{cursor,schema,snapshot_id,identity}` and consumes no rows. Use that cursor for
the first page. The last consumed cursor replays exactly the same response;
advancing invalidates older cursors. A lost response to ordinary `/scan` without
a cursor cannot be replayed: opening again creates a new scan. The SDK's scan entry points and the workbench use the explicit open protocol.

`POST .../scan/cancel {"cursor":"TOKEN"}` accepts either the next or most recently
consumed cursor and returns `{"cancelled":true}`. Tokens are scoped to database,
collection and representation; stale or missing tokens return 404, never restart
queries. Defaults allow eight live cursors per database and 64 globally, with
separate completed replay retention, a 60-second idle/retention timeout and no
absolute lifetime. Startup options configure these limits and global/per-database
replay bytes. Admission reserves the 16 MiB hard response maximum in the replay
quota and `32 MiB + 64 KiB` data memory from the shared process parent before
planning. Page assembly and outgoing copies have separate shared reservations.
Admission refusal preserves an existing cursor and its previous replay;
reason-specific 429 responses include `Retry-After: 1`.
Completed entries use observable oldest-first eviction; live cursors are never
evicted for another database. Close, reconciliation and shutdown cancel retained
sessions; reservations outlive active reads and outgoing copies as needed.
Cancellation does not wait for an active filesystem read. See
[scan session policy](SCAN_SESSIONS.md) for every option, effective-limit and
occupancy observations, EOF/replay/TTL semantics and the sufficient 32-reader profile.

Create/append results use the existing native serializer. Append preserves
`committed`, `definitely_not_committed` and `commit_unknown`, phase, original ID,
full receipt/error and recovery context. No 202 or asynchronous admission exists.
A timeout/disconnect/HTTP error alone never proves rollback. Retain the original
ID and payload, query its outcome, and reconcile/reopen when core requires it.
HTTP parsing/codec rejection precedes the core call and returns an error object;
it does not register an operation. Pre-execution `SERVER_BUSY` (503) and
`REQUEST_TIMEOUT` (408) refusals include `execution_started:false` and
`Retry-After: 1`. The SDK treats only the complete matching envelope as a definite
refusal; malformed or generic write failures remain uncertain. This describes
the current call and says nothing about an earlier attempt with the same ID.
No implicit retry occurs in server or CLI.

Errors are `{error:{code,message,retry_hint}}`, with the core retry hint unchanged.
400 invalid request, 401 authentication, 404 authoritative absence, 409 conflict,
412 precondition, 413 oversized body or decoded input, 415 media type, 429 bounded resource admission,
501 unsupported, 503 recovery/dependency/unresolved outcome, 504 deadline,
507 exhausted capacity, 500 corruption/unexpected error. Core `RESOURCE_EXHAUSTED`
uses 429 for bounded admission (busy writer, pending budget) and
507 when the core marks it `OperatorAction`: free-space reserves, store limits,
`max_collections`/`max_operations`/catalog bytes, or failed allocation. `CAPACITY_EXCEEDED`
is always 507. Writer preflight also currently emits `RESOURCE_EXHAUSTED` with
`FixInput` (413) when its separate row/input estimate exceeds a bound; reduce the
input instead of retrying it unchanged. The free-space reserve is the larger of 1 GiB and 5% of the data
filesystem plus 512 MiB for an attempt and journal. Database, collection and
operation absence have distinct codes; only `OPERATION_NOT_FOUND` proves an
operation lookup absent. Transport errors carry the shared retry policy; unexpected
write failures preserve the original ID for outcome resolution.
Collection ID/name/schema lookups return 503 `OPERATION_IN_PROGRESS` on a cache
miss during an active writer. Confirmed collections remain readable; 404 requires
rechecking absence with writer ownership. An absent lookup never cancels a queued
request or proves that a disconnected request rolled back.
`INPUT_TOO_LARGE` with `FixInput` denotes the deterministic 2 MiB decoded row or
16 MiB decoded batch limit; reducing the input is required. Retrying an unresolved
append still validates its canonical payload: changed content with the same ID
returns `OPERATION_CONFLICT` before reconciliation. The immutable schema used to
decode that retry does not open core readiness or admit another write.
Collections accept `page_size` 1..1000 and opaque `cursor` tokens. Follow
`next_cursor` until null, repeating the database and page size; the creation
high-water mark fixes membership across pages. See [metadata selectors and
cursors](#metadata-selectors-and-cursors).

## HTTP service profile

Standalone Asio provides asynchronous sockets, timers and signals. Separate pools
serve readers (by default four workers, two calls per database), publishers (four workers,
one per database), lifecycle (four workers, one per database), and controls/static
files (two workers, one per database). Each pool uses per-database FIFO queues and
round-robin dispatch: at most 64 queued calls and 64 MiB of queued bodies per pool,
with at most 16 calls and 32 MiB per database. Queue deadlines use
`--request-timeout-ms`; expiry and observed TCP resets or socket errors remove
calls before core entry. A complete request remains valid after the client closes
its send side; queued and active calls retain their response socket. Active core
calls finish independently of read/queue deadlines. Outcome
lookup never waits on an in-progress catalog publication. Health and capabilities
run on the I/O thread. Core retains its single serialized writer.

Operation outcome lookup returns 503 `OPERATION_IN_PROGRESS` with
`SameIdAfterReconcile` while the catalog lock is busy, including during another
metadata read. This transient response does not replace the recorded receipt;
look up the same operation ID again after the competing call finishes.

`--http-readers` configures active reader workers globally (1–4, default 4);
`--http-readers-per-database` configures active readers per database (1 through
the global limit, default 2). Invalid profiles fail before opening the service
directory. These are active-call limits, separate from scan cursor quotas and
TCP/body limits. Reducing reader admission preserves the independent publisher,
control and lifecycle pools, bounded fair queues, deadlines and response ownership.

For a small host whose shared metadata budget admits one heavy read, configure
both reader limits to 1 so simultaneous Explain/statistics calls queue instead
of competing for working reservations. Explain releases its temporary scan
cursor before collecting statistics; it no longer holds two full planning
envelopes at once. Verified-index scan opening uses a 16 MiB schema/projection
workspace; uncached planning keeps its history-derived envelope. Store,
result and transport reservations still apply. An example finite profile uses
512 MiB data, 1792 MiB
metadata, 32 MiB control, 64 MiB completion and 1536 MiB recovery, with
`--scan-live-limit 1 --scan-live-per-database 1 --http-readers 1
--http-readers-per-database 1`. These admission envelopes are not RSS measurements
or a guarantee that simultaneous independent publishers fit the same budget.

Ordinary header allowances are 64 globally and 32 per peer; excess accepted
connections have a 100 ms header probe so complete requests can still progress.
At most 16 request bodies are read at once, including at most 12 ordinary uploads.
The remaining four readers are reserved for authenticated database
create/open/close/reconcile/discard/drop and scan cancellation; these control bodies
can also use other free readers. Public/static POSTs cannot spend the reserve.
Additional headers wait within the connection ceiling and original read deadline;
eligible control bodies can pass ordinary waiters when only reserved capacity is
available. Authentication and body reservation precede `100 Continue`. Body slots
release when reading finishes, on disconnect, timeout or shutdown; retained
queued bodies use the separate byte limits above.
There are 96 total connections, including overload responses;
at the final ceiling, accept pauses and checks for capacity every 100 ms. Excess
connections wait in the kernel backlog instead of being accepted and reset. The
OS backlog remains finite; connect/read timeouts still require clients to preserve
the original write identity. Headers and bodies share `--request-timeout-ms`
(default 10 seconds); response writes have 30 seconds. Early errors half-close
their reply and drain at most 32 MiB for at most two seconds to preserve responses
to eager senders. Read deadlines never cancel admitted core work.
Transient accept failures retry with backoff. Header limit: 16 KiB; request/response:
16 MiB; append: 10000 rows. JSON parsing allows 160 nesting levels for compound
predicates. Event limits bound the single-pass parser and accommodate 10,000 rows
of the full 128-field append schema (2,580,100 parser events). These transport
bounds are not a qualified process-memory or storage budget. The same pass keeps
append `rows` as compact parser nodes (no per-row DOM) that become Arrow columns
once the collection schema is known, and JSON scan pages are written directly
from Arrow columns; `tests/server/json_codec_test.cpp` pins both to the previous
DOM codecs' batches, bytes and errors, and `glamdb_json_codec_bench` measures them.

The [shared resource budget matrix](SHARED_RESOURCES.md) specifies which limits
are per request, store or service and who reserves/releases them. A
[process resource owner](SHARED_RESOURCES.md) now admits all managed databases,
registry state, request bodies, parsing work, scan replay and outgoing responses.
HTTP body bytes, parsed JSON/Arrow allocations and core canonical bytes have
separate charges. Socket/header descriptors use protected completion capacity;
lookup and lifecycle bodies up to 64 KiB also use it. Larger control bodies use
ordinary data capacity within the existing 16 MiB wire limit. Ordinary data
saturation preserves exact-name create resolution, small append-outcome,
readiness and reconciliation requests.
These conservative working envelopes do not establish a universal RSS bound.

HTTP/1.1 uses Content-Length and closes each response connection. Duplicate
headers, folded headers, Transfer-Encoding, compressed requests and pipelined bytes
are rejected; keep-alive and chunked requests are not supported. `Expect: 100-continue`
is acknowledged after authentication and body admission. Oversized headers return
431, malformed line endings 400, and read timeouts 408. JSON Content-Type accepts
case-insensitive media names and UTF-8 charset parameters. Scan Accept supports
media lists, wildcards and quality weights.
Ordinary routes use buffered JSON requests. The [native bulk route](BULK_INGESTION.md#native-http-sdk-and-cli)
streams LF-terminated NDJSON with Content-Length through anonymous disk scratch:
4 GiB total wire, 16 MiB per batch, ten-minute upload deadline, two global/one
per-database spool permits retained through publisher completion. It uses separate
spill workers and shared disk/memory reservations; partial uploads never enter
core. Ordinary request and queue deadlines retain their existing values.
The server defaults to 127.0.0.1:8080.
`--listen` is explicit; terminate TLS and apply deployment limits at a trusted
reverse proxy for remote access. There is one bearer identity, no user/RBAC layer.
The token comes only from `GLAMDB_TOKEN`, never a URL, log, config response or UI
storage. API responses are uncached. CORS is disabled; the UI uses the same origin.

### Remote access through TLS

Keep the native listener on `127.0.0.1` and expose only the TLS reverse proxy.
Configure the proxy with the deployment certificate and key, forward
`Authorization` unchanged, and retain native bearer authentication on every API
request. `X-Forwarded-*` headers grant no access. The single bearer identity can
administer every managed database; a scan cursor is additionally bound to its
database, collection and representation. A cursor alone never authenticates.
There are no per-user or per-database grants; use separate service processes and
tokens for separate access. Authentication precedes body admission and
`100 Continue`. Validation errors omit unknown JSON field names and predicate
operators so pasted secrets are not echoed.

Set proxy header/body/concurrency limits and timeouts as well as the native
limits. If the proxy accepts chunked uploads, buffer them under its own finite
byte cap and forward an HTTP/1.1 Content-Length request. The native listener
rejects Transfer-Encoding and compressed request bodies before reading or
decompressing them, so neither can bypass its 16 MiB input cap. A proxy that
decompresses requests must independently cap decompressed bytes before forwarding.
Buffering and native limits bound different resources.

Use an HTTPS base URL and a trusted CA for CLI/SDK clients; verification must
cover both the certificate chain and hostname. Keep tokens out of URLs and proxy
access/error logs; do not log authorization headers or request bodies. The
native server emits its listening address but does not log request headers,
targets or bodies, and authentication errors contain no supplied credentials.

SIGINT/SIGTERM stop accepting, close incomplete requests, cancel scan sessions and
reject new scans before draining workers. Cancellation reaches core cursors without
waiting on their session locks; read leases remain owned until those calls finish.
Admitted writes and responses drain before Engine::Shutdown releases store
ownership. OS IO may still block; there is no forced rollback or detached writer.
Client disconnects do not cancel publication. Scan pages are synchronous bounded
responses; cancelled cursors cannot publish another page.

Static serving allows only index, theme and supported assets beneath `--ui-root`,
rejects traversal and symlinks, and never exposes the store. Administrators must
keep the UI tree trusted and separate from the store.

## Metadata selectors and cursors

Stats/history/files accept omitted `snapshot_id` for latest, `snapshot_id=initial`
for the initial empty view, or a canonical decimal snapshot ID. Explain accepts
the scan body (`snapshot_id: null` for initial). Root identity and resolved
selected `snapshot_id` are distinct: an older view never claims the newest
snapshot as its own. Metadata counts and IDs are decimal strings. Logical bytes,
unknown bounds and unsupported physical IO remain null/unavailable.
Explain retains the validated compound predicate tree, including NULL literals,
and uses reader admission so a blocked publisher does not delay the selected-view
plan.

Collections support `page_size` 1..1,000 and opaque `cursor`, using the core
fixed-view pages. Repeat the same page size and database until `next_cursor`
is null. `creation_high_water_mark` is a decimal string and remains fixed;
later collection creation never changes the inventory. Changed, malformed,
foreign, expired and pre-restart tokens return HTTP 400 `INVALID_CURSOR`.
The engine bounds these listings to 32 sessions and 16 MiB of DTO/token
allocations per database, with a five-minute absolute lifetime. Completed
collection inventories retain replay until expiry or reclamation by a new
listing; unread inventories are never evicted. Single-page results retain
no session. Capabilities advertise this separately from history/file JSON caches.

History/files support metadata pagination: `page_size` 1..1,000 and opaque
`cursor`, repeated with the same kind/database/collection/selector/page size.
The first bounded inventory is frozen; later appends cannot change it. Tokens
expire after 60 seconds idle or 300 seconds total. There are at most 64 metadata
sessions, eight per database, 16 MiB serialized JSON per inventory and 64 MiB
retained serialized JSON overall. These are JSON accounting limits, not a full
DOM/heap governor. Read admission is checked even for cached pages.
After the last page is consumed, its inventory remains available for replay
while capacity permits. New listings reclaim the oldest completed inventories
before refusing admission; inventories with unread pages are never evicted for
capacity. Reclaimed continuation tokens return `404` like expired tokens.
Lifecycle close/reconcile clears tokens. No raw file paths can be requested;
file IDs outside the selected snapshot fail even when the file exists elsewhere.
See [metadata](METADATA.md) for native field identities and ownership.

## Snapshot archive

Create accepts an optional `hot_snapshots` decimal string with three meanings:
omitted (or `null`, which equals an omitted member as elsewhere in request
JSON) selects the server default, `DefaultHotSnapshots` = 100 for a new append
collection while `features.snapshot_archive` is advertised and none for KV;
`"0"` (`KeepAllSnapshotsInRoot`) creates the collection without an archive;
`"1"`..`"1099511627776"` is an explicit value. Collections report the effective
value as a decimal string (`"0"` without an archive, never `null`) plus
`hot_snapshots_defaulted: true|false` (the value came from the default and was
never set explicitly; any update clears it). The effective value is part of the
definition like `write_settings`: a repeated creation compares effective values,
so an omitted value matches only a collection whose setting is still defaulted
(whatever the numeric default was), an explicit value must equal the created
value (an explicit `"100"` also matches a defaulted 100, `"0"` only a collection
without an archive), and anything else returns 409 `SNAPSHOT_ARCHIVE_MISMATCH`. A reported collection can therefore be repeated
as its own create request until it is updated. Consequence for collections
created before the default existed (by older servers, without an archive): a
create retry that omits the member no longer matches them; send `"0"`. Opening,
restarting or reopening such a database through the registry never adds the
default to existing collections or raises their catalog version; only a new
collection that omits the setting introduces catalog versions 5 and 6, which
older servers cannot open. Appends then keep only the newest snapshots in the
Iceberg root and move older ones to the catalog archive; see
[the snapshot archive tier](SNAPSHOT_ARCHIVE.md).

`GET /collections/{id}/snapshot-archive` returns `{hot_snapshots,
root_snapshots, archived_snapshots, expired_snapshots, archive_pending}` (decimal
strings; `hot_snapshots` is the effective value, `"0"` without an archive) from
the confirmed view without I/O; it uses the reader pool. `POST` on the same
route with `{hot_snapshots:"N"|"0"|null}` (the member is required) replaces the
setting with an explicit one under the writer and returns the collection;
nothing moves immediately, `"0"` and `null` stop archiving later snapshots and
archived snapshots never return to the root. Repeating an applied request
returns the collection unchanged, so a lost response is retried as is. `POST
.../snapshot-archive/maintenance` with `{}` archives at most 1024 root snapshots
above `hot_snapshots` and returns the status plus `snapshots_archived`; repeat it
while `archive_pending` is not `"0"`. Both POST routes run on the publisher
pool. There is no PUT route: the server accepts only GET and POST.

KV collections refuse a nonzero setting, at creation or update, and maintenance
with 409 `COLLECTION_MODE_MISMATCH` (`"0"` or `null` succeeds unchanged). Values
outside 0..2^40, non-decimal or non-string values and `hot_snapshots` above an
explicit retention `keep_last` return 400 `INVALID_ARGUMENT`. An uncertain
COMMIT of an update or maintenance call returns 503 `RECOVERY_REQUIRED`
(`SameIdAfterReconcile`): reconcile the database, then repeat the same request.

Full verification is not a separate route. A default open (server start,
registry auto-open, `POST /databases/{id}/open`) of a catalog version 6 store
rebuilds state from its verified checkpoints and falls back to full
verification on any mismatch; `POST /databases/{id}/reconcile` on an open
database always re-verifies every receipt, archive row, retained transition and
data-file footer and re-establishes the checkpoints (`Engine::Reconcile`, the
same path as `Engine::VerifyStoreFull`). The open body has no one-shot option
to extend (only `limits`, which the registry persists), so it has no
`verify_full`. Offline, `glamdb-admin verify-store` opens with
`OpenStoreRequest::verify_full` (see the [CLI guide](../../apps/cli/README.md)).

Archived snapshots need no other routes. `.../snapshots` lists them before the
root's snapshots in the same keyset pages, and `.../stats`, `.../files`,
`.../files/{file}`, `.../explain` and `.../scan` with their `snapshot_id` answer
exactly as while the snapshot was in the root; operation lookups and exact
retries keep their original receipts, and an `expected_snapshot` naming an
archived (non-current) snapshot is `PRECONDITION_FAILED`. `retained_snapshots`
in statistics counts archived snapshots.

## Explicit KV routes and receipts

Collection create accepts `mode: "append" | "kv"`; omission keeps the existing
append definition. The server reports mode on collection metadata and compares
it on every repeated create. A different mode returns
`409/COLLECTION_MODE_MISMATCH`; mode cannot be updated. KV requires a
nonnullable UTF-8 `key` field at any schema position. Ordinary `/append` rejects
KV, and KV mutation routes reject append collections.

The following routes are relative to `/databases/{id}` (or unscoped in legacy
single-store mode). They adapt the public synchronous facade and its existing
serialized coordinator, without a second writer or background queue.

| POST route | Request | Result |
| --- | --- | --- |
| `/collections/{id}/upsert` | `{operation_id, rows, expected_snapshot?}` | Full-row, last-input-wins `UpsertDurable` outcome |
| `/collections/{id}/delete` | `{operation_id, keys, expected_snapshot?}` | `DeleteDurable`; duplicate and missing keys are ignored for effects |
| `/collections/{id}/compact` | `{operation_id, expected_snapshot?, max_files?, target_file_bytes?}` | Bounded `CompactDurable` outcome, including a durable no-op |
| `/collections/{id}/lookup` | `{keys, snapshot_id?}` | `{collection_id, schema, snapshot_id, rows, backend, mock}` |

Upsert replaces the whole row; omitted nullable columns become NULL. Ordinary
upsert accepts up to 10,000 rows and delete up to 10,000 keys. Every key is UTF-8
within 1,024 encoded bytes. Lookup accepts at most 4,096 keys, preserves request
order and duplicate hits, and returns JSON `null` for each miss. Its snapshot is
resolved once. Omit `snapshot_id` for latest, use `null` for initial-empty, or a
canonical decimal int64 string for retained history. Result and HTTP body limits
remain 16 MiB. Compaction defaults to `max_files="64"` and
`target_file_bytes="8388608"`; supported bounds are 1–256 files and 1,024–16 MiB
target bytes. Numeric options and all native receipt counters use decimal
strings. No-op operations are still permanently recorded by operation ID.

Native capabilities advertise `features.key_value/upsert/delete/lookup/compact`
and include `kv` in `collection_modes`. `features.bulk_upsert` and
`features.migration` are false over HTTP. `native_only.bulk_upsert/migration`
identify the available C++ facade operations. No bulk-upsert or migration HTTP
route is exposed. Append collections use the separate streaming `/bulk-append`
route, advertised by `features.bulk_append`. The mock and browser demo explicitly
decline KV features.

Version-3 mutation receipts preserve the existing outcome envelope and recovery
context. `rows_appended` is always `"0"`; additional fields are `mutation_kind`
(`upsert`, `delete`, or `compact`), `snapshot_created` (boolean), `input_rows`,
`rows_affected`, `rows_written`, `rows_removed`, `data_files_removed`, and
`compressed_data_bytes_removed`. Existing added-file/byte counters remain.
A no-op has no new snapshot, zero changed counters and `snapshot_created_at_us`,
and an effective snapshot ID equal to its base or `"0"` for initial-empty.
Snapshot history contains publications only, with mutation kind and written /
removed counts. `GET /collections/{id}/operations/{operation_id}` also returns
no-op outcomes. Uncertain writes remain `commit_unknown`; resolve or repeat the
original prepared request after reconciliation. Version-1/2 append receipt bytes
and strict append bounds remain unchanged. Shared literal vectors are in
`fixtures/http/kv-v3.json`; executable route/schema inventory remains in
`fixtures/http/v0.json` and `v0.schema.json`.
