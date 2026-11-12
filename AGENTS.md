# GlamDB agent guide

This file applies to this repository. All paths and commands below are relative
to its root. A nested `AGENTS.md` provides additional guidance for its subtree.

## Keeping this guide current

Update this file in the same change whenever implementation makes its project
status, layout, commands, conventions, or validation guidance outdated. Replace
obsolete statements, including scaffold-only and placeholder descriptions, and
add guidance for new capabilities that agents need to work with. Remove
superseded instructions instead of accumulating a history of them.

Keep the affected README and repository documentation consistent with those
updates. Derive implementation status from the code and validation status from
recorded checks; implementing a feature does not by itself qualify its durability
or complete a roadmap gate. Keep this guide concise, with detailed design and
validation material in repository-local documentation. Its links and instructions
must remain usable from a standalone checkout.

## Project context

GlamDB is an open-source C++23 columnar key-value storage engine in early
development. The checkout implements the SQLite catalog, synchronous write and
recovery coordinator, canonical Arrow input, public C++ facade, outcome JSON,
and network CLI. Independent durable I/O and Arrow/Parquet/Iceberg
storage primitives provide file production, staging, publication validation,
recovery verification, and capacity checks. `core/backend/` connects them to the
coordinator StorageBackend and `core/scan/` implements `Engine::Scan`
(fixed snapshot, verified membership index, SQL three-valued compound predicates `True`/`Compare`/`In`/`IsNull`/`IsNotNull`/`And`/`Or`/`Not`, projection including zero columns, limit, positional Parquet column/row-group reads, conservative manifest file and Parquet row-group pruning, and a bounded immutable metadata/footer cache; page and Bloom pruning remain unimplemented). See
[the scanner guide](docs/development/SCANNER.md) for their contract and
validation status, [the coordinator guide](docs/development/COORDINATOR.md)
for the integration contract, and [the storage guide](docs/development/STORAGE_MODULE.md)
for the file-layer contract. A shared `ResourceOwner` now admits engines,
transport copies and tracked Arrow allocations under finite class and device
reservations; see [the resource owner](docs/development/SHARED_RESOURCES.md)
for conservative envelopes, retained-buffer lifetime and measured validation.
Scan plans, queued manifest descriptors, individual output descriptors and the
per-store 32 MiB immutable metadata/footer cache retain shared reservations;
cursor control admission and live/peak memory profiles are
described in [shared resources](docs/development/SHARED_RESOURCES.md).
Full engine durability remains unqualified. `Engine::GetHealth()` exposes
nonblocking read/write admission and recovery/shutdown state; HTTP
`GET /databases/{id}/readyz` (or legacy `/readyz`) maps write readiness to
200/503. A latched durable-I/O failure permits reads but requires reopen for
new writes; successful reconciliation does not clear that latch. `/healthz`
is process liveness only. Health never qualifies durability.

Independent verification tools, a Python network SDK and a browser workbench
are implemented. They can use the native Asio HTTP server over the public Engine facade.
`--data-dir` starts with no databases; authenticated API/UI operations create,
list, open, close and reconcile databases. Authenticated native operators can
explicitly drop an initialized database with its original store identity;
the SDK/CLI expose exact retries and `bench tpch clean --state`. The standalone
CLI also routes the complete TPC-H generation/init/load/resolve/verify/scan/status
lifecycle through the optional workload package. See
[managed deletion](docs/development/DATABASE_DROP.md) for durable tombstones,
admission draining, safe filesystem removal and startup recovery. Discard still
refuses initialized stores. A separate SQLite service registry (version 3,
transactionally migrated from version 2)
stores identities, persisted engine limits and open intent; collection/data writes remain in core.
Scans use bounded fixed-view pages with validated configurable global/per-database
live, completed replay, byte and lifetime limits. Each admitted cursor reserves
its maximum replay page in both the local quota and shared process memory before
planning. Capabilities expose effective limits, separate HTTP/session occupancy,
shared current/peak memory and bounded refusal/eviction counters. Completed replay
eviction is oldest-first; live cursors are never evicted. See
[scan session policy](docs/development/SCAN_SESSIONS.md) for startup options, the
32-reader profile and `glamdb.scan_policy` real native validation.
Separate HTTP reader, publisher, lifecycle and
control pools use bounded fair per-database queues with deadlines and cancellation.
`--http-readers` and `--http-readers-per-database` configure reader admission
(defaults 4/2); a 1/1 profile queues heavy reads within a smaller shared budget
without restricting publisher, control or lifecycle pools.
Outcome lookups refuse tentative catalog reads without waiting on publication.
HTTP body admission reserves four of sixteen readers from ordinary uploads for
authenticated database lifecycle and scan cancellation, including through a TLS
proxy; public/static POSTs cannot spend that reserve. Excess bodies wait within
the connection ceiling and their original read deadline.
The network CLI also builds independently with OpenSSL; printable bearer tokens
accept up to 8 KiB for organization-scoped OIDC JWTs, with control characters
rejected before connecting. Optional offline commands
are in `apps/admin` and never required for server initialization. Both expose
named collection/schema, append, scan, operation, snapshot and file commands.
The admin tool's `--url` mode calls HTTP without opening an engine; offline
commands require explicit root/identity and fail while the server owns the store.
Command grammar in `apps/cli/data.*` has no core dependency; metadata JSON in
`apps/transport/metadata.*` is shared with HTTP. Named JSON scans complete remote
pages within 10,000 rows/16 MiB, cancel unfinished cursors, and distinguish
unresolved outcomes (exit 3) from definite failures (exit 1). See the
[CLI guide](apps/cli/README.md). `glamdb serve` aliases existing server startup.
Collection routes are scoped by `/databases/{id}`; optional legacy
`--root/--store-id` mode retains unscoped routes. A
separate, explicitly disposable in-memory mock remains a development profile. The architecture and
invariants below describe the intended engine, not qualified guarantees.

Data studio keeps schema, compact reading and nested row editing together;
results stay inside Read rows. Light/dark appearance and sidebar preferences are
local display settings. Runtime design remains configurable through theme JSON.
Existing Keycloak realms use the client-only migration in
[compact workbench operations](docs/development/COMPACT_WORKBENCH.md); custom or
shared authentication flows require explicit review before migration. Native
Overview, Snapshots, Storage and Explain use public-facade HTTP metadata;
reload/restart restore these views from the server. Storage IDs derive from
selected committed manifest membership; details verify actual Parquet footers,
field IDs, statistics and row groups. Unsupported backends retain explicit
session/count fallbacks. Metadata requests are sequenced to respect per-database
admission; stale selections cannot replace a newer workspace.
Completed metadata inventories retain replay while capacity permits; new listings
reclaim completed inventories before refusing admission, preserving unread pages.
Unresolved create/append requests have explicit token-free export/import, retaining
their original identity/schema/payload. Import never submits a write; confirmed outcomes
survive failed metadata refreshes. Published snapshot operation IDs remain
available after reload and look up original outcomes.

The same UI can publish as a static browser-only demo on GitHub Pages. It has
no API token or shared backend: tab-local rows/operations disappear on reload.
JSON export is available there; network SDK and Arrow IPC remain connected-server
features. Do not present BrowserDemoAPI as the public HTTP or durable core API.
The organization gateway (`apps/workbench_access/`) uses Keycloak accounts and
organizations, OAuth2 Proxy browser sessions in a private ephemeral Valkey store,
Authlib/joserfc token verification
with live introspection, and the official Cerbos SDK with YAML policies. SQLite
stores database ownership, transactional audit, idempotent lifecycle receipts and
unresolved backend write admissions; it stores no passwords or password hashes.
Every list and direct database route uses current server-owned policy attributes.
A strict transfer closes admission, waits for requests and streams, and requires
terminal authoritative backend write outcomes before moving ownership. Unknown
writes survive restart and keep transfer closed. Cross-service identity failures
leave explicit pending receipts; emergency disables supersede older pending grants.
See [ORGANIZATIONS.md](docs/development/ORGANIZATIONS.md) and the detailed
[operator guide](docs/development/SSO_OPERATIONS.md) for setup, native account
management, migration, recovery and secret rotation. The former Basic and static
group modes have been removed. Production deployment of the gateway with the
native HTTP transport remains unqualified; explicit mock-backend tests do not
qualify that identity integration or durability.

`Engine::BulkAppendDurable` adds a replayable two-pass source for append collections
before KV. It validates/hash-completes before binding and produces bounded files
under one atomic snapshot/receipt; ordinary append retains its old limits. Read
[bulk ingestion](docs/development/BULK_INGESTION.md) for source lifetime, cancellation,
explicit bulk I/O attempts, result receipt version 2, conservative device reservations
and measured limits. Keep unmarked ordinary one-file guards strict and change
publication/recovery bounds together. Native `POST /collections/{id}/bulk-append` accepts bounded NDJSON through an anonymous
transport spool; the SDK prepares replayable disk-backed requests and the standalone CLI
streams regular files. Uploads are capped at 4 GiB wire bytes, two globally and one per
database; shared disk/memory admission lasts through execution. See the bulk guide for
framing, deadlines, retry, native-only availability and validation.

Native append preparation retains one schema mapping per traversal and reuses
inspected string sizes for its owned columns. Full validation remains per batch,
and fresh post-admission inspection plus the owned-hash comparison remain before
binding. See [append preparation](docs/development/APPEND_PREPARATION.md) for
fragmented-input measurements, rejection scratch and remaining admission limits.

The first milestone is an embedded, synchronous, durable **append store** on
local disk. A store has one owning process and one serialized writer; reads may
run concurrently. Collections are fixed-schema, unpartitioned Iceberg v2 tables.
Append preserves every input row, including duplicate keys. It provides neither
key uniqueness nor replacement semantics.

Ordinary and bulk appends use the pinned Iceberg merge append in the existing
atomic publication. Small manifests merge at the upstream threshold of 100,
targeting 8 MiB; data files and historical views remain immutable. Publication
and recovery compare every rewritten EXISTING entry exactly and count only
ADDED entries as new data. The shared 32 MiB verification guard includes removed
manifest inputs as well as new outputs, the list and the current root. Keep
the eight-new-manifest guard and ordinary/bulk added-file limits unchanged.
See [manifest merging](docs/development/STORAGE_MODULE.md#manifest-merging)
for retained-history growth and compatibility.

Collections carry Parquet write settings (collection-default compression codec
and level, dictionary policy and page limit; per-column codec, dictionary and
value-encoding overrides) that apply to data files of later appends.
They live in the SQLite catalog (version 2, created lazily by the first stored
settings), change by compare-and-set under the writer and are checked against
every written footer; see [the coordinator guide](docs/development/COORDINATOR.md#collection-write-settings).
The SDK (`create_collection(write_settings=...)`, `update_write_settings`) and the
workbench (creation controls, Schema panel) expose them only when capabilities
advertise `features.write_settings`; the mock and static demo do not.

[Explicit KV mode](docs/development/KV_MODE.md) adds full-row COW upsert/delete,
durable no-op receipts, fixed-view batched lookup, bounded bulk upsert, explicit
append migration and contiguous small-file compaction. Collection mode and table
marker must agree. Version-3 receipts and interval membership are mode-dispatched;
never relax legacy append validation/hashes/receipt bytes. No-op COMMIT uncertainty
uses the same recovery fence despite having no candidate root. Retained physical
usage never shrinks on deletion/compaction. The measured layout decision and
component-versus-native evidence are linked from that guide. Native capabilities, receipt-backed snapshot
history, selected-view statistics, snapshot-bound file/row-group details,
fixed-view collection/history/file pages, explain and cursor profiles use the public
facade; see [the metadata guide](docs/development/METADATA.md). HTTP/SDK/UI metadata,
snapshot/file/collection pagination and explain adapters are implemented; object
storage remains a later extension. Collection limits default to eight and accept
1–64 with additional shared catalog/control/recovery reservations. See
[collection profiles](docs/development/COLLECTION_PROFILES.md) for configuration,
complete inventory, fresh-process validation and measured profile limits.
Explicit C++ snapshot retention maintenance is available through
`Engine::ExpireSnapshots`; see [snapshot retention](docs/development/SNAPSHOT_RETENTION.md).
It lazily introduces catalog version 3, preserves all receipts and current data,
and resumes bounded metadata cleanup after recovery. Cleanup reclaims manifest
lists and merged inputs only after their last retained reference disappears;
checkpoint recovery verifies original file provenance and receipt totals.
Already planned cursors keep their fixed data view; new expired reads/preconditions
report `SNAPSHOT_EXPIRED`. Default history remains unchanged. Retention preserves
configured history budgets and pages expiration inventories beyond 8192 entries;
transport maintenance endpoints are not provided.
KV collections refuse snapshot expiration and preserve all retained file intervals
and no-op bound roots. Their first creation introduces catalog version 4 while
existing version-3 append retention stores remain compatible.
General garbage collection, asynchronous admission, replication,
multiple writers, and schema evolution are outside the initial append scope. Consult
[docs/ROADMAP.md](docs/ROADMAP.md) when assessing feature scope; its current
status table separates implemented code from executed evidence per gate and
keeps G4 power-loss qualification **NOT RUN**. Update that table when a change
adds or removes gate evidence.

Fixed schemas accept 1..128 fields, including the 105-column ClickBench layout.
The limit is advertised as `limits.schema_fields`; row, byte and shared-resource
budgets still apply. Wider-schema source support alone is not a completed
ClickBench run or a native durability qualification.

## Architecture and correctness boundaries

| Component | Responsibility |
| --- | --- |
| Arrow | Typed in-memory batches and columnar buffers. |
| Parquet | Immutable user-data files. |
| Iceberg C++ | Table schemas, manifests, file membership, and snapshots. |
| SQLite | Persistent catalog pointer, operation identities, and outcomes. |
| GlamDB core | Write ordering, publication, recovery, scanning, and resource ownership. |

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before changing persistence,
recovery, scan semantics, or resource ownership.
For resource limits, read [the shared budget contract](docs/development/RESOURCE_BUDGETS.md):
it names accounting units, owners, enforcement and release points across the
engine and transport. Preserve its distinction between current local checks and
shared reservation envelopes and remaining RSS/DTO qualification; update the matrix when either changes.

Preserve these boundaries when adding or changing engine behavior:

- **One write authority.** All writes and Iceberg commits go through the C++
  core. Transport and client layers adapt its API; they do not own independent
  writers. Keep HTTP, Python, and UI dependencies outside the public API and core.
- **Explicit store identity.** Creation and opening are distinct operations.
  `OpenExisting` validates the expected store identity and existing catalog;
  missing or corrupt state is an error, never an implicit empty-store creation.
  Store ownership remains locked while dependent operations or handles exist.
- **Durable publication.** A committed response requires durable Parquet,
  manifests, manifest lists, and metadata JSON before the final outer SQLite
  transaction commits. File finalization, buffer flush, file sync, checked close,
  and directory sync all matter. SQLite commits do not sync external files;
  successful internal Iceberg updates do not authorize a committed response.
  The coordinator reports the outer result through `PublicationFinished`:
  promote the exact candidate on success, discard it on rollback, invalidate
  the verified generation on uncertainty. Admission/preparation only use an
  explicitly confirmed base; reconciliation rebuilds invalidated generations.
  Preserve the first I/O failure through retries, repeated close, and cleanup.
  The initial catalog profile uses `journal_mode=DELETE` and `synchronous=EXTRA`,
  verified on the actual working connection.
- **Exact commit base.** A prepared write retains its metadata location, table
  UUID, and optional snapshot identity through publication. Reject a changed
  base instead of silently rebasing. A snapshot-only precondition cannot detect
  metadata-only changes.
- **Explicit outcomes and durable retries.** Distinguish `Committed`,
  `DefinitelyNotCommitted`, and `CommitUnknown`. An uncertain commit blocks
  further writes until authoritative recovery and a durability fence resolve
  it; a timeout alone does not prove failure. Bind a collection-scoped operation
  ID to its canonical content hash before file I/O. Persist the committed
  outcome atomically with the catalog pointer. The same ID and hash return the
  original terminal outcome, including failure; a different hash conflicts.
  Outcomes survive restart and do not expire. The
  [receipt retention decision](docs/development/RESOURCE_BUDGETS.md#operation-receipt-retention-decision)
  keeps them permanent; larger budget-derived operation/history profiles remain
  tracked in #156. Healthy terminal receipt lookups
  use the verified generation and query one catalog record without rereading
  warehouse files or enumerating history. Lookup is not a scrub: observed
  corruption or uncertain publication requires full reconciliation and its
  durability fence before lookups resume. The initial engine has no input
  payload journal or asynchronous job queue.
- **Snapshot reads.** Resolve the selected view once and enumerate its files
  through verified Iceberg membership. Native scans pin the index with the
  confirmed view; publication/recovery build it with strict closure validation.
  Snapshot prefixes require the append-only profile; see
  [the index contract and benchmark](docs/development/SCAN_INDEX.md).
  Read neither uncommitted nor orphan files by
  scanning warehouse directories. Retain historical snapshots; an initial empty
  view stays empty after later writes. Pruning is conservative and followed by
  exact filtering with SQL three-valued NULL logic; only TRUE matches survive.
- **Types and ownership.** Preserve stable field IDs and validate against the
  fixed schema. Timestamps use UTC microseconds in years 0001 through 9999;
  preserve integer precision across interfaces. Cursors and returned buffers
  retain their allocator, accounting, executor, and I/O resources until the last
  dependent handle is released, even after the engine or cursor is closed.
  Scan buffers share an owned Arrow pool and retain the engine and read-only
  FileIO, including after filtering. Cancellation is checked after reads and
  before delivering a batch or EOS; use I/O barriers in concurrency tests.
- **Exact decimal and date types.** Decimal128 precision/scale are immutable;
  use exact coefficients and strict calendar days, never float or timestamp
  substitutes. See [scalar types](docs/development/SCALAR_TYPES.md) for JSON,
  typed predicates, native file annotations and schema-version compatibility.
  Scalar-only schema/hash fixtures remain byte-for-byte version 1; extended
  schemas use version 2 while append/hash formats remain unchanged. Ordinary
  append result receipts remain version 1; bulk result receipts use version 2.
  Check decimal bounds against signed limits: absolute-value precision checks
  can overflow for the minimum physical signed 128-bit coefficient.
- **Bounded resources and retained data.** Account for data, metadata, and
  control memory separately, including allocations outside Arrow. Check that a
  published state can be reopened within the same limits. Capacity exhaustion
  produces backpressure before publication, not partial success or hidden
  deletion of history. Publication rechecks physical warehouse usage after
  production (the conservative admission inventory plus the attempt's own durable files).
  Linux local-filesystem admission uses bounded change notifications, with
  exact full-walk fallback; see [warehouse inventory](docs/development/WAREHOUSE_INVENTORY.md)
  for the exclusive-ownership contract, unsupported external mutations and strict mode.
  Ordinary notified growth after admission is caught by the next admission;
  recovery checks the same physical cap without reserving a future attempt. Logical reachable
  usage is accumulated with overflow and limit checks across collections.
  Warehouse orphan files consume capacity; their automatic cleanup is
  outside the initial scope. Interrupted recovery scratch files are reclaimed
  only after exclusive ownership and identity validation, before device admission.
  A metadata root superseded by a confirmed publication is removed once no
  scan is still resolving it. Explicit opt-in retention also reclaims expired
  manifest lists through its durable cleanup queue after collection planners
  release; see [snapshot retention](docs/development/SNAPSHOT_RETENTION.md).
  Current manifests, Parquet data and receipts remain retained. Detected
  corruption closes readiness; leave SQLite journal recovery to SQLite rather
  than deleting or truncating its journal.

## Repository navigation

| Path | Use |
| --- | --- |
| `public/storage/` | Installed C++ types, scan API and synchronous engine facade. |
| `core/api/`, `core/catalog/`, `core/engine/` | Canonical input, catalog, coordinator, and internal storage-backend seam. |
| `core/backend/` | Production StorageBackend over `glamdb_storage`, built with `GLAMDB_BUILD_STORAGE`. |
| `core/scan/` | Predicate evaluation, conservative zone pruning, snapshot planning, bounded metadata/footer cache and positional Parquet scan cursor. |
| `core/telemetry/` | Private typed event adapter over pinned Quill; bounded frontend/backend memory and stderr process counters. |
| `deploy/logging/`, `tools/logging/` | Pinned Loki/Alloy/private Grafana deployment, native macOS/Podman collection, control-plane adapter and real ingestion/redaction checks. |
| `core/io/` | Independent local filesystem, attempts, receipts, durable files and copying (`glamdb_io`). |
| `core/storage/` | FileIO, Parquet writer, Iceberg staging, publication/recovery and capacity checks (`glamdb_storage`). |
| `apps/cli/`, `apps/client/` | Standalone `glamdb-cli`: network database management and HTTP/HTTPS requests, without engine dependencies. |
| `apps/admin/` | Optional `glamdb-admin` store creation, offline data/inspection facade commands, and HTTP mode. |
| `apps/server/`, `apps/transport/` | `glamdb`: Asio HTTP server, shared precise codecs and native outcome serialization. |
| `cmake/` | Build options, sanitizers, quality targets, and dependency recipes. |
| `tests/` | Native unit/recovery/SqlCatalog/VFS/consumer suites, `vertical/` public-API storage tests, `mock/` in-memory scan tests, verification and actual SDK HTTP/HTTPS tests. |
| `tests/stress/` | Explicit manual native capacity campaigns; excluded from default pytest/CTest and CI. |
| `apps/mock_server/` | Disposable Python mock, real HTTP/Arrow, no warehouse/catalog access. |
| `apps/workbench_access/` | OIDC/Cerbos gateway, ownership/audit catalog, lifecycle CLI and offline migration. |
| `fixtures/`, `tools/verification/` | Literal canonical/metadata inputs, independent row oracle and evidence helpers. |
| `sdk/python/`, `ui/` | Standalone Python HTTP SDK; HTML/CSS/ES-module pink workbench and browser/unit tests. |
| `tools/workload/` | Mimesis datasets, reference eight-table TPC-H datasets/lifecycle, SDK/Locust workloads, immutable SQLite outcome journals, fixed-view verification and Q1/Q6 client reducers. |
| `probes/`, `benchmarks/` | Installed-dependency/compatibility probes and workload measurements. |
| `scripts/` | Developer tooling, including quality checks. |
| `deploy/alerting/`, `tools/alerting/` | Four-state Prometheus rules, Alertmanager routing, native Telegram receiver and executable replay/transition tests. |
| `core/telemetry/metrics.*`, `deploy/monitoring/`, `tools/monitoring/` | Library-backed native/Python telemetry, standard collectors/chart, application panels and checks. |
| `third_party/` | Pinned upstream submodules and `lock.json`. |

Read [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) when placing new
components. Read [the workbench guide](docs/development/WORKBENCH.md) for
installation, runtime theme changes and testing. The [native HTTP contract](docs/development/HTTP_SERVER.md) and executable
`fixtures/http/v0.json` inventory supported routes and explicit gaps. Keep
transport dependencies outside the public API; do not invent missing metadata.
Shared literal outcome/scalar vectors live in `fixtures/http/outcomes-v0.json`;
see [the fixture guide](fixtures/http/README.md) for their native, SDK, mock and UI
consumers. Keep complete recovery contexts in test doubles. Missing recovery
fields are protocol errors; supported extension fields remain in client responses.
SDK append preparation bounds canonical decoded bytes and serialized JSON bytes
independently. Requests above the native 16 MiB wire ceiling fail locally,
including explicitly constructed prepared bodies; response limits are separate.
For branding/static hosting read [PUBLIC_DEMO.md](docs/development/PUBLIC_DEMO.md).
Preserve the supplied PNGs unchanged. Brand paths must be local assets; sizes
belong in theme.json. Asset URLs must support both / and /glamdb/.

The complete single-host Linux monitoring/logging/alerting deployment is
`scripts/observability.py`, implemented in `tools/observability/`. Read the
[Russian quickstart](deploy/observability/QUICKSTART.ru.md) before deployment.
It requires local rootful Podman, systemd/cgroup-v2, actual application metrics
and logs, and a dedicated bounded Loki mount. `init/up/status/check/down` use
owned resources and preserve persistent data. Run its standard-library unit
suite with `python3 -m unittest discover -s tools/observability/tests -v`;
the real lifecycle test requires an explicitly disposable `glamdb-lab-*` config.
Linux arm64 runtime was exercised; amd64 plans/pins and boot-unit generation do
not establish an executed amd64 or actual-reboot test. Missing sources never
become mock producers or synthetic healthy metrics.

## Build and development commands

For alerting setup and validation, read [ALERTING.md](ALERTING.md) and run
`python3 tools/alerting/check.py` with the required Prometheus/Alertmanager tools
on PATH or passed explicitly. Its process tests run real Prometheus/Alertmanager
with a loopback Telegram protocol peer only.
The native receiver owns delivery; there is no webhook/outbox service.
Telegram uses one combined daily current-state summary in the 10:00–10:01
Europe/Moscow window. State changes outside that window do not notify.
`deploy/alerting/external-observer/` runs on a separate host/notification path.
Gateway application metrics cover requests after the edge/OIDC proxy. Separate
Caddy outer-edge counts include authentication rejections before the application.
Native operation counters measure actual core operations.
For real host/container/Kubernetes monitoring, private application endpoints,
source gaps and deployment steps, read [MONITORING](docs/development/MONITORING.md).
Install `tools/monitoring/requirements.txt` with hash verification and build the pinned
prometheus-cpp core/pull installation through `cmake/dependencies` (including
the recorded CivetWeb atomic stop-flag patch) before `python3 tools/monitoring/check.py
--promtool PATH`. See the coverage matrix linked from that runbook. Keep missing
collectors/unsupported fields as NoData; exporter reachability does not establish
native database health or durability. The short-lived CLI publishes no textfile.
`GLAMDB_BUILD_METRICS=ON` enables the standalone metric target in IO-only mode;
the normal engine build always includes its domain instrumentation.
The native server's `--metrics-listen-address IP:port` opts into a separate private
Exposer; omission disables the listener. `glamdb_server_ready` samples actual
managed engine health, without database identifiers. Run `glamdb.native_metrics`
for real storage/counter/readiness/restart coverage; `GLAMDB_TEST_TMPDIR` selects
its disposable test parent when `/tmp` cannot meet the storage disk reserve.
The listener includes bounded native HTTP request/status counters, duration and
in-flight metrics through asynchronous response completion/cancellation.
The server logs bounded JSON to stderr by default; `--log-directory DIR` selects
Quill 13 file rotation at 8 MiB and three backups. Success sampling is configured
with `--log-success-sample-every` and `--log-max-success-per-second`. Generated
HTTP correlation uses the existing scoped worker/engine context; no tokens, URLs or data are logged.
`glamdb.server_observability` checks real operations, publication replay counters,
read scopes and sampling; metrics remain unsampled. Retained cursor counts and
published rows/bytes are process observations, not database inventory.
Normal native builds consume the pinned header-only `third_party/quill`; IO-only
metrics builds retain no Quill dependency unless `GLAMDB_BUILD_LOGGING=ON`.

Client/mock development uses Python 3.13 and Node 22+:

```sh
bash scripts/bootstrap-workbench.sh
npm --prefix ui exec -- playwright install chromium
bash scripts/check-workbench.sh
```

On Linux, install browser system dependencies with Playwright's `--with-deps`.
All network tests require localhost sockets; HTTPS tests require OpenSSL. The
check script runs real HTTP/HTTPS tests and Chromium, not only transport doubles.
Node API tests also open localhost sockets to exercise Fetch gzip/Brotli decoding
and Arrow cancellation. Session-isolation browser tests delay responses across
collection, snapshot and connection changes.
Client conformance uses `GLAMDB_TEST_BACKEND=mock|native`; native requires an
executable `GLAMDB_NATIVE_SERVER` and fails on setup/identity/storage errors.
The same SDK TCP/TLS scenarios and `shared-contract.spec.mjs` run against both;
native checks inspect catalog-selected JSON/Avro/Parquet after closing storage.
`GLAMDB_TEST_BACKEND=static` runs only the browser demo, without an API server.
See [client conformance](docs/development/CLIENT_CONFORMANCE.md) for commands,
documented response differences and separate evidence. Normal native acceptance
runs the shared SDK suite before Chromium and propagates either failure.
Retain native browser child ownership through bounded shutdown and reaping;
`tests/integration/test_browser_lifecycle.py` covers interrupts, child exit
statuses and failed-startup cleanup.
The check script also packages/serves the public demo under /glamdb/ and verifies no API
requests, token prompt, cross-visitor state or credential files are introduced.
See [validation](docs/development/WORKBENCH_VALIDATION.md) for exact evidence and
limits. Keep secrets in environment or process memory; never browser storage.
Arrow downloads require the preview's fixed snapshot, MIME, identity encoding,
length, checksum and complete modern IPC framing before saving. The SDK checks
the first page against requested/opened snapshots and preserves consumer/decode
errors through owner cleanup; returned batches retain their buffers. See
[client conformance](docs/development/CLIENT_CONFORMANCE.md) for separate native
and mock export/ownership tests and the browser framing validator's scope.
The standard Python suite includes OIDC claim/CSRF/policy-route/admission,
workflow/retry/migration and disconnect regressions with explicit test doubles.
Real identity infrastructure tests are opt-in and require a disposable provisioned
Keycloak/OAuth2 Proxy/Cerbos deployment. They must not silently create accounts on
an unknown host. Full system verification targets a configured real GlamDB server
by default; mock backends require explicit selection and cannot certify native
integration. See [current validation](docs/development/ORGANIZATIONS_VALIDATION.md).
Initialize ownership state with `python3 -m apps.workbench_access.admin --catalog
/outside/runtime/resources.sqlite init`; start the private adapter with
`python3 -m apps.workbench_access --config /outside/runtime/gateway.json
--operator-socket /outside/runtime/operator.sock`. Only Caddy public HTTPS is exposed;
identity administration, the operator socket, backends, PDP and metrics stay private.
Keep catalog and credential files owned by the service user, mode0600, outside UI.
Do not edit a live catalog or remove its owner lease to bypass the owning process.
Operational events use the optional stderr logging adapter when installed; runtime
journals own rotation. They never replace transactional ownership audit. Optional
metrics are unavailable when the metrics component is absent, rather than fabricated.
Mock faults require explicit development configuration. Do not give the mock a
production store path. Every production write must eventually use the C++ core.

Requirements: CMake 3.28+, Ninja, Python 3.10+, Linux, and a C++23 compiler and
standard library with `std::expected` (GCC 14+ or Clang 18+ with a matching STL).
Quality targets require LLVM major version 18 for both `clang-format` and
`clang-tidy`.

Native CI runs normal, ASan, UBSan, and TSan on PRs via `scripts/ci-native.sh`.
Pushes to `main` and manual runs also include MSan and LSan.
MSan/TSan rebuild the dependency prefix and pinned
libc++/libc++abi with instrumentation; other profiles use ordinary dependencies.
MSan disables Arrow's custom allocators at build time and uses the system allocator.
CTest discovers GoogleTest cases, and CMake discovers test `.cpp` files within
the unit, I/O, storage, mock, vertical, and fault suites. Tooling tests use unittest discovery.
The normal profile also runs named `vertical`, `nostorage` (a separate
`GLAMDB_BUILD_STORAGE=OFF` build) and `mock` contours.
Python tests use repository-wide pytest discovery (excluding native tooling,
manual stress suites, vendor, and generated trees); Node and Playwright use their own discovery.
All four PR native profiles are required checks and run on every PR, including
documentation-only changes. The main ruleset is tracked in
`.github/rulesets/main.json`; changes also need to be applied in GitHub.
The ruleset also requires `Workflow security` and `clang-format`.
Workbench checks run only on pushes to `main` and manual dispatch; pushes still
skip documentation-only changes. See
[test CI](docs/development/BUILDING.md#test-ci) for logs and reproduction.

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
cmake --preset local
cmake --build --preset local
ctest --preset local
```

The application links the pinned SQLite, Arrow and Iceberg installation. Tests
also require GoogleTest. Applications use pinned header-only CLI11, Asio and nlohmann/json submodules;
The network client uses system OpenSSL for verified TLS; the server uses its Crypto
component for Arrow page checksums.
These dependencies are not part of the public engine API or dependency installation. Use `release`
or `asan-ubsan` in place of `local` for the corresponding configure, build, and
test presets. `GLAMDB_BUILD_STORAGE` defaults to `ON` (unless `GLAMDB_IO_ONLY=ON`)
and adds `glamdb_storage`, the production backend, the scanner, `glamdb_vertical`
(CTest labels `vertical;scan`), the in-memory planner/cursor suite `glamdb_mock`
(labels `mock;scan`) and the fresh-process `glamdb.scan_process` integration test;
with it `OFF`, public factories install no backend and collection creation,
appends and scans report `DEPENDENCY_UNAVAILABLE`.
`glamdb.installed_consumer` copies its sources into the build workspace, links
only installed public targets, and runs independent write/reopen processes.
It requires real A/B appends, latest/A/initial rows, persisted receipts and
idempotent retries when storage is enabled, and explicit dependency errors
when disabled. Run it in both builds with `ctest --test-dir <build-dir> -R
'^glamdb.installed_consumer$' --output-on-failure`; mock tests are separate.
Core protocol tests use a test-only backend through `EngineFactory::Create/Open`;
the real SqlCatalog test verifies the catalog integration separately. Neither
establishes durable user-file production or power-loss safety.

For Linux tests on macOS, `./scripts/test-in-docker.sh asan-ubsan` builds only
independent I/O with `GLAMDB_IO_ONLY=ON`, without Arrow/Iceberg/SQLite. The full
build preserves the installed `glamdb::core` engine API. To include storage
adapters, build the pinned prefix with `./scripts/build-deps-in-docker.sh`, then
run `./scripts/test-storage-in-docker.sh asan-ubsan`. See
[BUILDING](docs/development/BUILDING.md) for isolation and sanitizer compatibility.
`scripts/glamdb-local.sh up|down|status|logs|cli` builds and runs a local server
(native on Linux, Docker elsewhere) configured by `GLAMDB_PORT`, `GLAMDB_TOKEN` and
`GLAMDB_DATA`; it rebuilds the dependency prefix when its recipe fingerprint changes.

The coordinator supplies owned, immutable normalized batches and expected
operation summaries. Backend admission checks capacity and allocates the writer
batch before binding; production reuses it under the call's owned-input reservation.
The reservation includes Arrow slot growth for NULL and omitted fields.
Allocation refusal consumes no operation ID. The coordinator
confirms the outer catalog COMMIT. A publication guard's `GetCandidate()` is
tentative until that confirmation. Scan callers retain a `ReadOnlyView()` so
reads do not join the writer's attempt. Isolated storage tests use an in-memory
catalog; coordinator tests use a separate test backend. With storage enabled,
`glamdb.durable_storage_catalog` also checks the real file/catalog publication
boundary and catalog-selected recovery in a fresh process.

`TCombinedStorageFaultTest` composes `TLocalStorageOptions::Fs` with the SQLite
journal fault VFS through the internal `EngineFactory::CreateWith` seam. It
checks writes (short, zero-progress, EIO/ENOSPC), close and file/directory sync
failures followed by lost failure-recording results. `TCatalogStorageFaultTest`
checks binding, publication, Parquet finalization and recovery fences;
`TShortWriteStorageFaultTest` combines recovery reads and fence faults.
`TProcessCrashStorageFaultTest` exits without shutdown or reconciliation at
uncertain SQLite boundaries. Fresh-process public reopen checks literal rows,
exact acknowledged receipts, historical views and retry identity. Syscall EIO
stays latched until reopen; ENOSPC and zero-progress writes fail only the attempt.
These controls are test-only; production defaults remain POSIX and the default
SQLite VFS. Process exits do not qualify power loss. See
[coordinator fault evidence](docs/development/COORDINATOR.md#combined-storage-faults).

`GLAMDB_BUILD_IO_PROBE=ON` adds the disposable storage fault probe and requires
`GLAMDB_BUILD_STORAGE=ON`. Its Python runner checks native receipts and optional
strace evidence; passing cases without strace remain incomplete. Recovery path
deduplication uses a bounded temporary SQLite database, separate from the catalog.
See [qualification preparation](docs/development/QUALIFICATION.md) for commands,
temporary-workspace prerequisites and the remaining qualification boundaries.

The opt-in [scalable history profile](docs/development/SCALABLE_HISTORY.md)
coordinates public engine/HTTP operation and retained-storage budgets, including
recovery workspace and larger metadata roots. Defaults stay at 4,096 operations
and snapshots. Catalog inventory/recovery use 256-record keyset pages; healthy
outcome lookup remains indexed. `glamdb_history_bench` and `benchmarks/history.py`
exercise all eight SF=10 tables through ordinary appends and verify exact typed
row multisets in a fresh process with a bounded external-sort oracle.
The [validation report](docs/development/SCALABLE_HISTORY_VALIDATION.md) records
fresh-process checkpoint recovery, physical bytes, current-image RSS and review findings.
`glamdb.history_campaign` covers duplicates, original failed/successful outcomes,
retry/history, lowered budgets, restoration and oracle negative controls.

`glamdb_capacity_bench` and `benchmarks/capacity.py` measure native aggregate
ingestion, retained history/outcomes, physical orphan space, retained slices and
fresh-process reopen. The default campaign budgets 5,000 attempts of 1,000 rows
and records every 100 successes without raising caps; short real-engine checks
run as `glamdb.capacity_campaign`. See [the campaign guide](docs/development/CAPACITY_CAMPAIGN.md)
for profiles, counters, evidence and cache/qualification limits. Keep full
measurement runs manual and separate external-file sync counters from SQLite.

After configuring `local`, check project C++ sources and headers with:

```sh
cmake --build --preset local --target format-check tidy-check quality-negative-check
```

`quality-negative-check` verifies that deliberate formatting and analyzer
violations are rejected. `cmake --build --preset local --target format` rewrites
project C++ formatting. `.github/workflows/cpp-quality.yml` runs separate LLVM 18
format and tidy jobs. Formatting runs on PRs, pushes to `main`, and manual dispatch;
tidy runs only on pushes to `main` and manual dispatch. The tidy job verifies dependency
pins and reuses an exact installation cache, rebuilding on a miss. Its key covers
the build recipe, platform, toolchain, packages and flags. Storage, the I/O probe
and tests stay enabled; formatting requires no build. Tidy uses two concurrent processes
(`GLAMDB_TIDY_JOBS` overrides this), with grouped diagnostics and per-file timings. See
[docs/development/BUILDING.md](docs/development/BUILDING.md) for installation,
tool path overrides, and build details.

Operational logs are independent of store publication. The native adapter uses
pinned Quill BoundedDropping queues and its JSON sink on stderr; the Python
adapter uses logging, python-json-logger and standard bounded QueueHandler /
QueueListener. Runtime journald/Docker/Kubernetes owns stderr retention; optional native files
use Quill rotation. Do not add a custom writer or log transport.
Register native producer threads during worker initialization: only sixteen
lifetime producer allocations are admitted to bound thread churn even with a
blocked sink. Server file mode uses Quill directly; `GLAMDB_LOG_DIRECTORY` is not used.
Never add payload/header/query text or uncontrolled identifiers to the event
schema. Quill queue admission and backend callbacks are measurable; exact
native delivery/backlog are unavailable. Read [LOGGING.md](docs/development/LOGGING.md)
for actual counters, lifecycle, journal quotas, collection and private search.
The [macOS/Podman profile](deploy/logging/MACOS_PODMAN.md) installs native Alloy
and a Loki Quadlet with a dedicated 1 GiB filesystem and required mount guard;
its file source requires actual bounded schema-1 JSONL producer files.
Install the pinned formatter before Python/CTest: `python3 -m pip install
--require-hashes --only-binary=:all: --no-deps -r tools/logging/requirements.txt`
inside the development virtualenv. Run `python3 -m unittest discover -s
tests/logging -v` or `glamdb.logging` CTest. Tests exercise real Quill, blocked /
closed stderr, overload, thread churn, shutdown and secret projection.

The shared SHA-256 compressor uses runtime-gated x86 SHA-NI or ARMv8 SHA2,
with a portable fallback and no new I/O-only dependency. Read
[SHA-256 validation and measurements](docs/development/SHA256.md) for forced-path
tests, compatibility checks and the manual `glamdb_sha256_bench` target.

For dependency work, read [third_party/README.md](third_party/README.md).
Verify checked-out revisions without building them:

```sh
cmake -P cmake/dependencies/VerifySubmodules.cmake
```

When a task needs the dependency build, initialize submodules and use the
separate preset. It downloads upstream dependencies and installs to
`.deps/install`:

```sh
git submodule update --init --recursive
cmake --preset deps-local
cmake --build --preset deps-local --parallel 2
```

## Installed system acceptance

`tools/acceptance/run.py --prefix PREFIX --workspace NEW_CANONICAL_PATH` runs the
installed native service, CLI, SDK and Chromium against one persistent marked
store: A=1,000, latest=1,010, initial=0. It verifies duplicate rows, precise
JSON/Arrow, retained buffers, successful/failed outcomes, restart/exact retry,
browser reload and a separate UI-write collection. It then runs offline reads,
checks the physical catalog→JSON→Avro→Parquet chain and whole-store restore with
the original root unavailable. Missing native setup fails; `--no-browser` is an
explicit incomplete diagnostic profile. Normal native CI invokes the recipe.
See [the guide](docs/development/NATIVE_ACCEPTANCE.md) for dependencies/evidence.
`glamdb-admin quiesce-store` closes SQLite and retains the independent directory
lease until stdin closes; the backup helper holds it through copy and verification.
No live-service shutdown, automatic source deletion, power-loss or full governor
qualification is implied. `GLAMDB_TEST_TMPDIR` chooses a canonical disposable
parent for native test directories without disabling disk reserves.

## Code and dependency conventions

The network CLI's `generate`, `bench write/read/verify/resolve` and
`bench tpch generate/init/load/resolve/verify/scan/status/clean` use the optional
Python package through reproc; the CLI still builds without engine dependencies.
See [the workload guide](tools/workload/README.md). Install its hash-pinned
`tools/workload/requirements.txt` and local package before testing. Run
`bash scripts/check-workload.sh unit`; the default `bash scripts/check-workload.sh`
requires the built real server and CLI and runs native TLS E2E without mock fallback.
The normal native CI contour includes this E2E. Mock transports belong only in
explicit fault tests. Preserve exact saved append UUID/body/hash, never silently
retry or reconcile, and count only unique committed operations. Locust owns
scheduling, distributed statistics and percentiles; do not implement another load
engine or duplicate SDK codecs. Token values stay exclusively in the environment.

TPC-H generation requires explicit `--type-profile native|legacy-scaled` and
reference TPC-H Tools 3.0.1 provenance. Preserve all eight tables/61 columns and
exact decimal/date values. Bound both row buffers and metadata; the initial
profile admits at most 65,536 parts and refuses oversized layouts without
truncating data. `init` explicitly creates a dedicated managed target;
`load` assigns disjoint immutable chunks while publication remains serialized.
Keep state, dataset hashes, original UUID/body/hash and receipts through resume;
never replace a pending request or silently change the plan/limits. Full
verification checks the complete plan/receipt inventory and initial/first/final
snapshots before `load_complete`; scans bind the saved eight-snapshot
vector. Q1–Q22 scan coverage measures source access patterns; only Q1/Q6 have
complete client reductions. Read [the TPC-H guide](docs/development/TPC_H.md) for
capacity/recovery and [small native acceptance and independent review](docs/development/TPC_H_VALIDATION.md).
POST refusals can be unbound: require authoritative lookup and an explicit retry
after not-found; retain per-attempt evidence. Full SF=1/SF=10 CLI HTTP acceptance
remains **NOT RUN**; existing retained-history measurements do not qualify it.

Follow [the C++ style guide](docs/development/CPP_STYLE_GUIDE.md) for naming,
declarations, layout, and error handling. Keep `.clang-format` aligned with its
mechanical rules; vendor and generated code are excluded.

- Use C++23 with the checked-in `.clang-format`, `.clang-tidy`, and
  `.editorconfig` as the formatting and diagnostic authorities. Project compiler
  warnings and configured tidy diagnostics are errors. The C++ line limit is
  120 columns.
- Keep each component's private `.h` and `.cpp` together in `core/`. Installed
  public headers belong in `public/storage/` and must be usable without private
  headers, source-tree paths, or transport dependencies.
- Make ownership and error propagation explicit. Recoverable I/O, validation,
  and capacity failures must remain reportable outcomes, not process aborts or
  fabricated successful results.
- Prefer public dependency APIs. Check the pinned source before relying on
  upstream behavior, especially I/O hooks, close semantics, and transaction
  boundaries. Record any necessary upstream patch with its dependency revision.
- Treat `third_party/lock.json` as the authority for direct dependency pins.
  Change submodule revisions and matching lock entries together; preserve
  archive verification and upstream licenses. Keep unrelated vendor edits out
  of application changes.
- Keep generated builds, dependencies, and experiment output in the ignored
  `build/`, `.deps/`, and `artifacts/` directories.

The optional monitoring profile in `deploy/monitoring/collectors/` uses actual
macOS node_exporter, named Podman inventory and narrow read-only cgroup-v2
exporters. Its pinned collectors keep Mac, VM and container measurements separate
from Kubernetes. The live Grafana overview separates transport, engine outcomes
and edge/gateway layers. `tools/monitoring/extend_checks.py` applies optional
source checks to alerting config; unavailable producers remain NODATA. Validate
Prometheus and Loki dashboard queries with their respective engines.
Telegram summaries use bounded, escaped native Alertmanager HTML groups; run
`tools/alerting/check.py` for template rendering and loopback delivery checks.

ClickBench preparation/loading/verification supports native DATE or explicit
legacy-date profiles and preserves all 105 columns and exact append identities.
Query campaigns validate all 43 scan/reduce readings. See
[ClickBench](tools/workload/CLICKBENCH.md) and [physical inventory](docs/development/STORAGE_FOOTPRINT.md).
The complete hits load remains blocked by the 4096-operation/history profile;
do not raise constants, drop receipts, or replicate the dataset across workers.

## Validation and reporting

- For behavior changes, add and run focused tests in the appropriate suite.
  Storage tests need independent expected results, including duplicate rows,
  fixed snapshots, retries, and failures at the affected persistence boundary.
  Use a fresh process for reopen/recovery tests and sanitizers for lifetime work.
- Run the relevant build, tests, and quality checks for C++ changes. For
  documentation-only changes, verify factual claims, commands, and links;
  engine or dependency builds are unnecessary unless the change concerns them.
  Read `.github/workflows/` when changing CI; the current workflow-security
  checks apply to every pull request, including documentation changes.
- Separate designed behavior, source inspection, executed PASS/FAIL, and
  checks not run. Report exact commands, results, and missing prerequisites.
  A smoke test does not establish engine correctness or durability.
- Process crashes and power loss require different evidence. `kill -9` does
  not discard the OS cache. Initial durability qualification targets Linux
  x86-64 on a dedicated local ext4 volume with verified flush behavior; macOS
  builds and simulated faults alone do not qualify that environment. Record
  environment details with measurements and durability results.
  See [qualification preparation](docs/development/QUALIFICATION.md) for the
  separate controller, evidence and remaining campaign requirements.
- Run destructive fault tests only on disposable test stores or dedicated test
  volumes. Keep credentials and private data out of fixtures, traces, and logs.

## Contributions and security

For branch, pull request, or dependency publication work, read
[CONTRIBUTING.md](CONTRIBUTING.md). Development branches belong in forks;
upstream keeps only `main`. Use the repository's pull request template to
describe behavior, validation, and any storage compatibility impact.

When changing workflows, preserve SHA-pinned Actions, read-only PR permissions,
and separation from privileged release credentials. For vulnerabilities, follow
[SECURITY.md](SECURITY.md) and report privately rather than in public issues.
Keep `.github/allowed-actions.json` and GitHub's selected Actions setting in sync;
an unlisted Action prevents the entire workflow from starting.

Native HTTP verification includes `glamdb.http_review` for framing/admission and
`glamdb.native_http_fault` for registry, publication and listener recovery faults.
`glamdb.http_security` checks credential-free diagnostics and rejection before
Continue over HTTP and verified TLS, using the shared `tests/server/tls_proxy.py`.
Keep request values out of validation diagnostics.
Configure `Python3_EXECUTABLE` with SDK, PyArrow and
JSON Schema dependencies (`requirements-native-ci.txt` for the CI Python/platform),
then run `GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 ctest --preset local -R 'glamdb\.(native_http|http_security|transport\.|cli)'`.
The server requires `GLAMDB_BUILD_STORAGE=ON`; storage-disabled builds retain
the network CLI and codecs. `GLAMDB_BUILD_ADMIN=ON` additionally builds the offline
`glamdb-admin`; it is not required to initialize or operate a server. Run native browser checks with `GLAMDB_NATIVE_SERVER`
pointing to the built server; `GLAMDB_E2E_PYTHON` can select
the workbench Python environment. See the native HTTP guide for exact commands.
`glamdb.cli_data` (storage and admin enabled) runs real local/remote command parity,
restart/retry, precise scalar, pagination/bound and ownership tests. Run
`ctest --preset local -R 'glamdb\.(cli_data|cli_network|admin)' --output-on-failure`.
Literal wire outcome tests in `glamdb.cli_network` are separate protocol evidence.

SIGINT/SIGTERM close socket and scan admission and mark active scans cancelled
before worker draining. Cancellation never waits on a scan's session lock; retain
its read lease until the call finishes. Accepted writes still drain, and shutdown
deadlines or lost COMMIT replies never establish a definite failed outcome.

Native CI requires SDK/Arrow/schema checks in every profile and native browser
acceptance plus a standalone client build in the normal profile. MSan uses the
ordinary standalone TLS client against the instrumented server; system OpenSSL
is not instrumented. Test-only native HTTP VFS faults model COMMIT uncertainty
and lifecycle draining; they do not establish physical power-loss durability.

Server logging is connected to HTTP completion and synchronous core failure/recovery
paths through a scoped request context. Preserve logger lifetime through worker and
engine shutdown, and keep stable worker count within the sixteen lifetime producer
budget. `glamdb.server_logging` tests real requests, stored-data corruption, redaction
and admin stdout; the native HTTP fault suite checks uncertain-COMMIT correlation.
See [Reading server logs](docs/development/READING_SERVER_LOGS.md) for sampling,
status semantics and diagnostic commands. Embedded calls without a scope stay silent.
