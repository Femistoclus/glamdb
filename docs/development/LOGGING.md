# Operational logging

The native server, offline administrator and Python adapter emit best-effort JSON to **stderr**.
For a short operator guide, see [Reading server logs](READING_SERVER_LOGS.md).
Server stdout remains its listening-address JSON; admin stdout remains its command result. The application has no
file logging mode or file rotation. `GLAMDB_LOG_DIRECTORY` is no longer used.
The normal path is application → runtime journal → Alloy → Loki → Grafana.
Administrative audit and storage outcomes remain in transactional metadata;
a log admission or Loki acknowledgment never authorizes a committed response.

## Native integration

The server owns one logger across startup, HTTP worker draining and database
shutdown. HTTP session completion emits once on the I/O thread after the final
response write, or on early connection closure. The fourteen stable route
workers register on first use before entering the engine; with the I/O thread
this stays below the sixteen-producer lifetime budget. No logger pointer is
published globally. A scoped thread-local context passes generated request IDs
and typed operations into synchronous engine calls, and restores the previous
context on return. The scope itself never allocates a producer queue.

The coordinator emits `recovery_required` for uncertain create/append commits
and failed recovery, including corruption discovered by explicit reconciliation
or startup reopening. Its observed I/O/corruption paths emit `storage_failed`. These events share the HTTP request ID even if the client
has disconnected before the engine call finishes. They do not replace durable
outcomes or claim exhaustive tracing of every file operation. Embedded callers
without a scope remain silent. The installed core package carries the private
logging archive and thread dependency without requiring Quill headers.

Admin commands wrap the synchronous engine call in the same context. Read and
quiesce commands retain their stdout protocol. The network client is unchanged;
the Python adapter is not wired into a production service in this repository.

## Libraries and event boundary

Native logging uses [Quill 13.0.0](https://github.com/odygrd/quill/releases/tag/v13.0.0),
MIT, pinned at `eb802a37c7d585840324886a3d8648c9c2159952` in
`third_party/lock.json`. Its public JSON console sink owns JSON escaping,
serialization and writes. A small subclass redirects its inherited stream to an
owned duplicate of stderr because the upstream default selects stdout. The
stream is unbuffered; no application writer, queue, retry thread or rotation sits
beside Quill. Darwin's duplicate uses `F_SETNOSIGPIPE`; the Linux backend inherits
a blocked SIGPIPE mask. Process-wide signal handlers are preserved.

Python uses stdlib `logging`, `QueueHandler`, `QueueListener` and
[python-json-logger 4.2.0](https://pypi.org/project/python-json-logger/4.2.0/),
BSD-2-Clause. The verified wheel hash is in `tools/logging/requirements.txt`.
The few handler overrides count actual admission/errors, preserve the domain
projection and permit shutdown of a full bounded queue. A standard unbuffered
IO stream owns a duplicate of stderr so failed writes cannot poison CPython's
global stderr buffer and change its exit status to 120. No custom worker,
network transport or file writer remains.

`TLogEvent` has finite event/operation/outcome enums, generated correlation,
duration, row count and status. It accepts no text, paths, headers or payload.
Python `sanitize_record` projects the same fields plus a bounded revision and
validated organization/database IDs (lowercase UUID or legacy slug up to 64
characters). Integers are bounded at `2**63-1` in Python; native values retain
their unsigned types. Unknown fields and exception strings are discarded.

Quill's native JSON envelope includes static source metadata and a constant
message template; named argument values, including numbers, are strings.
Its timestamp is Unix nanoseconds. Python emits typed JSON numbers and a UTC
RFC3339 timestamp. Alloy validates schema/correlation, normalizes numeric fields
and timestamps and rebuilds the finite domain record. Static Quill metadata and
any unlisted fields are removed before forwarding. Request IDs are content,
never indexed labels. Only `service`, trusted `environment`, `event` and `level`
are configured stream labels; Loki can add bounded service/detected-level fields.

Generate correlation at the request boundary; do not trust a supplied header.
Never pass passwords, access/refresh tokens, Authorization, Cookie, request
bodies, SQL/user rows, raw URL/query/path or arbitrary exception text to logs.
Encoded secrets deliberately placed in allowed numeric/ID fields cannot be
recognized reliably. Collector projection is additional defense, not permission
to feed it raw access logs or credentials.

## Resource and lifecycle contract

`TStructuredLogger::Open` starts one process-owned Quill backend and preallocates
the calling thread. Only one logger may be active in a process. Call
`RegisterProducerThread()` during each worker's initialization, before serving
operations; unregistered callers are rejected without creating a queue.

The frontend explicitly selects **BoundedDropping** with **64 KiB per producer**.
At most **sixteen producer allocations per process lifetime**, including Open,
are allowed. Slots are intentionally never recycled: a stalled backend may
retain queues after producer exit, so a concurrent-thread limit alone would
not bound churn. Use a stable worker pool; after exhausting the lifetime budget,
new producer registration fails until process restart. Queues reserve up to
128 KiB each in Quill's backing mapping (logical capacity plus wrap scratch),
plus library metadata. The backend's per-producer transit buffer starts and
stops at **sixteen events**; the global soft processing limit is sixteen.
Records contain only fixed enums, a 32-hex correlation and fixed integers;
the maximum native JSON envelope is tested below **1,024 bytes**. This bounds
logging storage, not the application's entire RSS or allocator overhead.

Create one service-owned Python logger at startup and close it during teardown.
It has one listener and a default **4,096-record** `queue.Queue`; configurable
capacity is 2..65,536. Its finite record is below **4,096 bytes** serialized;
Python object overhead is additional. Thread count does not create more queues.
The admission lock is acquired without waiting; `QueueHandler` uses
`put_nowait`, so overflow cannot wait for the sink or recursively log failures.

For both adapters, successful `request_completed` events sample one candidate
per 100 successes and admit at most twenty candidates per monotonic second by
default. Lifecycle, administrative events, denied/unknown outcomes and errors
bypass success sampling but share bounded transport capacity. The native
cross-thread global limit uses a conservative atomic admission decision;
per-call-site library rate macros would change that aggregate policy.

`TryLog`/`emit` perform no network/file IO, flush or sink retry. Native queue
allocation belongs to Open/producer registration. Both APIs are best effort;
a failed event does not change the database or administrative outcome.
Shutdown closes admission, waits in-flight callers and lets the library drain.
Join application workers before destroying the logger. A blocked OS sink can
still delay clean shutdown; process crashes can lose queued events. Operational
logging is not a durable audit journal.

## Actual metrics integration

Native `GetStats()` and `GetProcessLogStats()` expose submitted, Quill-admitted,
sampled, rate-limited, queue-rejected, unregistered/stopped and adapter-error
counts. Quill's backend callback increments notification counts and parses the
pinned library's bounded-dropping report count. `QuillReportedDropped` is a
delayed observation of queue loss, **not an additional loss category**; do not
add it to `DroppedFull`. `SinkErrors` counts actual upstream `fwrite`/`fflush`
error callbacks. Other backend notifications remain visible without logging
arbitrary exception text. Capacity and lifetime producer count are known budgets.

Quill exposes no exact per-event delivery, sink-loss count or live backlog in
this integration. Those metrics are omitted; zeros must not imply availability.
A stuck backend can delay its drop callback. Queue rejection counters still
advance on the operation path. Process counters outlive logger destruction and
callback state is retained until the backend stops, independently of metrics
objects. Snapshots during concurrent updates are approximate.

Python `snapshot()` exposes actual QueueHandler admissions, queue-full,
contention/stopped drops, sample/rate decisions, adapter errors, StreamHandler
error callbacks and configured capacity. It makes no delivery/backlog claim.
Attach a standard metrics-library collector to the snapshot callback; the
logging adapter owns neither a Prometheus serializer nor an HTTP server.

```python
from glamdb_logging import StructuredLogger

logger = StructuredLogger()
logger.emit("request_completed", operation="auth", outcome="denied",
            request_id=logger.new_request_id(), status_code=403, duration_us=500)
# Stop request producers and unregister metrics consumers during service teardown.
logger.close()
```

## Runtime journal and collection

Keep journal storage separate from the warehouse and enforce an actual disk
quota. A retention period alone cannot bound bytes at arbitrary event rates.
For a systemd service use `StandardError=journal` and a fixed unit identifier;
do not redirect stderr to an application-managed rotating file. For example,
set `SystemMaxUse=64M`, `RuntimeMaxUse=64M`, `MaxRetentionSec=7day` and a
measured rate limit for a dedicated journal. These settings are owned by
journald; verify the effective configuration and size for other units if they
share that journal. Runtime rate limits may discard events too.

Install `deploy/logging/config.alloy` alongside **one** source configuration.
`deploy/logging/sources/journal.alloy` selects a specific systemd unit using
`GLAMDB_JOURNAL_MATCHES`, for example `_SYSTEMD_UNIT=glamdb.service`, reads the
persistent journal and forwards only JSON that passes the shared projection.
Grant the collector journal-group read access, not warehouse access. Preserve
Alloy's state directory across restart. Journal truncation or outage longer
than runtime retention causes unrecoverable gaps; duplicate replay is possible.

For Docker, either use the `journald` driver and the journal source, or select
the `json-file` driver with `max-size=16m`, `max-file=4`, `mode=non-blocking`,
`max-buffer-size=1m` and `sources/docker-json.alloy`. Mount only that application's
runtime JSON log directory read-only, set `GLAMDB_DOCKER_LOG_GLOB` to its
`*-json.log` file and mount the source as `/etc/alloy/source.alloy` instead of
the journal source. Add that variable explicitly to the collector environment
and replace the journal/machine-ID mounts with the selected Docker log-directory
mount; choose a collector UID with read access to those runtime-owned files. The source decodes Docker's envelope and drops stdout before
domain projection. It needs no Docker socket. Do not use this source with the
binary `local` driver. Docker rotates its own files; do not add logrotate.
Kubernetes deployments likewise use container stderr and kubelet/cluster logging
rotation; use trusted pod discovery and the same finite projection/label budget.

The supplied Compose defaults to journald collection on a Linux host:

```sh
export GLAMDB_ALLOY_UID=10001 GLAMDB_ALLOY_GID=10001
export GLAMDB_JOURNAL_GID=$(getent group systemd-journal | cut -d: -f3)
export GLAMDB_JOURNAL_MATCHES='_SYSTEMD_UNIT=glamdb.service'
export GLAMDB_ENVIRONMENT=production
export GLAMDB_ALLOY_STATE_DIR=/srv/glamdb-observability/alloy
export GLAMDB_LOKI_DATA_DIR=/srv/glamdb-observability/loki
# Create owned, quota-limited state directories; do not chown existing data blindly.
docker network create glamdb-observability
docker compose -f deploy/logging/compose.yaml config --quiet
docker compose -f deploy/logging/compose.yaml up -d
```

Loki/Alloy versions and image digests remain in `versions.lock.json`.
Only loopback ports 3100/12345 are published. Loki's authentication is disabled
inside the private observability network: membership is an operator trust
boundary, not tenant authorization. Never expose Loki/Alloy to application
users or public interfaces. For remote collectors use an authenticated TLS/mTLS
gateway and mounted secret files. Grafana login does not protect another exposed
Loki API. Connect the existing private Grafana datasource, or use
`compose.grafana.yaml` with `GLAMDB_GRAFANA_DATA_DIR` and the mandatory
`GLAMDB_GRAFANA_PASSWORD_FILE`. Anonymous access/signup remain disabled. Open
`http://127.0.0.1:3000` through an authenticated operator tunnel, then Explore → Loki.

The optional Grafana runs as UID/GID **472:472**. Compose file-backed secrets
preserve the host file owner/mode; an arbitrary owner's mode0600 file is not
readable by that container. For rootful Docker, install a secret-manager export
into a root-owned mode0700 parent, without printing its contents:

```sh
sudo install -d -m 0700 -o root -g root /var/lib/glamdb-observability/private
sudo install -m 0400 -o 472 -g 472 /operator-private/grafana-password \
  /var/lib/glamdb-observability/private/grafana-password
export GLAMDB_GRAFANA_PASSWORD_FILE=/var/lib/glamdb-observability/private/grafana-password
```

Replace the source with the actual private export. Rootless/user-namespace
owners require separate mapping/readability validation. The native
[Grafana file provider](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/#file-provider)
fails startup when the file cannot be read; Compose's `$$` keeps its expression
literal. The image's shell `__FILE` expansion does not reliably fail startup on
read errors and is not used. This setting bootstraps a new Grafana database;
rotate an existing administrator through native account administration. Recreate
the container after replacing the mounted file. Keep the existing private
Grafana when monitoring already provides it.

## Storage limits, outages and checks

Loki keeps the existing TSDB v13/filesystem setup with **168-hour** Compactor
retention, a ten-minute compaction interval and two-hour delete delay. Retention
is asynchronous and is not disk eviction. A suggested starting dedicated Loki
volume is 20 GiB; size it from measured event bytes and error rate and apply a
real filesystem/project quota. Loki limits are 1 MiB/s ingestion, 2 MiB burst,
256 KiB/s per stream, 1,000 streams and 4 KiB lines; queries are capped at a
24-hour window/1,000 entries. These are protections, not capacity guarantees.
At 600 bytes/event, twenty successes/s alone are about 7.3 GB raw per seven days,
before errors, WAL, indexes, compression and replicas.

The WAL rejects new writes above **90% filesystem use**. Watch
`loki_ingester_wal_disk_usage_percent` (0..1 despite its name),
`loki_ingester_wal_disk_full_failures_total` and discarded samples. A full log
volume must not fill the warehouse. Alloy batches 256 KiB/one second, has a
five-second timeout and six finite retries with 0.5..5-second backoff. Exhausted
batches are dropped and counted; no collector WAL is enabled. Observe actual
producer rejection/sink callbacks, runtime drop statistics,
`loki_write_dropped_entries_total`, batch retries, sent progress, readiness,
container memory, quota/free space and Compactor operations. Missing metrics
and a stuck sink must remain visible; sampled logs cannot establish request RPS.

```sh
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps -r tools/logging/requirements.txt
.venv/bin/python -m unittest discover -s tests/logging -v
cmake -S . -B build/logging -DGLAMDB_IO_ONLY=ON -DPython3_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build/logging --parallel 2
ctest --test-dir build/logging --output-on-failure
```

The native integration harness uses the exact archive hashes in the deployment
lock, unused loopback ports and disposable child Loki/Alloy processes. Its file
source is explicitly a **test runtime capture**, not application file logging:

```sh
.venv/bin/python tools/logging/run-local-e2e.py \
  --loki-binary artifacts/logging-tools/loki-darwin-arm64 \
  --alloy-binary artifacts/logging-tools/alloy-darwin-arm64 \
  --probe build/logging/tests/glamdb_logging_probe --exercise-outage
```

It queries actual native events/correlation, rejects malformed schema and
secret-marker fields, stops only its own Loki, checks finite retry loss and
verifies new events after restart. `--wal-disk-threshold 0.99` is a tiny-fixture
host override, never production configuration. Linux journald/Docker rotation
requires its own real runtime verification; the macOS harness cannot certify it.
See [validation evidence](LOGGING_VALIDATION.md) for current executed limits.

Useful narrow queries:

```logql
{service="glamdb", environment="production", level="error"} | json
{service="glamdb"} | json | request_id="0123456789abcdef0123456789abcdef"
{service="glamdb", event="request_completed"} | json | operation="append" | duration_us > 100000
```

Primary configuration references: [Quill](https://github.com/odygrd/quill/tree/v13.0.0),
[stdlib handlers](https://docs.python.org/3/library/logging.handlers.html),
[Alloy journal](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.journal/),
[Alloy processing](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.process/),
[Loki retention](https://grafana.com/docs/loki/latest/operations/storage/retention/) and
[Loki authentication](https://grafana.com/docs/loki/latest/operations/authentication/).
