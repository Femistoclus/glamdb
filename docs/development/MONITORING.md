# Monitoring

GlamDB uses prometheus-cpp 1.3.0 for native domain metrics and
prometheus_client 0.26.0 for Python application metrics. Standard node_exporter,
cAdvisor, kubelet and kube-state-metrics collect infrastructure measurements.
Exact pins, archive hashes, sources and licenses are in
[versions.lock.json](../../deploy/monitoring/versions.lock.json) and
[third_party/lock.json](../../third_party/lock.json).

The [coverage matrix](MONITORING_COVERAGE.md) records every requested resource
field, its actual scope/units, missing-source decisions and validation limits.
[Validation](MONITORING_VALIDATION.md) separates executed checks from unrun
production/Kubernetes work. An exporter returning 200 establishes reachability;
it does not establish database readiness, a committed write or durability.

## macOS and Podman profile

The [collector profile](../../deploy/monitoring/collectors/README.md) provides
actual Darwin host, Podman inventory and exact read-only cgroup-v2 measurements.
Its dashboards and optional four-state checks distinguish the Mac, the Linux VM
and container leaves. All additional discovery files are empty by default.
The live overview combines source availability, native/gateway/edge HTTP signals,
engine outcomes, log shipping and Telegram delivery health. Missing producers
remain No data. Logs need Loki/Alloy from the logging deployment; state checks
need the alerting deployment.

## Application integration

Native code records only real operation outcomes, in-flight/admission ownership,
measured coordinator regions and finite error codes. A pre-registered table of
prometheus-cpp handles replaces custom metric storage. The library owns the
registry, counters, gauges, histograms, serialization and HTTP exposition.
The private `GetMetricsCollectable()` seam can be registered with an
**application-owned** `prometheus::Exposer`. No HTTP type or endpoint is added
to installed `public/storage/` headers. The native `glamdb` server registers the
actual process collectable on a separate, opt-in private listener:

```sh
glamdb --data-dir /path/to/service --metrics-listen-address 127.0.0.1:9094
```

Metrics are disabled when the flag is omitted. The main API/UI listener does
not serve `/metrics`. The private endpoint has no bearer-token authentication;
bind it to loopback or a trusted private interface and restrict access to the
scraper. The application owns listener teardown; no textfile is published.

The native server also registers finite HTTP metrics with `service_instance="core"`:

- `glamdb_http_requests_total{route,method,status_class}` counts accepted connections
  through full asynchronous response completion or cancellation. Each connection
  carries at most one request. Classes are `1xx` through `5xx`, plus `cancelled`
  when the response was not fully written; cancellation is not a synthetic 499.
- `glamdb_http_responses_total{status_code}` counts only fully written responses.
  Thirty fixed common codes and `other` distinguish actual 401/403, 429 and 503
  without multiplying codes by routes/methods.
- `glamdb_http_request_duration_seconds{route}` measures the actual accepted
  connection lifetime, including header/body reading and the full response write.
- `glamdb_http_requests_in_flight{route}` retains ownership through that lifetime.

Seventeen route names and eight method names are registered once. Unparsed
connections and unknown paths use `other`; methods outside the fixed domain use
`OTHER`. IDs, target URLs, query strings, headers and payloads are never labels.
Idle histogram count/sum/buckets remain zero, so an idle quantile has no observed
latency. The library registry owns all samples and concurrent updates.

`--log-directory /path/to/private/logs` sends native operation and runtime
lifecycle records through the existing Quill 13 structured logger to files. Its
`RotatingJsonFileSink` appends schema-1 JSONL to `glamdb.jsonl`, rotating at 8 MiB
with at most three indexed backups (`glamdb.1.jsonl`, etc.). Rotation and writing
run on the Quill backend; the request thread uses its 64 KiB bounded dropping
queue. Successful requests retain the default one-in-100 sampling and are capped
at 20 per second; `--log-success-sample-every` and
`--log-max-success-per-second` configure those limits. Errors and lifecycle
records bypass the success limit and can still be dropped when the queue is full.
Native `glamdb_log_*` counters report those decisions.
The server registers its I/O and stable worker producers and drains the logger on
shutdown. Omitting the directory retains the existing stderr sink. Newly created
log directories have owner-only access; use a trusted private directory.
Records contain fixed operation/outcome names, generated request IDs, measured
duration and completed response status; they contain no database identities,
tokens, URLs or row values. Their outcome describes the HTTP request, not the
durable commit disposition: a cancelled transport can still have committed.
Actual engine call outcomes, including `commit_unknown`, are recorded separately
in `glamdb_operations_total`; resolve an individual write through its original
operation identity. The shared schema's `row_count` default is not a native HTTP
row measurement. Log shipping tails the active `glamdb.jsonl` file; numbered backups are not rediscovered as new sources.

`glamdb_server_ready{service_instance="core"}` observes actual `Engine::GetHealth`
at scrape time. It is one only when at least one managed database is open and
all open engines are write-ready. Registry/lifecycle errors, busy lifecycle gates,
failed health observation and no open databases produce zero. Explicitly closed
databases do not make another healthy open database unready. Legacy single-store
mode observes that engine directly. Database names, IDs and paths are absent
from metric labels. Readiness is a point-in-time observation, not a write
reservation or a durability qualification.

`glamdb_operations_total` counts calls, including retries. It is not the number
of distinct durable writes. `glamdb_phase_duration_seconds` observes actual
coordinator regions; `data` includes backend metadata work and is not a
Parquet/Iceberg subspan. `commit_unknown` is a returned outcome counter, not
current recovery state. No current-unknown gauge is fabricated.

All native operation/outcome/phase/error combinations are registered once:
714 domain time series, independent of request volume. The 15 finite duration
buckets are unchanged: 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
0.25, 0.5, 1, 2.5, 5, 15 and 60 seconds, plus infinity/count/sum. Unobserved
registered phase/error samples now appear as zero. Histogram sums now retain
the client's floating-point seconds instead of truncating each observation to
microseconds. Process restart resets cumulative counters. Initialization failure
can disable instrumentation but never changes an engine operation result; an
application requesting an unavailable collectable receives an error.

The short-lived `glamdb-cli` does not start an exporter or publish a textfile.
`GLAMDB_METRICS_TEXTFILE`, `GLAMDB_METRICS_INSTANCE`, the snapshot timestamp
metric and custom publisher thread are removed. CLI exit status, command
output and process/service supervision are the observation model for these
one-shot commands. Remove old `.prom` snapshots explicitly from a previously
configured node-exporter textfile directory during migration. The new exporter
configuration does not mount or scrape that directory.

For Python, install the exact client:

```sh
python3 -m pip install --require-hashes -r tools/monitoring/requirements.txt
```

`ApplicationMetrics` in `tools/monitoring/application_metrics.py` exposes
`observe_http(route, method, status, seconds)`, `in_flight(route, delta)`,
`observe_stage(stage, seconds)`, `set_ready(bool)` and `metrics_app()`.
Mount the library ASGI application on the server's **private metrics listener**.
The route/method/status-class/status-code/stage domains are finite and
pre-registered. Unknown route/method labels map to `other`/`OTHER`; uncommon
valid HTTP codes map to `other` in the status-only counter. Histogram labels
are route-only to limit the series product. Measure through response-body
completion, including streaming, and release in-flight ownership on every exit.
Readiness defaults closed and must be set by the authoritative admission/backend
owner. Install the dependency as a required production gateway dependency.
`LoggingMetrics(logger.snapshot)` may be registered once with that registry;
unsupported delivered/backlog counts are absent. Optional native logger metrics
also omit the former `written`, `backlog`, `dropped_sink` and record-count queue
capacity fields. Quill callback drop notifications and frontend admission drops
are separate overlapping observations; never add them together. The new queue
capacity gauge explicitly uses bytes.

`glamdb-gateway` scrapes Python application metrics. `glamdb-edge` independently
scrapes Caddy's private outer HTTP metrics. Caddy includes OAuth2 Proxy
rejections and static/helper traffic that never reaches the application.
Its actual route labels are `server="workbench",handler="subroute"`.
Keep their RPS/status denominators separate. Native metrics use `glamdb-core`.
No user, organization, database, operation ID, URL, query or error text becomes
a generated metric label. `service_instance="core"` is a fixed native label;
Prometheus supplies the real target `instance`, `cluster` and `service` labels.

## One Linux host

Kubernetes is optional. Use the two small Compose files on a Linux host, or run
the same pinned exporters under the host's service manager. All listening ports
must be private. Docker Desktop/Podman VM statistics describe that Linux VM,
not the surrounding macOS host. node_exporter's Linux collectors are not a
macOS equivalent.

1. Create the dedicated network once: `docker network create glamdb-observability`.
2. Provision the Grafana bootstrap password as described below and set
   `GLAMDB_GRAFANA_PASSWORD_FILE` to its absolute path. No default shared password
   is shipped; a missing variable or unreadable file prevents startup.
3. Set `GLAMDB_NODE_LISTEN` to a private host IP and port reachable from the
   Prometheus container. Restrict that address with the host firewall.
4. Edit the explicit inventories under `deploy/monitoring/prometheus/targets/`.
   Empty inventories create no pretend targets. Each entry is a file-SD group,
   for example `[{"targets":["node-private:9100"],"labels":{"cluster":"local","service":"glamdb"}}]`.
   `gateway.json` is the private Python endpoint, `edge.json` is private Caddy,
   `core.json` requires a real native server adapter, and `node.json`/`cadvisor.json`
   select the actual standard collectors. Optional `alloy.json`/`loki.json` retain
   the logging-stack health sources. External labels alone do not label local
   stored samples.
5. Start `docker compose -f deploy/monitoring/compose.exporters.yml up -d`, then
   `docker compose -f deploy/monitoring/compose.yml up -d`.
6. Inspect Prometheus `/targets` and `/rules`, then Grafana at localhost:3000.
   Grafana uses bundled plugins; preinstallation/automatic unpinned plugin
   updates are disabled. Node Exporter / Nodes is the upstream dashboard with one documented job-label
   override; the API/coordinator/PVC supplement remains GlamDB-specific.

Compose file-backed secrets retain their host ownership and mode; they are not
automatically readable by Grafana's UID/GID **472:472**. On a rootful Linux host,
prepare an operator-controlled strong password in a private source file, then
install it without printing its contents:

```sh
sudo install -d -m 0700 -o root -g root /var/lib/glamdb-observability/private
sudo install -m 0400 -o 472 -g 472 /operator-private/grafana-password \
  /var/lib/glamdb-observability/private/grafana-password
export GLAMDB_GRAFANA_PASSWORD_FILE=/var/lib/glamdb-observability/private/grafana-password
```

Replace the source pathname with the actual secret-manager export, not a public
file. The root-owned parent prevents host users from traversing to the file;
the daemon mounts only that file read-only, where Grafana can read it as UID472.
Rootless/user-namespace runtimes require their actual mapped UID, separately
verified; blindly applying the rootful owner is insufficient.

The native [Grafana file provider](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/#file-provider)
reads the mounted secret and fails if it cannot. Compose's `$$` preserves the
literal provider expression. Avoid the image's `GF_SECURITY_ADMIN_PASSWORD__FILE`
entrypoint expansion: with this pinned image an unreadable file logs a shell
error but does not stop startup. This bootstrap setting initializes a **new**
Grafana database; replacing it does not rotate an existing account. Use native
Grafana account administration for an existing installation and recreate the
container after atomically replacing a file-backed mount.

cAdvisor needs host filesystem/cgroup/runtime visibility and privileged access;
review those mounts against the actual container runtime. Its process/sched/TCP
collectors are optional expensive kernel scans. Disable detailed collectors
when unneeded and recheck the coverage matrix. Do not run standalone cAdvisor
alongside an equivalent kubelet scrape in Kubernetes. Prometheus sample/label
limits fail excessive scrapes; they do not prove a cardinality or CPU budget.
The cAdvisor scrape honors producer timestamps and tracks timestamp staleness.
A raw explicitly timestamped sample can otherwise remain queryable after a
container disappears. For freshness-sensitive panels/alerts, require a bounded
`time()-timestamp(actual_metric)` age plus the appropriate inventory/up source.
In cAdvisor 0.60.6, `container_last_seen` is generated at scrape time and does
not prove the underlying resource observation is current.

The `prometheus-alerting.yml` variant also scrapes native Alertmanager and sends
alerts there. The alerting Compose override mounts its generated application
rules; see the alerting runbook. No custom webhook transport is scraped.

Stop each Compose file with `down`. Named monitoring volumes remain; removal
is an explicit retention decision. Stopping monitoring does not require deleting
any database file or volume.

## Optional Kubernetes installation

Use official kube-prometheus-stack **92.7.0**, which packages standard upstream
Kubernetes/Node Exporter mixin dashboards and rules. `kubernetes/values.yml` is
a small values override, not a runtime manifest generator. It uses only kubelet
`/metrics/cadvisor`; there is no second cAdvisor DaemonSet. The override keeps
the CFS throttle-time metric that chart defaults otherwise discard.

```sh
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm pull prometheus-community/kube-prometheus-stack --version 92.7.0 \
  --destination artifacts/monitoring
# Verify the archive against versions.lock.json before rendering/installing.
helm template monitoring artifacts/monitoring/kube-prometheus-stack-92.7.0.tgz \
  --namespace monitoring -f deploy/monitoring/kubernetes/values.yml \
  > artifacts/monitoring/kubernetes.yml
kubectl apply --server-side --dry-run=server -f artifacts/monitoring/kubernetes.yml
helm upgrade --install monitoring artifacts/monitoring/kube-prometheus-stack-92.7.0.tgz \
  --namespace monitoring --create-namespace \
  -f deploy/monitoring/kubernetes/values.yml
kubectl apply -f deploy/monitoring/kubernetes/application-monitor.yml \
  -f deploy/monitoring/kubernetes/application-rules.yml
```

Before installation, create the `monitoring-grafana-admin` Secret with
`admin-user`/`admin-password`, choose storage classes/limits, set the real cluster
label in values and application monitors, review chart upgrade/CRD instructions,
and verify cluster/runtime compatibility. The pinned chart declares Kubernetes
>=1.25; its mixin metric requirements and actual cluster metrics still matter.
Rendered YAML is not a real scheduling/CSI/authorization test.

All kubelet/control-plane TLS verification remains enabled. Use serving
certificates signed by the configured CA with valid target SANs; an untrusted
self-signed certificate should fail collection. Restrict private ServiceMonitor
access and NetworkPolicy/firewall ingress to the monitoring namespace. The
application-monitor example expects an existing private metrics Service in
namespace `glamdb`, port named `metrics`; it does not create an application or
public ingress. Authorize only those namespace selectors actually required.

To provision the application supplement with the chart's Grafana sidecar, create
a ConfigMap from `deploy/monitoring/grafana/dashboards/glamdb-service.json` in
`monitoring` and label it `grafana_dashboard=1`. The chart owns infrastructure
panel/rule lifecycle. Its standard dashboards replace the removed bespoke
infrastructure dashboards and deployment generator.

## Verification

Build the pinned client using `cmake/dependencies` with the other locked
libraries. The recipe applies the recorded CivetWeb stop-flag patch to a
build-private source copy; build only from that prepared copy when installing
core/pull separately. An unpatched v1.3.0 listener races during shutdown.
Then run:

```sh
python3 tools/monitoring/check.py --promtool PATH_TO_VERIFIED_PROMTOOL
cmake -S . -B build/monitoring -DGLAMDB_IO_ONLY=ON -DBUILD_TESTING=OFF \
  -DGLAMDB_BUILD_METRICS=ON -DCMAKE_PREFIX_PATH="$PWD/.deps/install"
cmake --build build/monitoring --target glamdb_metrics
```

`check.py --helm PATH --chart PINNED_ARCHIVE` also verifies the chart's locked
SHA-256, lints/renders it and rejects rendered TLS verification bypasses.
The component tests exercise prometheus-cpp Exposer over localhost and check
listener teardown. `ctest --preset local -R glamdb.native_metrics` separately
checks the native server with actual temporary stores, committed operations,
readiness transitions and restart. `GLAMDB_TEST_TMPDIR` selects a writable test
parent on a filesystem with sufficient disk reserve; otherwise it uses `/tmp`.
Building the server and installed engine consumer requires the complete locked
dependency prefix. These checks do not qualify engine durability.

`tools/monitoring/container_smoke.py` requires an explicitly selected disposable
Podman connection/machine. It creates only its own bounded containers/network,
compares real standard collector samples with Linux files, runs Prometheus and
Grafana, and removes only resources it created. Never select a user/default VM
or delete unrelated volumes to run monitoring checks.
