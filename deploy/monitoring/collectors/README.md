# macOS and Podman collectors

This profile monitors the actual macOS host and one existing rootful Podman
Linux VM. It does not create a VM, install Docker, create Kubernetes, or equate
Linux VM metrics with macOS metrics. Collector artifacts are pinned in
`versions.lock.json`; copied upstream licenses are in `licenses/`.

## Endpoints and startup

The native Darwin arm64 node_exporter listens on **127.0.0.1:9101**. Its own
`org.glamdb.macos-exporter` user LaunchAgent starts at login and restarts it on
exit. Install from the verified official archive:

```sh
python3 deploy/monitoring/collectors/install_macos_node.py \
  --archive /path/to/node_exporter-1.12.1.darwin-arm64.tar.gz
```

The default runtime directory is `~/.local/share/glamdb/live/collectors`.
Prometheus inside the VM needs an existing private authenticated tunnel or
equivalent private bridge to this loopback listener. The installer does not
open the host listener to a network or change another service's tunnel.

Start the three Linux collectors using an **explicit existing rootful connection**:

```sh
bash deploy/monitoring/collectors/run-podman-collectors.sh \
  EXISTING_ROOTFUL_CONNECTION glamdb-observability
```

The script only creates missing collector containers and leaves existing
names intact. It uses the existing network and publishes no host ports:

- `glamdb-collectors-podman:9882/metrics`: official
  prometheus-podman-exporter, 128 MiB / 0.25 CPU, read-only root filesystem,
  dropped capabilities, no host namespaces. Its VM-local Unix socket mount
  grants Podman API authority inside this trusted collector; `:ro` on a socket
  mount does **not** make API operations read-only. No TCP API is enabled.
- `glamdb-collectors-cgroup:13232/metrics`: official cgroup-exporter,
  UID/GID 65534, 64 MiB / 0.25 CPU, read-only root filesystem,
  dropped capabilities, no API socket, host namespace or privilege. Its only
  host bind is read-only `/sys/fs/cgroup`.
- `glamdb-collectors-cgroup-systemd:13232/metrics`: the same narrow collector
  and resource/security settings, for actual Quadlet payload leaf paths.
  Scrape both cgroup endpoints with job `cgroup`.

All three use `--restart unless-stopped`. The units in `systemd/` can be installed
in the VM's `/etc/systemd/system/`, followed by `systemctl daemon-reload` and
`systemctl enable --now glamdb-collectors-podman.service
glamdb-collectors-cgroup.service glamdb-collectors-cgroup-systemd.service`. They start only these collectors at VM boot,
including when the global `podman-restart.service` is disabled. They neither
start the VM nor affect other containers. To disable boot startup, disable
these three units explicitly.

Scrape with jobs **macos-node**, **podman**, and **cgroup**. Keep `up` and
`{node,podman}_scrape_collector_success` visible. Use bounded scrape timeouts and
sample limits; the observed cgroup response was about 230 KiB / 121 families
for twelve containers. cgroup-exporter can log a partial collection failure
while returning HTTP success. Missing expected fields must remain No data;
an HTTP-success check alone cannot qualify their presence.

## Metric meanings

Darwin node_exporter uses Mach, sysctl, IORegistry, filesystem and interface
sources. Its memory names are lower-case, unlike the Linux collector:

- `node_memory_total_bytes`: `hw.memsize`.
- `node_memory_internal_bytes`: `internal_page_count × host_page_size`.
  Apple's `vm_stat` prints the same counter as **Anonymous pages**. This is
  resident anonymous memory; compressed pages are reported separately. It is
  neither generic RSS nor total anonymous committed/address-space memory.
- `node_memory_compressed_bytes`: physical compressor page count, not the
  number of uncompressed pages stored in the compressor.
- `node_memory_free_bytes` includes speculative pages. `vm_stat`'s printed
  **Pages free** subtracts speculative pages, so add **Pages speculative** for
  a source comparison.
- Active, inactive, anonymous, wired and purgeable categories overlap. Do not
  sum them or invent a Linux `MemAvailable` mapping.
- `node_memory_swapped_{in,out}_bytes_total` use Mach **pageins/pageouts** in
  this upstream implementation, despite their names. They do not measure the
  distinct `vm_stat` swapins/swapouts counters.
- `node_disk_{read,written}_bytes_total` and operation/time counters read
  IORegistry whole-device statistics. Per-direction service time is a proxy
  from Total Time / Operations; queue wait and end-to-end I/O latency are not
  provided. The Darwin sectors counters are not used by these dashboards.
- Thermal collection is explicitly disabled because this Mac's IOKit API
  returns `no CPU power status has been recorded`. It is not a zero-temperature
  reading. Unsupported fields are not synthesized.

The Podman exporter adds actual `name` labels with
`--collector.enhance-metrics`. On the qualified Podman 5.7.1 build:

- `podman_container_cpu_seconds_total` is actual cumulative CPU seconds.
- `podman_container_mem_usage_bytes` is
  `max(memory.current - memory.stat["inactive_file"], 0)`, a working-set
  convention, **not charged or anonymous memory**.
- `podman_container_mem_limit_bytes` falls back to VM physical RAM for an
  unlimited cgroup; it does not prove that a hard cap was configured.
- `podman_container_pids` matches Linux `pids.current`: tasks, including threads,
  rather than only process leaders. A separate process count is unavailable.
- `podman_container_{block_input,block_output,net_input,net_output}_total`
  count bytes. Network statistics aggregate reported interfaces; containers
  sharing the host network have no independent network accounting here.
- Stopped containers retain inventory/state but have no live resource samples.
- `podman_container_cpu_system_seconds_total` is intentionally omitted from
  panels: the installed Podman commit's cgroup-v2 conversion leaves
  `system_usec` unscaled before it is consumed as nanoseconds, yielding a
  value 1,000 times too small. Correct `cgroup_cpu_system_seconds_total` is used
  on the exact-cgroup dashboard instead. No metric is silently rewritten.

cgroup-exporter reads the **actual container leaf** selected by
`machine.slice/libpod-*.scope/container` (direct-run containers) and
`system.slice/*.service/libpod-payload-*` (Quadlet containers). Its `cgroup` label is a real kernel
path. These are not Kubernetes pod-parent metrics. The dashboard extracts
the container ID and joins the actual Podman inventory to display names; this
join is qualified for a single VM and needs an additional VM identity before
combining several VMs.

- `cgroup_memory_current_bytes` matches `memory.current`.
- `cgroup_memory_{anon,file,shmem,unevictable}_bytes` are literal corresponding
  `memory.stat` fields. File includes tmpfs/shared memory; unevictable is broader
  than mlock-only memory. The categories must not be blindly added.
- CPU usage/user/system, throttling and burst counters use `cpu.stat`; usec are
  converted to seconds. Presence and zero are distinct from absence.
- PSI some/full counters measure elapsed stall time for some/all tasks. They
  do not establish unconstrained demand, per-request I/O latency or scheduler
  latency.
- `cgroup_io_*` bytes/operations retain block-device major:minor labels. Do not
  sum layered devices without validating the topology.
- `cgroup_memory_pgmajfault` is the literal cumulative major-fault field.
  Upstream declares it a gauge; the dashboard displays the raw count.

## Dashboards and verification

`dashboards/` contains provisionable JSON for datasource UID `prometheus`:
`glamdb-macos-node`, `glamdb-podman`, and `glamdb-cgroup-v2`. They use the jobs
above, actual metric names, explicit units, and no Kubernetes labels. Install
them through the existing Grafana dashboard provider. Empty unsupported series
remain No data; no synthetic placeholder or `or vector(0)` covers a missing
producer.

Executed on Darwin arm64 / Fedora CoreOS 43 aarch64, Linux 6.17.7,
cgroup v2 / Podman 5.7.1:

- **PASS:** 108 independent live source comparisons: Mach CPU ticks; sysctl
  RAM/CPU count; `vm_stat` resident/anonymous/compressor categories;
  IORegistry bytes/operations/service counters; en0 bytes/packets; APFS size
  and available capacity; actual container memory/tasks/CPU/network/IO;
  exact direct-run and Quadlet leaf anonymous/file/unevictable/kernel fields, major faults,
  CPU user/system/throttling/burst, PSI and per-device IOPS.
- **PASS:** all 101 dashboard PromQL expressions parsed by pinned promtool
  3.13.5. Variable substitutions were `instance=.*`, `container=.*`, interval
  `5m`; label_replace's `$1` capture was preserved.
- Measured macOS hardware: 8 logical CPUs, 17,179,869,184 bytes RAM. The separate
  VM has 4 CPUs and 4,079,595,520 bytes RAM.
- Example real PostgreSQL leaf: `memory.current=109846528`, anonymous
  `9777152`, file `93921280`, unevictable `0` bytes; tasks `11`; major faults
  `57`. Its exported Podman working set was `91496448` bytes, exactly current
  minus inactive_file. These are measurements, not acceptance thresholds.
- Measured collector memory was approximately 7.4 MB for cgroup-exporter and
  7.9 MB for Podman exporter, inside their 64/128 MiB caps. This observation
  does not establish a production resource budget or arbitrary-host bounds.

Read-only verification records live source comparisons with non-atomic sampling
tolerances, not a source gap hidden as zero. Mac memory gauges allowed 64 MiB
sampling drift; exact leaf memory allowed 64 KiB. A nonzero quota/burst, memory
pressure, OOM or major-fault fault-injection campaign is not part of this local
deployment; current raw counter values were compared without inducing faults.

Kubernetes requests/limits, pod scheduling/lifecycle and Kubernetes mixin panels
require an actual authorized cluster, kubelet/cAdvisor endpoints and
kube-state-metrics. This profile supplies none of those identities. A distinct
process-leader count, mlock-only memory, application request-level I/O latency,
unconstrained CPU demand and unsupported macOS kernel fields remain unavailable.

## Primary sources

- [Darwin memory collector](https://github.com/prometheus/node_exporter/blob/6044da783597cc3b57aef7580ddcdcff58a4ee99/collector/meminfo_darwin.go),
  [Darwin CPU](https://github.com/prometheus/node_exporter/blob/6044da783597cc3b57aef7580ddcdcff58a4ee99/collector/cpu_darwin.go),
  [disk collector](https://github.com/prometheus/node_exporter/blob/6044da783597cc3b57aef7580ddcdcff58a4ee99/collector/diskstats_darwin.go),
  and [IORegistry reader](https://github.com/lufia/iostat/blob/v1.2.1/iostat_darwin.c).
- [Apple vm_stat](https://github.com/apple-oss-distributions/system_cmds/blob/main/vm_stat/vm_stat.c)
  prints `internal_page_count` as Anonymous pages and subtracts speculative
  pages from Pages free.
- [Podman exporter container metrics](https://github.com/containers/prometheus-podman-exporter/blob/dafd81078ef7651bd541234adb2dd59216f19235/collector/container.go)
  and [data adapter](https://github.com/containers/prometheus-podman-exporter/blob/dafd81078ef7651bd541234adb2dd59216f19235/pdcs/container.go).
- Installed Podman commit: [memory conversion](https://github.com/containers/podman/blob/f845d14e941889ba4c071f35233d09b29d363c75/vendor/go.podman.io/common/pkg/cgroups/memory_linux.go#L65)
  and [CPU conversion](https://github.com/containers/podman/blob/f845d14e941889ba4c071f35233d09b29d363c75/vendor/go.podman.io/common/pkg/cgroups/cpu_linux.go#L56).
- [Pinned cgroup collector](https://github.com/arianvp/cgroup-exporter/blob/9d9967ac99b61899030ffc24fcd99395bcd9a248/collector/collector.go),
  including leaf-only traversal, field definitions, unit conversions and
  partial-error logging.

## Optional four-state checks

With alerting files present in the same checkout, apply this profile to a copy
of the base configuration, then use the existing rule generator:

```sh
python3 tools/monitoring/extend_checks.py \
  --base deploy/alerting/checks.json \
  --profile deploy/monitoring/collectors/checks.extension.json \
  --output /path/to/private/checks.local.json
python3 tools/alerting/generate.py \
  --config /path/to/private/checks.local.json \
  --output /path/to/private/prometheus/alerting/states.yml
```

The example selectors use `cluster="local"`; change them with the scrape labels
before deploying another cluster. The extension adds native HTTP, macOS,
exact-cgroup, source-file and gateway/edge checks. Duplicate identities and
unknown overrides are rejected. Base Kubernetes checks still need real
Kubernetes sources; applying this profile never turns them into Podman data.
The memory-limit alert subtracts actual `inactive_file` from charged memory
before dividing by a finite hard cap: this conventional working set avoids
interpreting all reclaimable inactive cache as allocation pressure. Charged,
anonymous and file memory remain separate dashboard measurements.

Normal Prometheus profiles have empty file-discovery files for optional
`macos-node`, `podman`, `cgroup` and `grafana` jobs. Populate only actual sources.
Private bridge targets may set `__metrics_path__` in discovery labels. A
host-bound Caddy metrics listener also needs its expected upstream `Host`;
HTTP200 with zero scraped samples is insufficient. Verify raw families and `up`.

The normal Grafana provider includes the live overview and all three collector
dashboards. The overview separates native transport results, durable engine
outcomes, gateway requests and outer-edge rejection, and includes Loki logs
for `service="glamdb"`. NODATA remains visible. Provisioning JSON controls panels,
queries, units and colors without rebuilding a server.
