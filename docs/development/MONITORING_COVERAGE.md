# Resource metric coverage and source gaps

The default stack uses node_exporter 1.12.1, cAdvisor 0.60.6, or the cluster's
kubelet/cAdvisor and kube-state-metrics through kube-prometheus-stack 92.7.0.
Kernel/runtime absence is **No data**, never a synthetic zero or a differently
named substitute. Container rows describe one actual cgroup; regular-container
sums omit pod-parent overhead and must not be presented as exact hierarchical
pod totals. Avoid summing both parent and children. Use one cAdvisor source per
node. Raw resource samples have explicit observation timestamps. Require
`time()-timestamp(actual_metric)` within the selected freshness budget for
current-state views; `container_last_seen` in 0.60.6 uses scrape time instead.
The standalone config enables `track_timestamps_staleness`; kubelet/chart
consumers must preserve equivalent missing/stale-data behavior. The chart retains the native `job="kubelet"`/`job="node-exporter"` labels;
the single-host configuration uses `cadvisor`/`node`.

The table is a source contract. Runtime PASS is
limited to the exact checks in [MONITORING_VALIDATION](MONITORING_VALIDATION.md).
A listed source is not a claim that every runtime exposes it. Default source
gaps below remain unmet at pod-parent scope and are an explicit migration
boundary when removing the previous private collector.

| Requested measurement | Producer and Linux source | Scope, units and aggregation | Support and control-load verification |
| --- | --- | --- | --- |
| CPU / utilization | `node_cpu_seconds_total` from `/proc/stat`; `container_cpu_usage_seconds_total` from cgroup CPU accounting | Host utilization is `1-avg(rate(...{mode="idle"}[5m]))`; cgroup `rate` is cores. Sum regular containers once per pod. | Linux CPU controller; compare `cpu.stat usage_usec / 1e6` before/after a bounded CPU loop. Host node mixin also distinguishes iowait/steal. |
| CPU quota utilization | `container_spec_cpu_quota / container_spec_cpu_period`, when exposed; KSM resource limits describe configured Kubernetes limits | Divide usage cores by finite positive quota cores, not by CPU requests. Parent/ancestor/cpuset limits can constrain further. | Unlimited or missing quotas remain absent. Inspect actual `cpu.max`; chart KSM limits do not prove effective parent quota. |
| CPU burst | cAdvisor `container_cpu_cfs_burst_seconds_total` / `...burst_periods_total`, exact `cpu.stat burst_usec`; candidate also exposes `cgroup_cpu_burst_seconds_total` | Seconds, counter; rate is burst CPU seconds/second. | Pinned cAdvisor source exposes CFS burst time when a finite quota exists. Kernel/CFS burst support and configured `cpu.max.burst` required. Compare actual `burst_usec`; node utilization is not burst. Kubelet builds omitting this metric stay No data. |
| Constrained wait | `container_cpu_cfs_throttled_seconds_total`, cgroup `cpu.stat throttled_usec` | Throttled CPU seconds/second, explicitly named **CFS throttle time**. It can exceed wall time across CPUs. | CPU bandwidth controller required. Compare under finite quota + CPU loop. This is not a generic scheduling-wait counter. Chart defaults would drop it; values override retains it. |
| Unconstrained wait | No exact generic Linux/cAdvisor separation of constrained and unconstrained scheduler wait | **No data for the requested exact split.** Do not subtract unrelated throttle/runqueue/PSI counters. | Upstream kernel/exporter support would be needed to define this split. Optional `container_cpu_schedstat_runqueue_seconds_total` is separately labeled total runqueue wait, requiring `CONFIG_SCHEDSTATS`, enabled `kernel.sched_schedstats` and process visibility. |
| CPU/memory/I/O wall stall | Standard cAdvisor `container_pressure_*_waiting_seconds_total` / `..._stalled_seconds_total`, when built/enabled; host node PSI metrics | PSI some/full wall-stall seconds, separate from CPU consumption or CFS throttle time. | `CONFIG_PSI`, enabled PSI and readable pressure files; compare `*.pressure total / 1e6`. Node PSI is host scope. Missing PSI remains No data; the container pressure collector is optional. |
| Memory / charged usage | `container_memory_usage_bytes`, cgroup-v2 `memory.current` | Charged bytes of the selected cgroup, includes cache/kernel accounting; not RSS or working set. | Memory controller/runtime support; compare with `memory.current` during a 64 MiB allocator, allowing sampling-time movement. Sum containers only as a named container total. |
| Total host memory / host used | `node_memory_MemTotal_bytes`, `MemAvailable_bytes`, `/proc/meminfo` | Host physical total bytes; used proxy is total minus available, not cgroup usage. | Linux meminfo collector; compare actual file fields, in KiB multiplied by 1024. |
| Container/pod memory limit | `container_spec_memory_limit_bytes` when valid; KSM configured resource limit | Bytes; distinguish configured per-container limits from effective `memory.max`/ancestor constraints. | `memory.max=max` is unlimited, not zero. No effective pod-parent cap is inferred from summing requests/limits. |
| Anonymous memory | Host `node_memory_AnonPages_bytes`; pinned cAdvisor 0.60.6 unified-v2 `container_memory_rss` maps directly to `memory.stat anon`; candidate `cgroup_memory_anon_bytes` | Host anonymous pages and hierarchical cgroup anonymous bytes are different scopes. The RSS-named metric has proven anon semantics ONLY for the qualified 0.60.6 unified-v2 handler; no generic RSS rename. | Standalone pinned v2 source can be qualified against `anon` during the allocator. **Unqualified kubelet/version/v1 or exact pod-parent scope stays No data**; do not generalize the proven v2 mapping. |
| Instance counter | `up{job="glamdb-gateway"}` / `up{job="glamdb-core"}`; KSM `kube_pod_status_phase{phase="Running"}` | Count live scrape targets, or count Running pods as separate inventory. Neither proves DB readiness. | Explicit target discovery + real endpoint, or KSM. Removing inventory yields No data; a discovered failed endpoint yields up=0. The native server exposes its actual process registry on an opt-in private listener. |
| Threads | `container_threads` (process collector); candidate `cgroup_pids_current`; host `node_procs_running` has a different meaning | Kernel tasks/threads in the selected cgroup; compare `pids.current`. Never label running-process count as threads. | Readable process/cgroup data, pids controller; create three threads and compare the allocator's cgroup. Kubelet builds may omit process collector; then No data. |
| Major page faults | `container_memory_failures_total{failure_type="pgmajfault",scope="container"}` or exact candidate `cgroup_memory_pgmajfault` | Counter despite candidate gauge TYPE; `rate` is faults/second, not disk IOPS. Select hierarchy scope only when comparing hierarchical `memory.stat`. | Memory accounting support; compare before/after file-backed faults under an isolated bounded load. Cached reads may produce no major faults; do not force cache eviction on a shared host. |
| File cache | Host `node_memory_Cached_bytes`; pinned cAdvisor 0.60.6 unified-v2 `container_memory_cache` maps directly to `memory.stat file`; candidate `cgroup_memory_file_bytes` | Host cache, cAdvisor cache and exact v2 hierarchical file charge have distinct scope. `file` includes tmpfs/shmem. | The pinned unified-v2 handler proves file-charge mapping; compare a bounded temporary file with actual `file`. **Unqualified kubelet/v1 or exact pod-parent scope remains a gap.** Never substitute working set. |
| Unevictable memory | Host `node_memory_Unevictable_bytes`; exact cgroup `memory.stat unevictable` through candidate `cgroup_memory_unevictable_bytes` | Bytes, host or selected cgroup as labeled. | **Default exact pod/cgroup source gap.** Bounded `mlock` test requires sufficient `RLIMIT_MEMLOCK`; absence/denial stays No data/NOTRUN, not zero proof. |
| Net I/O | `node_network_{receive,transmit}_bytes_total`; cAdvisor `container_network_{receive,transmit}_bytes_total` | Bytes/second; host by device. For shared pod namespaces, max by pod/interface, then sum interfaces; exclude loopback and known hostNetwork pods. | Network namespace/runtime support. Compare interface `/proc/net/dev` in the actual namespace during bounded traffic. Never sum duplicated per-container namespace counters. |
| TCP established | cAdvisor `container_network_tcp_usage_total{tcp_state="established"}` and IPv6 equivalent | Connections, max within a shared namespace, then IPv4+IPv6 sum. | Optional `tcp` collector and namespace visibility. Open a local known connection and inspect namespace `/proc/net/tcp{,6}`. Host `node_netstat_Tcp_CurrEstab` also includes CLOSE-WAIT and is a different host metric. |
| TCP retransmits | cAdvisor `container_network_advance_tcp_stats_total{tcp_state="retranssegs"}`; host `node_netstat_Tcp_RetransSegs` | Cumulative TCP segments; rate is segments/second. cAdvisor declares a gauge but its pinned source reads the cumulative retransmit field. Not loss percentage. | Optional `advtcp` collector; compare namespace `/proc/net/snmp`. Packet-loss injection requires a dedicated network and is NOT implied by an ordinary traffic test. |
| Filesystem free p100/p50 | `kubelet_volume_stats_available_bytes / ...capacity_bytes`, KSM PVC mounts and immediate controller owner; `glamdb:podset_pvc_available:{p00,p50,p100}` | Per pod least-free mounted PVC, then quantile by `cluster,namespace,podset=Kind/name`: p100=max, p50=median, p00=worst. Free fraction, not used fraction. | Actual CSI/kubelet volume stats + KSM required. Read/write temporary data on disposable PVCs and compare statfs. RWX appears once for each mounting pod; missing owners/CSI gives No data. Never use writable-layer limit minus usage as free space. PromQL fixture verifies median 0.5 for 0.2/0.8. |
| Host filesystem utilization | `node_filesystem_avail_bytes / node_filesystem_size_bytes`, statfs | Free fraction of a real mount shared by workloads; node mixin is host scope. | Filesystem collector + mount visibility; compare statfs. It is not a per-pod ephemeral quota. |
| Disk bandwidth / directional bytes | cAdvisor `container_fs_{reads,writes}_bytes_total`, cgroup `io.stat rbytes,wbytes`; host `node_disk_{read,written}_bytes_total` | Bytes/second by real block device, summed only over explicitly selected devices/cgroups. | Block I/O controller/filesystem/writeback attribution. Compare direct/bounded fsync I/O with `io.stat` or `/proc/diskstats`; cached/network/object-store I/O needs other sources. |
| Read/write IOPS | `container_fs_{reads,writes}_total`, `io.stat rios,wios`; host `node_disk_{reads,writes}_completed_total` | Completed block operations/second. Sum distinct devices once, avoid layering parent+partition/device mapper duplicates. | Same attribution prerequisites. Compare counter deltas; application requests and page faults are not IOPS. |
| Total disk utilization | `rate(node_disk_io_time_seconds_total[5m])`, `/proc/diskstats` busy milliseconds | Device busy fraction; concurrent modern disks need throughput/latency/queue context. | Linux block stats; compare actual source with bounded I/O. This is not a hard throughput ceiling. |
| Separate read/write utilization | `rate(node_disk_read_time_seconds_total[5m])` and write equivalent | Explicit **directional service-time proxy**, can exceed 100% with concurrent I/O. Exact directional wall-busy percentages are **No data**. | Linux exposes aggregate device wall busy time, not separate directional wall fractions. Do not clamp or relabel service time as exact busy utilization. |

## Separate macOS and Podman profile

The [optional collector profile](../../deploy/monitoring/collectors/README.md)
adds an explicitly selected, source-qualified Darwin and rootful Podman setup.
It does not change the default Kubernetes source contract above. Actual Mac
anonymous resident memory comes from Mach internal pages; exact container
anonymous/file/unevictable memory comes from real cgroup-v2 leaves, including
both direct-run and Quadlet paths. Inventory joins name those actual containers.
Neither source establishes pod-parent or PVC coverage.

The profile pins cgroup-exporter 0.3.3 with read-only cgroup mounts, dropped
capabilities, 64 MiB/0.25 CPU process bounds and scraper sample limits. These
external bounds do not add an internal target/device budget or make partial
collection failures visible as a success metric. The 108 live comparisons and
101 dashboard queries qualify only the documented local environment; large
container fleets, path churn, induced OOM/pressure and Kubernetes remain unrun.
Exact TCP namespace statistics, unconstrained scheduling demand and mlock-only
memory are not inferred from other measurements.

## Explicit cgroup source-gap decision

The previous private `resource_exporter.py` read exact pod-parent cgroup-v2
fields, but also owned discovery, sampling, HTTP serving and custom exposition.
It is removed rather than retaining a whole private service for the remaining
fields. Exact v2 pod-parent anon/file/unevictable/effective limits remain
unqualified in the **default Kubernetes stack**. Standalone cAdvisor 0.60.6 on
verified unified-v2 maps its RSS-named field to `anon`, cache to `file`, and
exposes CFS burst counters. Those source-proven mappings do not qualify an
arbitrary kubelet version, v1 hierarchy or a sum of child scopes. Their old `glamdb_cgroup_*` series and
recordings are a breaking removal, not a rename to approximate cAdvisor data.
Default dashboards/alerts must show No data for unqualified scopes/versions.
No alias silently turns a generic RSS series into anonymous memory.

[arianvp/cgroup-exporter v0.3.3](https://github.com/arianvp/cgroup-exporter/releases/tag/v0.3.3),
commit `9d9967ac99b61899030ffc24fcd99395bcd9a248`, Apache-2.0, was inspected as a
ready upstream candidate. It supports actual `memory.current`, `memory.stat`
(anon/file/unevictable/pgmajfault), `cpu.stat` burst/throttle/usage, PSI,
`pids.current` and device-labeled `io.stat`, converting CPU/PSI microseconds to
seconds. `memory.stat` cumulative fault fields have gauge TYPE in this release;
their source remains cumulative. It does **not** export `cpu.max` quota/period.

This release walks **leaf directories only**. A Kubernetes pod parent with
container children is therefore not collected as the exact previous pod scope.
Labels are kernel cgroup path, device and NUMA node, with no Kubernetes inventory
join. Parent/child and restart/path churn must be controlled explicitly; a
leaf-series sum cannot recover parent accounting. The CLI accepts an
operator-controlled cgroup glob, anchors reads at `/sys/fs/cgroup` via `os.DirFS`,
and has no request-controlled path endpoint. That API is not a symlink sandbox;
only a genuine read-only cgroup filesystem and trusted startup selection are
acceptable. Never mount an attacker-controlled directory there.

There is no target/device/series budget or per-controller success metric in the
pinned collector. Glob traversal and file parsing occur on every scrape;
missing/parse/walk failures are logged, so `up=1` cannot prove field collection.
The server has no explicit HTTP timeouts or shutdown coordination in `main.go`.
These properties and leaf-only scope prevent enabling it as a production
default here. It is **evaluated, not an approved production dependency**.

The next integration should contribute parent-scope selection, collection-success
signals and bounded traversal/HTTP lifecycle upstream, or use an existing
upstream extension with those guarantees. A dedicated leaf-service deployment
can independently qualify this exact release using a narrow fixed subtree,
private loopback listener, service-manager resource limits, `scrape_timeout`,
`sample_limit`, and direct comparisons. Those measures do not solve pod-parent
scope. Do not change labels or claim the gaps resolved until that qualification
exists. No local fork or replacement collector service is introduced.

The optional standard `container_memory_numa_pages` family is also a candidate
for anon/file/unevictable by NUMA node. It is not adopted as a byte-level
replacement here: the pinned handler copies the upstream cgroups parser values,
so parser version, v1/v2 unit conversion and kernel NUMA availability must be
qualified before multiplying by a page size or relabeling units.

## Primary source contracts

- [Pinned cAdvisor metric inventory](https://github.com/google/cadvisor/blob/v0.60.6/docs/storage/prometheus.md)
  and [implementation](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go),
  [actual unified-v2 anon/file mapping](https://github.com/google/cadvisor/blob/v0.60.6/lib/container/libcontainer/handler.go).
- [node_exporter 1.12.1 collectors/deployment](https://github.com/prometheus/node_exporter/tree/v1.12.1),
  [Linux meminfo](https://github.com/prometheus/node_exporter/blob/v1.12.1/collector/meminfo_linux.go),
  [diskstats implementation](https://github.com/prometheus/node_exporter/blob/v1.12.1/collector/diskstats_linux.go).
- [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html),
  [PSI](https://docs.kernel.org/accounting/psi.html),
  [scheduler statistics](https://docs.kernel.org/scheduler/sched-stats.html),
  [disk statistics](https://docs.kernel.org/admin-guide/iostats.html),
  [TCP MIBs](https://docs.kernel.org/networking/snmp_counter.html).
- [KSM pod metrics at 2.20.0](https://github.com/kubernetes/kube-state-metrics/blob/v2.20.0/docs/metrics/workload/pod-metrics.md),
  [Kubernetes volume metrics](https://kubernetes.io/docs/reference/instrumentation/metrics/),
  [pinned chart](https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-92.7.0/charts/kube-prometheus-stack),
  [Kubernetes Mixin metric compatibility](https://github.com/kubernetes-sigs/kubernetes-mixin#kubernetes-compatibility).
- [Evaluated cgroup-exporter source](https://github.com/arianvp/cgroup-exporter/tree/v0.3.3),
  [collector](https://github.com/arianvp/cgroup-exporter/blob/v0.3.3/collector/collector.go),
  [server/path selection](https://github.com/arianvp/cgroup-exporter/blob/v0.3.3/main.go).
