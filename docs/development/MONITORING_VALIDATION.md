# Monitoring validation

Executed for the library/client and standard-collector configuration on
2026-11-11. Native component environment: Darwin arm64, AppleClang 21.0.0,
Python 3.14.3; formatting uses clang-format 18.1.8. These are current replacement
checks, not the earlier private collector/textfile tests. There is no claim of
full native engine durability or hosted CI success from these component results.
Native server integration checks below
are separate from the earlier component gate.

## Integrated local deployment

The macOS/Podman profile was additionally executed against a persistent native
server, existing authenticated gateway/edge, Loki/Alloy, Prometheus, Grafana and
Alertmanager. All 13 configured scrape targets were UP; 307 rules (49 checks
plus 13 recordings) evaluated without failures. Eight native HTTP/logging test
methods and the monitoring checker passed: nine Python tests, ASan/UBSan metric
contract/endpoint checks, rule fixtures and 152 dashboard PromQL expressions.

Infrastructure validation compared 108 values against actual Mach/sysctl,
vm_stat, IORegistry, filesystem, interface and container/cgroup sources. All
101 infrastructure dashboard queries returned samples; named joins covered all
14 running containers, including a Quadlet-managed Loki. The only observed
nonfinite samples were read/write mean service time on an idle disk (zero I/O).
They were not filled with zero. This does not qualify arbitrary container count,
source churn or a production load envelope.

The real identity/gateway checks passed 28 cases; an additional 120 authorized
edge-to-native reads all returned 200. Four Grafana dashboards and genuine Loki
records were checked in Chromium without page/datasource errors. An orderly
Alloy restart preserved offsets and delivered new genuine records without
observed duplicate completion IDs. Missing-mount refusal and recovery were
verified on the dedicated bounded Loki filesystem. Telegram grouping is tested
separately by the alerting suite; notification acceptance is not proof of human
receipt or exactly-once delivery.

Kubernetes/PVC and native administrative-audit sources remain unavailable in this
profile. Idle or low-volume p95 checks remain NODATA. Actual low APFS free space
continues to alarm. None of these local checks establish durability under power
loss, full hosted native CI, an independent observer or an always-on deployment.

## Native server integration

The native server additionally passes eight focused real-server test methods
after merging the current write-health fixes: existing readiness/durable counter
and restart tests, zero-initialized HTTP families, real unauthorized/success
request and status-only response counters, incomplete-upload cancellation,
an 8 MiB response held in-flight by a slow client, and Quill schema-1 records
from actual operations/lifecycle. The file-sink test verifies private input
markers never reach records, logging admission metrics are real, and no records
are rejected as unregistered producers. A separate seeded 8 MiB boundary checks
Quill rotation retains at most four `glamdb*.jsonl` files, including the active
file, within the per-file limit plus one bounded record. These tests use their
own disposable directories; they do not touch a running service or existing data.

On Darwin arm64 with AppleClang 21.0.0, Python 3.13.5, the cached pinned native
dependencies, prometheus-cpp 1.3.0 and Quill 13.0.0, the server and focused native
test targets build. LLVM 18.1.8 formatting passes for 188 project source/header
files. The five CMake configuration tests pass. Five focused CTest
entries pass: three outcome conformance cases, actual engine-health readiness,
and `glamdb.native_metrics` (eight Python test methods). The metrics tests use
only disposable stores on a filesystem with sufficient reserve:

```sh
GLAMDB_TEST_TMPDIR=/path/to/disposable/test-parent \
ctest --test-dir build/local --output-on-failure \
  -R 'glamdb.native_metrics|glamdb.routes.Databases.ServiceReadinessReadsCoreHealth|glamdb.transport.OutcomeConformance'
```

Executed coverage includes default-disabled/private-only exposition, actual
committed create/append counters, readiness with no open databases, explicit
close/reopen, one failed open while another engine remains healthy, recovery,
restart persistence, absence of database identities/paths, and listener teardown.
The C++ readiness case shuts down the actual owned engine while retaining an
open service descriptor, proving the observation consults core health.

The full host CTest attempt is **not green**: 142 failures out of 533 enabled
entries. Many existing fixtures hardcode `/tmp`, whose macOS symlink alias is
rejected by canonical store identity checks; collection writes there also hit
the real disk reserve. Additional failures include the Linux `/dev/full` CLI
case, `CiNativeRunnerTest.test_root_build_includes_the_io_probe`, and
`IcebergMetadataJsonCost.SerializationIsLinearInRetainedSnapshots` (3.28 ratio
against a limit of 3). The new readiness unit initially shared the `/tmp` alias
issue; its canonical-path correction passes the focused rerun above. These
results do not establish a passing Linux native suite or durability qualification.
LLVM 18 clang-tidy cannot parse this host's libc++ 21 headers (unsupported
`__builtin_clzg`/`__builtin_ctzg`); no clean tidy result is claimed.
The source-only dependency check stops at the uninitialized Arrow source
checkout; these native builds use the existing installed Arrow/Iceberg/SQLite
prefix. The new Quill checkout is at its exact locked commit.

## Executed PASS

With prometheus-cpp 1.3.0 installed from its exact submodule (upstream pinned
CivetWeb, pull enabled; push/tests/compression/TLS disabled), prometheus_client
0.26.0, Prometheus/promtool 3.13.5 and Helm 3.19.1:

```sh
.venv/bin/python tools/monitoring/check.py \
  --promtool artifacts/monitoring/prometheus-3.13.5.darwin-arm64/promtool \
  --helm artifacts/monitoring/bin/helm \
  --chart artifacts/monitoring/kube-prometheus-stack-92.7.0.tgz
```

- Seven focused Python contracts: concurrent finite HTTP counters, histogram
  count/sum/boundary, fresh-registry reset, default readiness/in-flight release,
  library ASGI response, actual library HTTP socket and thread/listener teardown,
  truthful logger snapshot families, pins and absence of synthetic source values.
  These are seven test methods; several assertions share a method.
- A fresh non-recursive checkout and clean Python 3.13.5 environment reproduced
  the missing `prometheus_client` import before setup. Workbench bootstrap now
  installs the monitoring requirement fragment when present, retaining hash
  verification, binary-only resolution and `--no-deps`. The exact workbench
  Python check passed **420 tests**, the installed client was **0.26.0**, and
  `python -m pip check` reported no broken requirements.
- The storage-disabled CMake fixture reuses the public headers copied by its
  shared setup. Source preparation runs before the optional dependency-prefix
  check so missing local packages cannot hide setup errors. The focused CMake
  suite passed with GCC 16.2.0; its full-engine configuration still requires the
  installed Arrow/Iceberg/SQLite prefix.
- Native component compiles with strict warnings and ASan/UBSan. Eight threads
  record 80,000 committed outcomes; exact count=80,000, sum=800 seconds and the
  inclusive 0.01-second bucket are checked. Tests also cover incomplete scopes,
  unknown-error bounding, admission ownership, phase/error pre-registration and
  scope gauge release. A second fresh process proves reset. The fixture exports
  exactly **714 domain series**, about **87.2 KiB**, before optional log metrics.
- Actual prometheus-cpp Exposer serves the domain registry over a localhost TCP
  socket. `promtool check metrics` accepts it. Destruction removes the listener;
  no publisher/snapshot file is used. It is a server-application fixture, not a
  production native database HTTP adapter.
- Both static Prometheus configurations pass `promtool check config`.
- All **13** application/PVC recording rules pass parsing. Four PromQL scenarios
  cover gateway reset/denominators, native endpoint-down suppression, absent
  sources and actual PVC median aggregation. All **33** application/upstream-node
  dashboard expressions parse after normal Grafana variable substitution.
- The exact official kube-prometheus-stack **92.7.0** archive matches locked
  SHA-256 `af35d3e93750ce46d90cbb6537483c8fa914f19fbffa19fbabcb2825181a0f1a`.
  Helm strict lint passes and rendering with Kubernetes 1.36.0 capabilities
  succeeds. No rendered `insecureSkipVerify: true` remains. This does not prove
  a real cluster's certificates, RBAC, CRDs, CSI, scheduling or runtime support.
- The standalone native metrics target builds with
  `GLAMDB_IO_ONLY=ON GLAMDB_BUILD_METRICS=ON BUILD_TESTING=OFF` and the pinned
  Prometheus prefix. IO-only also builds with no metric dependency when the
  standalone option is off. Native formatting passes LLVM 18.1.8. The metric adapter also compiles with
  strict warnings against the finalized optional logging stats header; combined
  native logging lifecycle execution remains a separate integration check.
- After rebasing onto main `6264b04`, rebuilding the IO/metrics profile with
  `BUILD_TESTING=ON` and running `ctest --test-dir BUILD_DIR --output-on-failure
  --no-tests=error` passed **173/173** tests normally and **173/173** with
  `GLAMDB_ENABLE_ASAN=ON GLAMDB_ENABLE_UBSAN=ON`. Each run includes 172 native
  I/O/core cases and the tooling suite; it does not include the full engine.
  The strict-warning native metric fixture also passed in two fresh processes
  without sanitizers, in addition to the ASan/UBSan component gate above.
- LLVM 18.1.8 formatting passed for all **134** project source/header files,
  and the quality negative controls rejected deliberate format/analyzer errors.
  Focused LLVM 18 tidy of `core/telemetry/metrics.cpp` and storage-enabled
  `core/engine/engine.cpp`, using matching libc++ 18.1.8 source headers and the
  current SDK sysroot, passed with no project diagnostics. The metric command
  first reproduced five empty-catch errors; explicit returns document telemetry
  failure isolation without disabling the check. These are focused
  translation-unit checks, not full-project tidy.
- Strict-warning compilation of the instrumented `core/engine/engine.cpp`
  passed with both `GLAMDB_HAS_STORAGE=0` and `GLAMDB_HAS_STORAGE=1`, using the
  pinned Arrow/Iceberg source headers. This checks the rebase's instrumentation
  and storage-enabled declarations without claiming a linked engine test.
- Official Docker Compose **5.7.0** parsed both monitoring Compose files without
  a daemon. The exporters render used the disposable configuration value
  `GLAMDB_NODE_LISTEN=127.0.0.1:9100`; omission correctly fails the required-bind
  check. These parser results do not establish collector deployment or host
  access. Strict-warning compilation of the rebased CLI translation unit passed;
  it retains CLI11 and has no metric publisher or endpoint.
- Pinned Grafana 13.3.0's native file provider failed with exit 1 for an
  unreadable root-owned mode-0600 secret. A UID-472 mode-0400 disposable secret
  produced health 200/database `ok` and password login 200; the default admin
  password was rejected with 401. These checks used UID/GID 472:472 and private
  SELinux `:Z` mounts.
- Changed workflow passes actionlint 1.7.12 and zizmor 1.32.0 offline pedantic
  checks with no findings.
- Fourteen mocked Docker-runner tests pass, including explicit recursive
  Prometheus/CivetWeb prerequisite failures and isolated dependency volume
  arguments. They do not start a Docker daemon or certify a real engine build.

Ignored per-run artifacts contain the exact command output; they are not
installed product files. Reproduce the component gate instead of relying on a
historical test-count claim.

## Component cost

The same one-million-call C++ fixture ran before and after replacement with
`-O2`, eight-thread comparison and no database/storage work. Original branch
head `b3f95b3ad074534f6120a372e1191965ce701e24` used the private atomic histogram:
731.477 ns/call single-thread, 0.370760 s for the eight-thread million calls,
211 observed series. The library implementation measured 1587.34 ns/call and
0.649090 s, with all 714 domain series pre-registered. This is approximately
856 ns more per instrumented fixture call; it is not a full-request or database
throughput result. No cache or revocation guarantee was changed for the result.

The Python observation fixture recorded 100,000 calls in 0.355255 s, approximately
3.55 microseconds each. Its full finite registry has **1,850 samples** including
the library's created timestamps, approximately 175.7 KiB of exposition and
515,837 retained bytes measured with tracemalloc during construction. These are
component measurements on this host, not a production memory/latency budget.
Request volume does not add metric identities.

## Standard collector runtime checks

`tools/monitoring/container_smoke.py` uses only an explicitly selected disposable
Linux VM/connection, a unique `glamdb-oss-monitor-*` network/container prefix,
CPU/memory limits and temporary in-memory Prometheus/Grafana data. It neither
starts/stops a VM nor removes any existing volume. Source comparisons run from
a separate probe container so the probe process does not inflate workload
`pids.current` or memory accounting.

The runtime fixture uses Fedora CoreOS 43, Linux 6.17.7 aarch64, cgroup v2,
4 CPUs / 4096 MiB, and an existing checkout share. Podman Docker-compatible API
detection in cAdvisor 0.60.6 expects Docker layerdb paths that Podman does not
provide; the fixture disables that Docker adapter while keeping the upstream
Podman factory and raw cgroups available. It selects the actual Podman child
scope (including its trailing-slash exporter ID) and compares the matching
Linux cgroup path. A real Docker or Kubernetes/containerd deployment still needs its own runtime
qualification. macOS-to-VM published-loopback forwarding reset connections in
this environment, so collector correctness is queried via actual HTTP over the
isolated Linux bridge. That does not qualify external browser/host forwarding.

Executed result: **PASS with documented source gaps** using:

```sh
.venv/bin/python tools/monitoring/container_smoke.py \
  --connection DEDICATED_ROOTFUL_CONNECTION --machine DEDICATED_VM \
  --vm-root /workspace
```

- Charged memory matched `memory.current` exactly: **73,666,560 bytes**. CPU
  usage matched **0.105229 seconds**, and cAdvisor reported **4 threads** against
  actual `pids.current=4`. Host total memory matched **4,079,595,520 bytes**;
  AnonPages/Cached/Unevictable comparisons use explicit live-gauge tolerances.
- The pinned unified-v2 handler's source-proven mappings were exercised with a
  64 MiB anonymous allocator and an 8 MiB file/cache control: RSS-named series
  **73,003,008 bytes** equals actual `memory.stat anon`; cache **8,556,544 bytes**
  equals actual `file`. This is a qualified version/hierarchy mapping, not a
  generic RSS rename. A targeted file fadvise/mmap control produced **one major
  fault**, matching the real counter; it did not evict host-wide cache.
- A bounded CPU loop under 0.5-core quota produced **0.983276 seconds** of CFS
  throttling, exactly matching `throttled_usec / 1e6`. The optional burst field
  existed and matched zero; nonzero burst behavior remains unqualified.
- Actual Grafana **13.3.0** provisioned the upstream node dashboard UID
  `7d57716318ee0dddbac5a7f451fb7753` and `glamdb-service`. Its authenticated
  datasource proxy queried the real Prometheus/node producer. Startup measured
  **2.268 seconds**, with a 512 MiB / 1 CPU test cap. Bundled plugins were used;
  unpinned preinstallation and automatic updates were disabled.
- Removing the workload removed its raw/fresh resource series while cAdvisor
  stayed `up=1`. The actual observation-age query used a 30-second bound.
  Before enabling standard `track_timestamps_staleness`, the real test exposed
  cached explicitly timestamped values lasting over 90 seconds. `container_last_seen`
  in the pinned source uses scrape time and is not the observation-age guard.
- Stopping node_exporter yielded `up=0` and removed old node memory values;
  restarting it restored `up=1`. Every uniquely named test container and its
  network were independently confirmed absent after cleanup. No preexisting
  volume or unrelated VM was removed.

The ignored result is `artifacts/monitoring/standard-collectors.json`.
[MONITORING_COVERAGE](MONITORING_COVERAGE.md) states remaining scope/kernel
requirements. These Linux checks do not certify Kubernetes or a database.

## NOTRUN / operator prerequisites

- Installed `glamdb::core` consumer with the telemetry-enabled installed package:
  run the existing installed-consumer CTest before release. The earlier
  component-only dependency blocker is superseded by the complete native build
  above, using the cached pinned Arrow/Iceberg/SQLite and prometheus-cpp prefixes.
- Combined optional native Quill/log metric lifecycle, Linux/GCC/TSan/MSan and
  full clang-tidy execution were not run. The compile-only stats compatibility
  check does not establish runtime teardown or sanitizer coverage.
- Full gateway/native comparative load and a distinct current-unknown gauge.
  Real native HTTP create/append, restart persistence and authoritative readiness
  are covered above; the earlier component endpoint alone did not establish them.
- Actual Kubernetes server dry-run/deployment, CSI PVC fill/free quantiles,
  kubelet certificate/RBAC/runtime checks and Kubernetes controller outages.
  Kubernetes is optional and was not introduced solely for a test.
- Controlled network-namespace byte/TCP-established comparisons and block-device
  byte/IOPS/service-time comparisons were not executed. Their table entries are
  producer/unit contracts, not runtime PASS claims.
- Nonzero burst, scheduler unconstrained-wait separation, mlock unevictable
  memory, packet-loss retransmits and directional
  wall-busy percentages are not certified by the allocator fixture. Exact
  unsupported fields remain No data; directional service time is only a proxy.
- arianvp/cgroup-exporter 0.3.3 production adoption/load/path/churn qualification
  and parent-scope support: intentionally not enabled. Its source/license/release
  review and explicit gap decision are in the coverage matrix.
- Hosted workflow, full environment load and operator retention/firewall/secret
  provisioning remain deployment checks. Helm render/component smoke results
  must not be reported as those checks.
