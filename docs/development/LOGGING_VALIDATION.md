# Native server logging validation — 2026-11-12

## Current server integration on main `dce0c5a`

The rebased native telemetry build passed on macOS arm64 with AppleClang 21
and Python 3.13. Nine focused CTest entries passed: `server_observability`,
`native_metrics`, `logging`, `server_logging`, `native_metadata`, `native_http`,
`http_review`, `http_security` and `http_lifecycle`. They cover actual HTTP/core
correlation, receipt-replay publication counts, unsampled metrics, queued-body
cancellation and preserving admin completion logs alongside sampled core events.
HTTP and core completions use separate sampling sequences with one shared rate cap.

The monitoring validator passed nine Python tests, its ASan/UBSan component and
real listener checks, 879 fixed series, 152 dashboard queries and the pinned chart
checksum/lint/render. LLVM 18 formatting passed all 251 project C++ files;
actionlint and zizmor reported no findings. The CivetWeb teardown race reproduced
under TSan before the build-private patch; its patched Exposer passed 20 fresh
start/scrape/shutdown processes.

These are focused integration checks. Broader local multi-store fixtures exceed
the dedicated test filesystem's capacity; Linux `/proc` and `/dev/full` cases
require Linux. Required hosted sanitizer checks remain separate merge gates.

## Earlier native integration

Rebased PR 14 onto main `0c5df05`. The native server now owns the logger,
records HTTP completion and propagates correlation into synchronous engine
storage/corruption and uncertain-commit observations. Admin read and quiesce
commands use the same bounded event path. Installed core consumers link the
private archive without exposing Quill headers.

Executed on Linux x86-64 with Clang 18 and the existing pinned storage prefix:

- The new real-server lifecycle test failed against the original binary with
  zero startup events, then passed after integration.
- The native SQLite VFS uncertainty test failed against the original binary
  with zero recovery events, then passed with matching request IDs on core
  `recovery_required` and HTTP 503 completion.
- Seven real server/admin logging tests passed: lifecycle, success sampling,
  HTTP 400/401/404/408, redaction, append/scan, damaged Parquet and correlated
  core failure, disconnected sessions, failed startup, `/dev/full`, admin
  creation/inspection/quiescence and failed read classification. Recovery failure
  coverage includes explicit reconcile and fresh-process reopening.
- `ctest --test-dir build/local -R '^glamdb.logging$' --output-on-failure` passed
  all 17 underlying native/Python adapter cases.
- The seven CTest entries `admin`, `native_http`, `native_http_fault`,
  `http_review`, `http_security`, `server_logging`, and `installed_consumer`
  passed. The installed consumer configures, links and executes actual writes
  and fresh-process reads through the exported core package.
- The complete build and `ctest --test-dir build/local --output-on-failure -j 4`
  passed: 698 tests passed, three child-process entrypoints skipped, and one
  pre-existing test disabled (701 enabled entries, zero failures).
- All seven CMake configuration tests passed using the pinned Clang 18/libc++
  toolchain and storage prefix, including the installed-archive regression.
- Focused LLVM 18 tidy passed for the five changed production translation units:
  server, server main, admin main, structured logger and engine. This is not a
  whole-repository tidy claim.
- LLVM 18 format checking of 220 files, pinned-submodule verification and the
  quality negative check passed.

Independent review found one instrumentation gap: corruption found by recovery
was not reported through the core event path. A real-server regression first
failed with only HTTP completion, then passed after instrumenting recovery's
failure handlers. The fix preserves existing readiness and exception handling.

The following older collector/adapter evidence is historical and was not rerun
for this integration. In particular, the separately configured producer below
is external collection evidence, not a test of the server source in the old
PR. Current native integration is established by the executable tests above.
No power-loss durability, live Loki deployment or production retention claim
is made by these tests.

---

# Logging validation — 2026-11-11

Executed on macOS arm64, AppleClang 21.0.0, Python 3.14.3, CMake 4.4.4,
Ninja 1.13.2 and LLVM format/tidy 18.1.8. These checks qualify the independent
logging adapters and collection pipeline. The native-service smoke check below
verifies operational event collection; complete HTTP semantics and engine
durability require their separate validation.

## Native request and engine instrumentation

The native telemetry integration includes main `6861b56`. A fresh Debug build
with AppleClang 21 and Python 3.13.5 passed all three focused CTest entries:
`glamdb.server_observability`, `glamdb.native_metrics`, `glamdb.logging`.
These cover five new HTTP/core cases, eight existing HTTP cases and nineteen
logging/schema cases; the Linux-only ENOSPC case skips on macOS. Actual native
HTTP/core events passed through fresh Alloy/Loki processes with generated
correlation and no private input markers. Metrics remained unsampled; append
receipt replay did not increase publication counters. File rotation and its
sample-every-success default remained covered.

The logging-enabled scope probe and standalone metrics checks passed ASan/UBSan;
LLVM 18 formatting passed 196 project files. The Python regression passed
633 tests, with 17 skips and 12 passing subtests. The independent workload
suite passed 36 tests. Six CMake configuration/installed logging consumer cases
passed; the unavailable dependency-prefix case skipped.

Expanded native vertical/routes/fault tests were attempted but their existing
hard-coded `/tmp` paths are rejected as symlink aliases on macOS, causing store
creation and subsequent startup failures. That run is not passing native
qualification. Linux CI sanitizer profiles and power-loss qualification remain
separate checks.

## Current-main integration and native file collection

The branch integrates main `46cad5a`, preserving the Quill and reproc pins,
workload bootstrap and both paged/legacy stream-memory fixtures. Fresh focused
checks passed: the rebuilt Quill probe, 16 logging cases, two stream-memory
tests, six CMake configuration tests and LLVM 18 formatting of 185 files.
The Linux-only ENOSPC logging case skipped on macOS.
The standalone network/workload CLI rebuilt with pinned reproc. Six native-CI
wrapper tests passed, preserving required-client/browser checks and workload
forwarding.
The storage-disabled CMake case skipped because its installed dependency prefix
is unavailable. Earlier full suites and sanitizer results below are historical;
fresh hosted native checks remain a merge gate.

A separately configured native service supplied genuine schema-1 JSONL events
to the macOS/Podman profile: startup, successful health requests and HTTP 404
responses were queried from pinned Loki 3.7.9 through pinned Alloy 1.21.3.
At the restart audit, Loki contained 40 events. An orderly collector restart
preserved the file position; its seven newly read lines matched seven sent
entries and seven new unique queried records. No duplicate `request_completed`
correlation IDs, pipeline drops or write drops were observed. The source was
the producer's actual active file; no synthetic event was added to that file.
The collector profile requires a producer that owns bounded JSONL rotation;
this smoke check does not qualify HTTP/storage outcome correctness.

Loki data, WAL, index and compactor state were confined to a preallocated
1,073,741,824-byte ext4 loop image. Its filesystem reported 1,020,702,720 bytes
after ext4 overhead. Stopping the mount stopped Loki. Making the unmounted image
unavailable caused the required-mount startup to fail, with no container and
no writes in the underlying directory; restoring the image restored readiness.
Both actual WAL metric families were present. The initial WAL usage fraction
matched the dedicated filesystem's used/total ratio exactly; the 90% guard was
unchanged. Retention remains asynchronous, and this check did not fill the
filesystem or exercise retention deletion.

Only `glamdb.jsonl` is discovered, using Alloy's built-in five-second retry.
Numbered backups are not independent targets. Rotation during collector
downtime, a missing target during discovery, lost positions and abrupt crashes
can still cause unrecoverable gaps or replay. The verified orderly restart
does not establish exactly-once delivery. Read-byte gauges are checkpoints and
can lag a quiet source; actual read/sent counters and queried events established
the ingestion result.

## Replacement and regression checks

The previous native/Python queue, worker and file-rotation implementations were
replaced with Quill 13.0.0 and stdlib logging/python-json-logger 4.2.0. The old
suite passed 15 tests before replacement. New contracts were observed failing
against that implementation (standard handler ownership, truthful delivery
availability and producer registration/budgets), then passing with the real
libraries. File ownership/symlink/rotation tests were removed with application
file mode; runtime journal quotas/rotation require the separate Linux gate below.

Final executed commands/results after rebasing onto main `6264b04`:

- In a fresh non-recursive checkout and clean Python 3.13.5 environment,
  `bash scripts/bootstrap-workbench.sh` initialized only the pinned Quill
  headers; Arrow/Iceberg remained uninitialized. The original missing-header
  compiler failure was reproduced before setup. The exact workbench Python
  check then passed **429 tests**, with one Linux `/dev/full` skip, and
  `python -m pip check` reported no broken requirements.
- `cmake -S . -B build/logging-quill -G Ninja -DGLAMDB_IO_ONLY=ON -DBUILD_TESTING=ON -DPython3_EXECUTABLE="$PWD/.venv/bin/python"`,
  build, then `ctest --test-dir build/logging-quill --output-on-failure`:
  **174/174 PASS**, including **17 logging tests** (sixteen pass on macOS; the Linux ENOSPC device test skips). The logging test uses the
  actual CMake-built Quill producer. It covers success sampling/rate limits,
  errors/lifecycle, maximum typed records, eight-producer saturation, sixty-four
  sequential/churning producer attempts constrained to sixteen lifetime queues,
  blocked and closed runtime pipes, concurrent shutdown, destruction/reopen and
  cumulative process counters. Python uses actual QueueHandler/QueueListener /
  JsonFormatter with secret projection, stdout preservation, slow/full queue,
  sink-error callback and close/admission tests. A real closed pipe exposed
  CPython exit status 120 from poisoned global stderr; the unbuffered standard
  stderr stream fix was verified RED/GREEN in a child process. Slow/error Python streams are
  explicit unit doubles; native pipes and Quill are real.
- The same configure/build/CTest with `GLAMDB_ENABLE_ASAN=ON` and
  `GLAMDB_ENABLE_UBSAN=ON` in `build/logging-quill-sanitized`:
  **174/174 PASS**. This includes logger teardown/reopen and pipe failure.
- `cmake --build build/logging-quill --target format-check quality-negative-check`:
  **PASS**, LLVM 18 formatting of 75 IO-profile sources and intentional
  formatting/analyzer violations rejected. Focused LLVM 18 tidy of
  `structured_log.cpp`, `logging_probe.cpp` and CLI `main.cpp`: **PASS**, no
  project diagnostics, using the previously verified libc++ 18.1.8 source
  headers and the current SDK sysroot for analysis. The ordinary full `tidy-check`
  target cannot use this host's SDK/STL with LLVM 18 directly; that attempted run
  failed on missing standard headers and unrelated existing source diagnostics.
  It is not recorded as a passing whole-project tidy gate. A separate global
  `python3 scripts/quality.py format-check` passed all **135** project files.
- The installed-archive regression in `test_cmake_configuration.py` first
  reproduced CMake's rejection of the non-exported private Quill target.
  Keeping Quill headers in the build interface and retaining `Threads::Threads`
  fixes the static dependency boundary. The test exports and installs the actual
  `glamdb_logging` target, then builds and runs a separate consumer with its real
  adapter header and installed archive, without defining a Quill target.
  This checks component packaging; the complete engine consumer still needs
  the installed storage dependency prefix.
- Storage-disabled fixture preparation reuses shared dependency directories
  and runs before the optional installed-prefix check. The focused CMake suite,
  including its real installed-archive consumer, passed with GCC 16.2.0.
  The native probe passed all nine cases with GCC normally and with ASan/UBSan.
- Strict native CLI and engine translation-unit compilation with
  `-std=c++23 -Wall -Wextra -Wpedantic -Werror`: **PASS**, including
  `GLAMDB_HAS_STORAGE=1` against the pinned Arrow/Iceberg source headers. CLI
  stdout generation is unchanged. A fresh full-engine configure stops at
  `find_package(SQLite3 CONFIG)`; this worktree has no installed
  Arrow/Iceberg/SQLite prefix, so no fully linked engine/CLI test is claimed.
- `cmake -P cmake/dependencies/VerifySubmodules.cmake`: **PASS**, including the
  exact Quill release commit. The installed formatter matches its pinned PyPI
  wheel. The upstream MIT/BSD licenses were inspected and dependency instructions
  updated. The Python runtime requires 3.10 or later.
- Pinned Alloy 1.21.3 `validate` of the common pipeline with each journal and
  Docker JSON source: **PASS**. Daemonless Compose 5.7.0 rendering of the base
  and optional private Grafana deployments: **PASS**. Archive SHA256s match the
  deployment lock. `py_compile` and `git diff --check`: **PASS**.
- Pinned Grafana 13.3.0's native file provider failed with exit 1 for an
  unreadable root-owned mode-0600 secret. A UID-472 mode-0400 disposable secret
  produced health 200/database `ok` and password login 200; the default admin
  password was rejected with 401. These checks used UID/GID 472:472 and private
  SELinux `:Z` mounts.

## Real ingestion and outages

The harness uses real pinned native Loki 3.7.9 and Alloy 1.21.3 binaries,
disposable child processes, unused loopback ports and a temporary directory.
The file source is explicitly a test runtime capture of stderr, not application
file logging. Actual producer output reaches the actual collector/storage.

```sh
.venv/bin/python tools/logging/run-local-e2e.py \
  --loki-binary artifacts/logging-tools/loki-darwin-arm64 \
  --alloy-binary artifacts/logging-tools/alloy-darwin-arm64 \
  --probe build/logging-quill/tests/glamdb_logging_probe \
  --exercise-outage --wal-disk-threshold 0.99
```

**PASS**: ten native append-error records and one real Python-adapter event
were searchable with literal status/duration/row fields and generated correlation.
A separate synthetic raw fixture verified collector projection: password,
Authorization, Cookie, query, body, path, exception and nested secret markers were
absent from stored content; missing schema was rejected. Record labels stayed
within the finite configured set and Loki's bounded service metadata.

Restarting only the harness-owned Alloy with its state directory made new events
searchable. Stopping only its Loki exhausted finite retries and increased actual
`loki_write_dropped_entries_total` by **32**. Restarting that Loki made new native,
Python and projection fixtures searchable. Both collector-restart and
post-outage results were **PASS**. No pre-existing service was managed.
The host was above Loki's normal 90% WAL filesystem threshold, so the small
fixture used the explicit 99% test override. Production remains at 90%; this run
does not demonstrate full-disk ingestion or retention deletion timing.

## Linux sink failure proof

An isolated read-only Linux arm64 container with Python **3.13.11**, no network,
128 MiB memory and a half-CPU limit ran the actual pinned formatter and adapter
from RAM. **8/8 Python tests PASS**, including the real `/dev/full` ENOSPC device
and a closed OS pipe. Before the stream-ownership fix, `/dev/full` counted one
sink error but the interpreter exited **120**; afterward the same child emitted
`submitted=1`, `accepted=1`, `sink_errors=1` and exited **0**. No custom writer or
retry loop was added: the handler uses standard unbuffered IO on owned stderr.
The test container was removed afterward. A separate bounded Ubuntu 24.04
arm64 container with Clang/libc++ 18 ran the actual Quill producer and Python
suite: **17/17 normal**, **17/17 ASan/UBSan** and **17/17 LSan PASS**. Three
hundred independent LSan maximum-record processes also passed strict JSON
checks. Invalid native stderr retains its offending line in failure diagnostics;
it is never filtered. The blocked-pipe check fills the queue with 10,000 calls,
retaining its ten-second deadline, exact submission count and positive drops.
Full-engine six-profile CI qualification remains a separate check.

## Measurement scope and costs

One local diagnostic trial compared the previous source revision
`d2ffdd2054598dcdc5055ff847e905cfa2b3b419` with the library adapters using
100,000 attempted single-thread error events and stderr attached to `/dev/null`.
Both use the same C++23 `-O2 -pthread` compiler; Python uses the same 3.14.3
interpreter. Per-call samples exclude shutdown, while CPU/RSS include the whole
process. Sample storage contributes to RSS. There was no CPU pinning or isolated
benchmark host. The queues saturate; these are attempted-admission measurements,
not end-to-end delivery latency or a production capacity claim.

Native previous p50/p95/p99 were **83/208/250 ns**; Quill was
**333/500/542 ns**. Producer elapsed time was **25,482/38,140 us**, CPU time
**55,274/107,915 us**, peak RSS **2,719,744/2,785,280 bytes**, and actual admitted
counts **11,520/8,032** (previous/library respectively). Different finite queue
representations and scheduling affect admission loss. The optimized diagnostic
executable grew from **44,568 to 461,608 bytes**; this is not an installed-engine binary comparison.

Python previous p50/p95/p99 were **8,041/9,167/15,125 ns**; stdlib logging was
**11,541/12,750/18,416 ns**. Producer elapsed time was **0.845/1.292 s**, CPU time
**1.111/1.618 s**, peak RSS **26,591,232/29,081,600 bytes**, and actual admitted
counts **4,745/9,426**. The bounded queue held 64 entries in both runs.
No speedup is claimed: the migration removes maintained infrastructure while
adding the measured library overhead.

The two implementation files shrank from **662 + 282 = 944 lines** to
**439 + 219 = 658 lines**. Tests/configuration/docs are additional; the upstream
Quill source and dependency footprint are not counted as removed project code.

## Remaining runtime/operator gates

- Real journald quota/rotation, Docker json-file rotation and native sink ENOSPC
  remain pending. Python's real Linux `/dev/full` behavior is verified above;
  it does not establish a runtime journal's retention or native Linux behavior.
  Use a task-owned capped volume, emit beyond the configured runtime limit,
  restart the collector and verify both retained bytes and new-event search.
- The full Docker logging deployment was rendered. An isolated Grafana
  file-provider startup/login check passed above; remote TLS/authentication,
  datasource wiring and multi-node retention remain deployment checks.
  Local Loki/Alloy ingestion was real.
- Native output has no exact delivered/backlog metric. Quill callback drop
  reports are delayed and duplicate the queue-loss observation; their count is
  never added to queue rejection. Runtime/collector loss can occur after a
  successful application admission and needs independent monitoring.
- A permanently blocked OS sink can delay graceful shutdown. Producer admission
  remains bounded; this suite releases its deliberate pipe block before joining.
  Join workers before object destruction; restart the process to obtain a new
  lifetime producer-allocation budget after worker churn exhausts sixteen slots.
