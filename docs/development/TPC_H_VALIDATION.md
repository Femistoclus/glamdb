# TPC-H workload validation and review

This records executed evidence for the command family in [TPC_H.md](TPC_H.md),
on 2026-11-12. The original campaigns use native source baseline
`5881c3bd1f8dd8c276f991771f39bf070c163241`; final integration and interruption
checks rebuild against official main `8f615bc`, followed by integration with
`8532c49` and another native rerun. Each artifact retains the
applicable binary/source hashes. It does not qualify the SF=1/SF=10 gates of
[issue #157](https://github.com/glamourdb/glamdb/issues/157).

## Native development-scale evidence

The [nine-case evidence](evidence/tpch-small-2026-11-12.json) records actual
standalone CLI commands, native binary and DBGEN hashes, input identities,
generation timing, source/JSONL sizes, server memory/store allocation, and
results. The shared Linux host had uncontrolled caches. These runs used Python
3.12.3; the package's declared runtime is Python >=3.13. Supported-runtime
verification is recorded separately below rather than relabelling those runs.

Every dataset contains all eight tables and 61 columns. SF=0.01 has 5 REGION,
25 NATION, 100 SUPPLIER, 1,500 CUSTOMER, 2,000 PART, 8,000 PARTSUPP, 15,000 ORDERS
and 60,175 LINEITEM rows: **86,805 rows overall**. Source size is 10,579,624 bytes;
converted JSONL is 29,233,184 bytes native or 32,054,559 bytes legacy-scaled.
The provenance pin and mirror-authenticity boundary are documented in the guide.

| Executed case | Result and assertions |
| --- | --- |
| Native and legacy-scaled, each over HTTP and verified TLS | Four PASS lifecycles: generate, init, disjoint load, full JSON/Arrow verification, exact Q1/Q6, native restart, unchanged resume, managed clean and fresh init/load/verify/clean. |
| All Q1–Q22 source scans | Native JSON and Arrow over both transports match 86 independently evaluated source aliases and 992,536 candidate rows per complete round. Two TLS worker processes complete 44 queries and 1,985,072 rows. SQL answers remain explicitly unverified in scan mode. |
| Lost committed append reply | Load returns INCOMPLETE. Restart preserves the original commit; read-only resolve sends no append, and resume completes exactly once. |
| Append not forwarded | Authoritative not-found does not resend by default. Explicit retry preserves the UUID and body digest. 24 requests produce 23 unique native commits. |
| Native pre-binding resource refusal | One-byte owned-input limit refuses before binding. After explicit operator close/open with restored capacity, read-only lookup reports absent; explicit identical retry plus resume commits 23 chunks. Full historical/receipt verification passes. |
| Lost database and collection creation replies | Repeated explicit init retains original database/store/load/table identities and schema/body hashes. One database and exactly eight collections exist. |
| Predictable preflight failures | Insufficient operation capacity, damaged dataset and foreign writes refuse before a benchmark append. |

Loads use three disjoint workers, 5,000 rows/request and one publication gate:
23 unique appends. The four initial append phases take 10.5–12.8 seconds;
these are small shared-host observations, not capacity or engine-only
throughput claims. All original generator inputs, datasets and schema manifests
remain unchanged. Cleanup checks native registry removal and an empty managed
database root before the subsequent load cycle.

The acceptance driver and byte-forwarding TLS fault helper are checked in:

```sh
"$GLAMDB_PYTHON" tools/workload/tests/tpch_native_e2e_test.py \
  build/client-standalone/glamdb-cli build/release/apps/server/glamdb \
  --dbgen /path/to/TPCH-software-code-3.0.1/dbgen/dbgen \
  --report-root /tmp/tpch-native-evidence
```

The report root must be new. The driver retains commands, outputs and structured
evidence and exits nonzero on a missing native binary, failed assertion or
changed source input. It cannot substitute a mock backend. A `--case` selection
executes only the named case; the compact nine-case evidence combines four
explicitly recorded invocations.

## Regression and supported-runtime checks

The complete final repository suite passed on Python 3.13.18: **1,757 tests,
39 skips and 12 subtests** in 202.59 seconds, with two dependency deprecation
warnings. This includes the final distributed completion and signal regressions;
the focused scan suite also passed all **34 tests**.

Integration with `8532c49` preserves both benchmark command families: 58 focused
TPC-H/ClickBench routing checks pass. Server/standalone builds, the four native
CLI/database CTests, formatting of 310 files, quality-negative checks and targeted
clang-tidy pass again. A separate reviewer verified that the resolved dispatcher
and merged C++ bridge retain the original TPC-H behavior.

Native server, linked CLI and standalone client builds passed. The native
`cli_workload_bridge`, `cli_network`, `database_drop` and `database_limits` CTest
selection passed all four tests. Formatting, quality-negative checks and targeted
`clang-tidy-18` for the changed CLI source passed.

Python 3.13 checks use an isolated interpreter and the existing hash-pinned
requirements, with no dependency or lockfile changes. The standalone bridge
passed all ten tests; the existing generic native TLS workload passed all four
cases, including interruption and lost-reply resolution.

A separate targeted run passed **1,148 workload/SDK tests on Python 3.13.18**,
including the ignored-signal regression; the final complete suite above covers
the subsequent integration and report-drain fix. A fresh native verified-TLS TPC-H lifecycle also
passes: all eight tables, full JSON/Arrow verification, Q1–Q22 candidate counts,
exact Q1/Q6, two processes, restart/resume, clean and reload. Its [separate
evidence](evidence/tpch-python313-2026-11-12.json) records the actual final
measurement fields: 6.884 seconds planning, 11.843 seconds preflight including
planning, 17.029 seconds append publication and 31.581 seconds verification.
The 23 commits contain 29,235,485 wire bytes and 2,840,380 compressed data bytes;
the local journal is 31,174,656 bytes. These are observed sizes, not SF=1/SF=10
projections or isolated storage throughput.

## Independent review, critique and alternatives

A separate read-only reviewer inspected the implementation, native contracts,
reference query templates and recorded evidence. Seven important defects were
reproduced and corrected; no critical defect was established. The final pass
also tested strict manifest parsing and actual compiled-CLI process ownership.

| Invariant / review defect | Correction and evidence |
| --- | --- |
| Preflight must bound predictable retained history | Require at least the planned retained-manifest count and native count-based recovery envelope. Regression tests reject insufficient profiles before sending; physical and merge-dependent bytes remain server admission. |
| Memory cannot grow without a declared metadata bound | Cap total descriptors and top/table/aggregate manifest bytes; bound reads before parsing. Oversized known plans fail before DBGEN. Tests cover finite limits and incomplete overflow without truncation. |
| A refused POST may leave its operation unbound | Preserve a lookup-required state and per-attempt outcome evidence. Only authoritative lookup seals a bound failure or permits explicit retry after absence. Real TLS refusal/restored-limit recovery passes. |
| Full verification requires every original receipt | Cross-check frozen state/journal plan digests, inventory bijection, body hashes and cached counters. Removing an earlier chunk/receipt or altering counters now fails. Native initial/first/final snapshots and original receipts pass. |
| CLI exit must stop its process descendants | Scoped termination unwinding and bounded private-group cleanup handle leader-first exit, stubborn workers, repeated signals and launch handoff. Actual CLI tests verify exits 130/143 and no live child/grandchild. |
| A reachable native cursor must be cancelled before forced process shutdown | A held-page native test reproduced a three-second Locust user-stop wait exceeding the controller's two-second grace period. Stop users immediately so SDK cursor contexts unwind first. Both real TLS interruption cases now cancel the native cursor before CLI exit and retain cancelled-query reports. |
| Normal distributed completion must collect terminal worker statistics | Immediate user stop must not shorten report collection. A reproduced two-worker refusal case completed all six queries but returned INCOMPLETE. A separate five-second drain and six-second outer bound preserve acknowledgements; production-zero local/distributed success and refusal regressions pass. |

Minor corrections reject duplicate JSON keys, boolean versions and contradictory
recorded generator provenance; distinguish advertised capabilities from server
build identity; add preflight/publication/verification and source/wire/Parquet
measurement fields; and ensure an ignored SIGINT cannot displace an actionable
SIGTERM during launch. Each behavior has regression coverage. The original
nine-case evidence predates the added measurement fields and does not claim to
have recorded them; the later Python 3.13 lifecycle records them explicitly.

| Alternative considered | Assessment |
| --- | --- |
| Bulk ingestion or a complete dataset per worker | Ordinary appends preserve the requested atomic units and retry receipts. Bulk requires an explicit future profile; dataset replication violates disjoint loading. |
| Streaming SQLite/JSONL part index | A useful successor for scales or tiny parts beyond the finite profile. It needs a separately versioned manifest/recovery contract. The present profile refuses rather than silently truncates. |
| Replace Locust or run it inside the caller | Keep Locust's scheduling/statistics and fresh interpreters. Bounded process ownership resolves the lifecycle defect while avoiding late gevent/TLS mutation. |
| Full client SQL engine | Separate work involving joins, spill and new dependencies. Source descriptors plus exact Q1/Q6 and independent SQLite/Decimal oracles meet the initial query boundary. |
| Several eight-table datasets in one database | Requires an additional lifecycle/capacity contract. A collection-name prefix alone does not implement this optional mode. |

The review supports the corrected architecture within the executed development
profile. It does not prove the absence of bugs at unmeasured scales.

The [native interruption evidence](evidence/tpch-interruption-2026-11-12.json)
uses the rebuilt current-main binaries and Python 3.13.18. Its normal TLS Arrow
round completes 44 queries across two processes, returning 1,985,072 rows with
all 86 stage counts independently checked. It then holds an
Arrow page after the native cursor opens and signals only the compiled CLI.
SIGINT exits 130 and SIGTERM exits 143 within the native bridge deadline. Each
cursor changes from live=1 to live=0 via an actual `/scan/cancel`, without a TTL
expiry or fallback test cancellation. No live or zombie descendants remain;
the saved state and request journal are unchanged. Both partial summaries are
INCOMPLETE with one offered/cancelled query and zero completed queries; the
driver requires those summaries to exist. The separate
`tpch_interrupt_test.py` driver preserves this behavioral regression.

## Append and KV comparison

The [KV evidence](evidence/tpch-kv-2026-11-12.json) covers implementation
`b8e4fdf` on main baseline `4820b2e`. Seven native scenarios pass on Python
3.13.18: KV HTTP/native and TLS/legacy lifecycles; lost committed reply;
authoritative not-found followed by explicit identical retry; pre-binding
refusal followed by lookup, explicit retry and resume; and both original append
TLS type profiles. The lifecycles check all source fields and derived keys,
JSON/Arrow verification, initial/first/final snapshots, exact Q1/Q6, restart,
unchanged resume, cleanup and reload. KV coverage also checks exact lookup,
mode reuse/conflict, and COW receipt accounting.

The first KV native run exposed managed cleanup rejecting valid nonzero Parquet
file ordinals. After fixing that validator, the full case passes in a fresh
invocation. All other six cases started after the final server rebuild. The
evidence records both original reports, their hashes and process-start times;
the preliminary failed report is preserved.

Regression checks pass: 331 workload tests, 11 CLI bridge tests, 106 native route
tests, native database-drop CTest and seven generic workload/ClickBench native
cases. The complete Python 3.13 suite reports 1,840 passed, 39 skipped and 12
subtests passed, with one unchanged baseline failure:
`test_supplied_logo_paths_and_sizes_load` expects a 196px logo while main's
theme uses 160px. LLVM 18 formatting of 339 files, targeted tidy for all three
changed C++ translation units, actionlint and zizmor pass.

The paired comparison uses the same native SF=0.01 dataset (86,805 rows) and
default server resource/write settings. Each of six runs gets a fresh native
process and store, in append/KV/KV/append/append/KV order, after acceptance tests
finish. Loading uses one worker, 10,000 rows/request and an 8 MiB body cap: 15
unique commits per run. Reads use loopback HTTP, Arrow batches, one user/process,
one warmup and five measured repetitions per query. The host exposes 48 CPUs
from an AMD EPYC 7713, with Linux x86-64, ext4 and Python 3.13.18. Filesystem
caches are uncontrolled and the host is shared.

| Measurement | Append | KV |
| --- | ---: | ---: |
| Load loop, s | 7.142 [7.005–7.651] | 10.165 [10.038–10.494] |
| Load throughput, rows/s | 12,155 [11,346–12,392] | 8,539 [8,272–8,648] |
| Complete load CLI, s | 34.114 [34.059–36.009] | 39.956 [39.740–40.173] |
| Retained Parquet, MB | 2.698 | 6.098 |
| Allocated store, MB | 3.027 | 6.533 |
| Q1–Q22 source scan round, ms | 1,523.0 [1,499.2–1,599.5] | 1,437.0 [1,424.2–1,507.2] |
| Q1 full client reduction, ms | 1,506.2 [1,490.1–1,544.1] | 1,484.9 [1,415.0–1,515.0] |
| Q6 full client reduction, ms | 31.97 [29.94–32.73] | 31.07 [30.57–31.57] |

Values are medians of three independent runs per mode; brackets give the
observed minimum–maximum. The load loop includes source decoding, SDK request
preparation, journaling and HTTP publication; the complete CLI command also
includes planning, preflight and full verification. The Q1–Q22 figure sums the
mean measured logical latency of each source scan. It does not include full SQL
joins or aggregation; Q1/Q6 reductions do include exact client computation and
result checking. Each run completes 110 measured source scans (4,962,680 rows),
ten verified reductions and managed cleanup.

Append remains the default for new runs: KV takes 42% longer in the load
loop and retains 2.26 times as much Parquet in this profile. KV source scans
have a 5.7% lower median, with overlapping observed ranges; the Q1/Q6
difference is small. Select `init --mode kv` explicitly to measure KV storage
and mutation semantics. The same 86,805 logical rows require 190,069 rows
written and 103,264 removed from the live view under COW (2.190 row write
amplification). Replaced files remain in history; the final live KV Parquet
is 2,867,322 bytes, compared with 6,097,687 bytes written and retained. MB
above means 1,000,000 bytes. These observations do not change existing saved
runs or choose a mode based on scan results automatically.

Reproduce a run with the same dataset, a new state file and a freshly started
native server/store; repeat the stated mode order for the paired comparison:

```sh
glamdb-cli bench tpch init --url "$GLAMDB_URL" --dataset "$TPCH_DATASET" \
  --name tpch-compare --mode "$TPCH_MODE" --state "$TPCH_STATE"
glamdb-cli bench tpch load --state "$TPCH_STATE" \
  --batch-rows 10000 --batch-bytes 8388608 --workers 1
glamdb-cli bench tpch scan --state "$TPCH_STATE" --queries all --mode scan \
  --format arrow --consume batches --users 1 --processes 1 --warmup 1 --repeat 5 \
  --report "$TPCH_SCAN_REPORT"
glamdb-cli bench tpch scan --state "$TPCH_STATE" --queries 1,6 --mode reduce \
  --format arrow --consume batches --users 1 --processes 1 --warmup 1 --repeat 5 \
  --report "$TPCH_REDUCE_REPORT"
glamdb-cli bench tpch clean --state "$TPCH_STATE"
```

These are development-scale end-to-end observations. They do not establish
statistical significance, isolated engine throughput, cold-cache physical I/O,
SF=1/SF=10 behavior or official TPC-H/QphH results.

## Open qualification gates

- **SF=1 complete command-family campaign: NOT RUN.**
- **SF=10 complete single-database campaign: NOT RUN.** Its limits must be
  explicitly sufficient; earlier retained-history evidence uses another producer
  and scalar profile and cannot close this gate.
- Optional multiple datasets in one database and future bulk ingestion are not
  implemented by this initial dedicated-database profile.
- Official TPC-H compliance, QphH, cold-cache physical I/O and physical power-loss
  durability are not established. G4 is unchanged.

Issue #157 must retain its scale acceptance items until actual campaigns supply
counts, resources, measurements, restart/verification and cleanup evidence.
