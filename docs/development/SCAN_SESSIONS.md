# Native scan session policy

The native listener uses one `TScans` and one process `ResourceOwner` for all
managed databases (also in legacy `--root` mode). Service policy is supplied at
startup, independently of each database's persisted engine limits. Changing it
requires a restart and does not change catalog or warehouse formats.

## Configuration

| Server option | Default | Valid range |
| --- | ---: | --- |
| `--scan-live-limit` | 64 | 1..4096 |
| `--scan-live-per-database` | 8 | 1..global live limit |
| `--scan-completed-limit` | 64 | 0..4096 |
| `--scan-completed-per-database` | 8 | 0..global completed limit |
| `--scan-replay-bytes` | 1073741824 | 16 MiB..1 TiB |
| `--scan-replay-bytes-per-database` | 134217728 | 16 MiB..global replay bytes |
| `--scan-idle-seconds` | 60 | 1..86400 |
| `--scan-lifetime-seconds` | 0 | 0..31536000; zero disables absolute expiry |
| `--scan-completed-ttl-seconds` | 60 | 1..86400 |

All retained sessions, live or completed, reserve the 16 MiB hard response
maximum against both replay byte limits. These are upper bounds, independent of
the live/completed counts. A per-database live limit below the global limit
leaves cursor slots for other databases. No admission policy evicts a live
cursor to make room elsewhere. Shared memory can refuse admission before a
logical count or byte limit is reached.

`--resource-data-bytes`, `--resource-metadata-bytes`, `--resource-control-bytes`,
`--resource-completion-bytes`, and `--resource-recovery-bytes` configure the five
process reservation classes. Defaults are 2 GiB, 8 GiB, 16 MiB, 64 MiB and 4 GiB.
They are shared across databases and cannot borrow from one another. Retained
readers cannot spend protected completion/cancellation or recovery capacity.

Before any service files are created, startup checks the structural limits and
the per-database envelope: `readers = min(live_per_database,
replay_bytes_per_database / 16 MiB)`. Data must cover `readers * (32 MiB + 64 KiB)
+ 320 MiB`, metadata `readers * 272 MiB + 768 MiB`, and ordinary control
`readers * 32 KiB`. These are conservative minimum reservation envelopes for
retained replay, cursors, a footer per reader, and working admission. They are
not a universal RSS promise or a guarantee that arbitrary files, simultaneous
uploads, appends and scans fit. Dynamic Arrow/file/planning reservations still
compete with other users of the same parent; see [shared resources](SHARED_RESOURCES.md).

A sufficient bounded profile for 32 readers of one database, also tested with
64 live sessions across two databases and a third database under overload:

```sh
export GLAMDB_TOKEN='replace-with-a-random-secret'
./build/local/apps/server/glamdb --data-dir /absolute/canonical/service \
  --scan-live-limit 64 --scan-live-per-database 32 \
  --scan-replay-bytes 1073741824 \
  --scan-replay-bytes-per-database 536870912 \
  --resource-data-bytes 4294967296 \
  --resource-metadata-bytes 25769803776
```

The caller must budget physical memory for the actual workload. Raising these
reservation caps does not allocate that many bytes, and is not evidence of RSS
qualification. Reader execution remains bounded at four calls globally and two
per database; queued HTTP calls acquire no cursor, planner or decoder lease.

## Lifetime and replay

Session admission acquires 16 KiB ordinary control and `32 MiB + 64 KiB` data
from the shared parent before planning, including explicit `/scan/open` calls.
The latter covers replay copies and replacement of the previous body. Core
planning/cursor/output allocations retain their existing shared reservations.
Each new page additionally admits 128 MiB assembly work and a separate
`32 MiB + 64 KiB` outgoing lease before consuming rows. Replaying uses an
outgoing lease without acquiring new assembly work. A concurrent call for the
same cursor is refused before acquiring that work. The route preserves the
outgoing lease through socket completion instead of reserving another response
copy after advancement. Scan request parsing separately reserves `64 KiB + 32
times body bytes`; assembly admission can reclaim completed responses before
refusing a new cursor. Minimal wire-body/parsing admission still precedes scan
admission and can refuse under unrelated shared pressure.

Explicit manifest, file-source/footer and output-descriptor admission in core
returns retryable `RESOURCE_EXHAUSTED` without consuming the next batch. File
descriptors stay pending until open succeeds. HTTP page assembly retains original
and newly fetched batches until advancement commits. An admission refusal before
the first row restores them in order without advancing tokens or replay. After
collecting rows, both JSON and Arrow finish a nonempty page with a continuation,
releasing retained batches before more decoding. Actual decode, I/O and allocator
failures retain the terminal failed-session contract.

EOF destroys the core cursor and its schema lease; only the completed response
and its descriptor/replay leases remain. Completed retention has separate
global/per-database counts and TTL. When that count is exceeded, or a new scan
needs replay bytes/shared replay memory, evict the oldest completed entry in the
affected scope. Active page calls are excluded from eviction. If no other
completed entry can be evicted, a completing call returns its final response
without caching it. Zero completed retention explicitly disables final-page
replay. Counters make this policy observable. Default retention preserves the
last page until TTL or capacity eviction; after eviction its token returns 404.

Newly consumed pages refresh live idle time; replay never refreshes idle,
completed TTL, or absolute lifetime. Idle expiry excludes an executing page;
absolute expiry also cancels an executing cursor. The reaper checks at most
one second apart. Cancellation, expiry, close/reconcile and service stop revoke
tokens and logical occupancy exactly once. Shared reservations release with the
last owner, including an executing read or an outgoing response. Repeated cleanup
cannot refund another session's capacity. Cancellation marks an active cursor
immediately and does not wait for its filesystem call to finish.

Sessions survive HTTP connections: a lost page response must remain replayable.
A queued disconnect never creates a cursor. An admitted request drains; an
abandoned cursor, including a lost open response, expires under its finite idle
timeout. No disconnect proves a write failed or changes publisher authority.

## Observation and refusal

Authenticated root and database `/capabilities` expose effective `limits`,
`scan_sessions.global`, optional `scan_sessions.database`, and
`shared_resources` current/peak reservations. Live cursors, completed replays,
executing page calls, reserved serialized replay bytes and actual body bytes are
separate values. Database capabilities report both the database and global
occupancy. `http_activity` separately reports connected clients, pending calls,
active calls, body waiters, and oldest queue age in milliseconds. Observations
are snapshots; they do not reserve admission.

Session admission refusals retain `RESOURCE_EXHAUSTED`/429 and add
`error.reason`, `error.retry_after_ms: 1000`, and `Retry-After: 1`. Bounded reason
labels and counters are `global_live_limit`, `database_live_limit`,
`global_replay_limit`, `database_replay_limit`, `shared_data_memory`,
`shared_control_memory`, `shared_planning_memory`, `shared_decode_memory`, and
`session_busy`. Cancel unused cursors, wait for expiry, or adjust the shared profile. One second is a
suggested minimum delay, not a guarantee of capacity. Admission refusal does not
advance a cursor or invalidate its previous replay. Actual core read/encoding
failures retain the existing failed-session contract. HTTP queue/body refusals
remain the separate listener policy (`SERVER_BUSY`, deadlines, or shared resource
capacity); they never enter session admission.

If explicit decoder admission fails after a page has accumulated rows, return
that nonempty page with a continuation instead of rolling it back. This releases
the page's retained batches before further decoding, so many tiny batches cannot
keep their own descriptor reservations permanently saturated. A refusal before
the first row still returns 429 and preserves the cursor and previous replay.
Neither case treats admission pressure as end of scan.

## Validation

Independent review and a follow-up review checked registry/session lock order,
repeated cleanup, outgoing ownership, concurrent EOF, token/snapshot stability,
and JSON/Arrow rollback. The review prompted regressions and fixes for completed
replay reclamation before working admission, allocation-free shutdown, and
decoder admission preserving partially assembled pages. The follow-up review
found no remaining important correctness defect. A later independent PR review
identified self-induced descriptor pressure with tiny wide batches. The native
64-field, 10,000-row, one-row-batch regression reproduced the failure under a
startup-valid 1100 MiB metadata cap before the partial-page fix; it now checks
progress, exact replay, all row identities and complete reservation cleanup.

Raising the former constants alone would leave aggregate memory unbounded by
the service profile. Reserving a second permanent decoder envelope in HTTP
would duplicate core accounting. This implementation keeps permanent replay
admission, reuses core's dynamic reservations, and retains batches until page
advancement commits. That temporary ownership costs memory, which remains
charged by the existing batch leases. Discarding every EOF response would save
replay memory at the cost of retry safety; it is available only through explicit
zero completed retention. The bounded oldest-completed default preserves replay
while capacity permits.

`ScanSessionsTest`, `ReplayAdmissionTest`, `PageRollbackTest`, and `ScanLimits` cover quota/byte
boundaries, parent pressure, retained outgoing ownership, oldest-completed
eviction, disabled retention, TTL, slow reads, duplicate executing calls,
cancellation, shutdown and exact replay. `glamdb.scan_policy` runs the real native
listener and file backend with the profile above: 32 interleaved fixed-view
readers with a concurrent publisher, 64 concurrent HTTP clients across databases,
reason-specific overload, protected cancellation and database close. It prints
successful queries/s, successful page latency, occupancy/rejection counters,
shared reservation peaks and process peak RSS. This small deterministic
regression is not a sustained throughput or power-loss campaign.

```sh
cmake --build --preset local --parallel 2
ctest --preset local --output-on-failure \
  -R 'ScanSessionsTest|ReplayAdmissionTest|PageRollbackTest|ScanLimits|glamdb\.(scan_policy|scan_http|http_review|resource_profile)$'
```

Executed on Linux x86-64 with Clang 18/libc++ on 2026-11-12:

- Full Debug CTest, with native clients and strict schemas required: zero
  failures across 781 enabled entries (seven helper/manual entries skipped;
  two benchmarks disabled).
- Native SDK, backend harness and browser-process lifecycle conformance:
  the combined SDK and native conformance selection passed all 595 checks after
  integrating the Arrow client changes; all 160 UI unit checks also passed.
- ASan/UBSan with leak detection: all 27 focused session, rollback, quota and
  real native profile cases passed. LeakSanitizer discovery was rerun outside
  the restricted ptrace sandbox.
- LLVM 18 formatting and formatting/analyzer negative checks passed.
- Clang-tidy 18 passed all seven modified C++ translation units. The broader
  repository sweep was stopped after confirmed existing diagnostics in unchanged
  `apps/transport/utf8-inl.h`, `apps/transport/rows_json.cpp`, and
  `core/storage/format_verifier.cpp`; it is not recorded as a clean full scan.
- A Debug 32-reader sample completed 34.40 successful queries/s, with maximum
  successful first-page latency 522.7 ms, queue age up to 133 ms and 16 pending
  calls. Data/metadata reservation peaks were 1,883,257,392 / 9,354,291,840 bytes;
  measured peak RSS was 83,361,792 bytes, below the fixture's separate 1 GiB
  regression limit. The 64-client sample observed 52 bounded `SERVER_BUSY`
  refusals, made progress through exact-token retries, and returned cursor and
  replay occupancy to zero after cancellation.

These are small-fixture regression measurements under concurrent development
work, rather than a sustained throughput comparison or universal RSS bound.
