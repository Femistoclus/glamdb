# End-to-end ClickBench CLI benchmark

The CLI must create one dedicated managed database and a complete hits collection,
load the real 105-column source without losing duplicates or integer precision,
verify it, and measure complete network scan/reduce workloads. The existing TPC-H
CLI lifecycle is the reference. Existing prepare/query commands stay compatible.

Extend the existing adapter and SDK/journal/Locust primitives. A second client,
writer, statistics engine or SQL engine is unnecessary. Defaults remain bounded;
larger runs use explicit persisted operation/history and device budgets. Remove
the obsolete adapter-only 4096-operation ceiling, not server resource admission.

`init --mode append|kv` defaults to append for new runs and preserves a saved mode.
KV adds a physical string key encoded from the global source row ordinal; all
105 logical fields remain unchanged and duplicate IDs/rows survive. Reusing
WatchID/UserID as a unique key would lose legitimate source rows.

Load freezes globally disjoint chunks under row/wire-byte bounds, stable UUIDs,
payload hashes and worker assignments before writes. Locust schedules workers;
one publication gate matches the engine's single writer. Unknown results stop
dependent work until original-ID resolution; retries preserve exact bodies.
Snapshot preconditions reject foreign writes. Verification independently compares
the source, every journal request/receipt and the complete persisted row multiset
using bounded disk-backed state, including derived KV keys.

`scan` exposes Q1..Q43 through the same campaign as `query`, with finite Locust
users/processes, warmups and repetitions. Native scans report candidates; complete
client reductions are labelled GlamDB + DuckDB. Reports retain timing, coverage,
failures/refusals, target mode, snapshot and environment identities. All pages are
consumed and cursor owners close on errors or cancellation.

Acceptance runs the built CLI against disposable real native HTTP and verified
HTTPS stores for append/KV, exact duplicates and late columns, JSON/Arrow checks,
all query descriptors/reductions, sizes, interruption/recovery, restart, cleanup
and reload. Official-source prefix measurements and full-data measurements must
identify their actual scale. No host-wide cache eviction is authorized.
