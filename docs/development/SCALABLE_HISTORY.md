# Configurable retained history

Ordinary appends retain their 10,000-row / 16 MiB input limits. Each successful
append has its own operation, Parquet file and snapshot. The default permanent
operation and snapshot limits remain 4,096. They are configurable budgets,
rather than universal implementation maxima.

See [the validation report](SCALABLE_HISTORY_VALIDATION.md) for the full SF=10
measurements, regression coverage and independent review findings.

## Selecting a profile

[The SF=10 profile](../../benchmarks/profiles/tpch-sf10.json) is a public `Limits`
object. Pass it as `limits` when creating an HTTP database, or while reopening
a closed database. In C++, set `CreateStoreRequest::limits` and use the same
values in `OpenStoreRequest::limits`. HTTP persists and returns the normalized
values in every database descriptor, including closed/error states. Embedded
callers retain their configuration; a store identity does not persist it.

| Budget | Default | SF=10 profile |
| --- | ---: | ---: |
| Permanent operations, including bound failures | 4,096 | 20,000 |
| Catalog bytes | 256 MiB | 512 MiB |
| Retained snapshots and snapshot-log entries | 4,096 | 12,000 |
| Retained manifests | 4,096 | 12,000 |
| Data files | 16,384 | 16,384 |
| Aggregate current root bytes | 64 MiB | 128 MiB |
| Individual root bytes | 16 MiB | 32 MiB |
| Aggregate manifest bytes | 256 MiB | 512 MiB |
| Aggregate retained manifest-list bytes | 512 MiB | 4 GiB |
| Aggregate footer bytes | 256 MiB | 512 MiB |
| Metadata verification bytes per commit | 32 MiB | 64 MiB |
| Recovery workspace bytes | 256 MiB | 512 MiB |

The last ten settings are nested under `limits.history`; their exact names are
in the linked JSON. Omitted HTTP fields keep existing values during reconfiguration
and use defaults during creation. The service accepts complete legacy persisted
seven-field limits objects and supplies default history budgets without rewriting
receipts or the core store format. A present persisted history object must be complete.

`max_operations` accepts positive signed 64-bit integers. Catalog and aggregate
history byte/count budgets have a 1 TiB / 2^40 upper bound. Individual root bytes
are at most 64 MiB, per-commit bytes at most 256 MiB, and recovery workspace bytes
are between 8 KiB and 1 TiB. Collections default to eight and support up to 64
with [separate resource admission](COLLECTION_PROFILES.md). These validation
ranges are not measured capacity guarantees. Different schemas and distributions
can exhaust a byte budget before a count budget.

Individual manifests, manifest lists and Parquet files retain their 8, 16 and
32 MiB bounds. Ordinary input and writer limits, attempt I/O, physical warehouse
capacity, free-space headroom, and shared process/device reservations also apply.
Larger configurations need enough capacity in the supplied `ResourceOwner`;
raising a stored count alone does not reserve unlimited memory or disk.

## Inventory, memory and recovery

Catalog inventories use primary-key keyset pages of at most 256 records, retaining
the existing per-query byte ceiling. Inventory validation and interrupted-operation
resolution stream those pages. Native storage recovery reconstructs committed
receipts for one collection at a time and checks their bijection with retained
snapshots. The legacy vector-returning internal seam remains for custom/test
backends; production recovery uses the streaming seam.

Healthy outcome lookup remains one indexed `(collection_id, operation_id)` query.
It does not enumerate history, read warehouse files or reconcile automatically.
Uncertain binding/publication still invalidates the verified generation and keeps
the recovery barrier until authoritative reconciliation and its durability fence.
Failed bindings count permanently toward the operation budget and retain their
original terminal failure. No receipt, snapshot or operation ID expires.

Root cache reservations remain `32 * serialized_root_bytes + 4 MiB`. Temporary
metadata work reserves at least 768 MiB and grows to `48 * max_root_file_bytes`
above the default 16 MiB root allowance. The default shared owner retains its
finite class/device limits. These are conservative admission envelopes; measured
RSS and upstream/STL/JSON allocator behavior are separate evidence.

The per-store read metadata cache keeps its separate 32 MiB reservation and
entry limits. An entry larger than its cache budget is read without caching;
the configured root limit still applies before a cache lookup.

Recovery's visited-path SQLite database uses the configured workspace cap and a
small fixed page cache. Publication and reopen conservatively require:

```text
8192 * (1 + snapshots + manifests + data_files) <= recovery_workspace_bytes
```

The implementation subtracts counts from available units to avoid overflow.
Two 4 KiB pages per at-most-2-KiB visited key cover overflow and B-tree split
slack; the extra unit covers database overhead. The store-wide envelope is
conservative because collection verification is sequential. The engine protects
the configured workspace plus two catalog-growth allowances on the actual device.

Every metadata reader receives the selected root limit, including history,
statistics, file details and Explain. A state published under a larger root
profile must remain readable and reopenable under that profile.

Lowered operation/catalog limits reject open with `CONFIG_TOO_SMALL`. Lowered
retained-storage limits preserve the existing storage error classification
(`CORRUPTION` for structures outside that configured profile). Reopen does not
prune or replace the store. Restore the sufficient profile and reopen it.

## Reproducing the acceptance campaign

Build `glamdb_history_bench` in Release with the pinned dependencies. Generate all
eight standard TPC-H SF=10 `.tbl` files with TPC-H dbgen 2.17.2, for example:

```sh
dbgen -s 10 -b /path/to/dbgen/dists.dss
python3 benchmarks/history.py --data /absolute/sf10-tables \
  --output /absolute/new-disposable-campaign
```

The supervisor uses separate load and reopen processes, checks literal SF=10
cardinalities, then rejects lowered operation, catalog, snapshot, manifest,
data-file, manifest-list, footer, root, per-commit and recovery budgets before
restoring the original profile.
Keep the source files and campaign directory until inspection is complete.

The native producer uses Int64 identifiers, Float64 numeric quantities/money,
and exact strings for text and dates. Those types preserve comparability with
the initial campaign; this campaign does not exercise the newer Decimal/Date
scalars or measure TPC-H SQL performance.
It submits ordinary public appends of at most 10,000 rows and deliberately binds
one failed precondition per collection. No bulk operation combines requests.

The independent oracle parses source numbers separately, compares exact numeric
bits and string bytes, and sorts source/scan rows externally with GNU sort's
64 MiB buffer and one worker. Exact sorted-row comparison verifies duplicates
without assuming scan order. Full latest verification requires source exhaustion;
historical and explicitly limited smoke profiles verify their declared prefixes.
Oracle scratch files need additional disk space proportional to the largest table.

Every 500 commits, the supervisor's load profile closes the writer, opens and
validates the accepted state in a separate process, then resumes ordinary appends.
Checkpoints record ingestion latency, metadata/catalog/physical warehouse growth,
and the probe's reopen time and peak RSS. Reports also include peak parent RSS.
Linux RSS measurements use the current image's `/proc/self/status` `VmHWM`;
`ru_maxrss` can inherit the writer's high-water mark across exec. Other platforms
use their `getrusage` measurement and are not qualified by the Linux campaign.
Fresh process does not imply an evicted OS page cache. Tests or other work on the shared host
may contend for CPU and I/O. Neither this profile nor a passing campaign qualifies
physical power loss, a universal RSS bound, or arbitrary larger configurations.

Run the short native regression with:

```sh
ctest --test-dir build/release -R '^glamdb.history_campaign$' --output-on-failure
```

It covers duplicates across append boundaries, failed bindings, original outcomes,
first retries, initial/historical views, lower-budget rejection/restoration, source
value changes, missing duplicate rows, and unconsumed source suffixes. Catalog
tests separately exercise 8,193 records; HTTP tests cover persisted larger budgets
and legacy registry restart. A padded valid root crosses the former 16 MiB reader
ceiling and exercises all public metadata APIs.

## Remaining growth cost

The recorded campaign predates the bounded latest-manifest-list producer in
[PR #183](https://github.com/glamourdb/glamdb/pull/183). Its quadratic list growth
and ingest timings describe that earlier producer and do not predict the newer
producer's growth. Retained snapshots and rewritten manifests still require
explicit count and byte budgets.

A normalized snapshot index could accelerate recovery, but would need its own
publication/recovery invariant and compatibility work. Receipt expiration and
silent request coalescing would change the contract and are not alternatives
implemented by this profile. Physical durability qualification remains unchanged.
