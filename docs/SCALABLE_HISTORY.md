# Configurable retained history

Ordinary appends retain their 10,000-row / 16 MiB input limits. Each successful
append has its own operation, Parquet file and snapshot. Defaults for new
databases are chosen so that counts do not bind: permanent operations are
bounded by catalog bytes, root snapshots by the snapshot archive's hot window,
retained manifests, data files and archived metadata by byte budgets derived
from the 1 TiB warehouse budget (`DefaultWarehouseBytes`). Every budget stays
finite and configurable; existing databases keep their persisted limits and
embedded callers that pass explicit `Limits` keep them.

## Selecting a profile

[The SF=10 profile](../benchmarks/profiles/tpch-sf10.json) is a public `Limits`
object. Pass it as `limits` when creating an HTTP database, or while reopening
a closed database. In C++, set `CreateStoreRequest::limits` and use the same
values in `OpenStoreRequest::limits`. HTTP persists and returns the normalized
values in every database descriptor, including closed/error states. Embedded
callers retain their configuration; a store identity does not persist it.

| Budget | Default (new databases) | Former default (`LegacyHistoryLimits`) | SF=10 profile |
| --- | ---: | ---: | ---: |
| Permanent operations, including bound failures | 2^40 (catalog bytes bind) | 4,096 | 20,000 |
| Catalog bytes | 64 GiB (warehouse / 16) | 256 MiB | 512 MiB |
| Root snapshots and snapshot-log entries | 8,192 (64 × (hot 100 + 16)) | 4,096 | 12,000 |
| Retained manifests (incl. archived) | 2^40 (bytes bind) | 4,096 | 12,000 |
| Data files | 2^40 (footer/warehouse bytes bind) | 16,384 | 16,384 |
| Aggregate current root bytes | 64 MiB | 64 MiB | 128 MiB |
| Individual root bytes | 16 MiB | 16 MiB | 32 MiB |
| Aggregate manifest bytes (incl. archived) | 256 GiB (warehouse / 4) | 256 MiB | 512 MiB |
| Aggregate retained manifest-list bytes | 256 GiB (warehouse / 4) | 512 MiB | 4 GiB |
| Aggregate footer bytes | 64 GiB (warehouse / 16) | 256 MiB | 512 MiB |
| Metadata verification bytes per commit | 32 MiB | 32 MiB | 64 MiB |
| Recovery workspace bytes | 64 GiB (warehouse / 16) | 256 MiB | 512 MiB |

The catalog default follows the measurement of about 2.75 KB of catalog per
one-row archived append (receipts, archive rows, checkpoints): 64 GiB holds
roughly 25 million such appends, while the 1 TiB warehouse holds about 50
million at the measured 21 KB per one-row append, so both budgets end in the
same order of magnitude for the smallest appends and the warehouse binds for
real ones. These maxima reserve nothing by themselves: the engine reserves
recovery memory for the operations that exist (1 KiB each, in chunks of 256)
and device space for twice the actual catalog plus 64 MiB steps and the actual
recovery workspace envelope, growing both before a binding. The SF=10 profile is
now narrower than the defaults for most counts and is kept to reproduce its
recorded measurements.

The last ten settings are nested under `limits.history`; their exact names are
in the linked JSON. Omitted HTTP fields keep existing values during reconfiguration
and use defaults during creation. The service accepts complete legacy persisted
seven-field limits objects and supplies the former history budgets
(`LegacyHistoryLimits`) without rewriting receipts or the core store format. A
present persisted history object must be complete.

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
the existing per-query byte ceiling. A collection's inventory seeks its own key
range and reads no other collection's records. Inventory validation and
interrupted-operation resolution stream those pages. Native storage recovery
reconstructs committed receipts for one collection at a time and checks their
bijection with retained snapshots. The legacy vector-returning internal seam
remains for custom/test backends; production recovery uses the streaming seam.

Bindings check the operation budget against an exact count that the catalog
derives once per connection: the owning process increments it for each
committed binding, and an uncertain binding commit discards it until reopening
counts the table again. Opening or reconciling validates the whole catalog
inventory (SQLite integrity and foreign keys, registry and every operation
record) when the connection opens and again after the durability fence and
interruption resolution; storage verification in between reuses the first
validation because no catalog transaction intervenes. Finding interrupted
(`Bound`) operations still reads every record: no index covers the state column,
and adding one would change the catalog format. `glamdb_catalog_history_bench`
measures open and append cost against the number of permanent receipts with a
fixed small retained history.

Healthy outcome lookup remains one indexed `(collection_id, operation_id)` query.
It does not enumerate history, read warehouse files or reconcile automatically.
Uncertain binding/publication still invalidates the verified generation and keeps
the recovery barrier until authoritative reconciliation and its durability fence.
Failed bindings count permanently toward the operation budget and retain their
original terminal failure. Receipts and operation IDs never expire. Snapshots
remain retained by default; the separate explicit [retention API](SNAPSHOT_RETENTION.md)
can expire an opted-in oldest prefix without changing configured budgets.

Root cache reservations remain `32 * serialized_root_bytes + 4 MiB`. Verified-index
scan planning uses a 16 MiB schema/projection workspace. Uncached temporary
metadata work reserves at least 768 MiB and grows to `48 * max_root_file_bytes`
above the default 16 MiB root allowance. The default shared owner retains its
finite class/device limits. These are conservative admission envelopes; measured
RSS and upstream/STL/JSON allocator behavior are separate evidence. The explicit
[snapshot retention extension](SNAPSHOT_RETENTION.md) also reserves success-history
memory from the lesser of operation and data-file budgets, because expired
ancestry can grow while the retained root stays small.

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

## Manifest merging and the decoded-size bound

Appends use Iceberg's merge append (bins of up to 100 small manifests, 8 MiB
encoded target). Iceberg packs bins by encoded length while the verifier caps a
manifest's decoded Avro bytes at 8 MiB, so a single merged manifest once failed
at about 29,500 one-row files. The verifier now records every current manifest's
decoded size; each append lowers the merge target for its own Iceberg transaction
(set and removed inside it, never persisted) so that any merged bin stays within
7/8 of the decoded cap by the largest measured decoded/encoded ratio. Merging then
produces several bounded manifests (about 22,000 one-row files each for a
two-column schema) and the manifest list grows instead; the number of live files
is bounded by the configured budgets, not by one manifest. Every verifier limit
is unchanged.
