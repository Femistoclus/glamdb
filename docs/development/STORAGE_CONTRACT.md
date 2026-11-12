# Compute-facing native storage contract

The installed `storage/contract.h` defines native semantic contracts **1.0**, **1.1**, **1.2** and **1.3**.
`Engine::NegotiateStorageContract(request)` describes supported operations for an
intended collection mode and returns the engine's effective limits. It is a
convenience over the pure `glamdb::NegotiateStorageContract(request,
capabilities, limits)` function. The pure function performs no I/O, admission,
collection lookup or publication; adapters can negotiate against a previously
captured capability/limit description. The supplied description must come from
that engine, and its limits must already be validated by store creation/open.
The request does not change an engine or downgrade the behavior of its methods.

Compute owns SQL parsing, logical/physical plans, joins, aggregation, operators
and task scheduling. Storage owns committed state, typed schema/field identity,
physical access, fixed snapshot selection, bounded resource admission,
publication, authoritative outcomes and recovery. Compute must call the public
Engine operations below. Private StorageBackend, Iceberg implementation types,
SQLite handles, core headers and client-side file writers cannot cross this
boundary. Arrow batches are the existing public data interchange.

## Negotiation and compatibility

```cpp
#include <storage/engine.h>

// engine is an already opened glamdb::Engine.
glamdb::TStorageContractRequest request;
request.MinVersion = {1, 0};
request.MaxVersion = {1, 0};
request.Mode = glamdb::CollectionMode::Append;
request.RequiredOperations = {
    glamdb::EStorageOperation::Scan,
    glamdb::EStorageOperation::Append,
    glamdb::EStorageOperation::FindOutcome,
};
request.RequiredFeatures = {
    glamdb::EStorageFeature::FixedSnapshotScan,
    glamdb::EStorageFeature::TypedNulls,
    glamdb::EStorageFeature::PreserveAppendDuplicates,
};
auto contract = engine.NegotiateStorageContract(request);
// Inspect the Result before using contract->EffectiveLimits or starting work.
```

Versions are `(Major, Minor)` pairs ordered lexicographically. The request range
is inclusive and negotiation selects the highest implemented version in it.
Versions 1.0 through 1.3 are implemented; a range spanning several majors selects
1.3. The default request range remains [1.0, 1.0], preserving existing negotiation
and feature results. New view consumers must include 1.1 and require
`IndependentReadView`; a 1.0-only request with that requirement fails.
A new major denotes incompatible semantics; a minor may add optional capabilities
without changing existing behavior. Callers must require every operation/feature
they depend on and inspect the returned version. Unknown optional discoveries
must be ignored; unknown **required** enum values fail closed. Repeated
requirements are harmless. Operation/feature numeric values are stable within a
major, but their C++ representation is not a serialized protocol.

| Condition | Result error (`RetryHint::FixInput`) |
| --- | --- |
| Reversed range or unknown collection mode | `INVALID_ARGUMENT` |
| No implemented version inside the range | `UNSUPPORTED_CONTRACT_VERSION` |
| Any required operation missing for this mode/backend, including unknown values | `UNSUPPORTED_OPERATION` |
| Any required semantic feature missing, including unknown values | `UNSUPPORTED_FEATURE` |

Validation order is range/mode, version, required operations, then required
features. Unsupported requirements never silently degrade. Optional capabilities
are queried with `HasOperation` and `HasFeature`. An empty requirement list is
valid, including for a storage-disabled engine; it discovers what is available.
Negotiate separately for append and KV. `Mode` is an intended mode, not evidence
that a collection exists: resolve `CollectionInfo.mode` before operating on an
existing collection. Passing a wrong mode to an existing write method continues
to return `COLLECTION_MODE_MISMATCH`; incompatible Arrow types return
`INVALID_ARGUMENT`. Negotiation adds no alternate dispatcher or execution path.

This is a C++ source/semantic contract, **not a stable binary ABI or remote wire
protocol**. Consumers rebuild with compatible C++23/Arrow/toolchain dependencies.
The CMake package version, store/catalog formats, canonical hash versions,
receipt versions and HTTP protocol versions remain independent. Negotiation
changes none of their bytes and does not rewrite persisted data. Existing Engine
callers remain source-compatible and may continue calling the facade directly.

## Operation mapping

The following describes the production local backend. Backend capability flags
still gate the corresponding operations; discovery is independent of current
admission and resource occupancy. Custom backends receive only operations their
existing flags support; local bulk, KV mutation, migration and retention
operations are conservatively withheld from custom backends.

| Contract operation | Existing public Engine entry point(s) | Append | KV |
| --- | --- | --- | --- |
| `CollectionMetadata` | `GetCollection`, `ResolveCollection`, `ListCollections`, `ListCollectionsPage`, `Schema`, `SchemaForAppend` | Yes | Yes |
| `CreateCollection` | `CreateCollection` with the intended mode | Yes | Yes |
| `UpdateWriteSettings` | `UpdateWriteSettings` | Yes | Yes |
| `SnapshotHistory` | `ListSnapshots`, `ListSnapshotsPage` | Yes | Yes |
| `Statistics` | `GetStatistics` | Yes | Yes |
| `Files` | `ListFiles`, `ListFilesPage` | Yes | Yes |
| `FileDetails` | `GetFileDetails` | Yes | Yes |
| `ExplainScan` | `ExplainScan` | Yes | Yes |
| `Scan` | `Scan`, then `ScanCursor::Info`, `Next`, `Cancel`, `Profile` | Yes | Yes |
| `AcquireReadView` / `ReadViewInfo` | `AcquireReadView` / `GetReadViewInfo` (1.1, native only) | Yes | Yes |
| `ScanReadView` | `ScanReadView` by captured entry index (1.1, native only) | Yes | Yes |
| `RenewReadView` / `ReleaseReadView` / `CancelReadView` | Corresponding Engine methods (1.1, native only) | Yes | Yes |
| `PlanScan` / `NextScanSplits` | Bounded opaque split planning (1.2, native only) | Yes | Yes |
| `NegotiateScan` | Pure typed pushdown negotiation (1.3, native scan support) | Yes | Yes |
| `ReadSplit` / `CancelScanPlan` | Repeatable split cursors / plan cancellation (1.2, native only) | Yes | Yes |
| `Append` | `AppendDurable` | Yes | No |
| `BulkAppend` | `BulkAppendDurable` | Yes | No |
| `Upsert` / `Delete` | `UpsertDurable` / `DeleteDurable` | No | Yes |
| `BulkUpsert` | `BulkUpsertDurable` | No | Yes |
| `Lookup` | `Lookup` | No | Yes |
| `FindOutcome` | `FindAppendOutcome` for append, bulk and KV mutation IDs | Yes | Yes |
| `Compact` | `CompactDurable` (separate explicit maintenance) | No | Yes |
| `ExpireSnapshots` | `GetSnapshotRetention`, `ExpireSnapshots` | Yes | No |
| `MigrateAppendToKV` | `MigrateAppendToKV` from this source mode into a new KV target | Yes | No |
| `Reconcile` | `Reconcile` (store-wide recovery) | Yes | Yes |
| `StorageFootprint` | `GetStorageFootprint` (store-wide physical inventory) | Yes | Yes |
| `Shutdown` | `Shutdown` (store lifecycle) | Yes | Yes |

Catalog discovery, outcome lookup, reconciliation and shutdown are coordinator
operations advertised even without a storage producer. This does not fabricate
collections or outcomes: unknown collections still fail, and an unknown operation
returns an absent optional only after an authoritative successful lookup.
Storage-disabled builds omit physical reads/writes and creation; existing calls
continue returning `DEPENDENCY_UNAVAILABLE` where applicable.
`GetStoreInfo`, `GetLimits`, `GetResourceOwner`, `GetCapabilities` and
`GetHealth` describe the engine itself and require no negotiated collection
operation.

Implemented semantic features are `TypedNulls`, `FixedSnapshotScan`,
`PreserveAppendDuplicates` (append only), `AuthoritativeOutcomes`,
`SnapshotPreconditions` (when writes are supported), and `DurableNoOp` (native
KV mutation support). KV upsert replaces full rows and deterministically applies
last-wins duplicate input; keyed delete is safe for absent/repeated keys. This
is not SQL MERGE or arbitrary update support. See [KV mode](KV_MODE.md).

## Schema and field identity

Field identity is scoped to a collection. Its full `SchemaSpec.fields`, returned
in `CollectionInfo.schema` or by `SchemaForAppend`, assigns stable `FieldID`
values **1..N in original field order**: zero-based ordinal `i` maps to
`FieldID = i + 1`. Fixed schemas preserve this mapping across snapshots; the
same numeric ID in another collection does not identify the same field.

Correlate `TFileFieldMetrics.FieldID` from file metrics and
`TColumnChunkMetadata.FieldID` from row-group columns with
`collection.schema.fields[FieldID - 1]`. Match the resulting original field name
to projected Arrow field names. The current public Arrow schema carries names,
types and nullability but **no field-ID metadata**. A projection can omit or
reorder fields, so its output ordinal must not be used to reconstruct IDs.
For original fields `[key, n]`, projection `[n, key]` returns `n` first, but its
stable ID remains 2 and `key` remains 1. Consumers must retain the full original
collection name-to-ID mapping and collection identity when correlating metrics
with projected batches. See [metadata](METADATA.md).

## Views, batches and lifetime

`Scan` resolves Latest exactly once to a retained snapshot ID or
`InitialEmpty`; historical and initial empty selectors are explicit. The cursor
plans and executes that fixed view, returns Arrow batches, retains its engine
and store lease, and supports cooperative `Cancel`. EOF is a null batch;
errors are distinct. Duplicate append rows and typed NULLs are preserved.
Projection, predicates, zero-column cardinality, limit and scan profiling keep
[the existing scanner contract](SCANNER.md). `ExplainScan` is an explanation,
not an independently executable or transferable plan.

There is no independently shareable read-view handle, split planner, split
executor or serializable descriptor in 1.0. Native cursors are process objects;
callers release them and retained Arrow results to release resources. They are
not authorization tokens. Trusted adapters own authentication, authorization and
cancellation and must not share cursors across principals. Existing metadata
page tokens are bounded, engine-instance-specific, parameter-bound and bound to
the supplied `AuthorizationContext`; they expire after five minutes and fail
across restart. See [metadata](METADATA.md) and [shared resources](SHARED_RESOURCES.md).
Negotiation itself introduces no handles or resource owners.

Contract 1.1 adds native `IndependentReadView`: `AcquireReadView` atomically
captures an ordered vector of collection identities/schemas and resolved snapshots
under one owning engine's state mutex. Repeated/delayed `ScanReadView` calls reuse
those verified states and root leases across publication, retention and compaction.
`InitialEmpty`, historical, no-data and zero-limit views retain real identities.
The vector is a coherent per-store cut, not atomic multi-table writes or a
cross-store timestamp. See [read views](READ_VIEWS.md) for public signatures,
trusted authorization scopes, fixed quotas, bounded expiry/renewal, release and
cancellation, independent Arrow buffer lifetime and distinct restart errors.
Contract 1.2 adds native split execution; remote views and remote split execution
remain unimplemented.

## Writes, publication and outcomes

All publication uses the existing Engine/coordinator operations. Ordinary writes
accept typed Arrow batches; bulk sources synchronously rewind and replay under
existing bounds. A bulk source is not a staged-input session. Preconditions
`Any`, `NoSnapshot` and `Equals` retain their exact existing validation and
binding behavior. A validated write binds the original operation ID, canonical
payload hash and exact prepared base. Retrying preserves that identity, payload
and precondition; a confirmed original outcome wins over a stale precondition.

`Committed`, `DefinitelyNotCommitted` and `CommitUnknown` retain their distinct
meanings, phases, recovery contexts and original receipt bytes. Never map
`CommitUnknown` to failure or resubmit under a new identity. Use authoritative
`FindAppendOutcome`; blocked recovery/publication is an error, not absence.
Creation resolves through collection identity/exact name and `CreateOutcome`;
migration preserves its separate creation and mutation outcomes. Retention is
explicit metadata maintenance with its existing reconciliation/retry contract,
not an append receipt. See [the coordinator](COORDINATOR.md),
[bulk ingestion](BULK_INGESTION.md) and [snapshot retention](SNAPSHOT_RETENTION.md).

HTTP/SDK/CLI and future compute/transport adapters call these same operations.
Negotiation neither grants authorization nor reserves a writer, and adapters
must not introduce their own catalog commit or publication path. Existing
transport capability formats are unchanged by this native contract.

## Limits, readiness and unsupported features

`EffectiveLimits.Engine` copies the actual `Engine::GetLimits()` selection,
including configured collection, operation, catalog, owned-input, writer wait,
deadline and retained-history budgets. The additional fields expose current
validation ceilings: 128 schema fields, 10,000 ordinary append/upsert input rows,
16 MiB canonical decoded append/upsert input bytes, 65,536 scan batch rows,
25,600,000 bulk rows, 4 GiB bulk decoded bytes, 4,096 lookup keys and 10,000 delete
keys. `AppendRows` and `AppendDecodedBytes` constrain ordinary `AppendDurable`
and `UpsertDurable` input before KV duplicate resolution. They remain applicable
in KV mode when `Upsert` is advertised, even though `Append` is not advertised.
Other operation ceilings apply when their corresponding operation is advertised.
`EffectiveLimits.ReadViews` is present only with the 1.1 native read-view feature:
32 active views per engine, 8 per authorization scope, 64 entries per view,
256 total entries, 256 scope bytes and five minutes maximum absolute lifetime.
Shared reservations can refuse admission before those ceilings.
Bulk sources still obey their per-batch 10,000-row/16 MiB bounds, per-file limits
and replay/lifetime rules in `BulkAppendLimits` and the bulk guide. These limits
are not wire-body limits and do not promise that every combination fits shared
memory/disk admission. Schemas, row/string sizes, options and retained-history
budgets impose additional documented validation.

Capability discovery remains available after shutdown and during recovery;
`GetHealth()` reports current read/write admission. Supported operations may
still fail due to `RECOVERY_REQUIRED`, `OPERATION_IN_PROGRESS`,
`RESOURCE_EXHAUSTED`, cancellation, corruption or I/O errors. Discovery is not a
reservation, a readiness result or a power-loss durability qualification.

Version 1.0 advertises neither `IndependentReadView` nor `ScanSplits`. Version
1.1 adds `IndependentReadView` only for the local scanner; version 1.2 also adds
`ScanSplits` and `EffectiveLimits.ScanSplits`. See [scan splits](SCAN_SPLITS.md)
for paging, whole-file target size, original-view lifetime, quotas and explicit
rejection of global LIMIT. Version 1.3 adds `ScanNegotiation` and `NegotiateScan`
for native scans; versions 1.0–1.2 never advertise them. See
[scan negotiation](SCAN_NEGOTIATION.md) for exact/necessary-condition/residual
classification, trusted translation assertions, stable field mapping, compute
projection inputs and legal limits. The pure scan planner uses bounded shared
metadata admission; this is separate from pure capability discovery above.

`StagedWrites`, `StableCAbi`, `ArrowFlight`, `RemoteDescriptors`, `Overwrite`,
`Merge`, `SqlMutations`, `SchemaEvolution` and `CrossCollectionTransactions`
remain reserved and unimplemented. Requiring one fails explicitly in all four
versions. Native scan cursors and bulk sources do not imply these protocols.

## Executable evidence

`StorageContract.*` unit tests cover compatible/disjoint/reversed ranges,
required/unknown features and operations, invalid modes, append/KV capability
differences, backend availability, conservative custom-backend support, effective
limits and readiness-independent
Engine discovery after shutdown. `glamdb.installed_consumer` builds copied source
against the installed public headers/library, negotiates before real operations,
and writes/reopens in separate processes. Its literal multiset checks preserve
initial empty and held historical views, typed NULLs, duplicates, original
receipts, exact retries and KV no-op/lookup behavior. It also checks mode/type
refusals before binding and unsupported negotiation requirements. Local executed
results are recorded in the implementation report; no physical durability
qualification or new transport/ABI qualification is claimed.
