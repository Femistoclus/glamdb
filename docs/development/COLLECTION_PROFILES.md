# Collection profiles

`Limits::max_collections` accepts 1–64 and defaults to eight. Two isolated sets
of the eight TPC-H base tables therefore fit in a 16-slot database; 64 slots
support eight such sets or explicitly requested experiment tables. Benchmark
manifests and journals remain external. A selected count is a finite admission
limit, subject to the process resource owner and the separate catalog/storage
limits; it is not a promise that every request fitting the count has capacity.

## Configure and reopen

Managed HTTP creation accepts partial limits:

```json
{"name":"comparison","limits":{"max_collections":64}}
```

Use `Client.create_database("comparison", limits={"max_collections": 64})` or
`glamdb-cli create-store --url URL --name comparison --max-collections 64`.
The registry persists the complete normalized limits and open intent across
close/restart/open. Change limits only while closed, using
`POST /databases/ID/open` with a partial `limits` object. Omission preserves the
stored values. Capabilities report effective `limits.max_collections` and
`max_supported_collections=64`, including closed/error entries.

Embedded callers select the limit in `CreateStoreRequest`/`OpenStoreRequest`.
`Engine::GetLimits()` exposes the immutable effective configuration. Offline
`glamdb-admin` and legacy `glamdb --root ROOT --store-id UUID` accept
`--max-collections 64`; supply that profile on every reopen. Embedded stores
do not persist caller limits. Their catalog/store formats are unchanged;
default stores and old registry entries retain eight. A store configured above
eight requires a binary that supports that selection; the older eight-slot
implementation cannot reopen an enlarged profile. Opening under a limit
below existing usage fails with `CONFIG_TOO_SMALL`; restore a sufficient limit
and reopen. No collection, schema, history or outcome is removed or hidden.

## Resource admission

The supported count is validated before arithmetic and before creating a store
root. For `E = max(0, max_collections - 8)`, engine-lifetime reservations are:

| Class | Reservation | At 64 slots |
| --- | --- | --- |
| Catalog/coordinator metadata | 64 MiB + E × 1 MiB | 120 MiB |
| Protected recovery metadata/work | 768 MiB + E × 1 MiB | 824 MiB |
| Additional ordinary control descriptors | E × 4 KiB | 224 KiB |

The added envelopes cover bounded SQLite query/schema decoding, collection and
confirmed-view copies, backend map/control entries and recovery descriptors.
They reserve the selected capacity even for an empty store. Cached Iceberg
roots independently reserve 32 times serialized root bytes plus 4 MiB per root;
verified scan indexes and retained file descriptors charge separately under
the [shared resource policy](SHARED_RESOURCES.md).
Inventories and returned pages retain capacity-sized metadata leases, including
per-column write settings. Failed collection-profile admission releases partial
reservations before filesystem changes. The native immutable metadata/footer
cache separately reserves 32 MiB per store after root creation; refusal of that
reservation can leave an unpublished root. Shared-resource pressure refuses
admission rather than silently reducing the selected count.

The catalog byte cap, operation/history caps, aggregate file/root caps and the
recovery SQLite workspace cap are store-wide and unchanged by the collection
count (see [the defaults](SCALABLE_HISTORY.md)). More collections share those
global budgets. The workspace deduplicates globally bounded
files/history and processes collections serially, so collection count does not
multiply its disk cap. Catalog/journal/workspace device reservations remain in
force. Catalog enumeration checks the count before materializing schema blobs
and bounds its query. Creation checks count before backend file production and
again inside catalog publication; retries of existing definitions still resolve
at capacity. These are conservative reservation envelopes, not RSS guarantees.

## Complete inventory

`GET /collections` uses `Engine::ListCollectionsPage`: `page_size` 1–1,000,
default 100, and optional opaque `cursor`. Repeat the same database/page size
until `next_cursor` is null. Each page includes a decimal-string
`creation_high_water_mark`. Later collection creation does not alter an existing
inventory. Replays return the same page while retained. Tokens are engine- and
authorization-context-bound; malformed, changed, foreign, expired and
pre-restart tokens fail with HTTP 400 `INVALID_CURSOR`.

The core admits 32 retained/loading inventories and 16 MiB of measured
DTO/token capacities per engine. Listings expire after five minutes without
renewal. New listings reclaim the oldest completed inventories under count/byte
pressure and preserve unread continuations. Returned pages retain their own
leases after reclamation or engine close. Single-page results retain no session.

All 64 accepted collections fit the default HTTP/SDK page and the CLI's
1,000-item request. Even maximum 64-field schemas and per-column settings fit
the 16 MiB complete CLI result bound under the existing 128-byte name limits.
The SDK exposes `Page.next_cursor`; the named `collections` CLI completes
continuations before output and rejects a changed high-water mark. The browser
workbench already follows collection pages. History/file HTTP inventories use
their separate JSON cache quotas.

## Validation and measurement

Run native validation with the installed SDK/PyArrow/schema dependencies:

```sh
cmake --build --preset local --parallel 8
GLAMDB_REQUIRE_NATIVE_CLIENTS=1 GLAMDB_VALIDATE_SCHEMAS=1 \
  ctest --preset local --output-on-failure \
  -R 'CollectionProfiles|TMetadataPagesTest|MetadataResources|glamdb\.(database_limits|cli_data|cli_network)$'
```

`glamdb.database_limits` emits `COLLECTION_PROFILES` JSON with catalog bytes,
process RSS/high-water RSS, shared current/peak reservations, collection creation
time, concurrent append/inventory time and fresh-process recovery time for
8/16/64 slots. It uses all eight base-table shapes with synthetic rows, precise
int64 keys and duplicate rows. Decimal/date columns use existing float64/ISO
string representations. This is collection capacity evidence, not a TPC-H
query or scale-factor benchmark. It verifies exact schemas, isolated identities,
all rows and original append outcomes/retries after process restart, SDK pages,
full CLI inventory, capacity refusal and too-small reopen recovery.

The separate continuation regression checks fixed membership across later
creation, parameter binding, replay, repeated completed-list reclamation,
preservation of unread pages and close/open invalidation. Unit tests cover
pre-filesystem memory refusal/release, cached/returned settings accounting and
count/byte pressure. Offline/legacy CLI tests reopen a default-format store
with 16 collections, exercise the ninth collection and compare all identities.

Executed on 2026-11-12, Linux x86-64 (5.4.210), Clang/libc++ 18, Debug engine
with the pinned Release Arrow/Iceberg dependencies, based on upstream
`85f54cf`. The figures are single-run observations on a shared development host.
The same native service handled each profile; every reopen used a newly started
process. Four client workers interleaved appends and full inventory reads. Each
table contains two identical synthetic rows; this measures collection/descriptor
scaling.

| Collections | Create all, ms | Concurrent append/inventory, ms | Fresh-process recovery, ms | Catalog bytes | RSS / peak RSS, MiB |
| --- | --- | --- | --- | --- | --- |
| 8 | 400.4 | 866.7 | 317.5 | 61,440 | 47.38 / 47.38 |
| 16 | 984.1 | 2,445.5 | 608.9 | 86,016 | 49.79 / 49.79 |
| 64 | 6,770.6 | 13,003.1 | 1,333.0 | 167,936 | 54.95 / 54.95 |

Shared reservations are deliberately larger than physical RSS. These process
totals include the registry (16 MiB protected recovery), the native cache
(32 MiB metadata) and transport/cache leases; the engine-only envelopes are
listed above:

| Collections | Metadata current / peak, MiB | Data peak, MiB | Recovery held, MiB | Additional control held, KiB |
| --- | --- | --- | --- | --- |
| 8 | 144.54 / 1712.66 | 512.42 | 784 | 0 |
| 16 | 185.07 / 1785.31 | 512.45 | 792 | 32 |
| 64 | 428.28 / 2221.24 | 512.59 | 840 | 224 |

Native capacity regressions passed, including exact post-restart rows,
receipts/retries, complete SDK/CLI inventories, smaller-profile refusal and the
independent-review regressions. Maximum escaped schemas/settings, including
dictionary and encoding fields, fit the complete 16 MiB client result bound in
the serializer test.

Broader validation passed: the full normal CTest suite (1,004 passed across the
full run and isolated reruns, eleven intentional skips, two disabled cases),
repository pytest (1,258 passed, 31
skipped, 12 subtests passed), Node (188 passed), and the shared SDK contract
against the final native server (24 passed). LLVM 18 checked
formatting of all 276 sources, static analysis of all 16 changed C++ translation
units, and rejection of intentional quality violations. Sanitizer profiles were
not run locally.

Three unchanged upstream checks failed on timing during the parallel run:
Arrow scratch duration, child-process startup and a serialization timing ratio.
All passed in isolation without changing their assertions or timeouts; the
scratch test's final CTest run took 26.3 seconds under its 30-second limit.

Independent review found completed-inventory exhaustion and missing per-column
settings accounting. Both were reproduced with failing regressions, corrected
and independently rechecked; no concrete findings remained.

Fresh-process recovery does not flush OS caches. These small fixtures do not
qualify maximum histories, sustained ingestion, universal RSS bounds or power
loss durability. Existing durability qualification gates remain unchanged.
