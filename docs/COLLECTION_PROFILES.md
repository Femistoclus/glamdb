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

Default budgets are 256 MiB catalog bytes, 4,096 operations/snapshots and
256 MiB recovery SQLite workspace. These can be configured through the
[retained-history profile](SCALABLE_HISTORY.md); raising the collection count
alone leaves them unchanged. More collections
share those existing global budgets. The workspace deduplicates globally bounded
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
1,000-item request. Complete CLI output is independently bounded to 16 MiB; an oversized result
fails explicitly. Collection schemas support up to 128 fields.
The SDK exposes `Page.next_cursor`; the named `collections` CLI completes
continuations before output and rejects a changed high-water mark. The browser
workbench already follows collection pages. History/file HTTP inventories use
their separate JSON cache quotas.
