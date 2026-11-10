# Provisional workbench HTTP contract

This is extension-N development transport over `glamdb_mock`, version `/v0`.
It is not the public C++ API or a qualified production service. The mock owns
no store/warehouse/catalog path; all data is process-local and disposable.
Every production write must eventually route through the C++ authority.

All `/v0` requests need `Authorization: Bearer TOKEN`. JSON writes need
`Content-Type: application/json`. Errors use
`{"error":{"code":"...","message":"...","retry_hint":"..."}}`.
Health and read-only workbench config/static assets are public. API responses
identify `backend=mock`, `mock=true` and `durability_validation=designed` where
applicable; terminal mock receipts carry `durable=false`.

## Routes

- `GET /healthz`: explicit mock health, never production storage readiness.
- `GET /workbench/config`: validated, uncached runtime theme JSON.
- `GET /v0/capabilities`: append mode, unavailable extensions, limits and
  development-fault flag.
- `GET /v0/collections`: collection page.
- `POST /v0/collections`: caller `collection_id` UUID, exact `name`,
  `schema:{fields:[{name,type,nullable}]}`, optional `mode:"append"`. Returns
  collection info/schema with assigned stable field IDs. Same UUID/name/schema
  returns the same identity; a conflicting UUID or name returns 409.
- `GET /v0/collections:resolve?name=EXACT_NAME`: exact-name lookup. A lost create
  is resolved only after original UUID and exact schema verification.
- `GET /v0/collections/{id}` and `/schema`: collection identity/schema.
- `GET /v0/collections/{id}/stats?snapshot_id=SELECTOR`: actual mock row count,
  retained snapshot/file counts; unknown physical byte metrics remain null.
- `GET /v0/collections/{id}/snapshots`: retained snapshot page.
- `GET /v0/collections/{id}/files?snapshot_id=SELECTOR`: file page belonging to
  the fixed view. `/files/{file_id}?snapshot_id=SELECTOR` adds synthetic details.
- `POST /v0/collections/{id}/append`: caller `operation_id` UUID, nonempty
  `rows`, optional `expected_snapshot`.
- `GET /v0/collections/{id}/operations/{operation_id}`: original outcome or
  unresolved state; 404 only for authoritative absence. Recovery barriers are
  errors, not absence.
- `POST /v0/collections/{id}/scan`: `projection`, `predicate`, optional
  `snapshot_id`/`limit`. Default JSON response has `schema`, `snapshot_id`,
  `rows`, `identity`, and `profile`. `Accept: application/vnd.apache.arrow.stream`
  selects actual Arrow IPC stream bytes instead.
- `POST /v0/collections/{id}/explain`: same query shape, conservative mock plan;
  estimates stay separate from actual executed counters.

Collection info contains `collection_id`, `name`, `table_uuid`, fixed `schema`,
`mode`, `creation_sequence`, `current_snapshot_id` and mock identity fields.
Fields use `field_id`, `name`, `type`, `nullable`; types are `string`, `int64`,
`float64`, `bool`, `timestamp_us_utc`. Initial field IDs are 1..N in declaration
order. The required string `key` is not unique. Unsupported types/modes fail.

## Exact scalar, view, and predicate rules

Int64 values and snapshot IDs travel as decimal strings. UTC timestamps have
exactly six fractional digits and `Z` on output; input supports an explicit
offset with at most six fractional digits. UTC range is years 0001–9999.
Booleans are not integers; NaN/Inf and implicit string-to-float casts are
rejected. Append canonical hashing normalizes negative zero, preserves UTF-8
bytes, input row order, duplicate rows and nulls. There is no Unicode
normalization or key sorting. See the literal golden fixtures for bytes/hashes.

Omitted append precondition is Any; JSON null is NoSnapshot; a decimal ID is an
exact precondition. Identical original retries are resolved before evaluating
the current precondition. Omitted read selector resolves latest once. JSON null
or `"initial"` selects the initial empty schema-bearing view; GET uses
`snapshot_id=initial`. Historical snapshot IDs remain immutable in this process.

Predicates use `op` with `field,value` for eq/ne/lt/le/gt/ge;
`field,values` for in; `field` for is_null/is_not_null; nonempty `args` for
and/or; `arg` for not. Only SQL TRUE survives. Filter-only columns are read
even when omitted from projection. An empty projection preserves cardinality;
`limit=0` preserves output schema and fixed-view identity.

## Outcomes and resources

Append returns `outcome:"committed"` with `result`,
`outcome:"definitely_not_committed"` with `error`, or
`outcome:"commit_unknown"` with `operation_id,recovery_required`.
The first two are terminal. A committed mock result includes original IDs,
snapshot ID, exact creation time, `receipt_version:1`, `hash_version:1`,
canonical `payload_hash`, row counts and `durable:false`. Physical byte/file
publication metrics are omitted; a production receipt must meet full C-RESULT.
Unknown HTTP status/transport failure never proves rollback. Keep original IDs,
schema and payload; retry once explicitly or query the operation. No early 202,
payload journal, async admission or automatic retry exists here.

Listings return `{items,next_cursor}`. Use `page_size` 1..1000 (default100) and
the opaque cursor with exactly the original endpoint, selector, page size and
parameters. Cursors bind a fixed view/high-water mark and expire after 15min
without TTL extension on subsequent pages. Restart invalidates cursors.

IPC has a schema/EOS even when empty, plus `Content-Length`,
`X-Content-SHA256`, `X-GlamDB-Snapshot-Id` (`initial` for the initial view).
The SDK releases HTTP/Arrow resources on context exit, early close and error;
only full verified consumption claims integrity. Browser downloads are bounded
and checksum checked before download. Returned buffers outlive client handles.

Mock defaults: request16MiB, response32MiB, 100k total rows, eight collections,
4096 snapshots/operations per collection, conservative retained-state estimate
64MiB. Append also enforces 10k rows/16MiB decoded values, row/string/key limits.
These are development bounds, not qualified engine memory/disk profiles.

Development-only faults and resolution routes require explicit enablement;
see [the mock README](../../apps/mock_server/README.md). None supply evidence of
native I/O, persistence, pruning speedups or physical power-loss correctness.
