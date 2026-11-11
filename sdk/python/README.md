# GlamDB Python network SDK

This package talks only to the public HTTP endpoints. It does not import
the C++ core, a Python storage writer, the development mock, or verification
tools. The native server manages databases; a disposable mock adapter is also
available. Passing SDK tests does not establish storage durability.

Install from this directory with `python -m pip install .`. Dependencies are
HTTPX 0.28 and PyArrow 25; Python 3.10 or later is required. For local development,
`python -m pip install -e '.[test]'` installs the tests as well.

```python
from uuid import UUID
from glamdb_sdk import Client, Field, Schema, col

# Generate and persist these UUIDs in your application BEFORE sending a write.
collection_id = UUID("00112233-4455-6677-8899-aabbccddeeff")
operation_id = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
schema = Schema([
    Field("key", "string", False),
    Field("n", "int64", True),
    Field("at", "timestamp_us_utc", True),
])

with Client("http://127.0.0.1:8080", token="your-token") as admin:
    database = admin.create_database("analytics")  # same name reuses the same ID
    print(admin.list_databases().items)

with Client("http://127.0.0.1:8080", token="your-token",
            database_id=database["database_id"]) as client:
    created = client.create_collection("example", schema, collection_id=collection_id)
    if created.outcome != "committed":
        raise RuntimeError(created.raw)

    request = client.prepare_append(collection_id, [
        {"key": "same-key", "n": 9007199254740993, "at": "0001-01-01T00:00:00Z"},
        {"key": "same-key", "n": None, "at": None},
    ], operation_id=operation_id, schema=schema)
    outcome = client.append(request, deadline=60)
    print(outcome.outcome, request.operation_id, request.payload_hash)

    result = client.scan_json(collection_id, predicate=col("n", "int64").ge(9007199254740993))
    print(result.rows)  # Python int and timezone-aware UTC datetime values

    with client.scan_batches(collection_id) as stream:
        for batch in stream:
            print(batch.to_pylist())
    print(stream.snapshot_id, stream.integrity, stream.checksum_verified)
```

`get_database`, `open_database`, `close_database` and `reconcile_database` accept
a database UUID. `database_id=` scopes collection requests and capabilities while
management calls remain at the server root. Keep a separate client per database:
schema caches and open streams belong to that client's selection. Omitting the
argument preserves legacy single-store/mock behavior. Database creation never
accepts a local filesystem path; after a lost response repeat the same name/UUID.

Append preserves duplicates, including duplicate `key` values. There is no
upsert, delete, compaction, or asynchronous job API here. Inspect
`client.capabilities()` before relying on a service feature.

Use `examples/quickstart.py COLLECTION_UUID OPERATION_UUID` with
`GLAMDB_URL` and `GLAMDB_TOKEN` set for a runnable version of this flow. The
example does not create new UUIDs or retry a write automatically.

## Write outcomes and recovery

`append` returns `AppendOutcome` with `outcome` equal to `committed`,
`definitely_not_committed`, or `commit_unknown`. Only the first two are terminal.
`result`, `phase`, `error` and the full `raw` response remain available. An
`ErrorDetail` retains `code`, `message`, `retry_hint` and every additional field
in `error.raw`; recovery context remains in `outcome.raw`.

Unknown append/create responses must include complete, consistent recovery
contexts, including explicitly nullable candidate fields. Missing fields,
conflicting collection/operation identities, or mismatched original payload/schema
hashes raise `ProtocolError` with the original caller identity and raw response.
Blocked lookup cannot claim publication or candidate metadata/snapshots. Additional
response/recovery fields remain available in `raw`; validation does not discard them.
The [shared conformance fixtures](../../fixtures/http/README.md) exercise these
rules alongside native and browser parsing.

A committed append receipt must include a valid SHA256 digest with
`hash_version=1`, and its digest must match the frozen request. Outcome lookup
checks digest format and version without guessing the original payload. Both
paths validate shared receipt counters, version and creation timestamp, plus
physical metrics when the service supplies them. An invalid receipt raises
`ProtocolError` with the original IDs and full response; it never becomes a
fabricated failed write outcome.

`TransportError`, `DeadlineExceeded`, `ProtocolError`, `AuthenticationError`
and `HTTPError` are separate from those outcomes. A lost response or malformed
write response does not prove that publication failed. Exceptions retain the
caller operation and collection IDs when applicable. A server/gateway 5xx write
response without an authoritative outcome raises `OutcomeUnknownError` (an
`HTTPError` subclass); preserve the original IDs and reconcile or look up that
operation before deciding whether to retry. Database and route 404 errors never
mean an append operation is absent. The SDK does not add
credentials to error messages, and redirects are disabled.

Keep the original operation UUID, schema and input payload across a disconnect
or restart. `AppendRequest` owns a frozen JSON body and canonical v1 payload
hash. Retrying `client.append(request)` sends exactly that body and ID once;
the client performs no implicit write retry. `request.to_dict()` returns a
detached copy. After restarting, prepare the same payload with the same IDs.

`client.find_append_outcome(collection_id, operation_id)` returns the original
terminal outcome, an `OperationInProgress`, `commit_unknown`, or `None` for an
authoritative `OPERATION_NOT_FOUND`. A recovery-pending HTTP error remains an error;
it is not absence. Do not change the ID just because a response was lost or an
outcome is unknown. A terminal failed outcome may request a *new, explicit*
operation ID for a corrected attempt. A retry whose content conflicts with the
bound operation (`OPERATION_CONFLICT`/`IDEMPOTENCY_CONFLICT`) raises
`HTTPError`; it is not an outcome of that operation, which may already be
committed. A `definitely_not_committed` reply must use an HTTP error status and
carry no recovery state, otherwise it is a `ProtocolError`.

`create_collection` requires a caller collection UUID. Its returned
`CreateOutcome.info` contains collection metadata. For a lost create response,
`resolve_create(name, schema, collection_id=original_id)` performs exact-name
lookup **and checks the original UUID and schema**. A name match alone is
insufficient.

The omitted `expected_snapshot` means `Any`. Pass `None` for `NoSnapshot`, or an
int64 snapshot ID for an exact precondition. The same distinction applies to
read selectors: omitted selects latest once; explicit `snapshot_id=None` selects
the initial view. Snapshot IDs are opaque int64 values, not row counts or
sequences. The SDK preserves their decimal-string wire form in metadata.

## Scalars, predicates and metadata

`Field(name, type, nullable=False, field_id=None)` supports `string`, `bool`,
`int64`, `float64`, and `timestamp_us_utc`. `Schema` assigns missing field IDs in
order and preserves supplied stable IDs. Append accepts Python int64 integers
or canonical decimal strings, finite floats, bools, strings, and UTC timestamps.
An omitted nullable field becomes NULL; required, unknown or mismatched fields
are rejected. Empty scan projections preserve an empty output schema and every
matching row. The SDK enforces the v0 10,000-row/16-MiB decoded append ceiling
during normalization, plus row and string limits.

Timestamps accept timezone-aware `datetime`, RFC3339 text with a required
timezone and up to six fractional digits, or exact integer microseconds as an
`int`. Text is always parsed as RFC3339: digit-only text such as `"20260101"`
is rejected rather than read as microseconds.
Normalization uses integer arithmetic. UTC years 0001 through 9999 are allowed;
offset overflow, naive datetimes, leap seconds and excess precision are
rejected. Wire timestamps always have six fractional digits and `Z`; int64 wire
values are decimal strings. `scan_json` decodes these to Python int and aware
UTC datetime without passing through floats. NaN/infinity are rejected, bools
are not integers, and append negative zero becomes positive zero.

`col(name, logical_type)` provides `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in_`,
`is_null` and `is_not_null`. Combine `Predicate` values with `&`, `|`, `~`,
`and_(...)` and `or_(...)`. These serialize the public predicate tree; the
server performs SQL three-valued filtering. A typed builder prevents implicit
literal casts. A raw predicate JSON object may also be passed to scan/explain.

Read methods are `capabilities`, `list_collections`, `resolve_collection`,
`get_collection`, `get_schema`, `stats`, `snapshots`, `files`, `file`, `explain`,
`scan_json` and `scan_batches`. `Page.items`, `Page.next_cursor` and `Page.raw`
retain the server's page. Reuse the opaque cursor with exactly the same
parameters, selector and page size; do not resolve latest again between pages.

## Streaming and transport ownership

Always use `with client.scan_batches(...) as stream:` or call `stream.close()`.
The client tracks iterators weakly; dropping an iterator releases its cursor via
a nonthrowing finalizer. Explicit close remains preferable because garbage
collection timing varies. Breaking a loop whose iterator is retained does not
immediately release it.
Exiting the context or closing the client closes every reader and HTTP
response, including on a consumer exception. Already returned Arrow batches
remain usable after those owners close. Iteration failures remain sticky: another
`next()` raises the original failure, and integrity never becomes complete.

`scan_json`, `scan_batches` and `scan_pages` use `/scan/open` before fetching a
page, so retries and cleanup use the same known cursor even if the first page
response is lost. JSON and Arrow convenience scans support legacy single-response
adapters only when the open route explicitly returns `ROUTE_NOT_FOUND`; other
404 errors and transport failures never restart the scan. The development mock
remains a single-response adapter and does not emulate native cursor sessions.
Use `with client.scan_pages(collection_id) as pages:` to consume JSON pages
incrementally; early close cancels the continuation and keeps integrity
`not_complete`.

The response reader accepts bounded reads and keeps only a small network chunk
plus the current Arrow read; it never materializes the complete HTTP body.
`max_arrow_read_bytes` defaults to 64 MiB per Arrow read, and
`max_json_bytes` defaults to 16 MiB. These are client read bounds, not a claimed
global PyArrow allocator or process RSS limit. The service must enforce its own
batch, decompression and shared memory budgets.

`stream.integrity` starts as `not_complete`. Only full iteration through explicit
IPC EOS and HTTP EOF checks supplied byte counts and hashes. It becomes
`verified` when both `Content-Length` and `X-Content-SHA256` are present and
match. With missing integrity metadata it becomes `complete`;
`checksum_verified` is true only when a supplied SHA256 was checked. Early
close, truncation, trailing data, decode failure or digest mismatch never
claims completion. The SDK requires a fixed `X-GlamDB-Snapshot-Id` header
(`initial` for the initial view) and identity HTTP content encoding.

TLS certificate verification is enabled by default. `verify` accepts a bool or
an `ssl.SSLContext`; disable it only when deliberately configuring a local test.
`timeout=30` supplies HTTPX connect/read/write/pool timeouts. Each operation's
optional `deadline=seconds` adds a cooperative total budget checked before and
after network reads, with the initial HTTP timeout capped by the remaining
budget. It cannot interrupt an uninterruptible OS call. A deadline is never
translated into a fabricated failed server outcome.

## Executed validation

From the repository root:

```sh
.venv/bin/python -m pytest -q -c sdk/python/pyproject.toml sdk/python/tests
```

The SDK tests first failed for missing codec/client/stream implementations, then
passed incrementally: 42 codec tests; 81 codec/client/predicate/request tests;
32 Arrow tests; followed by targeted failing regressions for bounded input
copying, canonical output scalars, identity on schema-fetch timeout, and closing
all streams after a close error. Receipt-validation regressions first exposed
39 failures for invalid hashes, versions and receipt fields before the fix.
The current SDK unit suite has 263 tests, including cursor abandonment, sticky
iteration failures, first-page replay, distinct operation absence and uncertain
5xx write failures. Transport doubles isolate the HTTP boundary; codec, HTTP
client logic, canonical hash and PyArrow IPC encoding/decoding are real. Native
HTTP and browser suites separately exercise the C++ server. The
canonical append hash is independently checked against fixed single-type and
all-type golden vectors.
Repository-level actual HTTP integration is maintained separately from these
isolated SDK tests:

```sh
PYTHONPATH=sdk/python/src:apps/mock_server/src:tools/verification/src \
  .venv/bin/python -m pytest -q tests/integration/test_network_sdk.py
```

All 10 actual localhost integration cases passed against subprocess mock
servers, including JSON/Arrow type round trips, immutable outcomes and views,
explicit unknown/lost-response scenarios, a dropped real HTTP response, and the
quickstart script. Actual HTTPS uses a temporary test CA: an explicitly trusted
context succeeds; system trust and an unrelated CA reject the certificate while
retaining the caller operation ID. Those tests require localhost socket access
and an OpenSSL CLI. Temporary server processes and test keys are confined to
test directories and reaped on exit. This verifies the network boundary, not
C++ storage or power-loss durability.

Native scans use fixed-view pages. `scan_json()` collects every page into one
result (caller memory grows with the full result); `scan_batches()` drains
independently checksummed Arrow pages as batches and checks the schema and
snapshot remain unchanged. Both retain compatibility with single-response
servers. Their first request starts a new scan, so a lost initial response must
be reported rather than silently restarting against a newer snapshot.

For bounded JSON consumption with safe first-page retry, use the native
`scan_pages()` API. It opens a fixed view before consuming rows and retries a
lost cursor-page response once using the same token. No write is retried:

```python
with client.scan_pages(collection_id, projection=["key", "value"]) as pages:
    for page in pages:
        consume(page.rows)
```

Closing the iterator or client cancels an unfinished scan. `cancel_scan(id,
opaque_cursor)` is also available for explicitly managed tokens. Native tokens
are database-, collection-, and representation-scoped; only the most recently
consumed page can be replayed. Inactive sessions expire after 60 seconds and
restart/close/reconcile invalidates tokens. An expired token fails explicitly;
it never restarts a scan. A failed page is an error, never a successful end of
stream. The server permits eight concurrent live sessions, so close unused
iterators promptly. Completed replay caches may be evicted to admit new scans.

Managed `create_database(name, limits={"max_collections": 2})` accepts partial
engine limits. `open_database(database_id, limits={"max_collections": 3})` changes
them for a closed database. Omit `limits` to preserve the stored configuration;
responses include all normalized limits. See the [native server contract](../../docs/development/HTTP_SERVER.md)
for ranges, retry semantics and persistence.
