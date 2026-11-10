# GlamDB Python network SDK

This package talks only to the public `/v0` HTTP endpoints. It does not import
the C++ core, a Python storage writer, the development mock, or verification
tools. The repository currently supplies a disposable mock HTTP adapter; passing
SDK tests against it does not establish storage durability.

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

with Client("http://127.0.0.1:8765", token="your-development-token") as client:
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
caller operation and collection IDs when applicable. The SDK does not add
credentials to error messages, and redirects are disabled.

Keep the original operation UUID, schema and input payload across a disconnect
or restart. `AppendRequest` owns a frozen JSON body and canonical v1 payload
hash. Retrying `client.append(request)` sends exactly that body and ID once;
the client performs no implicit write retry. `request.to_dict()` returns a
detached copy. After restarting, prepare the same payload with the same IDs.

`client.find_append_outcome(collection_id, operation_id)` returns the original
terminal outcome, an `OperationInProgress`, `commit_unknown`, or `None` for an
authoritative `NOT_FOUND`. A recovery-pending HTTP error remains an error;
it is not absence. Do not change the ID just because a response was lost or an
outcome is unknown. A terminal failed outcome may request a *new, explicit*
operation ID for a corrected attempt.

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
timezone and up to six fractional digits, or exact integer microseconds.
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
Breaking a loop whose iterator is retained does not immediately release it.
Exiting the context or closing the client closes every reader and HTTP
response, including on a consumer exception. Already returned Arrow batches
remain usable after those owners close.

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
The current suite has 172 passing tests.
Transport doubles represent the unimplemented server boundary; codec, HTTP
client logic, canonical hash and PyArrow IPC encoding/decoding are real. The
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
