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

Append collections preserve duplicates, including duplicate `key` values.
Create an immutable KV collection with `mode="kv"` to replace full rows by key.
Inspect `client.capabilities()` before using a service feature. Native HTTP,
SDK, and CLI support streaming bulk append for append collections. Bulk upsert
and append-to-KV migration are available only in the native C++ facade.

Use `examples/quickstart.py COLLECTION_UUID OPERATION_UUID` with
`GLAMDB_URL` and `GLAMDB_TOKEN` set for a runnable version of this flow. The
example does not create new UUIDs or retry a write automatically.

## Gateway and OAuth authentication

Pass an **access token**, including for direct-server connections:

```python
import os
from glamdb_sdk import Client

with Client(
    "https://database.example/databases/00112233-4455-6677-8899-aabbccddeeff",
    token=os.environ["GLAMDB_ACCESS_TOKEN"],
) as client:
    print(client.list_collections().items)
```

HTTP Basic `username=` mode has been removed and raises a migration
`ValidationError`. Organization passwords are managed by the identity provider.
An ID token is not an API access token. Configure the registered client and
identity provider to issue the API's required audience and roles; the gateway
validates those claims and current organization/database access.

All SDK paths remain relative to the base URL. The `/databases/UUID` prefix is
preserved for metadata, writes, outcomes, JSON scans and Arrow scans; a trailing
slash is optional. Keep credentials out of URLs. Direct Bearer behavior remains
compatible, including explicit local HTTP adapters; use HTTPS for deployed
connections and retain certificate verification.

For automatic acquisition, install `python -m pip install '.[oauth]'`.
The optional **Authlib 1.9.5** dependency uses its
[BSD-3-Clause license](https://github.com/authlib/authlib/blob/v1.9.5/LICENSE).
Its [OAuth client](https://docs.authlib.org/en/stable/client/oauth2.html)
owns token acquisition, expiry checks, client-credentials reacquisition and
refresh-token requests. HTTPX 0.28 remains supported by this pinned release;
Authlib emits a deprecation notice about its future HTTPX migration.

For a registered service account, use a callable token provider:

```python
import os
from glamdb_sdk import Client
from glamdb_sdk.oauth import ClientCredentialsTokenProvider

with ClientCredentialsTokenProvider(
    "https://identity.example/realms/glamdb/protocol/openid-connect/token",
    client_id=os.environ["GLAMDB_OAUTH_CLIENT_ID"],
    client_secret=os.environ["GLAMDB_OAUTH_CLIENT_SECRET"],
) as credentials:
    with Client(
        "https://database.example/databases/00112233-4455-6677-8899-aabbccddeeff",
        token_provider=credentials,
    ) as client:
        print(client.capabilities())
```

For a human native client, enable the identity provider's Device Authorization
flow for a public client and display the returned instructions to that user:

```python
from glamdb_sdk import Client
from glamdb_sdk.oauth import DeviceAuthorizationClient

with DeviceAuthorizationClient(
    "https://identity.example/realms/glamdb/protocol/openid-connect/token",
    "https://identity.example/realms/glamdb/protocol/openid-connect/auth/device",
    client_id="registered-native-client",
    scope="openid",
) as credentials:
    challenge = credentials.begin()
    print("Open", challenge.verification_uri, "and enter", challenge.user_code)
    credentials.poll(challenge, timeout=300)
    with Client(
        "https://database.example/databases/00112233-4455-6677-8899-aabbccddeeff",
        token_provider=credentials,
    ) as client:
        print(client.list_collections().items)
```

The identity provider supplies device codes; the SDK does not generate or sign
tokens and has no password, callback or session service. The small device poller
uses Authlib's native device-code grant and follows
[RFC 8628](https://www.rfc-editor.org/rfc/rfc8628#section-3.5): pending responses
wait the advertised interval; `slow_down` adds five seconds for subsequent polls,
including after a caller timeout/resume. Polling ends at the earlier of the
caller timeout and device-code expiry. A denied/expired grant stops immediately.
A terminal error requires a new `begin()` and explicit user authorization.

Authlib owns access/refresh token state in memory. No token/secret is written to
files or included in helper/challenge representations or error descriptions.
Display verification instructions only to the intended user; do not persist
the device challenge. `OAuthError.code` contains a sanitized failure category;
server error descriptions, raw response bodies and request URLs are omitted.
OAuth redirects are refused. Helpers require verified HTTPS, with
`allow_insecure_loopback=True` available only for literal loopback/localhost
test endpoints. `verify=False` is likewise restricted to that explicit test
case. Token HTTP operations default to 30-second per-phase timeouts (maximum
300); each device poll is also capped by its remaining budget. Polling timeout
and server-reported device lifetime are capped at 900 seconds. OAuth helpers do
not inherit proxy or trust configuration from environment variables; pass an
explicit trusted `ssl.SSLContext` when the IdP uses a private CA.

`token_provider` accepts any synchronous callable returning a nonempty ASCII
Bearer access-token string. Supply exactly one of `token` and `token_provider`.
The provider is called before each HTTP request, including before a single
mutating POST. Provider failure sends no API request; the cooperative operation
deadline is checked again afterwards. A generic provider must enforce its own
network/time limits. API 401/403 and token refresh never trigger automatic replay
of a POST. A refreshed token changes the next explicit call's authorization
header. The SDK client does not close a caller-owned token provider; use both
context managers as shown. Token renewal does not change the gateway's current
access checks or make revoked access valid.

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
caller operation and collection IDs when applicable. A server/gateway 5xx or 408 write
response without an authoritative outcome raises `OutcomeUnknownError` (an
`HTTPError` subclass); preserve the original IDs and reconcile or look up that
operation before deciding whether to retry. A valid pre-execution refusal
(`503 SERVER_BUSY` or `408 REQUEST_TIMEOUT`, `execution_started: false`, and
`SameIdAfterReconcile`) raises ordinary `HTTPError` instead; the SDK never retries
it automatically. Missing or contradictory refusal fields remain conservative.
Database and route 404 errors never
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
lookup **and checks the original UUID and schema** (plus any original
`write_settings`, see below). A name match alone is insufficient.

The omitted `expected_snapshot` means `Any`. Pass `None` for `NoSnapshot`, or an
int64 snapshot ID for an exact precondition. The same distinction applies to
read selectors: omitted selects latest once; explicit `snapshot_id=None` selects
the initial view. Snapshot IDs are opaque int64 values, not row counts or
sequences. The SDK preserves their decimal-string wire form in metadata.

## Parquet write settings

Collections can choose how later appends write their Parquet data files: the
**compression** codec and level, the **dictionary** policy and dictionary page
size limit, and per-column **value encodings**. This is unrelated to HTTP
transport compression (`Content-Encoding`). Check `client.capabilities()` first:
`features.write_settings` must be true, and the top-level `parquet` object lists
the codecs of the server build with their level ranges (`level` is `null` for
codecs without levels), `default_compression`, the `encodings` with the field
types each accepts, and `dictionary` (policies, dictionary types and the
`page_size_limit` range and default). The development mock does not support
write settings and rejects them.

```python
settings = {"parquet": {
    "compression": {"codec": "zstd", "level": 3},  # collection default; level None = codec default
    "dictionary": "auto",                          # auto (default) | enabled | disabled
    "dictionary_page_size_limit": 262144,          # bytes; sent as the decimal string "262144"
    "columns": [                                   # fields of `schema`; each overrides the defaults
        {"field": "at", "encoding": "delta_binary_packed"},  # explicit encoding: dictionary off
        {"field": "key", "compression": {"codec": "uncompressed"}, "dictionary": "disabled"},
    ],
}}
created = client.create_collection("example", schema, collection_id=collection_id,
                                   write_settings=settings)
info = client.get_collection(collection_id)
print(info["write_settings"], info["write_settings_version"])  # canonical form, "1"

updated = client.update_write_settings(collection_id, info["write_settings_version"],
                                       {"parquet": {"compression": {"codec": "snappy"}}})
print(updated["write_settings_version"])  # "2"; existing files keep their codecs and encodings
```

Column options override the collection defaults option by option; a level is
never inherited from another codec. `dictionary` is `auto` (dictionary pages for
every type except `bool`), `enabled` (the same, rejected for `bool` columns) or
`disabled`. A dictionary that outgrows `dictionary_page_size_limit` (4 KiB to
8 MiB, default 1 MiB) is kept and the remaining data pages fall back to `plain`.
A column `encoding` selects its non-dictionary value encoding and must fit the
field type: `plain` (all), `delta_binary_packed` (`int64`, `timestamp_us_utc`, `date`),
`delta_length_byte_array` and `delta_byte_array` (`string`), `byte_stream_split`
(`int64`, `float64`, `timestamp_us_utc`, `date`) and `rle` (`bool`). Decimal fields
support `plain` and dictionary policies across precisions 1–38. An explicit encoding
turns that column's dictionary off and overrides the collection policy, so it
cannot be combined with `enabled` (or `auto` on a non-`bool` column). A
collection `enabled` policy requires every `bool` column to set its own
`dictionary` or `encoding`. See the [native HTTP contract](../../docs/HTTP_SERVER.md)
("Requests and results") for the authoritative rules.

Settings are plain JSON objects in the wire shape. The SDK rejects unknown keys,
codecs, policies and encodings, non-integer levels or page size limits,
duplicate columns and column entries that set none of `compression`,
`dictionary` and `encoding` before sending. Field types, the conflicts above,
level ranges and the page size limit range are checked by the server
(400 `INVALID_ARGUMENT`). `canonical_write_settings(value, schema)` returns the
form the server reports: every option key present (`None` when unset), columns
in schema order, `level` explicit (`None` selects the codec's default) and the
page size limit as a decimal string. `PARQUET_CODECS`, `DICTIONARY_POLICIES` and
`PARQUET_ENCODINGS` list the accepted names. Without `write_settings`, create
sends exactly the same body as before.

Write settings are part of the collection definition. Repeat a lost create with
the same value: a different value is refused with 409 `WRITE_SETTINGS_MISMATCH`.
`write_settings_version` is `"0"` for a collection created without settings and
`"1"` after a creation with non-default settings; each applied update adds one.
`create_collection` and `resolve_create(..., write_settings=...)` compare the
returned settings with the canonical request while the version is still the
creation version. A higher version means later updates replaced the creation
settings; only the UUID, name and schema remain comparable then (a version-1
collection created with defaults and updated once cannot be told apart).

`update_write_settings(collection_id, expected_version, write_settings)` is a
compare-and-set that replaces the complete settings object: it returns the
collection with version `expected_version + 1` and the requested settings,
otherwise `ProtocolError`. Reported settings may contain newer options unknown
to this SDK; unset (`null`) ones are ignored, set ones raise `ProtocolError`. A
stale version raises `HTTPError` 409 `WRITE_SETTINGS_CONFLICT`; reload the
collection before deciding again. A lost response (`TransportError`) or a 5xx
such as 503 `RECOVERY_REQUIRED` (`OutcomeUnknownError`) leaves the result
unknown: reconcile if requested, then repeat the same call; an identical request
that was already applied returns the collection unchanged. File details report
what each column chunk actually used: `compression`, `encodings` and
`encoding_stats` (page counts per page type and encoding; a dictionary that fell
back shows both `rle_dictionary` and `plain` data pages).

## Snapshot archive

Servers advertising `features.snapshot_archive` (native local storage) keep only
the newest `hot_snapshots` snapshots of an append collection in the Iceberg root;
older snapshots move to the catalog archive and stay readable by ID through
`snapshots`, `stats`, `files`, `file`, `explain` and scans exactly as before.
A new append collection created without `hot_snapshots` gets the server's
default (100); pass `KEEP_ALL_SNAPSHOTS_IN_ROOT` (0) to create it without an
archive. KV collections never have one.

```python
from glamdb_sdk import KEEP_ALL_SNAPSHOTS_IN_ROOT

created = client.create_collection("events", schema, collection_id=collection_id)
created.info["hot_snapshots"], created.info["hot_snapshots_defaulted"]   # "100", True
client.create_collection("audit", schema, collection_id=other_id,
                         hot_snapshots=KEEP_ALL_SNAPSHOTS_IN_ROOT)       # no archive; 1..2**40 for a value
status = client.snapshot_archive(collection_id)         # {"hot_snapshots": "100", "root_snapshots": ...}
client.update_snapshot_archive(collection_id, 50)       # later appends apply it; 0 or None stops archiving
while True:                                             # bounded maintenance, at most 1024 per call
    status = client.archive_snapshots(collection_id)
    if status["archive_pending"] == "0":
        break
```

Counts are decimal strings. Collections report the effective `hot_snapshots`
(`"0"` without an archive) and `hot_snapshots_defaulted` (the value came from the
server default and was never set; any update clears it). The effective value is
part of the collection definition: repeating a lost create with another effective
value is refused (409 `SNAPSHOT_ARCHIVE_MISMATCH`, a `definitely_not_committed`
outcome). An omitted value matches only a collection whose setting is still defaulted
(not one created with an explicit 100 or later updated); an explicit 100 also
matches a defaulted 100; an explicit 0 matches only a collection without an archive, so a
create that omits the value no longer matches a collection created without an
archive by an older server. The reported value is the current one, so
`resolve_create` accepts a resolved collection only when that proves the creation
(a still-defaulted value for a create that omitted it, a KV collection without a
value, or a server without the archive); otherwise it repeats the original create
and the server compares it with the created value. `update_snapshot_archive` is
idempotent and returns the collection; after a lost response or
`OutcomeUnknownError` (503 `RECOVERY_REQUIRED`) reconcile if requested and repeat
the same call, as for `archive_snapshots`. KV collections refuse a nonzero value
and maintenance with 409 `COLLECTION_MODE_MISMATCH`; values outside 0..2^40 raise
`ValidationError` before sending. The mock does not implement the archive: it
rejects `hot_snapshots` (422) and has no archive routes (404), and the SDK raises
`ProtocolError` if a collection created with `hot_snapshots` does not report it.

## Scalars, predicates and metadata

`Field(name, type, nullable=False, field_id=None, precision=None, scale=None)`
supports `string`, `bool`, `int64`, `float64`, `timestamp_us_utc`, `decimal`, and
`date`. `Schema` assigns missing field IDs in
order and preserves supplied stable IDs. Append accepts Python int64 integers
or canonical decimal strings, finite floats, bools, strings, and UTC timestamps.
An omitted nullable field becomes NULL; required, unknown or mismatched fields
are rejected. Empty scan projections preserve an empty output schema and every
matching row. The SDK enforces the v0 10,000-row/16-MiB decoded append ceiling
during normalization, plus row and string limits. The serialized JSON request
also has an independent 16-MiB ceiling, including escaped strings, repeated
field names, framing and the optional snapshot precondition. Preparation raises
`ValidationError` locally when it exceeds that wire limit; it does not split
the operation or replace its IDs. Explicitly constructed prepared bodies are
checked again before transport. `max_json_bytes` bounds responses only.

Append preparation validates each row once against a cached per-schema plan
(keyed by the fields' values, never by caller data). The same pass yields the
wire object, the row's canonical hash bytes and its decoded size, and converts
each timestamp to microseconds once. Sized inputs (lists, tuples) are hashed as
they are read; for other iterables the canonical rows (bounded by the 16-MiB
ceiling) are kept until the row count is known. A conservative wire-size bound
keeps the fast whole-body encoder for requests guaranteed to fit; larger bounds
switch preparation to bounded row serialization. Admitted inputs preserve the
exact original JSON bytes and canonical hash. Generators are consumed only up
to the first limit or invalid row. To measure preparation alone:
`python sdk/python/benchmarks/prepare_bench.py` (compares against the frozen
pre-plan implementation in `sdk/python/tests/legacy_prepare.py`).

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

Decimal fields require integer `precision` (1–38) and `scale` (0–precision):
`Field("amount", "decimal", True, precision=18, scale=2)`. Other field types
forbid these schema parameters. Decimal inputs are plain strings matching
`-?[0-9]+(\.[0-9]+)?` or finite `decimal.Decimal` objects; floats and integers are
rejected. Plain text cannot contain exponents, signs other than a leading minus,
or whitespace, and is limited to 128 characters. Decimal objects expand to plain
text exactly within that bound, independently of the active Decimal context.
Leading zeros and negative zero normalize; fractional zeros pad to the declared
scale. Extra fractional digits must all be zero, so normalization never rounds.
Wire values always use fixed-scale strings, and JSON scans decode to `Decimal`.

Dates accept strict `YYYY-MM-DD` text or `datetime.date` excluding `datetime`.
They cover calendar years 0001–9999 and decode to `date`. Arrow scans require
`decimal128(precision, scale)` and `date32`; unsupported widths, scales, decimal
coefficients outside the declared precision, and dates outside the calendar
range are rejected. Schemas containing decimal/date use canonical schema
version 2, while append/hash and ordinary append result receipts remain version 1.
Bulk operation lookups may return result receipt version 2. Existing schema and
append hashes retain their original bytes.

`col(name, logical_type)` provides `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in_`,
`is_null` and `is_not_null`. Combine `Predicate` values with `&`, `|`, `~`,
`and_(...)` and `or_(...)`. These serialize the public predicate tree; the
server performs SQL three-valued filtering. A typed builder prevents implicit
literal casts. A raw predicate JSON object may also be passed to scan/explain.
Use `col("amount", "decimal", precision=18, scale=2).ge(Decimal("12.30"))`
for decimals. Its literal includes `type`, matching `precision`/`scale`, and an
exact fixed-scale `value`. Date literals use `{type: "date", value: "2026-11-12"}`.
The builders retain those type parameters for NULL comparison and IN literals.

Read methods are `capabilities`, `list_collections`, `resolve_collection`,
`get_collection`, `get_schema`, `stats`, `snapshots`, `files`, `file`, `explain`,
`scan_json` and `scan_batches`. `Page.items`, `Page.next_cursor` and `Page.raw`
retain the server's page. Reuse the opaque cursor with exactly the same
parameters, selector and page size; do not resolve latest again between pages.

## Native selected-view metadata

These methods also operate against the native C++ service. `stats` and `explain`
retain root identity separately from the resolved selected view; the SDK checks
collection/selector consistency. `snapshot_id=None` selects initial empty,
`ANY` or omission selects latest, and a precise integer selects a retained view.
Native file IDs are opaque and usable only in a view containing that file.
`file` includes actual footer field IDs, typed bounds and row groups. Missing
physical measurements remain null; metadata/index presence is not pruning.
History and file cursors are finite, scoped and expiring. Reuse their original
parameters; a rejected token is not permission to silently restart at latest.
The [installed cross-client recipe](../../tools/acceptance/README.md)
checks this SDK alongside CLI and UI on the same persistent data and restored copy.

## Streaming and transport ownership

Always use `with client.scan_batches(...) as stream:` or call `stream.close()`.
The client tracks iterators weakly. Dropping an iterator queues its HTTP owners
and cursor cleanup through a nonthrowing finalizer; GC never closes a response or
sends a request. The next client request or `Client.close()` drains that queue.
Use explicit close for prompt release: abandoned owners remain retained until a
drain, and the server cursor TTL is the fallback when no client call follows.
Breaking a loop whose iterator is retained does not immediately release it.
Exiting the context or closing the client closes every reader and HTTP
response, including on a consumer exception. Already returned Arrow batches
remain usable after those owners close. Iteration failures remain sticky: another
`next()` raises the original failure, and integrity never becomes complete.
Cleanup still attempts every owner and cursor cancellation when a close fails;
an existing consumer or protocol exception takes precedence over cleanup errors,
including when leaving the client context or rejecting a later page's view.

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
Before returning the first batch, its snapshot header must match an explicitly
requested snapshot and, when supplied, the snapshot selected by `/scan/open`.

TLS certificate verification is enabled by default. `verify` accepts a bool or
an `ssl.SSLContext`; disable it only when deliberately configuring a local test.
`timeout=30` supplies HTTPX connect/read/write/pool timeouts. Each operation's
optional `deadline=seconds` adds a cooperative total budget checked before and
after network reads, with the initial HTTP timeout capped by the remaining
budget. It cannot interrupt an uninterruptible OS call. A deadline is never
translated into a fabricated failed server outcome.

## Validation

From the repository root:

```sh
python -m pip install -e 'sdk/python[test]'
.venv/bin/python -m pytest -q -c sdk/python/pyproject.toml sdk/python/tests
```

Unit tests cover codecs, canonical hashes, outcomes and Arrow ownership.
Transport doubles exercise protocol handling; native HTTP tests separately
exercise persisted C++ storage. OAuth protocol peers are separate from a real
Keycloak deployment.

Actual HTTP/HTTPS adapter tests are maintained separately:

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
responses include all normalized limits. See the [native server contract](../../docs/HTTP_SERVER.md)
for ranges, retry semantics and persistence.

Use `root.drop_database(database_id, store_id=original_store_id)` to delete one
entire managed database, including its collections and history. Retain both
original UUIDs from database creation; `store_id` must match `database_id`.
The client sends one authenticated `POST /databases/{id}/drop` with only the
saved `store_id`, and validates both returned identities and `dropped: true`.
Malformed or mismatched success replies raise `ProtocolError`.

A timeout, transport failure or lost reply is ambiguous. The SDK never retries
the drop automatically. Call `drop_database` again with the **same saved UUIDs**
to retrieve or finish that deletion. Never resolve a name for cleanup: another
database may already have reused it. This is a network operation; the SDK does
not remove local files. See [managed database deletion](../../docs/DATABASE_DROP.md).

The native collection range is 1–64, with eight as the default. Use
`create_database("comparison", limits={"max_collections": 64})` to opt in;
the shared resource owner must admit the profile. Collection pages retain one
creation inventory, so later creates do not appear midway through enumeration:

```python
cursor = None
while True:
    page = client.list_collections(page_size=7, cursor=cursor)
    for collection in page.items:
        consume(collection)
    cursor = page.next_cursor
    if cursor is None:
        break
```

See [collection profiles](../../docs/COLLECTION_PROFILES.md) for the
native restart, refusal, inventory and measured larger-profile evidence.

## Native streaming bulk append

`prepare_bulk_append(collection_id, batches, rows=..., operation_id=..., schema=...)`
validates and hashes bounded batches into an anonymous local temporary file.
`bulk_append(prepared, deadline=...)` streams that file in 64 KiB blocks, preserving
one operation and receipt across explicit retries. Use the prepared request as a
context manager or call `close()`. There is no automatic retry or reconciliation.
Set `Client(..., timeout=650)` when the server may use its ten-minute execution
budget. Ordinary append keeps its 10,000-row/16 MiB limits. Bulk permits declared
totals up to 25,600,000 rows, subject to 4 GiB wire/canonical, file and device limits;
each batch remains within ordinary row/canonical limits and 16 MiB wire. The
[native bulk guide](../../docs/BULK_INGESTION.md#native-http-sdk-and-cli)
specifies framing, memory/disk ownership, interrupted upload and retry semantics.
The mock, UI import and organization gateway do not expose this upload route.

## Explicit KV collections

The native service advertises `collection_modes: ["append", "kv"]` and
`features.key_value`, `upsert`, `delete`, `lookup`, and `compact`. The disposable
mock and static demo advertise these features as false. Mode is part of the
immutable create definition and lost-create confirmation:

```python
created = client.create_collection("users", schema, collection_id=collection_id, mode="kv")
request = client.prepare_upsert(collection_id, [
    {"key": "alice", "n": 1},
    {"key": "alice", "n": 2},
], operation_id=operation_id, schema=schema)
outcome = client.upsert(request)
# The last input occurrence wins; omitted nullable fields become NULL.
found = client.lookup(collection_id, ["alice", "absent", "alice"])
assert found.rows[1] is None
```

A KV schema requires a nonnullable string `key`, at any column position.
`prepare_delete(..., keys, operation_id=..., schema=...)` retains key order and
duplicates even though duplicate and absent keys change nothing. Submit with
`delete(prepared)`. `prepare_compact(..., operation_id=..., max_files=64,
target_file_bytes=8388608)` creates a bounded compaction request; submit with
`compact(prepared)`. Convenience `upsert`, `delete`, and `compact` accept the
same arguments directly. Every call sends one mutation POST, without automatic
retry. Prepared `MutationRequest` values own immutable body bytes and their
kind-specific canonical hash; reuse the original value after uncertain delivery.
`expected_snapshot=ANY` is the default, `None` requires an initial empty view,
and a decimal ID requires that snapshot. A matching retry returns the original
terminal result before evaluating a changed current view.

Lookup accepts at most 4,096 UTF-8 keys and returns a `LookupResult` with a
resolved `snapshot_id`, typed schema, and one row or `None` per requested key.
Repeated keys repeat their hits. `snapshot_id=ANY` selects latest once, `None`
selects the permanent initial-empty view, and an exact ID selects retained
history. Native lookup bounds the complete result to 16 MiB; request fewer keys
when it refuses capacity. Upsert and delete accept at most 10,000 input rows or
keys per ordinary request, under the existing 16 MiB body/decoded limits.

KV receipts use `receipt_version=3` and `rows_appended="0"`. Read
`mutation_kind`, `input_rows`, `rows_affected`, `rows_written`, `rows_removed`,
`data_files_added`, `data_files_removed`, and compressed byte counters instead.
A committed no-op has `snapshot_created=false`, zero changed counters and time,
and an effective `snapshot_id` equal to its base (or `"0"` for initial empty).
It creates no snapshot. `find_append_outcome` reads all three receipt versions,
including KV outcomes, using the permanent collection-scoped operation ID.
Failures and `commit_unknown` retain the same three-outcome/recovery contract.
