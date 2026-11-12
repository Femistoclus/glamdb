# GlamDB CLI

`glamdb-cli` is an independent HTTP client. It does not link the engine, SQLite,
Arrow, Parquet or Iceberg. It uses the pinned CLI11 and standalone Asio submodules,
OpenSSL 1.1.1+ (TLS 1.2 or newer), and the pinned JSON header for command input and header output.

Build the client alone with CMake 3.28+, Ninja, a C++23 compiler and OpenSSL development
headers/libraries. No engine dependency installation is needed:

```sh
git submodule update --init third_party/cli11 third_party/asio third_party/json third_party/reproc
cmake -S apps/cli -B build/client -G Ninja -DCMAKE_CXX_COMPILER=clang++-18
cmake --build build/client
```

Set `GLAMDB_TOKEN` in the environment. Keep tokens out of command-line arguments.
Local HTTP accepts only hostnames whose resolved endpoints are all loopback addresses;
remote servers require HTTPS. HTTPS verifies the certificate chain and hostname,
sets SNI for DNS names (not IP literals), and uses system trust with an optional additional `--ca-file CA.pem`.
There is no certificate-verification bypass and redirects are never followed.

```sh
export GLAMDB_TOKEN='replace-with-your-token'
build/client/glamdb-cli create-store --url https://db.example.test --name analytics
build/client/glamdb-cli drop-database --url https://db.example.test --database DATABASE_UUID --store-id ORIGINAL_STORE_UUID
build/client/glamdb-cli request --url https://db.example.test/databases
build/client/glamdb-cli request --host localhost --port 8080 --path /capabilities
```

`create-store` creates a managed database over `POST /databases`; it accepts a
server base URL and a database `--name`. The generic `request` command exposes every
server route, including collection creation, append, operation lookup, scan, cursor
continuation/cancellation, and database open/close/reconcile/discard. Discard only
removes an uninitialized reservation without a ready store. Supply the complete
route in `--url`, or append a `--path` to a base URL. Consult the
[native HTTP contract](../../docs/development/HTTP_SERVER.md) for route bodies and
supported features. A body file implies POST unless `--method` is explicit.
Supported methods are GET and POST. A base URL combined with `--path` must not
contain a query or fragment; put a query in the path or supply a complete URL.
`--body -` reads stdin. Cursor cancellation uses POST to the scan cancellation
route with its cursor body.

```sh
build/client/glamdb-cli request --url https://db.example.test/databases/DB/collections \
  --body collection.json
build/client/glamdb-cli request --url https://db.example.test/databases/DB/collections/COLLECTION/append \
  --body append.json
build/client/glamdb-cli request --url https://db.example.test/databases/DB/collections/COLLECTION/scan \
  --body scan.json --accept application/vnd.apache.arrow.stream \
  --output result.arrow --headers result.headers.json
```

For fixed-snapshot paging, POST the scan body to `.../scan/open` to obtain a
`cursor`, then POST `{"cursor":"TOKEN"}` to `.../scan`. Continue using the JSON
`next_cursor` or Arrow `X-GlamDB-Next-Cursor` header until it is absent. Preserve the
same Accept type used at open. Retrying the current token returns the same page.
Cancel with POST `.../scan/cancel` and the cursor body; the token also expires after
60 seconds. The CLI issues one exchange per invocation, so scripts retain the token
and use `--headers` for Arrow continuation metadata.

Output is the exact response body, including binary Arrow IPC, without an added
newline. `--output FILE` replaces a regular file atomically after writing, syncing,
and closing a temporary file in the same directory; a failed write preserves the
existing file and its permissions. New files have owner-only permissions.
Both output destinations are validated and temporary files reserved before any
HTTP request. Symlinks, directories and special files (including `/dev/null`)
are rejected before a mutation can execute. The default is stdout. `--headers FILE`
writes a separate JSON object containing `status` and all lower-case response
header names. A repeated header has an array of values, in received order; a single
header has a string value. Header bytes above ASCII map to Latin-1 code points in
JSON. This exposes content type, checksum, selected snapshot and any cursor
metadata without corrupting the body. Any supplied `X-Content-SHA256` is verified
before output. The client bounds requests to 16 MiB, response headers to 16 KiB,
and response bodies to 64 MiB; larger scans use server continuation. It rejects
ambiguous framing, duplicate Content-Length, Transfer-Encoding, X-Content-SHA256
or X-GlamDB-Next-Cursor headers, unsupported transfer/content encoding,
truncated responses, and checksum mismatches. It buffers one bounded response
before writing output.

Exit status is:

- `0`: complete 2xx response with successful output.
- `1`: complete non-2xx response, local validation failure, or transport failure
  before sending a POST (also transport failures for GET).
- `2`: command-line parsing error.
- `3`: POST transmission started, but no valid complete response was received;
  the write outcome may be unknown.
- `4`: output failed after a complete response; stderr includes its HTTP status.

`--timeout SECONDS` is a whole-request deadline (default 65 seconds, maximum 3600).
A DNS, connection, HTTPS policy or TLS verification failure before request
transmission does not claim an uncertain write. Once a POST starts transmission,
a timeout or interrupted response does not prove that a write failed. A failed
output write/flush/close or cancelled client process likewise does not undo a
server operation. Reconcile a write through its original
operation ID; never retry it under a new ID merely because the response was lost.
A header-output failure after a complete response returns `4`, even when the
server already committed. Destination preflight failures return `1` before sending.
Header and body output files are replaced independently; they are not one atomic
multi-file transaction.

`drop-database` deletes an entire dedicated managed database and its history.
Save its original canonical `database_id` and `store_id` from creation; these
UUIDs must match. The command sends one authenticated
`POST /databases/{database_id}/drop` with `{"store_id":"ORIGINAL_STORE_UUID"}`.
It validates both returned UUIDs and `dropped: true` before producing successful
output. A malformed success reply fails without claiming deletion. No Python
installation is needed for this command.

After a timeout or lost response, retry with exactly the same URL and saved
UUIDs. The completed deletion receipt makes a repeated request safe, even if
another database reused the old name. Never resolve a name for cleanup. There
is no automatic retry, force option, or local filesystem removal. See
[managed database deletion](../../docs/development/DATABASE_DROP.md).

The optional `glamdb-admin` executable retains local `create-store --root ...
--store-id ...` and `inspect-store` for offline administration. Build the full
repository with `-DGLAMDB_BUILD_ADMIN=ON` to enable it. It links the engine and must
not open a store already owned by the server.

## Data and inspection commands

Both executables offer the following named JSON commands. `glamdb-cli` uses HTTP;
`glamdb-admin` uses the public embedded facade with `--root ABSOLUTE_PATH
--store-id UUID`, or HTTP with `--url BASE_URL`. Remote mode never opens a store.
Combining `--url` with offline ownership options is an error. No data command
creates a missing store.

| Command | Additional options | Result |
| --- | --- | --- |
| `create-collection` | `--collection-id UUID --name NAME --schema FILE [--write-settings FILE]` | Created collection or original matching collection |
| `collections` | None | Collection inventory |
| `resolve-collection` | `--name NAME` | Exact-name lookup, including after a lost create reply |
| `collection` | `--collection-id UUID` | Collection identity and schema |
| `schema` | `--collection-id UUID` | Fixed schema with stable field IDs |
| `append` | `--collection-id UUID --body FILE` | Complete native write outcome and receipt |
| `scan` | `--collection-id UUID [--body FILE]` | Complete bounded JSON rows with schema and selected identity |
| `operation` | `--collection-id UUID --operation-id UUID` | Original append outcome |
| `snapshots` | `--collection-id UUID [--snapshot SELECTOR]` | Receipt-backed snapshot history |
| `files` | `--collection-id UUID [--snapshot SELECTOR]` | Committed file membership and opaque IDs |
| `file` | `--collection-id UUID --file-id ID [--snapshot SELECTOR]` | Verified Parquet footer and row groups |
| `write-settings` | `--collection-id UUID --expected-version N --settings FILE` | Collection after a compare-and-set of its Parquet write settings |

Remote commands accept `--database-id UUID` for a managed server, or a base URL
already scoped to `/databases/UUID`. Omit both for legacy single-store servers.
The network executable also accepts the existing `--host`/`--port`, TLS, timeout,
atomic `--output`, and `--headers` options. `glamdb-admin` remote mode accepts
`--ca-file` and `--timeout` and writes JSON to stdout. Tokens remain exclusively
in `GLAMDB_TOKEN`; TLS verification and loopback-only HTTP rules apply in both.

Write settings files contain the HTTP `write_settings` object, for example
`{"parquet":{"compression":{"codec":"zstd","level":9},"columns":[{"field":"payload","compression":{"codec":"uncompressed"}}]}}`.
They choose the Parquet compression of data files written by later appends;
on creation they are part of the definition. `write-settings` applies only when
`--expected-version` is the collection's current `write_settings_version`; repeating
an applied command succeeds unchanged, a stale version fails with
`WRITE_SETTINGS_CONFLICT` (see the [HTTP contract](../../docs/development/HTTP_SERVER.md)).
Schema files contain `{"fields":[...]}`. Append files contain the original
`operation_id`, `rows`, and optional `expected_snapshot` (`null` means no previous
snapshot; an exact decimal string requires that snapshot). Files may be `-` for
stdin. Scan bodies use the same `snapshot_id`, `projection`, `predicate`, `limit`
and `batch_rows` fields as HTTP; the default is `{}`. Metadata selectors are
`latest`, `initial`, or a canonical decimal snapshot ID. Initial scan and metadata
views report JSON `null` as their snapshot ID. Int64 values remain decimal
strings, timestamps retain six UTC fractional digits, and duplicate keys remain
separate rows.

```sh
# Create a store explicitly before opening it offline or serving it.
glamdb-admin create-store --root /absolute/store --store-id STORE_UUID
glamdb-admin create-collection --root /absolute/store --store-id STORE_UUID \
  --collection-id COLLECTION_UUID --name events --schema schema.json
glamdb-admin append --root /absolute/store --store-id STORE_UUID \
  --collection-id COLLECTION_UUID --body append.json
glamdb-admin scan --root /absolute/store --store-id STORE_UUID \
  --collection-id COLLECTION_UUID --body scan.json

# The server owns the store until it stops. Offline access now fails STATE_IN_USE.
glamdb serve --root /absolute/store --store-id STORE_UUID --port 8080
# In another terminal, use HTTP without acquiring another store lease:
glamdb-cli scan --url http://localhost:8080 --collection-id COLLECTION_UUID
glamdb-admin operation --url http://localhost:8080 \
  --collection-id COLLECTION_UUID --operation-id ORIGINAL_OPERATION_UUID
```

`glamdb serve --data-dir DIRECTORY` runs the managed database server. The existing
server invocation without `serve` remains supported. Serving an existing
single-store root calls `OpenExisting`; it never creates a missing store.

Named scans complete fixed-view HTTP continuations automatically, up to 10,000
rows and 16 MiB of JSON. Use predicates, projections or `limit` for larger data;
use `request` and its explicit cursor/Arrow protocol for larger exports. Named
snapshot/file inventories also complete HTTP continuations within 16 MiB.
An oversized result or failed continuation produces no partial JSON result;
unfinished remote scans are cancelled on a best-effort basis, with server expiry
as fallback. `--timeout` applies to each HTTP exchange, including continuations.
Aggregated `--headers` describe the output length and omit per-page checksums
and continuation tokens. Other metadata comes from the first response.

Named commands use exits `0` for success, `1` for definite failure, `2` for usage
errors, `3` for unresolved outcomes (including complete `commit_unknown`,
`OPERATION_IN_PROGRESS` and `RECOVERY_REQUIRED` replies), and `4` for output failure
after a complete response. These outcome-aware exits apply to both local and
remote named commands; the generic `request` command retains its HTTP-level
exit contract above. JSON responses, including failed write outcomes, go to
stdout; diagnostics go to stderr. Lost append/create POST replies also return `3`;
interrupted named scans are read failures (`1`). Offline shutdown failures preserve
the original response and go to stderr. A confirmed success followed by failed
shutdown returns `4`; unresolved outcomes retain exit `3`, even if output fails.
Retain the
original collection/operation IDs and body, resolve through exact-name or
operation lookup, and never interpret a lost reply as rollback. Writes are never
automatically retried or reconciled.

Run wire/TLS/output-fault tests, or include the native server for the complete
JSON/Arrow lifecycle test (the latter requires Python `pyarrow`):

```sh
python tests/client/cli_network_test.py build/client/glamdb-cli
python tests/client/cli_network_test.py build/client/glamdb-cli build/local/apps/server/glamdb
python tests/client/cli_data_test.py build/client/glamdb-cli \
  build/local/apps/admin/glamdb-admin build/local/apps/server/glamdb
```

These checks use real localhost sockets and disposable certificates/stores. They
verify client transport and engine API access, not storage power-loss durability.
`glamdb.cli_data` uses native SQLite/Arrow/Parquet/Iceberg for local/remote parity,
precise scalars, duplicate rows, retained snapshots, exact retries after restart,
multi-page scans, bounded results and exclusive ownership. The wire suite's
literal outcome fixtures separately test exit classification; they are protocol
evidence, not native publication evidence.
See the [validation record](../../docs/development/CLI_VALIDATION.md) for the
independent review findings, regressions and evidence boundaries.
Managed databases default to eight collections. Opt in through
`glamdb-cli create-store --url URL --name comparison --max-collections 64`.
The lifecycle API also accepts partial limits when opening a closed database
(use `request --path /databases/ID/open --body FILE`). The server persists
those limits; omission preserves them. Named `collections` completes bounded
continuations before output, checking the creation high-water mark.
Offline `glamdb-admin` commands and legacy `glamdb --root ... --store-id ...`
servers accept `--max-collections 64`; supply the same value on every reopen.
See [collection profiles](../../docs/development/COLLECTION_PROFILES.md).

# Workload commands

The same executable also offers `generate` and `bench write/read/verify/resolve`
through an optional Python environment. See the
[workload guide](../../tools/workload/README.md) for installation, exact outcome
journals, Locust reports and native E2E. Ordinary requests do not require Python.

## Native bulk upload

```sh
glamdb-cli bulk-append --url https://db.example.test --database-id DATABASE_UUID \
  --collection-id COLLECTION_UUID --body upload.ndjson --timeout 700
```

This command streams a regular replayable file in 64 KiB blocks, up to 4 GiB.
Its first LF-terminated NDJSON record declares `operation_id`, total `rows` and an
optional `expected_snapshot`; later `{"rows":[...]}` records hold bounded batches.
Preserve the exact file and operation UUID for explicit retries and do not mutate
the file while uploading. Stdin is unsupported. The named append outcome exit
codes and HTTPS verification apply. `glamdb-admin bulk-append --url ...` is also
available; offline bulk upload is unsupported. The generic buffered `request`
command keeps its 16 MiB limit. See the [bulk contract](../../docs/development/BULK_INGESTION.md#native-http-sdk-and-cli)
for per-record limits, deadlines and interrupted-upload behavior. UI import and
the organization gateway upload adapter remain outside this native route.
