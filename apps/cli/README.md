# GlamDB network CLI

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
existing file. New files have owner-only permissions. Symlinks and special files
are rejected. The default is stdout. `--headers FILE`
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
A header-output failure returns `4`, even when the server already committed.
Header and body output files are replaced independently; they are not one atomic
multi-file transaction.

The optional `glamdb-admin` executable retains local `create-store --root ...
--store-id ...` and `inspect-store` for offline administration. Build the full
repository with `-DGLAMDB_BUILD_ADMIN=ON` to enable it. It links the engine and must
not open a store already owned by the server.

Run wire/TLS/output-fault tests, or include the native server for the complete
JSON/Arrow lifecycle test (the latter requires Python `pyarrow`):

```sh
python tests/client/cli_network_test.py build/client/glamdb-cli
python tests/client/cli_network_test.py build/client/glamdb-cli build/local/apps/server/glamdb
```

These checks use real localhost sockets and disposable certificates/stores. They
verify client transport and engine API access, not storage power-loss durability.
# Workload commands

The same executable also offers `generate` and `bench write/read/verify/resolve`
through an optional Python environment. See the
[workload guide](../../tools/workload/README.md) for installation, exact outcome
journals, Locust reports and native E2E. Ordinary requests do not require Python.
