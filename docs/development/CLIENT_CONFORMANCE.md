# Client conformance against explicit backends

`tests/integration/test_network_sdk.py` and
`ui/tests/e2e/shared-contract.spec.mjs` run the same compatible scenarios against
the disposable mock and the native C++ service. Set `GLAMDB_TEST_BACKEND` to
`mock` or `native`. For compatibility, an unset selector chooses native when
`GLAMDB_NATIVE_SERVER` is present, otherwise mock. An explicit selector wins.
An unknown selector fails; native never falls back to mock or skips setup.

Every shared server fixture checks authenticated capabilities for `backend=mock`,
`mock=true` or `backend=local`, `mock=false`, respectively. Both currently report
`durability_validation=designed`. This identity does not qualify durability.

From the repository root, with workbench dependencies installed:

```sh
# Disposable mock only; no native executable or storage dependencies required.
GLAMDB_TEST_BACKEND=mock .venv/bin/python -m pytest \
  tests/integration/test_network_sdk.py tests/integration/test_network_mock.py \
  tests/integration/test_backend_harness.py -q
GLAMDB_TEST_BACKEND=mock npm --prefix ui run test:e2e

# Native SDK and browser scenarios; all prerequisites are mandatory.
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps \
  -r tools/acceptance/requirements.txt
export GLAMDB_NATIVE_SERVER="$PWD/build/local/apps/server/glamdb"
GLAMDB_TEST_BACKEND=native .venv/bin/python -m pytest \
  tests/integration/test_network_sdk.py tests/integration/test_backend_harness.py -q
GLAMDB_TEST_BACKEND=native npm --prefix ui run test:e2e

# Browser-only demo, independently of both API backends.
GLAMDB_TEST_BACKEND=static npm --prefix ui run test:e2e
```

Build the native executable using [BUILDING.md](BUILDING.md). Chromium, its OS
libraries and OpenSSL are required for browser/TLS tests. `GLAMDB_E2E_PYTHON`
selects the browser harness Python; it needs the SDK and physical-inspection
dependencies. `GLAMDB_E2E_PORT`, `GLAMDB_STATIC_PORT`, `GLAMDB_BROWSER_PATH` and
`GLAMDB_E2E_TEMP_ROOT` retain their existing meanings.

## Shared contracts and explicit differences

| Scenario | Both backends |
| --- | --- |
| SDK authentication and capabilities | Expected backend identity and rejected credentials. |
| SDK create and exact resolution | Original UUID/name/schema; case-sensitive names. |
| SDK JSON and Arrow | Duplicate rows, NULL, int64 beyond 2^53, years 0001/9999, fixed snapshots, zero-column projection, verified checksums and early close. |
| SDK Arrow integrity and ownership | Empty schema/EOS, a scan opened before another append, corrupt checksums/lengths, missing EOS, truncation, extra bytes, bad schema and wrong/missing snapshot headers. Early close, consumer/client errors and simultaneous later-page view/close errors release owners; returned batches remain readable. |
| SDK operations | Retry A after B returns the original receipt; failed preconditions stay terminal; changed payload conflicts; a dropped real response retains the original request. |
| SDK metadata | History/file pages retain their selected view across writes; file details and explain select the same snapshot. |
| SDK TLS and quickstart | Trusted CA succeeds, system/wrong CA fails without fabricating an outcome; the runnable example verifies its stream. |
| Browser | Create, append duplicates and precise integers, render hostile text safely, select history, filter and download Arrow. |
| Browser exports | Empty schema, append between preview and download, and damaged real checksum/EOS/snapshot responses that never start a download. A healthy retry succeeds. |

Assertions preserve documented representation differences: native metadata counts
are decimal strings, while mock counts can be integers; native authentication
uses `401/UNAUTHORIZED`, mock uses `403/PERMISSION_DENIED`; precondition and
operation-conflict codes also differ. Native metadata selects the snapshot in a
top-level `snapshot_id`; the mock includes it in `identity`. Foreign native
metadata cursors return `NOT_FOUND`, while the mock reports `CURSOR_MISMATCH`.
These differences do not weaken row, snapshot, receipt or backend checks.
The later-page view/close fault requires native cursor sessions and is explicitly
skipped in the legacy single-response mock profile.

`tests/integration/helpers/arrow_transport.py` wraps the real HTTP transport;
only deliberate fault cases alter native/mock response bytes or headers. Owner
tests require more than 10,000 rows, assert native cancellation acknowledgements
and check retained data after closing the HTTP pool. Unit doubles cover exact
header/framing boundaries and close failures separately.

Browser downloads require Arrow MIME, identity content encoding, the preview's
fixed snapshot, exact length and SHA256, followed by modern IPC V4/V5 framing
with a schema message and explicit EOS at HTTP EOF. The bounded validator walks
Message metadata and body lengths, including table bounds, so a batch body that
resembles EOS cannot hide a missing framing marker. It verifies the transport
envelope; logical Arrow schema/value decoding remains the native/PyArrow
encoder's contract and is checked by PyArrow in SDK and browser integration.
It does not implement a second Arrow decoder. Multi-page browser exports remain
an explicit error directing the user to reduce the limit or use the SDK.

Native runs create fresh managed databases over real sockets. SDK scenarios stop
their process before using the independent inspector from `tools/acceptance`.
The browser scenario closes its own database before inspection. The inspector
follows SQLite's committed pointer through Iceberg JSON and Avro membership to
actual Parquet footers; tests require the independent expected row count and
nonempty manifest/file inventories. It cannot pass on capabilities or arbitrary
orphan files alone. Temporary storage is removed after each run.

Mock development faults and collection pagination remain in
`test_network_mock.py`; native collection pagination is unsupported. Existing
native browser recovery cases remain in `native.spec.mjs`. The mock browser
profile excludes that file at discovery; native selects it and the shared
contract. Static selects only `public-demo.spec.mjs` and starts only the static
server. A missing native executable, wrong identity, process failure or absent
physical artifact is a test failure.

The native browser wrapper retains ownership through shutdown. SIGINT/SIGTERM
enter cleanup, allow five seconds for an orderly child exit, then force termination
and reap it before removing the state marker and temporary storage. Repeated
signals cannot interrupt cleanup. Startup failures also remove the marker, and
ordinary child exit codes remain visible to the test runner.

`scripts/check-native-http.sh` runs native CLI, workload, shared SDK conformance
and browser tests in sequence. SDK JUnit evidence is written separately to
`artifacts/native-normal/sdk-conformance-junit.xml`; browser results use
`artifacts/e2e/<backend>/results`. Report mock and native results separately.
These checks do not replace restart/restore acceptance, sanitizers or G4
power-loss qualification.
