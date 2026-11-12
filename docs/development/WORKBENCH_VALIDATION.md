# Workbench validation record

## Creation choices and combined demo login — 2026-11-12

Started from upstream `main` at `f3fdb09`, then copied into a separate checkout
and refreshed onto `1268952` before opening the PR. The existing creation UI already exposes
append/KV mode, collection and column compression, levels, dictionary policies
and type-filtered encodings when the connected server advertises them. The
running demonstration's Keycloak client had no browser-flow override, so its
organization discovery still displayed a username-only first page. Applying the
existing client-only migration fixed that deployment without changing its UI
assets or stored data.

The login migration command now has an explicit `--demo-user-id` option for the
user-requested public `hello`/`world` constants. It changes an existing managed
account through Keycloak and retains its UUID, organization, attributes and
roles. Original password policy and username-editing settings are restored after
the temporary administrative changes. Tests reproduce Keycloak's read-only
username refusal and cover restoration after reset/rename failures and refusal
of unrelated accounts. Normal provisioning does not invoke this option.

Executed on macOS arm64 with Python 3.13.5, Node 26.5.0 and Playwright
1.56.1/Chromium 1194:

- **PASS — 257 Node tests:** `npm --prefix ui test`.
- **PASS — 162 access tests, 12 explicit infrastructure skips:**
  `python -m pytest tests/access -q`. This includes 15 login-migration and demo
  credential tests. The isolated worktree used its own mock/SDK/verification
  sources through `PYTHONPATH`; an initial run with the original checkout's
  editable packages failed 12 capability checks, then passed with those paths
  corrected. No application change was needed for that environment mismatch.
- **PASS — 56 mock/static Chromium scenarios:**
  `npm --prefix ui run test:e2e`, with `GLAMDB_E2E_PYTHON` selecting the development
  interpreter and `GLAMDB_BROWSER_PATH` selecting Chromium. Five focused
  `write-settings.spec.mjs` scenarios include both modes with compression and
  column encoding in one create request and retention after adding a column.
  Their capability/metadata interception is UI contract evidence, not native
  Parquet-write evidence.
- **PASS — two native Chromium creation scenarios:** both modes create real
  collections with `gzip` level 6 and an `amount` override using `zstd` level 3
  plus `delta_binary_packed`. They write repeated keys through the UI, inspect
  verified Parquet column-chunk codecs/encodings, close/open each database and
  compare canonical settings and exact rows again (two rows for append, the last
  full row for KV, including int64 `9007199254740993`). The run used the installed
  Linux x86-64 native binary `26a0344` in a separate localhost process and
  disposable directory, serving the UI from `1268952` through an SSH tunnel.
  The new cases are in `native.spec.mjs`; reproduce locally with the native
  profile and `--grep "creation persists compression and encoding through reopen"`.
- **PASS — actual HTTPS demonstration:** a fresh Chromium context showed both
  credential fields before submission, accepted `hello`/`world`, and returned
  the original account subject and organization. The native backend advertised
  both modes, five compression codecs and six value encodings. Both modes and
  collection/column settings were selectable in its real creation dialog.
  That live check closed the dialog without submitting user-data writes.
- **PASS — UI formatting, changed Python compilation, local documentation links
  and `git diff --check`.** Independent final review reported no findings.

The full opt-in identity-infrastructure suite, full native suite and rebuild, sanitizers
and power-loss qualification were **NOT RUN** for this change. The focused live
login/form check does not replace those suites. The access tests retain the
existing Starlette/Authlib deprecation warnings.

## Arrow downloads and SDK stream ownership — 2026-11-12

Issue #57, based on official `main` at `d7c6be1`. Executed on Linux x86-64,
Python 3.13.18, HTTPX 0.28.1, PyArrow 25.0.0, Node 24.13.0 and pinned
Playwright 1.56.1/Chromium 1194. The existing native Debug service supplies real
exports; no native runtime, dependencies or persistent format changed.

- **PASS — 903 repository Python tests**, explicit mock profile. One later-page
  fault case is explicitly skipped because the legacy mock has no cursor pages.
- **PASS — 38 native SDK/setup/lifecycle tests**, including 23 SDK TCP/TLS,
  export-integrity and owner-lifetime cases. These require real native storage;
  native setup never falls back to mock.
- **PASS — 160 Node tests**, including 24 browser Arrow envelope cases.
- **PASS — ten native Chromium scenarios and 44 mock/static Chromium scenarios.**
  Empty downloads retain schema; an append after preview does not change its
  download; corrupted checksum/EOS/snapshot responses start no download, and a
  subsequent healthy export succeeds. PyArrow independently decodes saved files.
- **PASS — five native Arrow IPC encoder cases**, after rebuilding
  `glamdb_transport_test`: scalars/snapshot, empty schema, zero-column rows,
  schema/batch/EOS capacity accounting and invalid schema/unresolved view.
- **PASS — UI formatting, changed-file Python compilation, local documentation
  targets and whitespace checks.**

Focused regressions failed before the corresponding fixes. A fresh independent
review and Node/PyArrow probes found malformed schema-table pointers and metadata
versions, cleanup replacing consumer/later-page protocol errors, and the valid
null initial selector. Those cases now pass, including real native simultaneous
later-page snapshot/transport-close failure. No substantive review finding remains.
The bounded browser Message-envelope checker was retained over buffering SDK
responses or adding another Arrow runtime; logical schema/value decoding remains
with native/PyArrow encoding and integration tests, as described in
[client conformance](CLIENT_CONFORMANCE.md).

The full mock run exposed collection-budget interference from the additional
scenarios; they now own a separate disposable mock process. Two existing pending
recovery scenarios timed out during concurrent runs and passed in the final
sequential full run, without changing those tests or their implementation.
Chromium required the already available local shared-library bundle through
`LD_LIBRARY_PATH`; no system package or host trust-store changes were needed.
Python retains the existing Starlette/httpx deprecation warning.

Commands are in [CLIENT_CONFORMANCE.md](CLIENT_CONFORMANCE.md). Native pytest
additionally selects `tests/integration/test_backend_harness.py` and
`tests/integration/test_browser_lifecycle.py`. Encoder checks use
`cmake --build build/local --target glamdb_transport_test --parallel 2` and
`ctest --test-dir build/local -R '^glamdb\.transport\.ArrowIpc\.' --output-on-failure`.
Raw logs, exact commands and the review record remain in ignored
`artifacts/issue57/`, with separate native/mock browser reports. Sanitizers,
restart/restore and physical power-loss qualification were **NOT RUN** for this
client/transport change.

## Native browser process lifecycle — 2026-11-12

Follow-up to the #53 conformance implementation merged in PR #144, based on
official `main` at `09fa09e`. The native browser wrapper could remain in
`wait()` after forwarding SIGINT/SIGTERM; killing that wrapper could leave its
native child alive. A failed child launch also left a stale state marker.
Focused regressions failed on that base before the fix.

Signals now enter bounded TERM/KILL cleanup and reap the child before removing
its marker and disposable storage. Repeated signals cannot interrupt cleanup;
ordinary child exit codes are preserved. The independent review confirmed these
invariants with a TERM-resistant child and repeated mixed signals. Its remaining
test-readiness race was fixed by publishing the PID marker atomically. The
existing shared-backend design was retained; no alternative backend or process
manager was added.

Executed with Linux x86-64, Python 3.13.18, Node 24.13.0 and Chromium 1194:

- **PASS — 883 repository Python tests**, explicit mock profile, including five
  lifecycle cases covering SIGINT/SIGTERM, successful/failed child exits and
  failed startup. The focused five-case run also passed after the PID fix.
- **PASS — 23 native SDK/setup/lifecycle tests**: the existing eight shared
  TCP/TLS cases, ten backend setup cases and five lifecycle cases.
- **PASS — 136 Node tests, 40 mock/static Chromium scenarios, six native
  Chromium scenarios and five independently selected static scenarios.**
- **PASS — UI formatting, actionlint, offline zizmor and whitespace checks.**

Commands: `GLAMDB_TEST_BACKEND=mock .venv/bin/python -m pytest -q` and the native
SDK/browser commands in [CLIENT_CONFORMANCE.md](CLIENT_CONFORMANCE.md), adding
`tests/integration/test_browser_lifecycle.py` to the native pytest invocation.
Raw output is in ignored `artifacts/issue-53/followup-*.log`; browser reports
retain separate backend directories. Python emits the existing Starlette/httpx
deprecation warning. No native runtime, dependencies or persistent formats
changed, and no durability qualification is claimed.

## Shared client conformance — 2026-11-12

Executed on Linux x86-64 (Ubuntu 24.04), Python 3.13.18, Node 24.13.0 and pinned
Playwright 1.56.1/Chromium 1194. The native Debug server was rebuilt from upstream
base `135651d`; no native runtime source or persistent format changed.

- **PASS — 878 repository Python tests** with the explicit mock profile.
- **PASS — 18 native SDK/setup tests**: eight shared TCP/TLS scenarios and ten
  selection, identity, readiness-deadline and cleanup regressions. Native writes
  require independent SQLite → Iceberg JSON → Avro → Parquet inspection.
- **PASS — 136 Node tests** and eight native CI-runner tests. SDK failures stop
  browser acceptance, and its separate JUnit filename matches the CI upload glob.
- **PASS — six native Chromium scenarios**, including the shared contract and
  the five existing native recovery/database cases. The shared Arrow download
  is parsed and compared with the expected row and exact int64 value. Native
  physical inspection occurs after closing the test database.
- **PASS — five standalone static Chromium scenarios**, with an invalid inherited
  native executable, proving the static profile does not start an API backend.
- **PASS — 40 mock/static Chromium scenarios** with Python 3.13 selected through
  PATH, including isolated editor mock processes and the shared contract.
- **PASS — UI formatting, shell syntax, local documentation links and whitespace.**

A fresh independent review identified three harness regressions: partial startup
output could outlive the readiness timeout, a bare Python executable name was
incorrectly resolved as a relative file, and extracted mock tests lost proxy
isolation. Regressions were observed failing, then passing after a single bounded
readiness deadline, consistent interpreter selection and shared loopback fixture
were added. The mock scenarios also pass with a deliberately broken inherited
HTTP proxy. Review additionally caught the JUnit upload mismatch, which is fixed.
The shared-scenario design was retained to prevent separate backend suites from
drifting; mock-only fault injection and unsupported collection pagination stay
separate. A pre-existing stale mock logo expectation was updated to the supplied
transparent wordmark so the complete mock Python suite passes.

Commands are in [CLIENT_CONFORMANCE.md](CLIENT_CONFORMANCE.md). Native browser
verification additionally used `GLAMDB_E2E_PYTHON=python3` with the development
virtualenv on PATH, exercising the interpreter regression in a real browser.
Raw evidence is in ignored `artifacts/issue53-*.log` and `artifacts/issue53-*.xml`;
backend browser reports are separate. Chromium libraries were prepared locally;
no host trust store or system packages changed. Python reports the existing
Starlette/httpx deprecation warning. Sanitizer, restart/restore and G4 physical
power-loss qualification campaigns were **NOT RUN** for this test-harness change.
The earlier records below retain their original dates and scope.

## Native workbench and request recovery — 2026-11-11

Executed against the native C++ service and separately against the explicit
mock/static-demo profiles:

- **PASS — 127 Node tests**, including real Fetch transports, selected-view
  metadata identity/cursor guards, explicit native/mock separation, token-free
  pending request round trips, exact int64 bounds and optional editor settings.
- **PASS — 63 Python mock tests**, including 18 theme cases. Old themes without
  `editor` remain valid; `editor.query_max_rows` accepts integers 1..10000 and
  rejects booleans, other types and unknown settings.
- **PASS — 39 mock/static-demo Chromium scenarios.** The five native-only cases
  are explicitly excluded in this profile. Recovery cases use their own
  disposable mock child and retain the normal collection budget.
- **PASS — five native Chromium scenarios**, with `backend=local`, `mock=false`
  and real stats/history/files/explain capabilities. They cover all seven data
  views, browser reload, database close/open, latest/A/initial rows and files
  (2/1/0), precise values, published operation IDs and authoritative lookups,
  out-of-order selections, reconciliation and unknown writes. A lost committed
  receipt is exported, imported after reload and retried with the original
  request; the collection remains one row, one snapshot and one file.
- **PASS — UI Prettier and whitespace checks.**

The native profile exposed a normal-path admission failure: four parallel
metadata requests exceeded the two per-database data-call slots. Metadata now
loads sequentially. Creation and append outcomes remain terminal if metadata
refresh fails. Import restores original UUIDs/schema/rows/preconditions without
submitting a write; a definite creation failure unlocks the form, and resolved
creation drafts stop appearing in cached database recovery lists.

The actual native screenshot audit exposed file details hidden behind long raw
JSON. The dialog now leads with row/file/footer counts and selected snapshot,
row groups and tables of field IDs, types and bounds; complete JSON is collapsed. Desktop
and 390px mobile screenshots were visually inspected. A full-visibility browser
assertion catches the mobile grid overflow that a document-width check missed.
Artifacts are under `artifacts/e2e-native-results/` as
`native-file-details-desktop.png` and `native-file-details-mobile.png`.

Connected result limits keep the 1000 default and allow up to 10000, bounded by
`limits.scan_page_rows` and optional `editor.query_max_rows`. Browser tests send
an actual 1010 request and verify runtime clamping to 512 without losing drafts.
The static demo retains its 1000 maximum. Regressions for the new flows were
observed failing before their fixes.

Commands: `npm --prefix ui test`, `npm --prefix ui run format:check`,
`npm --prefix ui run test:e2e`, and the explicit native profile documented in
[WORKBENCH.md](WORKBENCH.md). Python used
`python -m pytest apps/mock_server/tests -q`. Native browser setup used an
isolated temporary child root and separate ports, with the locally available
Chromium executable selected through `GLAMDB_BROWSER_PATH`.

This isolated browser profile does not perform a service-process restart,
whole-store restore or physical-chain inspection. Those belong to the shared
native acceptance workflow. Browser reload, orderly reopen and footer reads do
not qualify physical power-loss durability. The records below retain their
original dates and scope.

## Review corrections — 2026-11-10

All five findings from [review 5843480730](https://github.com/glamourdb/glamdb/pull/4#pullrequestreview-5843480730)
have focused regression coverage:

- Disconnect/reconnect resets private connection/view/query state and uses a
  new API identity. Old create responses cannot clear the next session's pending
  identity or refresh its UI. A pending Arrow export is cancelled and discarded;
  late operation errors cannot restore private text in notifications.
- Fetch gzip/Brotli responses use a bound on actual decoded bytes; encoded
  Content-Length is not compared to the decompressed body. Identity/truncated
  response checks and append outcome handling remain covered.
- ACK-ledger creation and every reopen require a successful parent-directory
  fence before reading receipts or accepting writes. Four fresh-process tests
  cover ordinary/interrupted initialization and successful/failed reopen fences;
  failed opens leave the ledger bytes unchanged.
- SDK row sizes use one canonical decoded-size helper, including presence and
  string-length markers, fixed-size integers/timestamps, booleans and NULLs.
  Exact 2 MiB and one-byte-over cases cover input, wire decode, append preparation
  and scan JSON; existing literal canonical hash vectors are unchanged.
- File details capture their connection, collection, resolved snapshot and view
  epoch; obsolete reads are cancelled and late responses cannot open a dialog.

After these source changes, `bash scripts/check-workbench.sh` exited **0**:
**413 Python tests, 36 Node tests and 31 actual Chromium scenarios** passed.
Python in-process coverage is **89%** (2386 statements, 262 missed). Both literal
golden checks and the 204-cut simplified ordering model passed; UI format check
passed. New regressions were observed failing before their fixes, including the
original UI's foreign-snapshot reconnect error. Desktop screenshots were checked.
Raw output: `artifacts/review-5843480730-check.log` (ignored).

Public demo refreshed at https://nnrpi.github.io/glamdb/ from code commit
`8a4489638294f4c8969787354d2aab36d2b600fe`; artifact commit
`5983b3cd24b7b43e1f2decc38de3f9053cd4540d`. GitHub Pages reports a successful
deployment. Anonymous HTTPS200 and exact SHA256 matches passed for index.html,
app.js, api.js, model.js and both PNG logos; **all five external Chromium
scenarios passed** against the updated site. This remains a browser-only demo.

These results exercise client/verifier/mock behavior, including real local
HTTP/HTTPS and browser transports. Native C++ sources, public headers, native
tests, CMake and dependencies remain unchanged from main `b3fed8b`. Native
Linux suites and physical power-loss qualification were **not rerun** here;
the following earlier records retain their original scope and limitations.

## Earlier integration and publication

Rebase integration: source branch rebased onto main `b3fed8b` (PR #5 native
catalog/coordinator/API). Six documentation conflicts were combined so both
native implementation status/build guidance and client/public-demo instructions
remain. Native sources, public headers, CMake and CTest drivers are unchanged
relative to main. Root pytest explicitly collects `test_*.py`; native
`*_test.py` command drivers remain owned by CTest and require compiled binaries.
The native Linux dependency/build suites are separate from this macOS client
verification; the earlier scaffold results below are historical, not a claim
that the newly merged C++ API was rebuilt here.

Public-demo/branding follow-up: both supplied PNGs are copied unchanged and
SHA256 checked. New browser-only mode runs without a token, network API or
browser storage, including under /glamdb/. New local validation has **374 Python
tests, 26 Node unit tests and 23 Chromium scenarios**. Capacity rejection and
legacy image-free theme regressions were observed RED before fixes, then GREEN.
Desktop/mobile logo screenshots were inspected. See [PUBLIC_DEMO.md](PUBLIC_DEMO.md)
for the separate UI-only semantics and publishing path. The original baseline
record below remains evidence for its earlier scope; neither mode qualifies core
storage durability.

Live publication: https://nnrpi.github.io/glamdb/, fork gh-pages artifact
`eeeb476523020dc0f4c7b8ed8a7452647edc16e2`. GitHub reports built/public/HTTPS
enforced. Anonymous HTTP200 and exact logo SHA256 checks passed; all five
public-demo browser scenarios passed against the external URL. No token or
private/runtime files were published. PR synchronization with newer main is
tracked separately from the successful public deployment.

Scope: independent participant-four tools, Python network SDK, browser UI and a
disposable mock for absent core/HTTP dependencies. **No G1–G4 or production N
gate is declared complete.** The HTTP/HTTPS and Chromium tests below use real
transports against synthetic process-local state, not a durable C++ database.

Base: `glamourdb/glamdb` commit
`e0dfdc9ab6004f45a2dd53bfb47786cb3c0d2d45`. Fork: `nnrpi/glamdb`, branch
`feat/pink-workbench-and-role4-verification`, upstream target `main`.
Published for review as [PR #4](https://github.com/glamourdb/glamdb/pull/4);
no merge performed. HTTPS OAuth lacked workflow scope; the existing SSH
credential successfully published the complete branch including CI. Only this
repository's push URL was set to SSH; global Git configuration was unchanged.

Environment: Darwin25.3.0/arm64, Python3.13.5, AppleClang21.0.0,
CMake4.4.4, Ninja1.13.2, Node26.5.0/npm11.17.0, LLVM format/tidy18.1.8,
Playwright1.56.1/Chromium1194. Python dev versions and release SHA256 hashes
are fixed in `requirements-dev.txt`; browser tooling is locked by npm.

## Executed final checks

`bash scripts/check-workbench.sh` exited **0** after the final source changes:

- **PASS — 366 Python tests:** 142 verifier/CLI/metadata-fixture cases,
  172 SDK cases, 42 mock cases, 10 actual localhost HTTP/HTTPS integration cases.
  In-process package coverage: **89%** (2384 statements, 263 missed).
  CLI subprocesses and browser-server processes are exercised but their lines
  are not included in this in-process coverage number.
- **PASS — both literal canonical golden vectors:** exact preimages/hash,
  stable schema IDs, NULLs, duplicates, UTF-8, timestamp boundaries and int64
  above 2^53. Additional metadata fixtures cover full long/UTF-8 key bounds,
  missing/truncated footer stats and explicit expected matches. Physical
  writer/manifest comparison remains NOT RUN.
- **PASS — simplified ordering helper:** seed17, 104 deterministic plus100
  seeded cuts, with omitted-data-sync, omitted-directory-sync and early-ACK
  negative controls detected. This model assumes one atomic durable catalog
  commit, without SQLite page/journal emulation. Power durability stays
  **NOT VALIDATED** and the unified-image T-POWER gate stays open.
- **PASS — UI format check and 16 Node unit tests.**
- **PASS — 17 actual Chromium browser scenarios**, no uncaught JS errors or
  unexpected console errors. Covered desktop/mobile, keyboard/reduced motion,
  all metadata views, create/append/duplicates, precise int64, typed filters,
  hidden predicate columns/NULL, initial/zero-limit/history, Arrow download,
  runtime config edits and invalid reload, failed authentication, escaped HTML,
  lost-create exact identity recovery, conflicting name correction, frozen
  unknown retries across collections, late-dialog/read/status races, metadata
  refresh failure after commit, and disabling retry once a receipt is resolved.

The HTTP/HTTPS integration uses child processes and real TCP sockets. It covers
JSON↔Arrow↔SDK, retained historical results, old retries, pagination, real
dropped responses, temporary CA trust and wrong/untrusted CA rejection. Test
keys are disposable and the host trust store is untouched. This is network
evidence only; mock restart intentionally discards state.

The final command used `GLAMDB_BROWSER_PATH` to point at the local official
Chromium binary, as documented in [WORKBENCH.md](WORKBENCH.md). Raw output is
`artifacts/final-check.log`; coverage XML, ordering-model JSON and Playwright
report are under `artifacts/`. Generated artifacts stay ignored.

## Packaging, build and tooling

- **PASS — non-editable wheels** built for `glamdb-verify`, `glamdb-sdk`, and
  `glamdb-mock` with `pip wheel --no-deps --no-build-isolation`.
- **PASS — independently installed SDK:** a fresh venv, execution from outside
  the checkout with Python `-I`, no installed mock/verifier, and all172 SDK tests.
  The other installed-wheel suites also pass; CLI fixture checks/help work
  without source-path imports. No source-tree/private C++ dependency is needed.
- **PASS — all28 locked Python dependency wheels validated with
  `--require-hashes --only-binary=:all: --no-deps`; `pip check` reports no broken
  requirements. npm dependency versions/integrity are in `ui/package-lock.json`.
- **PASS — local/debug, release and ASan+UBSan CMake configure/build/smoke**,
  one no-op smoke test per profile. The debug executable also installs/runs.
  This tests the existing scaffold, not engine lifetimes or storage behavior.
- **PASS — LLVM18 format-check, tidy-check and quality-negative-check**:
  intentional formatting and analyzer violations are rejected.
- **PASS — pinned submodule revisions** with
  `cmake -P cmake/dependencies/VerifySubmodules.cmake`. Submodules were
  initialized at the pinned commits; no vendor code or pins were changed.
  Building/linking the complete C++ dependency stack was not required or run.
- **PASS — actionlint1.7.12** for both workflows, using the official macOS
  archive verified against its release checksum.
- **PASS — zizmor1.32.0**, pinned by the existing hash lock,
  `--no-online-audits --pedantic --format plain .github/workflows`: no findings.
  PR permissions remain read-only; the new workflow reuses existing SHA-pinned
  checkout/setup-python Actions and adds no release credentials.
- **PASS — shell syntax, relative documentation file links and git diff
  whitespace checks.** The GitHub-hosted Linux run is a separate CI result;
  local macOS success does not imply that remote run has already executed.

## Visual inspection and fixed regressions

The UI was operated in real Chromium by Playwright. Desktop overview/data and
mobile screenshots were inspected visually for typography, controls, pink
decoration, layout and clipping:

- `artifacts/e2e/overview-desktop.png`
- `artifacts/e2e/data-desktop.png`
- `artifacts/e2e/data-mobile.png`

Regression tests were observed failing before fixes for lost-create response,
cross-dialog append status, pending-operation replacement, older scan response,
duplicate-create rejection and terminal-outcome regression. SDK receipt tests
first exposed39 failures for malformed/mismatched hashes, versions and receipt
fields. Subsequent runs pass; a committed append must match its prepared hash.
Errors fetching metadata after commit no longer relabel the committed write.

During final integration, duplicate `test_cli` module names exposed pytest's
default import collisions. Root pytest configuration now uses importlib mode
and explicit test roots; integration helpers use a relative package import.
Metadata expectations live separately from canonical-byte fixture files so
the canonical checker continues to reject incompatible fixture formats.

## Environment issues and explicit limits

The default sandbox denied localhost sockets/browser bootstrap. Those tests
were rerun with allowed execution access, without skips. The CUA browser host
was unavailable; native CUA/manual-browser control is **NOT RUN**. Chromium was
instead controlled through Playwright and its screenshots inspected.

The Node26 Playwright installer stalled during extraction after downloading
Chromium. The official ZIP passed CRC checking and was extracted with system
unzip; the documented executable override ran all17 tests. No local absolute
binary path or modified vendor code is committed. Starlette's TestClient warns
about deprecated httpx support; Node26 emits upstream deprecation/color warnings.
They remain visible and do not fail the checks.

This client report did not run native Parquet/Iceberg/SQLite integration, real
create/close/reopen and recovery, or C++ retained-buffer lifetime tests. Native
suites added later cover them; see the [roadmap status](../ROADMAP.md#current-status).
The following remain **NOT RUN / NOT VALIDATED**: physical manifest
metrics and pruning, capacity/ingestion qualification, full SQLite-image fault
modeling, native process-crash campaigns, qualified Linux/ext4 power cuts, S3 and
KV/COW/compaction. No destructive test was run on the user's volume. The mock
and simplified model cannot establish these claims.

See the [workbench guide](WORKBENCH.md) for reproducible commands and the
[roadmap](../ROADMAP.md) for the remaining product milestones.
