# Workbench validation record — 2026-11-10

## Review corrections

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

The following remain **NOT RUN / NOT VALIDATED**: native Parquet/Iceberg/SQLite
integration, physical manifests/metrics/pruning, real create/close/reopen and
recovery, C++ retained-buffer lifetime/capacity/ingestion qualification,
full SQLite-image fault modeling, native process-crash campaigns, qualified
Linux/ext4 power cuts, S3 and KV/COW/compaction. No destructive test was run on
the user's volume. The mock and simplified model cannot establish these claims.

See the [workbench guide](WORKBENCH.md) for reproducible commands and the
[roadmap](../ROADMAP.md) for the remaining product milestones.
