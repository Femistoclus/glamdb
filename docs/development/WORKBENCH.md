# Workbench: run, use, configure, and test

The browser workbench and Python SDK can connect to the [native HTTP server](HTTP_SERVER.md)
for persistent create/append/scan operations and native snapshot history,
selected-view statistics, manifest file inventory and query plans. Capabilities
select the available adapters; unknown measurements remain unknown. The **disposable mock**
is a separate development profile: it writes no Parquet or catalog, and all its
data and outcomes vanish on restart. Storage power-loss qualification remains open.

For token-free access by external visitors, use the separately packaged
[public browser demo](PUBLIC_DEMO.md). That mode keeps data in each page and
exports JSON. The instructions below start authenticated HTTP/Arrow mode.

## Start from a clean checkout

Requirements: Python 3.13, Node 22+ with npm, and an OpenSSL CLI for HTTPS tests.
Mock development needs no C++ dependency build; the native profile needs a built
or installed `glamdb` service. From the repository root:

```sh
bash scripts/bootstrap-workbench.sh
export GLAMDB_TOKEN='choose-a-local-development-token'
.venv/bin/python -m glamdb_mock --ui-dir ui --seed-demo
```

Open **http://127.0.0.1:8765**, leave Server URL unchanged, enter your token and
choose **Open my workspace**. The two sample collections are synthetic. Without
`--seed-demo`, the workbench starts empty. Stop the server with Ctrl-C. Change
the port with `--port 8767`; the token is mandatory and never printed by the
server. The default loopback binding is intended for local development.

If Python 3.13 has another executable name, prefix bootstrap with
`GLAMDB_PYTHON=/path/to/python3.13`. Packages are installed in `.venv`; npm uses
`ui/package-lock.json`. Python dev dependencies have exact versions and SHA256
release hashes in `requirements-dev.txt`. Local packages install without build
isolation after their pinned build dependencies are present.

## What the screens do

- **Overview:** selected-view row count, column count, retained snapshots,
  file count, schema preview and history.
- **Data studio:** two compact panels separate reading from writing. Select
  output columns and supported typed filters, then run a bounded preview.
  Fill schema-driven fields in **Write rows**; **Add row** adds another record
  and nullable fields have an explicit NULL switch. JSON is generated and
  validated automatically, preserving int64 precision. **Batch editor** opens
  the optional JSON array editor. **Query JSON** previews and downloads the
  request assembled by the read controls. Arrow export retains the exact result
  snapshot; key lookup is a filter scan.
- **Schema:** fixed types, field IDs and nullability. There is no schema editor
  after collection creation.
- **Snapshots:** explore retained historical views or the initial empty view.
  Selecting a view clears the previous result; results identify their own fixed
  snapshot. Snapshot IDs are opaque, not row counts. Without the metadata
  capability, show only confirmed append receipts observed in this session,
  explicitly labelled as incomplete history.
- **Storage:** list committed files belonging to the selected snapshot. Native
  file details verify Parquet footer metadata and summarize row counts, file/footer
  bytes, row groups, field IDs, types, bounds and reported bound precision. Complete JSON
  remains available in a collapsed section; wide tables scroll within the dialog.
  The explicit mock and browser demo show synthetic descriptors; they write no
  physical files. Unknown byte and I/O measurements remain unknown.
  On a connected server without file listing, show confirmed receipt additions
  (snapshot, rows, files and compressed bytes where supplied), not a fabricated
  file inventory or total storage size.
- **Explain:** inspect the selected query plan separately from execution
  measurements. Page/Bloom/row-group pruning are explicitly unsupported here.
- **Operations:** check collection-scoped append outcomes or look up an original
  operation UUID after reload. A timeout or unknown response freezes the payload
  and original UUID for retry. Native **Published snapshot operations** lists IDs
  from retained server history after reload and queries authoritative outcomes.
  Terminal outcomes stay terminal even if an older status reply arrives late.
  Confirmed create and append results remain confirmed if the following metadata
  refresh fails. A definite creation failure releases its frozen form for correction.
- **Style studio:** preview palette/decoration changes and download JSON.
  Permanent configuration is an operator edit on disk.

Append preserves duplicate keys and input rows. Quote int64 values in JSON,
especially those above 2^53. Use RFC3339 timestamps with a timezone and no more
than six fractional digits. NULLs are explicit or omitted nullable fields;
only SQL TRUE survives a predicate. The browser defaults to a 1,000-row preview and
displays 100 rows per page. Connected previews can request up to 10,000 rows,
within the backend page bound and configured editor ceiling; the static demo
retains its 1,000-row maximum. Append batches are capped at 10,000 rows plus
decoded/body byte limits. This is append v0, without upsert, delete, compaction,
jobs, S3, uniqueness or durable mock outcomes.

The field editor accepts up to 100 rows; the optional JSON batch editor accepts
up to 10,000, within server byte limits. Unknown writes freeze both editors
around the same operation ID and payload until a terminal outcome is known.

When native statistics are unavailable, **Count rows** in Overview reads one
column of the selected snapshot through existing scan pages, discards each page
and shows a total only after completion. This can be expensive; it runs only on
request and can be cancelled. Collection, snapshot and connection changes cancel
it too. A complete confirmed receipt chain from the initial empty view can also
establish a count. Partial query previews never become collection totals.
Session receipts vanish on disconnect/reload; connected native history,
statistics and files are loaded from the server again. Session receipts do not
replace those APIs. Metadata failures do not silently switch to a mock or turn
partial query previews into totals.

Credentials stay in process/tab memory, never localStorage, sessionStorage or
URLs. Disconnect clears credentials, metadata, results, snapshot selection,
query drafts and operation history; only the server URL and theme preferences
remain. Reads from the previous connection are cancelled or ignored, including
file details and pending Arrow exports. Browser session operation history is also
ephemeral. Use **Pending requests** to export an unresolved append or creation
before reload or disconnect. The JSON file contains the original collection and
operation UUIDs, schema, rows and optional snapshot precondition, without the
access token. Disconnect with unresolved requests offers export before explicit
**Disconnect and clear**. After reconnecting, select the same database, open
**Pending requests**, choose **Recovery JSON file** and **Import pending request**.
Import validates the server/database/backend/store binding and collection schema,
then restores the frozen request. It never submits a write or generates a new
UUID. Choose **Check outcome** or **Retry same operation** explicitly; an imported
creation keeps its original UUID, name and schema. The SDK likewise requires
caller-supplied IDs. Recovery files contain user row data and remain caller-owned;
no recovery state or credentials are written to browser storage. Export and import
share a separate 32 MiB UTF-8 recovery-file limit, including the complete JSON
envelope, schema, server/database scope and formatting. This limit is independent
of HTTP request and response limits. Export refuses an oversized file rather than
offering a download that cannot be imported; import checks file size before
reading or parsing it. Existing version-1 pretty-printed files remain supported.
The mock serves same-origin API/UI; direct access to another origin requires
appropriate production proxy/CORS work at the future adapter boundary.

## Runtime design

Edit [`ui/config/theme.json`](../../ui/config/theme.json) while the server and
browser are open. `/workbench/config` rereads it on every request; the browser
polls every two seconds. There is no frontend build step or server restart.
`--theme-file /path/to/theme.json` selects an operator-owned file instead.

Version 1 has six visual sections: `brand`, `colors`, `typography`, `sizing`,
`decoration`, and `labels`, plus an optional `editor` object for request limits. The supplied configuration exposes all palette
colors, body/display/mono fonts, major layout widths/gaps, radii, shadows,
control/table heights, titles, main copy and navigation labels. Decoration has
independent `bows`, `kisses`, `sparkles`, and `motion` switches. OS reduced-motion
preferences override animation. Icons are local SVG, fonts are local/system;
no analytics or third-party asset request is needed.

The read/write layout uses `sizing.editor_gap`, `editor_padding`,
`editor_control_gap`, and `editor_rows_max_height`. Its headings and batch-editor
label are in `labels`, so spacing and copy can change without rebuilding.

`editor.query_max_rows` is an optional integer from 1 through 10,000 and defaults
to 10,000. The connected **Result limit** control uses the smaller of that
ceiling, the backend's `limits.scan_page_rows`, and 10,000; its default value
remains 1,000, reduced when the effective maximum is smaller. The static demo
keeps a 1,000-row ceiling. Runtime edits update the control's maximum and clamp
an oversized value while preserving the other query and write drafts. For
example, enter **1010** in **Result limit** to read and export all 1,010 rows of
the shared A/B acceptance fixture. The request uses that value; changing only
the DOM's `max` attribute does not change validation or the query.

`brand.wordmark` and `brand.mark` select the supplied local PNGs under
`assets/brand/`; remote URLs/traversal are rejected. `sizing.logo_width`,
`login_logo_width` and `logo_mark_size` control placement without image editing
or a binary rebuild. Old themes without image paths retain text/icon fallbacks.

CSS custom properties are generated from validated tokens. Colors are hex,
dimensions use px/rem/em/% or zero, and font values are constrained text.
Malformed or unsafe configuration shows a warning and preserves the last valid
theme. Style studio previews suspend disk overrides until **Reset preview**;
downloaded JSON can replace the configured file. Fine layout rules remain in
`ui/assets/styles.css`, also served directly and editable without compilation.
There is no authenticated server-configuration write endpoint in this slice.

## Verification

Install the test browser once:

```sh
cd ui
npx --no-install playwright install chromium
cd ..
bash scripts/check-workbench.sh
```

On Linux use `playwright install --with-deps chromium`. The script runs Python
unit and real localhost HTTP/HTTPS tests, golden CLI checks, the simplified
ordering model, UI format/unit checks and real Chromium scenarios. HTTPS uses
temporary test certificates; no host trust store is modified. The browser
and Node API tests need localhost sockets; Node tests use real gzip/Brotli HTTP
responses, decoded-byte limits, truncated bodies and cancelled Arrow requests.
Browser regressions include reconnecting with a different token/collection,
late file/create/export responses, token-free pending export/import after reload,
exact retry bodies and non-2xx create outcomes. New recovery scenarios use their
own disposable mock child so they do not consume another suite's collection
budget. The browser server is a disposable child on port 8766, with a separate copied theme. Override
it using `GLAMDB_E2E_PORT`. Open the HTML report with:

```sh
cd ui
npx --no-install playwright show-report ../artifacts/playwright-report
```

`npm --prefix ui run test:e2e:headed` visibly operates Chromium. For a locally
installed Chromium binary, `GLAMDB_BROWSER_PATH=/absolute/path/to/Chromium`
overrides Playwright's executable path. No host-specific path is committed.
Tests fail if browser prerequisites or socket permissions are missing; they do
not silently skip integration. Run in an environment allowing localhost and
browser sandbox access.

The explicit native profile starts a fresh owning child in a temporary test
root, uses real native capabilities and never substitutes a mock:

```sh
GLAMDB_NATIVE_SERVER=/absolute/path/to/glamdb \
GLAMDB_E2E_TEMP_ROOT=/absolute/path/to/disposable-test-parent \
npm --prefix ui run test:e2e
```

The parent directory must already exist. The profile checks every native view,
reload, initial/historical/latest selection, database reopen, original outcome
lookup and lost-receipt export/import with exact retry. Metadata reads run one
at a time to leave admission capacity for queries. This fresh browser profile
complements the shared CLI/SDK/browser restart acceptance harness; it does not
replace restart or physical-storage verification.

See [WORKBENCH_VALIDATION.md](WORKBENCH_VALIDATION.md) for executed results,
[the SDK guide](../../sdk/python/README.md) for Python calls, and
[the verifier guide](../../tools/verification/README.md) for fixture/fault tools.
The simplified model assumes atomic durable catalog commit and cannot close
the full SQLite/power-loss qualification gate.

## Remaining engine integration

The [native HTTP server](HTTP_SERVER.md) is the adapter over the single C++
write authority; keep preserving caller identities, precise scalar encoding,
immutable receipts, all three outcomes, fixed-view cursors and context-managed
stream ownership there. The mock deliberately reuses the row oracle, so it is
not an independent test of the native scanner; compare native outputs with the
independent golden fixtures instead. Native history, selected-view statistics,
file membership and verified footer details, and explain use the public metadata
facade through the HTTP adapter.
Snapshot IDs remain exact decimal strings; initial empty, historical and latest
views are distinct. Cursor pages must retain the same collection and selected
view; stale reads are cancelled or ignored on selection/connection changes.
Execution profiles and plan estimates remain separate, and page/Bloom execution
is unavailable. Full fault and physical power-loss campaigns are still required
before any durability claim.

See the [architecture](../ARCHITECTURE.md), [roadmap](../ROADMAP.md) and
[validation report](WORKBENCH_VALIDATION.md) for supported behavior and the
boundaries of the current client/mock implementation.

See [interaction checks and comparisons](WORKBENCH_UX.md) and the [installed shared-store acceptance recipe](NATIVE_ACCEPTANCE.md) for actual native evidence.
