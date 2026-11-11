# Workbench: run, use, configure, and test

The browser workbench and Python SDK can connect to the [native HTTP server](HTTP_SERVER.md)
for persistent create/append/scan operations. Native capabilities identify
metadata and explain operations that remain unavailable. The **disposable mock**
is a separate development profile: it writes no Parquet or catalog, and all its
data and outcomes vanish on restart. Storage power-loss qualification remains open.

For token-free access by external visitors, use the separately packaged
[public browser demo](PUBLIC_DEMO.md). That mode keeps data in each page and
exports JSON. The instructions below start authenticated HTTP/Arrow mode.

## Start from a clean checkout

Requirements: Python 3.13, Node 22+ with npm, and an OpenSSL CLI for HTTPS tests.
No C++ dependency build is needed for this slice. From the repository root:

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
- **Storage:** inspect synthetic file/row-group descriptors. Unknown physical
  byte and I/O values stay unknown; no physical file exists in this mock.
  On a connected server without file listing, show confirmed receipt additions
  (snapshot, rows, files and compressed bytes where supplied), not a fabricated
  file inventory or total storage size.
- **Explain:** inspect the selected query plan separately from execution
  measurements. Page/Bloom/row-group pruning are explicitly unsupported here.
- **Operations:** check collection-scoped append outcomes submitted in this tab.
  A timeout or unknown response freezes the payload and original UUID for retry.
  Terminal outcomes stay terminal even if an older status reply arrives late.
- **Style studio:** preview palette/decoration changes and download JSON.
  Permanent configuration is an operator edit on disk.

Append preserves duplicate keys and input rows. Quote int64 values in JSON,
especially those above 2^53. Use RFC3339 timestamps with a timezone and no more
than six fractional digits. NULLs are explicit or omitted nullable fields;
only SQL TRUE survives a predicate. The browser caps previews at 1,000 rows and
100 displayed rows per page. Append batches are capped at 10,000 rows plus
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
Session receipts vanish on disconnect/reload; they do not replace durable
history or file metadata APIs.

Credentials stay in process/tab memory, never localStorage, sessionStorage or
URLs. Disconnect clears credentials, metadata, results, snapshot selection,
query drafts and operation history; only the server URL and theme preferences
remain. Reads from the previous connection are cancelled or ignored, including
file details and pending Arrow exports. Browser session operation history is also
ephemeral: persist original IDs and payloads in the calling application when
real recovery across restarts is required. The SDK requires caller-supplied IDs.
The mock serves same-origin API/UI; direct access to another origin requires
appropriate production proxy/CORS work at the future adapter boundary.

## Runtime design

Edit [`ui/config/theme.json`](../../ui/config/theme.json) while the server and
browser are open. `/workbench/config` rereads it on every request; the browser
polls every two seconds. There is no frontend build step or server restart.
`--theme-file /path/to/theme.json` selects an operator-owned file instead.

Version 1 has six sections: `brand`, `colors`, `typography`, `sizing`,
`decoration`, and `labels`. The supplied configuration exposes all palette
colors, body/display/mono fonts, major layout widths/gaps, radii, shadows,
control/table heights, titles, main copy and navigation labels. Decoration has
independent `bows`, `kisses`, `sparkles`, and `motion` switches. OS reduced-motion
preferences override animation. Icons are local SVG, fonts are local/system;
no analytics or third-party asset request is needed.

The read/write layout uses `sizing.editor_gap`, `editor_padding`,
`editor_control_gap`, and `editor_rows_max_height`. Its headings and batch-editor
label are in `labels`, so spacing and copy can change without rebuilding.

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
Browser regressions include reconnecting with a different token/collection and
late file/create/export responses from the previous session. The browser
server is a disposable child on port 8766, with a separate copied theme. Override
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

See [WORKBENCH_VALIDATION.md](WORKBENCH_VALIDATION.md) for executed results,
[the SDK guide](../../sdk/python/README.md) for Python calls, and
[the verifier guide](../../tools/verification/README.md) for fixture/fault tools.
The simplified model assumes atomic durable catalog commit and cannot close
the full SQLite/power-loss qualification gate.

## Handoff to the actual engine

Replace the provisional HTTP mock with an adapter over the single C++ write
authority. Preserve caller identities, precise scalar encoding, immutable
receipts, all three outcomes, fixed-view cursors and context-managed stream
ownership. Reuse the independent golden fixtures against actual C++ outputs;
the mock deliberately reuses the row oracle, so it is not an independent test
of a native scanner. Implement actual file/row-group metadata and report only
supported pruning. Run fresh-process reopen, full fault and physical power-loss
campaigns before making durability claims.

See the [architecture](../ARCHITECTURE.md), [roadmap](../ROADMAP.md) and
[validation report](WORKBENCH_VALIDATION.md) for supported behavior and the
boundaries of the current client/mock implementation.
