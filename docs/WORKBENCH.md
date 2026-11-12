# Browser workbench

The browser workbench and Python SDK can connect to the [native HTTP server](HTTP_SERVER.md)
for persistent create/append/scan operations and native snapshot history,
selected-view statistics, manifest file inventory and query plans. Capabilities
select the available adapters; unknown measurements remain unknown. The **disposable mock**
is a separate development profile: it writes no Parquet or catalog, and all its
data and outcomes vanish on restart. Storage power-loss qualification remains open.

For token-free access by external visitors, use the separately packaged
[public browser demo](PUBLIC_DEMO.md). That mode keeps data in each page and
exports JSON. The instructions below start authenticated HTTP/Arrow mode.

## Connect to a server

Start the persistent local service with [the quick start](../README.md#get-started),
open `http://127.0.0.1:8080`, and enter `GLAMDB_TOKEN`. Create or select a database,
then create an append or KV collection. Remote deployments use the same-origin
HTTPS proxy described in [the HTTP guide](HTTP_SERVER.md).

An organization deployment instead uses [Keycloak sign-in](SSO_OPERATIONS.md)
and lists only authorized databases. The [mock adapter](../apps/mock_server/README.md)
is an explicit disposable option for local UI work.

## What the screens do

- **Overview:** selected-view row count, column count, retained snapshots,
  file count, schema preview and history.
- **Data studio:** two compact panels separate reading from writing. Select
  output columns and supported typed filters, then run a bounded preview.
  Fill schema-driven fields in **Write rows**; **Add row** opens the new record,
  collapses the other rows and scrolls the editor list to the new row without
  moving the page. Nullable fields have an explicit NULL switch. JSON is generated
  and validated automatically, preserving int64 precision. **Batch editor** opens
  the optional JSON array editor. **Query JSON** previews and downloads the
  request assembled by the read controls. Arrow export retains the exact result
  snapshot. Append key searches use filters; KV adds native batched key lookup.
- **Schema:** fixed types, field IDs and nullability. There is no schema editor
  after collection creation. When the server advertises `features.write_settings`
  and a `parquet` capability object, **Parquet write settings** shows the
  collection's canonical settings and version for data files written by later
  appends (not HTTP compression; existing files keep their codecs and encodings):
  collection compression codec/level, dictionary policy and dictionary page size
  limit, and per column a codec/level, dictionary policy and value encoding.
  Encoding and dictionary choices are filtered by the column type from
  `parquet.encodings` and `parquet.dictionary`; servers without those
  capabilities show compression only. Choosing an encoding locks that column's
  dictionary select and clears an `enabled` (or non-`bool` `auto`) policy, because
  an explicit encoding turns the dictionary off. A collection `enabled` policy
  with a `bool` column lacking its own policy or encoding is explained before
  sending; the server remains authoritative. **In effect** follows the server's
  precedence. **Save write settings** sends a compare-and-set with the shown
  version; if another client changed them first (409 `WRITE_SETTINGS_CONFLICT`)
  the panel reloads the current version and asks you to review it. After an
  unknown outcome, saving the same settings again is safe. **New collection**
  offers the same choices (level and page size ranges come from capabilities);
  with no choice it sends no `write_settings`, and requests omit unset options.
  Created/resolved collections are verified against the requested canonical
  settings while `write_settings_version` is still the creation version (`"0"`
  without settings, `"1"` with them); later updates leave only identity and
  schema comparable. Settings that set options unknown to this workbench are
  shown read-only. The explicit mock and the public demo do not advertise write
  settings and never show these controls.
- **Snapshots:** explore retained historical views or the initial empty view.
  Selecting a view clears the previous result; results identify their own fixed
  snapshot. Snapshot IDs are opaque, not row counts. Without the metadata
  capability, show only confirmed append receipts observed in this session,
  explicitly labelled as incomplete history.
- **Storage:** list committed files belonging to the selected snapshot. Native
  file details verify Parquet footer metadata and summarize row counts, file/footer
  bytes, row groups, field IDs, types, bounds, reported bound precision and the
  Parquet codec and value encodings each column chunk was written with; a
  dictionary that outgrew its page size limit is marked as a fallback (data
  pages in both a dictionary encoding and `plain`, from `encoding_stats`). Complete JSON
  remains available in a collapsed section; wide tables scroll within the dialog.
  The explicit mock and browser demo show synthetic descriptors; they write no
  physical files. Unknown byte and I/O measurements remain unknown.
  On a connected server without file listing, show confirmed receipt additions
  (snapshot, rows, files and compressed bytes where supplied), not a fabricated
  file inventory or total storage size.
- **Explain:** inspect the selected query plan separately from execution
  measurements. Native capabilities report file and row-group pruning; page
  and Bloom-filter pruning remain unavailable.
- **Operations:** check collection-scoped append outcomes or look up an original
  operation UUID after reload. A timeout or unknown response freezes the payload
  and original UUID for retry. Native **Published snapshot operations** lists IDs
  from retained server history after reload and queries authoritative outcomes.
  Terminal outcomes stay terminal even if an older status reply arrives late.
  Confirmed create and append results remain confirmed if the following metadata
  refresh fails. A definite creation failure releases its frozen form for correction.
- **Appearance:** light/dark theme switch at the bottom of the collapsible sidebar.
  Display preferences persist locally; credentials and row drafts do not.
  Overview puts schema beside statistics, and Data studio combines schema, reading
  and nested row editing. Read results stay inside the Read rows panel.
  See [combined-login operations](COMBINED_LOGIN.md) for the combined
  Keycloak login screen and deployment steps.

Append preserves duplicate keys and input rows. Quote int64 values in JSON,
especially those above 2^53. Use RFC3339 timestamps with a timezone and no more
than six fractional digits. NULLs are explicit or omitted nullable fields;
only SQL TRUE survives a predicate. The browser defaults to a 1,000-row preview and
displays 100 rows per page. Connected previews can request up to 10,000 rows,
within the backend page bound and configured editor ceiling; the static demo
retains its 1,000-row maximum. Append batches are capped at 10,000 rows plus
decoded/body byte limits. Append collections preserve duplicates; KV collections
expose capability-gated upsert, delete and compaction with one full row per key.
Asynchronous jobs, S3 and durable mock outcomes remain unavailable.

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
operation UUIDs, schema, rows, optional snapshot precondition and a creation's
optional Parquet write settings (canonical, unset options omitted), without the
access token. Disconnect
with unresolved requests offers export before explicit **Disconnect and clear**. After reconnecting, select the same database, open
**Pending requests**, choose **Recovery JSON file** and **Import pending request**.
Import validates the server/database/backend/store binding and collection schema,
then restores the frozen request. It never submits a write or generates a new
UUID. Choose **Check outcome** or **Retry same operation** explicitly; an imported
creation keeps its original UUID, name, schema and write settings. The SDK
likewise requires caller-supplied IDs. Recovery files contain user row data and remain caller-owned;
no recovery state or credentials are written to browser storage. Export and import
share a separate 32 MiB UTF-8 recovery-file limit, including the complete JSON
envelope, schema, server/database scope and formatting. This limit is independent
of HTTP request and response limits. Export refuses an oversized file rather than
offering a download that cannot be imported; import checks file size before
reading or parsing it. Existing version-1 pretty-printed files remain supported.
The mock serves same-origin API/UI; direct access to another origin requires
an explicitly configured proxy/CORS policy. Native serving is same-origin by default.

## Runtime design

Edit [`ui/config/theme.json`](../ui/config/theme.json) while the server and
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
theme. Light colors use the existing color keys; matching `dark_` keys override
colors in dark mode. Both palettes reload from the configured file. Fine layout rules remain in
`ui/assets/styles.css`, also served directly and editable without compilation.
There is no authenticated server-configuration write endpoint in this slice.

## KV collection controls

When native capabilities advertise KV, creation includes an immutable **Collection
mode** choice. KV requires a nonnullable string `key` at any column position.
The schema-driven write panel and Batch editor use **Upsert rows** for KV:
last input occurrence wins and every input replaces the full row, with omitted
nullable values becoming NULL. Append collections retain duplicate-preserving
writes. The footer and Schema panel explain the selected collection's mode.

KV adds **Lookup keys**, **Delete keys**, and **Compact files** controls only for
advertised operations. Lookup uses the selected view and displays ordered hits,
repeated keys and explicit misses. Delete and compact reuse the existing
permanent operation/recovery flow. Pending request exports include the mutation
action and original body; import validates the server/database/store, schema,
and collection mode before allowing an explicit same-ID retry. Import itself
never writes. Uncertain requests stay frozen until a terminal outcome is known.

Version-3 receipts display their mutation kind and distinguish **No new snapshot**
from a publication. No-op receipts remain available in Operations by ID and do
not add snapshot-history rows. Native snapshot history displays rows written and
removed, including value-preserving compaction, rather than treating mutations
as appended rows. Mock and public demo modes do not expose these KV controls.
Bulk upsert and append-to-KV migration are native C++ operations only; there are
no corresponding HTTP/workbench controls.
