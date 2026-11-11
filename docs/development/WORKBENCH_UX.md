# Workbench interaction checks

Checked against the installed native service on macOS arm64 with real Chromium
141.0.7390.37, at 1440×1000 and 390×844. The persistent acceptance fixture has
1,000 A rows, ten exact duplicate B rows, retained snapshots and a separate
typed UI-write collection. Screenshots were visually inspected; Chromium also
checks actual actions, downloads, reload, keyboard focus and mobile overflow.
This is an implementation check, not a study with representative users.

## Problems found and resolved

- Native view loading issued four concurrent metadata reads while one database
  admits two data calls. Normal create/refresh could therefore show busy errors.
  Metadata loading now sequences reads, preserves cancellation and rejects late
  results from another database/collection/snapshot.
- An unknown typed append could use the JSON editor's missing textarea during
  error handling. The form now retains the actual draft and original operation
  ID. Unresolved create/append requests support explicit export/import without
  credentials or automatic writes; terminal results survive failed refreshes.
- After reload the Operations list lacked discoverable original IDs. Retained
  snapshot operations now expose server-provided IDs and read-only outcome lookup.
- File inspection showed a long JSON object, pushing useful row/byte/group
  information out of view. It now presents a file/selected-view summary, actual
  row groups and typed field-ID/null/bound tables. Per-group observations remain
  separate when no aggregate is available; precision/unknowns are labelled.
  Raw JSON is collapsed. The mobile summary fits its viewport and wide tables
  scroll inside the dialog with keyboard focus.
- A fixed 1,000-row query ceiling prevented checking the complete 1,010-row
  fixture in the UI. The default remains 1,000; the native maximum is bounded
  by the server and optional runtime `editor.query_max_rows` up to 10,000.
  Hot reload can lower the ceiling without losing the typed write draft.

The shared browser recipe reads/export-checks all three exact row sets, including
duplicates and int64 precision, then repeats after server restart and restore.
It also visits Schema, Snapshots, Storage, Explain, Operations and Style studio,
checks a typed write and independently decodes downloaded Arrow. Style studio
remains a local preview/download workflow; permanent changes use the runtime
theme file. Pink palette, configurable decorations and branding are preserved.

## Comparison of interaction patterns

These comparisons use the official descriptions of the other products' actual
interaction models; their authenticated GUIs were not tested in this run.

DuckDB's UI attaches to the native instance that launched it and exposes already
loaded data; its database discovery also polls for changes. The useful pattern
for GlamDB is the same visible storage authority across clients. The installed
recipe verifies one persisted store through CLI/SDK/UI, including reload and
restart, rather than independently seeded interfaces.
([DuckDB UI documentation](https://duckdb.org/docs/2.0/core_extensions/ui))

ClickHouse describes prominent query navigation, a collapsible sidebar and
query results/runtime feedback in a consistent workspace. GlamDB keeps Data
studio accessible from persistent navigation, separates read/write forms and
identifies the exact result snapshot. It has typed projection/filter controls,
not a SQL editor; explain estimates remain distinct from execution measurements.
([ClickHouse interface walkthrough](https://clickhouse.com/blog/new-clickhouse-cloud-experience))

Redis Insight groups data navigation with readable values and format-aware
inspection. GlamDB applies the relevant pattern to collections, fixed-schema
rows and Parquet fields: precise values, explicit NULL/unknown markers and
human-readable metadata tables. Redis mutation/overwrite semantics are not
GlamDB's append contract; duplicate rows are preserved.
([Redis Insight Browser and Workbench](https://redis.io/docs/latest/develop/tools/insight/))

## Evidence and limits

Run [native acceptance](NATIVE_ACCEPTANCE.md) to generate the exact current
commands, binary/UI hashes, versions, JSON report, screenshots and sanitized
traces. Separate native browser cases also cover lost receipts, preserved
create/append drafts, hot reload and selection races. Mock/static cases are
reported separately. Native setup failures cannot become mock successes.

This does not establish screen-reader certification, broad browser compatibility,
production load, a completed resource governor or physical power-loss safety.
No external cloud account was created for the comparison.
