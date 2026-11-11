# Public metadata

`storage/engine.h` exposes native metadata through transport-independent DTOs
in `storage/metadata.h`. Adapters must use these methods instead of reading the
catalog or warehouse themselves. These APIs require `GLAMDB_BUILD_STORAGE=ON`;
disabled builds return `DEPENDENCY_UNAVAILABLE`.

| API | Result |
| --- | --- |
| `GetCapabilities()` | Backend identity and supported append, scan and metadata operations; no I/O or writer wait. Custom backends identify themselves explicitly. |
| `GetHealth()` | Current admission/recovery/shutdown state, separate from supported operations. |
| `ListSnapshots(collection)` | Retained snapshots, oldest first, joined with their original committed append receipts. Initial empty is not a snapshot. |
| `GetStatistics(collection, selector)` | Rows, live data-file count and compressed data-file bytes from the selected view's manifests. |
| `ExplainScan(request)` | Validated projection, physical read fields, predicate, batch size, limit, resolved view and candidate statistics. No Parquet reads. |
| `ScanCursor::Profile()` | Thread-safe cumulative completed work, available after completion, cancellation and errors. |

Each metadata call admits a read against one confirmed root and holds that
root through planning, including concurrent appends. No SQLite read transaction
is held while reading files. History reads durable receipts through the engine's
catalog interface; receipts for concurrent, newer snapshots are excluded.
Missing or inconsistent committed metadata reports `CORRUPTION` and closes
readiness. Environmental read errors retain their scan error classification.

`identity` describes the confirmed current root; `selector_resolved` identifies
the chosen historical or initial view separately. Latest resolves exactly once
per call. Independent calls may observe different roots. Retained snapshots and
receipts survive reopen, including after superseded root files are removed.

Every `Metric` contains an optional integer and a `MetricKind`:

- `Metadata`: counts summed from committed live-file descriptors. These are
  exact within the append-only model, but do not measure every data page.
- `Estimate`: planned output rows for an unfiltered view, bounded by its limit;
  zero is also known for a zero limit or empty view.
- `Measured`: completed execution work observed by this cursor.
- `Unavailable`: null. Logical/uncompressed size, elapsed time and nontrivial
  predicate selectivity remain unavailable; unknown does not mean zero.

Explain lists all candidate files before execution. File and row-group pruning
are unsupported and explicitly false. A small limit may stop execution early;
this is not pruning. Projection includes predicate fields in physical reads.
The scanner reads entire Parquet files, then decodes only requested columns.

Profile counters count successfully decoded manifests, successful whole-file
reads and their bytes, decoded rows before filtering/limit, and rows returned
after filtering/limit. Bytes include footers and unprojected column bytes but
exclude metadata, failed/partial reads and device/cache I/O. `Complete` means
the caller reached EOS, including limit exhaustion; it does not mean every
candidate was read. Open profiles are partial. Cancellation is observable
before another `Next()`; terminal complete/failed states are retained through
shutdown. Custom cursors may leave unsupported counters unavailable.

No persistent format changes, snapshot expiration, directory inventory, HTTP
routes, SDK/UI integration or durability qualification are introduced here.
The existing HTTP metadata/explain routes still return `501`.

Validation uses real dependencies in `NativeMetadata`, metadata cases in
`ScanReadErrorClassificationTest` and `SupersededRootTest`, and the installed
consumer's separate write/reopen processes. The consumer checks explicit
dependency errors in the storage-disabled contour. In-memory scan tests remain
separate evidence. See the PR for executed commands and results.
