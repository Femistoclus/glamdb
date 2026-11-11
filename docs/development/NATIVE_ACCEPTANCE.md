# Installed native cross-client acceptance

This recipe uses the installed C++ service, installed network CLI, Python SDK
and the installed workbench against **one persistent store**. Native setup
failure is fatal; it cannot switch to the mock. Mock/static browser suites
remain independently runnable through the workbench test profile.

It is local integration evidence. Restarts, fault tests and verified copies
do not qualify physical power-loss durability; G4 remains **NOT RUN**.

## Prerequisites and install

Use the pinned native dependency prefix and toolchain from [BUILDING](BUILDING.md),
Python 3.13, Node 22+ and a filesystem with the engine's required free-space
reserve. Configure `Python3_EXECUTABLE` with the current SDK/PyArrow environment.
Use an absolute canonical workspace path; symlink aliases are rejected.

```sh
bash scripts/bootstrap-workbench.sh
.venv/bin/python -m pip install --require-hashes --only-binary=:all: --no-deps \
  -r tools/acceptance/requirements.txt
npm --prefix ui exec -- playwright install chromium
cmake --preset local -DGLAMDB_BUILD_ADMIN=ON
cmake --build --preset local
cmake --install build/local --prefix "$PWD/artifacts/native-prefix"
.venv/bin/python tools/acceptance/run.py \
  --prefix "$PWD/artifacts/native-prefix" \
  --workspace "$PWD/artifacts/native-system"
```

For Linux browser libraries install Playwright's `--with-deps` prerequisite.
`--browser-path /absolute/path/to/chromium` selects an existing verified browser.
On a nearly full development disk, put the workspace on a dedicated filesystem;
do not lower the engine reserve. `GLAMDB_TEST_TMPDIR` selects disposable native
test directories, independently of the explicit acceptance workspace.

The workspace must be new or contain this recipe's `acceptance-state.json`.
An arbitrary existing directory is refused. UUIDs, exact A/B requests, expected
receipts and the private mode0600 token survive subsequent invocations. Repeating
the recipe resolves the same identities, never reseeds under new operation IDs.
The service always binds loopback and chooses its own available port. No existing
service or data directory is stopped or modified.

## What the command proves

1. Explicit store creation, backend `local`, `mock=false`, exact store identity
   and actual write readiness from an installed native service.
2. One fixed schema (`key`, required int64 `n`, nullable `label`). Append A uses
   the CLI: **1,000** rows and one data file. Append B uses the SDK: **10** exact
   duplicate rows and one additional file. Unicode/NULL and int64 values above
   JavaScript's safe range are retained exactly.
3. On the same store: latest **1,010** rows/two files, snapshot A **1,000** rows/one
   file, initial **0** rows/files. CLI JSON, SDK JSON and verified Arrow IPC
   compare exact multisets, not only row counts. Batches remain usable after
   stream/client teardown. Schema, selected identity, EOS and checksum are checked.
4. Real UI Overview, Data studio, Schema, Snapshots, Storage, Explain, Operations
   and Style studio; typed browser write uses a separate `acceptance_ui`
   collection in this same store, preserving the A/B expectations. Selected
   views, file details, Arrow download, reload, desktop/mobile and keyboard
   checks use the actual server. The Arrow download is decoded independently.
5. A deliberate `expected_snapshot=None` append after A/B returns a bound
   `definitely_not_committed`/`PRECONDITION_FAILED` outcome. Its original ID,
   payload/hash and exact terminal response are preserved alongside successes.
6. Clean shutdown/restart without reseeding. Retry the original A bytes/ID;
   its original receipt, row/file totals, snapshot history and failed outcome
   remain unchanged. Browser reload/reconnection reads persisted server metadata.
7. Offline public-facade reads refuse concurrent ownership and reproduce
   latest/A/initial after the owning server closes.
8. Independent inspection starts from the read-only catalog pointer and checks
   root/table identity, selected snapshot, Avro manifest list/manifest membership,
   actual Parquet length/row count/field IDs and row-group metadata. Warehouse
   listing is used only to report genuinely unreferenced files separately;
   those files are never included as snapshot data or automatically removed.
9. A complete quiesced copy is synchronized and verified. Another owner is
   refused while SQLite is closed and source ownership remains held. Restore
   opens a different physical root while the original path is renamed out of
   reach, and repeats successful/failed outcomes and current/historical/initial
   CLI/SDK/UI reads. The original root is restored in `finally`.

An explicit `--no-browser` diagnostic reports `INCOMPLETE` and exits 2, with
browser checks `NOT RUN`; it cannot satisfy the normal CI recipe. A failure
returns nonzero and records the failed stage/exception. It never silently skips
native setup or marks an unresolved write successful.

## Read the evidence

`WORKSPACE/report.json` records PASS/FAIL/INCOMPLETE, individual checks, exact
row/file expectations, commands/exit codes, physical chain paths and copy hashes.
Screenshots, downloaded Arrow and sanitized Playwright traces live in distinct
`browser-<stage>-<run>` children. Server logs and original synthetic request
files are retained. Raw trace archives are deleted after token redaction.
The token stays in `WORKSPACE/token`; do not include that file or the private
workspace wholesale when publishing evidence. Reports and sanitized traces do
not contain the token. Store/operation UUIDs are test identities, not credentials.

The normal native CI profile installs the extra pinned Avro reader, installs the
native prefix, then invokes this exact `tools/acceptance/run.py` recipe with the
browser enabled. Other profiles retain their native CTest/sanitizer contours.
Tests under `tests/system/` check owned-path/traversal/symlink boundaries and
trace redaction; native metadata and browser suites exercise actual storage.

## Use the offline tools directly

`glamdb-admin read` is a bounded offline read (at most 10,000 rows/16 MiB); an
oversized result is an error. It uses the public facade and refuses an owned store.

```sh
PREFIX/bin/glamdb-admin read --root /canonical/store --store-id STORE_UUID \
  --collection-id COLLECTION_UUID --snapshot initial
PREFIX/bin/glamdb-admin read --root /canonical/store --store-id STORE_UUID \
  --collection-id COLLECTION_UUID --snapshot SNAPSHOT_ID --limit 1000
```

For a complete copy, stop the owning service cleanly. The helper obtains a new
owner through `OpenExisting`, performs recovery if required, calls `Shutdown`
to close SQLite, and retains that engine's independent directory lease until
the copying controller releases stdin. It has no cursors or dependent batches.

```sh
.venv/bin/python tools/acceptance/backup.py \
  --admin PREFIX/bin/glamdb-admin \
  --source /canonical/store --store-id STORE_UUID \
  --destination /canonical/new-copy
```

The destination must not exist or overlap the source. Copying preserves the
entire store, including orphans and durable outcomes, rejects links/special
files, verifies SHA-256 and syncs files/directories; `store.ready` is copied last.
A failed partial destination is left for inspection, never advertised as a
successful copy or automatically deleted. This is a trusted-local-filesystem
operator tool, not online backup, remote object storage or disaster qualification.

## Executed validation

The feature run on macOS arm64 used a fresh installed native prefix, a dedicated
test volume, AppleClang 21, Python 3.13.5 and real Chromium. Native metadata,
HTTP/SDK/CLI and browser suites are separate from mock/static evidence. The final local run passed the full shared-store CLI/SDK/Chromium recipe on
upstream `85bd283`, including all three UI row sets, physical API/file comparison,
unchanged warehouse hashes after exact retry, independent ownership during copy
and successful/failed outcomes after source-independent restore. Its JSON report
records exact binary/UI SHA-256 hashes and runtime versions.

The selected native CTest run completed 128 entries with no failures (three
Linux-only entries skipped). Eight native metadata cases cover all scalar bound
encodings, null unknowns, paging, recovery admission and shared fixtures. The
separate native Chromium profile passed five scenarios; mock/static Chromium
passed 39 independently. Node passed 128 tests; the full Python regression passed
644. Workflow lint/security checks and C++ formatting passed. Hosted Linux
sanitizer checks remain separate required PR checks; no power-loss result is
inferred from the macOS runs.
