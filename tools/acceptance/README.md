# Installed native acceptance

Run `run.py` with an installed native prefix and an owned persistent workspace.
It exercises one real store through the installed CLI, the Python SDK and
Chromium, then verifies restart, exact retry, physical files, offline reads and
quiesced backup/restore. It never substitutes a mock for missing native setup.

Install the pinned native dependencies, Python 3.13 SDK/PyArrow and Node 22+
browser prerequisites as described in [building](../../docs/BUILDING.md), then
run from the repository root:

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

Use a new canonical workspace or one already marked by this recipe, on a
filesystem meeting the engine's free-space reserve. Repeated runs reuse saved
identities and requests. The service binds loopback and selects an available
port. The recipe expects 1,000 rows after append A, 1,010 after B (including
duplicates), and zero in the initial view. UI writes use a separate collection.
It compares exact rows and original successful/failed receipts after restart
and source-independent restore. Physical power-loss durability remains unqualified.

`WORKSPACE/report.json` contains statuses and per-stage results. Sanitized
traces, screenshots and Arrow downloads occupy separate browser directories.
The private token remains in `WORKSPACE/token`; do not publish the workspace
wholesale. `--no-browser` is an incomplete diagnostic profile, exits 2 and
does not satisfy complete acceptance.

- `physical.py`: read-only confirmed catalog root → Iceberg JSON → Avro
  manifest list/manifest → actual Parquet inspection with standard readers.
- `backup.py`: hold native ownership after SQLite closes, copy the complete
  store, verify SHA-256 and synchronize the destination. No overwrite or deletion.
- `browser.mjs`: actual pages, typed write, metadata, Arrow download, reload,
  desktop/mobile and keyboard checks on the same installed service.
- `sanitize_trace.py`: removes the private bearer from trace actions/network
  records before retaining the archive. Raw traces are deleted.

See [whole-store backup](../../docs/BACKUP.md) for the operator copy procedure.

The Python SDK/PyArrow and existing browser development dependencies are shared
with the workbench. `requirements.txt` adds the hash-pinned MIT fastavro reader.
It is a test/inspection dependency, not a native runtime or storage writer.
