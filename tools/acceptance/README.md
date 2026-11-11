# Installed native acceptance

Run `run.py` with an installed native prefix and an owned persistent workspace.
It exercises one real store through the installed CLI, the Python SDK and
Chromium, then verifies restart, exact retry, physical files, offline reads and
quiesced backup/restore. It never substitutes a mock for missing native setup.

The complete setup, commands, expected results and evidence are in
[the system guide](../../docs/development/NATIVE_ACCEPTANCE.md).

- `physical.py`: read-only confirmed catalog root → Iceberg JSON → Avro
  manifest list/manifest → actual Parquet inspection with standard readers.
- `backup.py`: hold native ownership after SQLite closes, copy the complete
  store, verify SHA-256 and synchronize the destination. No overwrite or deletion.
- `browser.mjs`: actual pages, typed write, metadata, Arrow download, reload,
  desktop/mobile and keyboard checks on the same installed service.
- `sanitize_trace.py`: removes the private bearer from trace actions/network
  records before retaining the archive. Raw traces are deleted.

The Python SDK/PyArrow and existing browser development dependencies are shared
with the workbench. `requirements.txt` adds the hash-pinned MIT fastavro reader.
It is a test/inspection dependency, not a native runtime or storage writer.
