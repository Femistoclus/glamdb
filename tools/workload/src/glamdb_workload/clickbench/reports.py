"""JSON, CSV and readable load results with one common report stem."""

import csv
import io
from pathlib import Path

from .model import save_json


def validate_report(path, state_path, data, run_dir):
    path = Path(path).resolve()
    destinations = [destination.resolve() for destination in
                    (path, path.with_suffix(".csv"), path.with_suffix(".md"))]
    state_path, dataset_path, run_dir = Path(state_path).resolve(), data.path.resolve(), Path(run_dir).resolve()
    if path.suffix != ".json" or any(
            destination == state_path or destination.is_relative_to(dataset_path) or destination.is_relative_to(run_dir)
            for destination in destinations):
        raise ValueError("load report must be a separate .json path outside the dataset and journal")
    return path


def write_report(path, result):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    save_json(path, result)
    fields = ("status", "collection_mode", "load_complete", "snapshot_id", "confirmed_rows",
              "newly_confirmed_rows", "elapsed_seconds", "rows_per_second", "performance_eligible")
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerow(result)
    path.with_suffix(".csv").write_text(stream.getvalue())
    path.with_suffix(".md").write_text(
        "# ClickBench load\n\n| Metric | Value |\n|---|---|\n" +
        "\n".join(f"| {name} | {result.get(name, 'unavailable')} |" for name in fields) + "\n\n" +
        "Elapsed ingestion includes source part reads, encoding, journal syncs and network writes. "
        "Source preparation, capacity planning and final source/receipt/row verification are reported separately.\n")
