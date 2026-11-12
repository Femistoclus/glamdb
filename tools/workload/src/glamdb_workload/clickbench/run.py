"""Reproducible 43-query campaigns over the saved real server target."""

import csv
import io
from pathlib import Path
import platform
import sys

from ..runtime import client_for, error_code
from .ingest import Blocked, _backend, _config, _dataset, read_state
from .model import read_json, save_json, sha256_file
from .queries import QueryCatalog
from .reduce import execute_query, source_factory


def query_ids(value):
    if value == "all":
        return list(range(1, 44))
    values = [int(item) for item in value.split(",")]
    if not values or len(values) != len(set(values)) or any(not 1 <= item <= 43 for item in values):
        raise ValueError("query IDs must be unique values in 1..43 or all")
    return values


def campaign(path, catalog, report, *, source=None, ids="all", mode="both", warmup=1, repeats=3,
             timeout=120, ca_file=None, **budgets):
    import duckdb
    import pyarrow
    import sqlglot

    state = read_state(path)
    data = _dataset(state)
    catalog = QueryCatalog(catalog)
    if not state["load_complete"] or state["snapshot_id"] is None:
        raise Blocked("query campaign requires the complete loaded snapshot")
    verification = read_json(Path(state["run_dir"]) / "verify.json")
    if any(verification.get(key) != value for key, value in
           (("status", "PASS"), ("dataset_hash", data.digest), ("load_id", state["load_id"]),
            ("snapshot_id", state["snapshot_id"]))):
        raise Blocked("verify the complete unchanged dataset before querying")
    report = Path(report).resolve()
    if report.suffix != ".json" or report == Path(path).resolve() or report.is_relative_to(data.path):
        raise ValueError("campaign report must be a separate .json path")
    if type(warmup) is not int or not 0 <= warmup <= 10 or type(repeats) is not int or not 1 <= repeats <= 100:
        raise ValueError("warmup must be 0..10 and repeats 1..100")
    modes = ("scan", "reduce") if mode == "both" else (mode,)
    if not set(modes) <= {"scan", "reduce"}:
        raise ValueError("invalid campaign mode")
    oracle = None
    if "reduce" in modes:
        if source is None:
            raise Blocked("reduce validation requires the original source Parquet path")
        if sha256_file(source) != data.manifest["source"]["sha256"]:
            raise ValueError("oracle source checksum differs from the recorded input")
        oracle = source_factory(source, data.manifest["rows"])
    report.parent.mkdir(parents=True, exist_ok=True)
    measurements, warmups, coverage = [], [], []
    config = _config(state, timeout, ca_file)
    with client_for(config) as client:
        _backend(client.capabilities(deadline=timeout), state["test_backend"])
        for identifier in query_ids(ids):
            plan = catalog.plan(identifier, snapshot=state["snapshot_id"], type_profile=data.manifest["type_profile"])
            for selected in modes:
                verified = True
                for index in range(-warmup, repeats):
                    try:
                        result = execute_query(client, state["collection_id"], plan, mode=selected,
                                               workspace=report.parent, oracle_factory=oracle if index >= 0 else None,
                                               timeout=timeout, **budgets)
                    except Exception as error:
                        result = {"status": "BLOCKED" if isinstance(error, ValueError) else "FAIL",
                                  "reason": error_code(error), "query_id": identifier, "mode": selected}
                    result["iteration"] = index
                    (warmups if index < 0 else measurements).append(result)
                    if index >= 0:
                        verified = verified and result["status"] == "PASS"
                coverage.append({**plan.describe(), "mode": selected, "verified": verified})
    good = all(item["status"] == "PASS" for item in measurements)
    result = {"status": "PASS" if good else "FAIL", "dataset_hash": data.digest,
              "query_catalog_sha256": catalog.digest, "source_sha256": data.manifest["source"]["sha256"],
              "snapshot_id": state["snapshot_id"], "load_id": state["load_id"], "rows": data.manifest["rows"],
              "columns": 105, "profile": data.manifest["profile"], "type_profile": data.manifest["type_profile"],
              "performance_eligible": good and state["performance_eligible"], "warmup": warmup, "repeats": repeats,
              "cache_policy": "warm/no-cold; no OS/device cache eviction",
              "budgets": budgets, "coverage": coverage, "measurements": measurements, "warmups": warmups,
              "versions": {"python": sys.version, "platform": platform.platform(), "duckdb": duckdb.__version__,
                           "pyarrow": pyarrow.__version__, "sqlglot": sqlglot.__version__}}
    save_json(report, result)
    fields = ("query_id", "mode", "iteration", "status", "candidate_rows", "candidate_arrow_bytes", "batches",
              "output_rows", "elapsed_seconds", "client_cpu_seconds", "sql_validation", "reason")
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(measurements)
    report.with_suffix(".csv").write_text(stream.getvalue())
    save_json(report.with_name(report.stem + "-coverage.json"),
              {"dataset_hash": data.digest, "snapshot_id": state["snapshot_id"],
               "query_catalog_sha256": catalog.digest, "coverage": coverage})
    report.with_suffix(".md").write_text(
        "# ClickBench campaign\n\n" + f"Status: {result['status']}; rows: {result['rows']}; columns: 105.\n\n" +
        "Reduce is GlamDB + DuckDB; scan reports candidates. Cache policy: warm/no-cold.\n\n" +
        "| Query | Mode | Iteration | Status | Seconds |\n|---|---|---|---|---|\n" +
        "\n".join(f"| {m['query_id']} | {m['mode']} | {m['iteration']} | {m['status']} | "
                  f"{m.get('elapsed_seconds', 'unavailable')} |" for m in measurements) + "\n")
    return result
