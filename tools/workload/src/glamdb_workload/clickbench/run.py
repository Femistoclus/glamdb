"""Reproducible finite ClickBench campaigns over the saved network target."""

import csv
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import platform
import socket
import sys

import psutil

from glamdb_sdk import Schema
from glamdb_sdk.codec import int64

from ..runtime import client_for
from ..tpch_scan import _run_owned_process
from .ingest import Blocked, _backend, _config, _dataset, read_state
from .model import MAX_METADATA_BYTES, read_json, save_json, sha256_file
from .queries import QueryCatalog
from .reduce import validate_options
from .target import collection_mode, target_schema


INLINE_SAMPLE_BYTES = 4 * 1024**2


def query_ids(value):
    if value == "all":
        return list(range(1, 44))
    if not isinstance(value, str):
        raise ValueError("query IDs must be unique values in 1..43 or all")
    values = [int(item) for item in value.split(",")]
    if not values or len(values) != len(set(values)) or any(not 1 <= item <= 43 for item in values):
        raise ValueError("query IDs must be unique values in 1..43 or all")
    return values


def _predicate_size(predicate):
    if predicate is None:
        return set(), 0, 0, 0
    arguments = predicate.get("args", []) if predicate["op"] != "not" else [predicate["arg"]]
    children = [_predicate_size(p) for p in arguments]
    operations = {predicate["op"]}.union(*(p[0] for p in children))
    literals = len(predicate.get("values", [])) if predicate["op"] == "in" else int("value" in predicate)
    return (operations, 1 + max((p[1] for p in children), default=0),
            1 + sum(p[2] for p in children), literals + sum(p[3] for p in children))


def _preflight(state, data, plans, config):
    timeout = config["timeout"]
    with client_for(config, scoped=False) as root:
        root_caps = root.capabilities(deadline=timeout)
        database = root.get_database(state["database_id"], deadline=timeout)
    _backend(root_caps, state["test_backend"])
    if root_caps.get("features", {}).get("database_management") is not True:
        raise Blocked("server does not advertise managed benchmark databases")
    if (database.get("state") != "open" or database.get("database_id") != state["database_id"] or
            database.get("store", {}).get("store_id") != state["store_id"]):
        raise Blocked("benchmark database must retain its original open store identity")
    with client_for(config) as client:
        caps = client.capabilities(deadline=timeout)
        _backend(caps, state["test_backend"])
        if caps.get("store_id") != state["store_id"]:
            raise Blocked("scoped benchmark store identity changed")
        if any(caps.get("features", {}).get(name) is not True for name in
               ("scan", "scan_pagination", "scan_cancel", "arrow_export")):
            raise Blocked("server does not advertise required ClickBench scan capabilities")
        info = client.get_collection(state["collection_id"], deadline=timeout)
        schema = client.get_schema(state["collection_id"], deadline=timeout)
        if (info.get("mode", "append") != collection_mode(state) or
                schema != target_schema(data.schema, collection_mode(state)) or
                schema != Schema.from_dict(state.get("target_schema", state["schema"]))):
            raise Blocked("benchmark collection mode or physical schema changed")
        if info.get("current_snapshot_id") != state["snapshot_id"]:
            raise Blocked("benchmark target changed outside the verified load")
        advertised, limits = set(caps.get("predicate_ops", [])), caps.get("limits", {})
        for plan in plans:
            used, depth, nodes, literals = _predicate_size(plan.predicate)
            if not used.issubset(advertised):
                raise Blocked("server does not advertise required ClickBench predicates")
            for name, required in (("predicate_depth", depth), ("predicate_nodes", nodes),
                                   ("predicate_literals", literals)):
                if name in limits and (type(limits[name]) is not int or limits[name] < required):
                    raise Blocked("server predicate capacity cannot represent Q" + str(plan.query_id))
    return {"root_capabilities": root_caps, "capabilities": caps, "database": database,
            "collection": info}


def _versions(modes):
    result = {"python": sys.version, "platform": platform.platform()}
    packages = ("glamdb-sdk", "locust", "gevent", "pyarrow", "sqlglot")
    if "reduce" in modes:
        packages += ("duckdb",)
    for name in packages:
        try:
            result[name] = version(name)
        except PackageNotFoundError:
            raise Blocked("install the optional ClickBench dependency: " + name) from None
    return result


def _write_reports(report, result, artifacts):
    fields = ("query_id", "mode", "iteration", "worker", "user", "status", "candidate_rows",
              "candidate_arrow_bytes", "pages", "batches", "arrow_wire_bytes", "http_requests",
              "request_body_bytes", "output_rows", "elapsed_seconds", "client_cpu_seconds",
              "result_validation_ms", "sql_validation", "reason")
    measurements, warmups, kept_bytes, total = [], [], 0, 0
    files = sorted(artifacts.glob("measurements-*.jsonl"))
    with report.with_suffix(".csv").open("w", encoding="utf-8", newline="") as stream, report.with_suffix(".md").open(
            "w", encoding="utf-8") as markdown:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        markdown.write("# ClickBench campaign\n\n" +
                       f"Status: {result['status']}; rows: {result['rows']}; columns: 105; " +
                       f"collection mode: {result['collection_mode']}.\n\n" +
                       "Reduce includes bounded DuckDB candidate staging and SQL. Independent source validation " +
                       "is excluded from logical-query latency; wall throughput includes validation between queries. " +
                       "Cache policy: warm/no-cold.\n\n" +
                       "| Query | Mode | User | Iteration | Status | Seconds |\n|---|---|---|---|---|---|\n")
        for path in files:
            with path.open(encoding="utf-8") as source:
                for line in source:
                    sample = json.loads(line)
                    total += 1
                    if kept_bytes + len(line.encode()) <= INLINE_SAMPLE_BYTES:
                        (warmups if sample["warmup"] else measurements).append(sample)
                        kept_bytes += len(line.encode())
                    if not sample["warmup"]:
                        writer.writerow(sample)
                        markdown.write(f"| {sample['query_id']} | {sample['mode']} | {sample['user']} | " +
                                       f"{sample['iteration']} | {sample['status']} | " +
                                       f"{sample.get('elapsed_seconds', 'unavailable')} |\n")
    result.update(measurements=measurements, warmups=warmups,
                  measurement_files=[str(path) for path in files],
                  inline_samples_complete=len(measurements) + len(warmups) == total)
    save_json(report.with_name(report.stem + "-coverage.json"),
              {"dataset_hash": result["dataset_hash"], "snapshot_id": result["snapshot_id"],
               "collection_mode": result["collection_mode"],
               "query_catalog_sha256": result["query_catalog_sha256"], "coverage": result["coverage"]})
    save_json(report, result)


def campaign(path, catalog, report, *, source=None, ids="all", mode="both", warmup=1, repeats=3,
             timeout=120, ca_file=None, users=1, processes=1, **budgets):
    identifiers = query_ids(ids)
    modes = ("scan", "reduce") if mode == "both" else (mode,)
    if not set(modes) <= {"scan", "reduce"}:
        raise ValueError("invalid campaign mode")
    if (any(type(value) is not int or value < 1 for value in (users, processes)) or
            type(warmup) is not int or not 0 <= warmup <= 10 or
            type(repeats) is not int or not 1 <= repeats <= 100):
        raise ValueError("users/processes must be positive integers; warmup 0..10; repeats 1..100")
    budgets = validate_options(timeout, **budgets)
    report = Path(report).resolve()
    if report.suffix != ".json" or report == Path(path).resolve():
        raise ValueError("campaign report must be a separate .json path")
    artifacts = report.with_name(report.stem + "-locust")
    destinations = (report, report.with_suffix(".csv"), report.with_suffix(".md"),
                    report.with_name(report.stem + "-coverage.json"))
    if any(destination.exists() for destination in destinations) or artifacts.exists() and any(artifacts.iterdir()):
        raise ValueError("campaign report and artifact directory must be unused to preserve run evidence")
    state = read_state(path)
    if state.get("load_complete") is not True or state.get("snapshot_id") is None:
        raise Blocked("query campaign requires the complete loaded snapshot")
    try:
        canonical = isinstance(state["snapshot_id"], str) and str(int64(state["snapshot_id"])) == state["snapshot_id"]
    except ValueError:
        canonical = False
    if not canonical:
        raise ValueError("query snapshot ID must be a canonical int64 string")
    data = _dataset(state)
    if report.is_relative_to(data.path):
        raise ValueError("campaign report must be outside the immutable dataset")
    catalog = QueryCatalog(catalog)
    verification = read_json(Path(state["run_dir"]) / "verify.json")
    if any(verification.get(key) != value for key, value in
           (("status", "PASS"), ("dataset_hash", data.digest), ("load_id", state["load_id"]),
            ("snapshot_id", state["snapshot_id"]))) or verification.get("collection_mode", "append") != collection_mode(state):
        raise Blocked("verify the complete unchanged dataset and collection mode before querying")
    if "reduce" in modes:
        if source is None:
            raise Blocked("reduce validation requires the original source Parquet path")
        source = Path(source).resolve()
        if sha256_file(source) != data.manifest["source"]["sha256"]:
            raise ValueError("oracle source checksum differs from the recorded input")
    else:
        source = None
    plans = [catalog.plan(identifier, snapshot=state["snapshot_id"], type_profile=data.manifest["type_profile"])
             for identifier in identifiers]
    config = dict(_config(state, timeout, ca_file), collection=state["collection_id"], store_id=state["store_id"],
                  collection_mode=collection_mode(state), plans=[{"plan": asdict(plan), "mode": selected}
                                                               for plan in plans for selected in modes],
                  source=str(source) if source else None, rows=data.manifest["rows"],
                  users=users, processes=processes, warmup=warmup, repeats=repeats, mode=mode,
                  report=str(artifacts), stop_timeout=0, master_host="127.0.0.1", budgets=budgets,
                  dataset_hash=data.digest, snapshot_id=state["snapshot_id"], load_id=state["load_id"],
                  query_catalog_sha256=catalog.digest,
                  measurement={"logical_query": "full SDK scan plus bounded DuckDB candidate staging and SQL; source validation excluded",
                               "warmup_policy": "each Locust user completes warmup full rounds before measured full rounds",
                               "cache_state": "uncontrolled; warmup only", "physical_io": "not measured",
                               "per_query_cpu": "process CPU delta; overlapping users are not additive",
                               "engine_only_latency": "not measured", "read_mode": "arrow-batches"},
                  hardware={"platform": platform.platform(), "python": platform.python_version(),
                            "machine": platform.machine(), "processor": platform.processor(),
                            "logical_cpus": os.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False),
                            "memory_bytes": psutil.virtual_memory().total})
    versions = _versions(modes)
    config["preflight"] = _preflight(state, data, plans, config)
    # Per-attempt evidence streams to JSONL/CSV. Only a bounded sample is inlined;
    # aggregate worker/query dictionaries must fit before any users are launched.
    estimated_report_bytes = (len(json.dumps(config).encode()) * 2 + INLINE_SAMPLE_BYTES +
                              processes * len(config["plans"]) * 2048 + 1024**2)
    if estimated_report_bytes > MAX_METADATA_BYTES:
        raise ValueError("users/processes/query profile exceeds the finite report metadata budget")
    config["report_metadata_budget_bytes"] = MAX_METADATA_BYTES
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        config["master_port"] = probe.getsockname()[1]
    artifacts.mkdir(parents=True, exist_ok=True)
    save_json(artifacts / "run.json", config)
    with (artifacts / "locust.log").open("wb") as log:
        returncode = _run_owned_process([sys.executable, "-m", "glamdb_workload.clickbench.locust",
                                         str(artifacts / "run.json")], stdout=log)
    summary = artifacts / "summary.json"
    result = read_json(summary) if summary.exists() else {
        "status": "INCOMPLETE", "result_verified": False, "counts": {}, "queries": {},
        "configuration": config, "reason": "Locust exited without a complete report"}
    if returncode and result["status"] == "PASS":
        result.update(status="INCOMPLETE", result_verified=False, reason="Locust process exited unsuccessfully")
    warmup_counts = result.get("warmup", {})
    result.update(returncode=returncode, dataset_hash=data.digest, query_catalog_sha256=catalog.digest,
                  source_sha256=data.manifest["source"]["sha256"], snapshot_id=state["snapshot_id"],
                  collection_mode=collection_mode(state), load_id=state["load_id"], rows=data.manifest["rows"],
                  columns=105, profile=data.manifest["profile"], type_profile=data.manifest["type_profile"],
                  performance_eligible=result["status"] == "PASS" and state["performance_eligible"],
                  warmup=warmup, warmup_counts=warmup_counts, repeats=repeats,
                  cache_policy="warm/no-cold; no OS/device cache eviction", budgets=budgets, versions=versions)
    result["coverage"] = []
    for plan in plans:
        for selected in modes:
            stats = result["queries"].get(f"Q{plan.query_id}/{selected}", {})
            verified = stats.get("completed", 0) == users * repeats
            if selected == "reduce":
                verified = verified and stats.get("verified", 0) == users * repeats
            result["coverage"].append(dict(plan.describe(), mode=selected, verified=verified))
    _write_reports(report, result, artifacts)
    return result
