"""TPC-H scan controller and complete SDK cursor consumption.

The controller never patches the caller's sockets. A fresh Locust interpreter
owns users, repetitions, standard histograms and distributed statistics.
"""

import hashlib
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time

import httpx

from glamdb_sdk import ProtocolError, Schema
from glamdb_sdk.codec import int64
from glamdb_sdk.request import uuid_string

from .dataset import write_json
from .runtime import client_for, error_code, failure_kind, read_mode
from .tpch_queries import make_reducer, query_plans, reference_results
from .tpch_target import collection_mode, target_schema


TABLES = frozenset({"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"})


def _cleanup_owned_process(process):
    """Stop the entire private group within the native bridge's five seconds."""
    def send(signum):
        try:
            os.killpg(process.pid, signum)
            return True
        except ProcessLookupError:
            return False

    # The leader can exit before distributed workers. Reaping the leader alone
    # does not establish that its process group is gone.
    if send(signal.SIGTERM):
        until = time.monotonic() + 2
        while time.monotonic() < until:
            process.poll()
            if not send(0):
                break
            time.sleep(min(0.02, max(0, until - time.monotonic())))
        send(signal.SIGKILL)
    # SIGKILL normally makes this immediate. Keep even a pathological kernel
    # wait bounded so the bridge need not kill the controller during cleanup.
    process.wait(timeout=1)


def _run_owned_process(command, *, stdout=None):
    """Launch, wait for, and reap an owned session, preserving caller signals.

    Defer interruption while Popen transfers ownership to Python. Children
    receive ordinary signal dispositions on exec, rather than inherited blocked
    signals. Once unwinding starts, repeated SIGINT/SIGTERM cannot cut cleanup
    short. All temporary handlers are restored before returning to the caller.
    """
    process, handlers = None, {}
    phase = {"launching": True, "cleaning": False, "interrupted": False, "pending": None}

    def interrupt(signum, frame):
        if handlers[signum] == signal.SIG_IGN:
            return
        if phase["cleaning"] or phase["interrupted"]:
            return
        if phase["launching"]:
            phase["pending"] = phase["pending"] or (signum, frame)
            return
        phase["interrupted"] = True
        previous = handlers[signum]
        if callable(previous):
            previous(signum, frame)
        elif previous != signal.SIG_IGN:
            raise SystemExit(128 + signum)
        phase["interrupted"] = False

    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, interrupt)
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        phase["launching"] = False
        if phase["pending"] is not None:
            interrupt(*phase["pending"])
        return process.wait()
    finally:
        phase["cleaning"] = True
        try:
            if process is not None:
                _cleanup_owned_process(process)
        finally:
            for signum, previous in handlers.items():
                signal.signal(signum, previous)


class ReductionMismatch(ValueError):
    """The remote reduction differs from the independent source evaluation."""


class PageCountingTransport(httpx.BaseTransport):
    """Count real Arrow HTTP page responses; SDK owns decoding and checksums."""

    def __init__(self, ca_file=None):
        verify = ssl.create_default_context(cafile=ca_file) if ca_file else True
        self.inner = httpx.HTTPTransport(verify=verify, retries=0)
        self.pages = 0

    def handle_request(self, request):
        response = self.inner.handle_request(request)
        if response.status_code == 200 and response.headers.get("content-type", "").startswith("application/vnd.apache.arrow.stream"):
            self.pages += 1
        return response

    def close(self):
        self.inner.close()


def execute_query(client, plan, *, mode="scan", format="arrow", consume="batches",
                  timeout=300, page_counter=None, expected=None, emit=None, progress=None):
    """Execute a full logical query, retaining only one source batch at a time.

    `progress` retains partial work when a cursor fails or the user is cancelled.
    `emit` receives standard Locust request-event arguments for each source scan.
    Returned latencies are client-observed, including SDK and client consumption.
    """
    read_mode({"format": format, "consume": consume})
    if mode not in {"scan", "reduce"}:
        raise ValueError("mode must be scan or reduce")
    if mode == "reduce" and (not plan["reduction_supported"] or expected is None):
        raise ValueError("supported reductions require an independent expected result")
    progress = {} if progress is None else progress
    progress.update(query_id=plan["query_id"], stages=[], complete=False,
                    result=None, result_verified=False, client_compute_ms=0.0,
                    result_validation_ms=0.0, logical_latency_ms=0.0)
    began = time.perf_counter()
    until = time.monotonic() + timeout
    reducer = make_reducer(plan["query_id"], plan["type_profile"]) if mode == "reduce" else None

    def remaining():
        seconds = until - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("complete logical query deadline exceeded")
        return seconds

    try:
        for stage in plan["stages"]:
            measured = {"stage_id": stage["stage_id"], "table": stage["table"], "alias": stage["alias"],
                        "rows": 0, "pages": 0, "arrow_batches": 0,
                        "arrow_wire_bytes": 0, "arrow_batch_bytes": 0,
                        "client_compute_ms": 0.0, "row_materialization_ms": 0.0,
                        "latency_ms": 0.0, "integrity": "not_complete", "status": "incomplete"}
            progress["stages"].append(measured)
            first_page = page_counter.pages if page_counter is not None else None
            query = {key: stage[key] for key in ("projection", "predicate", "snapshot_id")}
            failed, kind = None, "SDK"
            started = time.perf_counter()
            try:
                if format == "arrow":
                    with client.scan_batches(stage["collection_id"], **query, deadline=remaining()) as stream:
                        try:
                            if stream.snapshot_id != stage["snapshot_id"]:
                                raise ProtocolError("scan returned a different fixed snapshot")
                            for batch in stream:
                                remaining()
                                measured["rows"] += batch.num_rows
                                measured["arrow_batches"] += 1
                                measured["arrow_batch_bytes"] += batch.nbytes
                                if consume == "rows":
                                    materializing = time.perf_counter()
                                    rows = batch.to_pylist()
                                    measured["row_materialization_ms"] += (time.perf_counter() - materializing) * 1000
                                    if reducer:
                                        computing = time.perf_counter()
                                        reducer.add_rows(rows)
                                        measured["client_compute_ms"] += (time.perf_counter() - computing) * 1000
                                elif reducer:
                                    computing = time.perf_counter()
                                    reducer.add_batch(batch)
                                    measured["client_compute_ms"] += (time.perf_counter() - computing) * 1000
                            measured["integrity"] = stream.integrity
                        finally:
                            measured["arrow_wire_bytes"] = stream.bytes_read
                else:
                    with client.scan_pages(stage["collection_id"], **query, deadline=remaining()) as stream:
                        for page in stream:
                            remaining()
                            if page.snapshot_id != stage["snapshot_id"]:
                                raise ProtocolError("scan returned a different fixed snapshot")
                            measured["rows"] += len(page.rows)
                            measured["pages"] += 1
                            if reducer:
                                computing = time.perf_counter()
                                reducer.add_rows(page.rows)
                                measured["client_compute_ms"] += (time.perf_counter() - computing) * 1000
                        measured["integrity"] = "complete"
                measured["status"] = "complete"
            except Exception as error:
                kind = failure_kind(error_code(error))
                failed = RuntimeError(error_code(error))
                measured["status"] = "refused" if kind == "REFUSED" else "error"
                raise
            except BaseException:
                failed, kind = RuntimeError("CANCELLED"), "ERROR"
                measured["status"] = "cancelled"
                raise
            finally:
                measured["latency_ms"] = (time.perf_counter() - started) * 1000
                if format == "arrow":
                    measured["pages"] = page_counter.pages - first_page if first_page is not None else None
                progress["client_compute_ms"] += measured["client_compute_ms"]
                if emit:
                    emit(request_type=kind, name=f"Q{plan['query_id']}/{stage['alias']}/source",
                         response_time=measured["latency_ms"], response_length=measured["arrow_wire_bytes"], exception=failed)
        remaining()
        if reducer:
            computing = time.perf_counter()
            progress["result"] = reducer.finish()
            progress["client_compute_ms"] += (time.perf_counter() - computing) * 1000
            validating = time.perf_counter()
            progress["result_verified"] = progress["result"] == expected
            progress["result_validation_ms"] = (time.perf_counter() - validating) * 1000
            if not progress["result_verified"]:
                raise ReductionMismatch("Q" + str(plan["query_id"]) + " reduction differs from independent source result")
        progress["complete"] = True
        return progress
    finally:
        progress["logical_latency_ms"] = (time.perf_counter() - began) * 1000


def _preflight(saved, dataset, plans, config):
    with client_for(config, scoped=False) as root:
        root_capabilities = root.capabilities(deadline=config["timeout"])
        database = root.get_database(saved["database_id"], deadline=config["timeout"])
    if root_capabilities.get("features", {}).get("database_management") is not True:
        raise ValueError("server does not advertise managed benchmark databases")
    if database.get("state") != "open":
        raise ValueError("benchmark database must already be open")
    if database.get("database_id") != saved["database_id"] or database.get("store", {}).get("store_id") != saved["store_id"]:
        raise ValueError("benchmark store identity changed")

    def predicate_size(predicate):
        if predicate is None: return set(), 0, 0, 0
        children = [predicate_size(p) for p in predicate.get("args", [])]
        operations = {predicate["op"]}.union(*(p[0] for p in children))
        literals = len(predicate.get("values", [])) if predicate["op"] == "in" else int("value" in predicate)
        return operations, 1 + max((p[1] for p in children), default=0), 1 + sum(p[2] for p in children), literals + sum(p[3] for p in children)

    with client_for(config) as client:
        capabilities = client.capabilities(deadline=config["timeout"])
        if capabilities.get("mock") is not False or capabilities.get("backend") != "local":
            raise ValueError("TPC-H scans require the real native storage backend")
        if capabilities.get("store_id") != saved["store_id"]:
            raise ValueError("scoped benchmark store identity changed")
        required = ["scan", "scan_pagination", "scan_cancel"]
        if config["format"] == "arrow": required.append("arrow_export")
        if any(capabilities.get("features", {}).get(key) is not True for key in required):
            raise ValueError("server does not advertise required TPC-H scan capabilities")
        advertised = set(capabilities.get("predicate_ops", []))
        limits = capabilities.get("limits", {})
        for plan in plans:
            for stage in plan["stages"]:
                used, depth, nodes, literals = predicate_size(stage["predicate"])
                if not used.issubset(advertised):
                    raise ValueError("server does not advertise required predicates: " + ",".join(sorted(used - advertised)))
                for name, required in (("predicate_depth", depth), ("predicate_nodes", nodes), ("predicate_literals", literals)):
                    if name in limits and (type(limits[name]) is not int or limits[name] < required):
                        raise ValueError("server " + name + " capacity cannot represent Q" + str(plan["query_id"]) + "/" + stage["alias"])
        for table in sorted(TABLES):
            target = saved["tables"][table]
            schema = client.get_schema(target["collection_id"], deadline=config["timeout"])
            if schema != Schema.from_dict(target["schema"]) or schema != target_schema(dataset.tables[table].schema, collection_mode(saved)):
                raise ValueError("benchmark schema changed for " + table)
    return {"root_capabilities": root_capabilities, "capabilities": capabilities, "database": database, "sdk_version": version("glamdb-sdk"), "locust_version": version("locust")}


def run(state, *, queries="all", mode="scan", format="arrow", consume="batches",
        users=1, processes=1, warmup=1, repeat=5, report, timeout=300, ca_file=None):
    # Reject independent parameter mistakes before touching state or the server.
    query_plans(queries, mode=mode)
    read_mode({"format": format, "consume": consume})
    if any(type(v) is not int or v < minimum for v, minimum in ((users, 1), (processes, 1), (warmup, 0), (repeat, 1))):
        raise ValueError("users/processes/repeat must be positive integers; warmup must be nonnegative")
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a finite positive number")
    from .tpch_dataset import TPCHDataset
    from .tpch_state import read_state

    saved = read_state(state)
    if saved.get("load_complete") is not True or set(saved.get("snapshots", {})) != TABLES or set(saved.get("tables", {})) != TABLES:
        raise ValueError("TPC-H scan requires a verified completed load and fixed eight-table snapshot vector")
    if any(saved["tables"][t].get("snapshot_id") != saved["snapshots"][t] for t in TABLES):
        raise ValueError("TPC-H recorded table snapshots differ from fixed vector")
    collections = set()
    for table in TABLES:
        snapshot = saved["snapshots"][table]
        if snapshot is not None:
            try:
                canonical = isinstance(snapshot, str) and str(int64(snapshot)) == snapshot
            except ValueError:
                canonical = False
            if not canonical:
                raise ValueError("TPC-H fixed snapshot IDs must be canonical int64 strings or null")
        collection = uuid_string(saved["tables"][table]["collection_id"], "collection_id")
        if collection in collections:
            raise ValueError("TPC-H collection assignments must be distinct")
        collections.add(collection)
    dataset = TPCHDataset(saved["dataset"])
    digest = dataset._manifest_sha256
    if digest != saved.get("dataset_sha256"):
        raise ValueError("TPC-H dataset identity changed")
    validation_started = time.perf_counter()
    dataset.validate()
    dataset_validation_ms = (time.perf_counter() - validation_started) * 1000
    plans = query_plans(queries, profile=dataset.profile, scale_factor=dataset.scale_factor, mode=mode)
    for plan in plans:
        for stage in plan["stages"]:
            stage.update(collection_id=saved["tables"][stage["table"]]["collection_id"], snapshot_id=saved["snapshots"][stage["table"]])
    config = {"url": saved["url"], "database": saved["database_id"], "store_id": saved["store_id"],
              "timeout": timeout, "ca_file": ca_file, "queries": queries, "plans": plans,
              "mode": mode, "collection_mode": collection_mode(saved), "format": format, "consume": consume,
              "users": users, "processes": processes, "warmup": warmup, "repeat": repeat,
              # Interrupted users must unwind SDK cursor contexts before the
              # supervisor's two-second process-group termination deadline.
              "report": str(Path(report).resolve()), "stop_timeout": 0,
              "master_host": "127.0.0.1", "dataset": str(dataset.path),
              "dataset_sha256": digest, "dataset_id": dataset.manifest.get("dataset_id"),
              "scale_factor": dataset.scale_factor, "type_profile": dataset.profile,
              "snapshots": saved["snapshots"], "load_id": saved["load_id"],
              "dataset_validation_ms": dataset_validation_ms,
              "measurement": {"logical_query": "complete source scans plus client reduction and result comparison", "warmup_policy": "each Locust user performs warmup full rounds before repeat measured full rounds", "cache_state": "uncontrolled; warmup only", "physical_io": "not measured", "engine_only_latency": "not measured", "read_mode": read_mode({"format": format, "consume": consume})},
              "hardware": {"platform": platform.platform(), "python": platform.python_version(), "logical_cpus": os.cpu_count()}}
    config["preflight"] = _preflight(saved, dataset, plans, config)
    reference_started = time.perf_counter()
    config["reference_results"] = reference_results(dataset, plans) if mode == "reduce" else {}
    config["reference_evaluation_ms"] = (time.perf_counter() - reference_started) * 1000 if mode == "reduce" else 0
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        config["master_port"] = probe.getsockname()[1]
    destination = Path(config["report"])
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("report directory must be empty to preserve run evidence")
    destination.mkdir(parents=True, exist_ok=True)
    write_json(destination / "run.json", config)
    with (destination / "locust.log").open("wb") as log:
        returncode = _run_owned_process([sys.executable, "-m", "glamdb_workload.tpch_locust", str(destination / "run.json")], stdout=log)
    summary = destination / "summary.json"
    if not summary.exists():
        result = {"status": "INCOMPLETE", "result_verified": False, "error": "Locust exited without a complete report", "returncode": returncode, "report": str(destination)}
        write_json(summary, result)
        return result
    result = json.loads(summary.read_bytes())
    if returncode and result["status"] == "PASS":
        result.update(status="INCOMPLETE", result_verified=False, error="Locust process exited unsuccessfully")
        write_json(summary, result)
    return result
