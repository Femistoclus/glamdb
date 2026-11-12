"""Explicit target setup, immutable ordinary appends, and original-ID recovery."""

import math
from pathlib import Path
import time
from uuid import UUID, uuid4, uuid5

from glamdb_sdk import AppendRequest, Schema
from glamdb_sdk.request import uuid_string

from ..ledger import Ledger
from ..runtime import append_once, client_for, resolve_ledger, validate_url
from ..verification import verify_ledgers
from .dataset import ClickBenchDataset
from .model import exclusive_state, read_json, save_json


class Blocked(ValueError):
    """A missing capability/budget, never a successful partial benchmark."""


def _timeout(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be finite positive seconds")
    return value


def _config(saved, timeout, ca_file):
    return {"url": saved["url"], "database": saved["database_id"],
            "timeout": _timeout(timeout), "ca_file": ca_file}


def read_state(path):
    state = read_json(path)
    if (type(state.get("version")) is not int or state["version"] != 1 or
            state.get("kind") != "clickbench" or
            not state.keys().isdisjoint({"token", "password", "authorization", "client_secret",
                                        "access_token", "refresh_token"})):
        raise ValueError("invalid ClickBench state or persisted credentials")
    state["url"] = validate_url(state["url"])
    for name in ("database_id", "store_id", "collection_id", "load_id"):
        uuid_string(state[name], name)
    if state["store_id"] != state["database_id"]:
        raise ValueError("benchmark database/store identity differs")
    if state.get("test_backend") not in (None, "mock"):
        raise ValueError("unsupported explicit test backend")
    return state


def _dataset(state):
    data = ClickBenchDataset(state["dataset"])
    if data.digest != state["dataset_hash"] or data.schema.to_dict() != state["schema"]:
        raise ValueError("dataset differs from the immutable run")
    return data


def _backend(capabilities, selected):
    mock = capabilities.get("mock")
    backend = capabilities.get("backend")
    if selected == "mock":
        if mock is not True or backend != "mock":
            raise Blocked("explicit mock test profile requires the disposable mock backend")
        return False
    if mock is not False or backend != "local":
        raise Blocked("a real local backend is required by default; mock requires explicit test selection")
    return True


def _request(state, data, ordinal, rows):
    operation = str(uuid5(UUID(state["load_id"]),
                          f'{state["collection_id"]}:{data.digest}:{ordinal}'))
    return AppendRequest.prepare(state["collection_id"], operation, data.schema, rows)


def _plan(state, data, capabilities, *, require_management=False):
    limits = capabilities.get("limits", {})
    if type(limits.get("schema_fields")) is not int or limits["schema_fields"] < len(data.schema.fields):
        raise Blocked("105-column schema is blocked by issue #161")
    features = capabilities.get("features", {})
    if require_management and features.get("database_management") is not True:
        raise Blocked("server does not support explicit database management")
    for feature in ("append", "scan", "scan_pagination", "scan_cancel"):
        if features.get(feature) is not True:
            raise Blocked("server is missing a required ClickBench capability")
    operations = len(data.manifest["parts"]) + 1
    if state["limits"]["max_operations"] > 4096 or operations > 4096:
        raise Blocked("current HTTP database profile is bounded to 4096 operations; see #156")
    if operations > state["limits"]["max_operations"]:
        raise Blocked(f"operation/history profile is insufficient: need at least {operations}; see #156")
    row_limit = limits.get("append_rows", min(limits.get("rows", 10000), 10000))
    byte_limit = limits.get("request_bytes", limits.get("request_body_bytes"))
    if type(row_limit) is not int or row_limit <= 0 or type(byte_limit) is not int or byte_limit <= 0:
        raise Blocked("server did not advertise finite append limits")
    total = maximum = 0
    for ordinal, rows in data.iter_parts():
        request = _request(state, data, ordinal, rows)
        if len(rows) > row_limit or len(request.body) > byte_limit:
            raise Blocked("immutable dataset parts exceed server row/body limits; prepare a new bounded dataset")
        total += len(request.body)
        maximum = max(maximum, len(request.body))
    if total > state["max_journal_bytes"]:
        raise Blocked(f"journal payload budget is insufficient: need {total} bytes, excluding SQLite overhead")
    return {"required_operations": operations, "request_body_bytes": total,
            "max_request_body_bytes": maximum, "storage_history_capacity": "runtime admission"}


@exclusive_state(1)
def initialize(dataset, path, *, url, name, limits=None, max_journal_bytes=1024**3,
               timeout=30, ca_file=None, test_backend=None):
    data = ClickBenchDataset(dataset)
    url = validate_url(url)
    _timeout(timeout)
    selected_limits = dict(limits or {})
    selected_limits.setdefault("max_operations", 4096)
    if (type(selected_limits["max_operations"]) is not int or selected_limits["max_operations"] < 1 or
            type(max_journal_bytes) is not int or max_journal_bytes < 1 or not isinstance(name, str) or not name):
        raise ValueError("invalid target name or finite operation/journal budgets")
    path = Path(path).resolve()
    if path.exists():
        state = read_state(path)
        expected = {"url": url, "name": name, "dataset_hash": data.digest,
                    "limits": selected_limits, "max_journal_bytes": max_journal_bytes,
                    "test_backend": test_backend}
        if any(state.get(key) != value for key, value in expected.items()):
            raise ValueError("init parameters differ from the original run")
        if state["phase"] != "initializing":
            return state
    else:
        database = str(uuid4())
        state = {"version": 1, "kind": "clickbench", "url": url, "name": name,
                 "database_id": database, "store_id": database, "collection_id": str(uuid4()),
                 "load_id": str(uuid4()), "dataset": str(data.path), "dataset_hash": data.digest,
                 "schema": data.schema.to_dict(), "limits": selected_limits,
                 "max_journal_bytes": max_journal_bytes, "test_backend": test_backend,
                 "run_dir": str(path.parent / (path.stem + "-artifacts")),
                 "phase": "initializing", "load_complete": False, "snapshot_id": None}
    config = _config(state, timeout, ca_file)
    with client_for(config, scoped=False) as root:
        caps = root.capabilities(deadline=timeout)
        state["performance_eligible"] = _backend(caps, test_backend)
        state["plan"] = _plan(state, data, caps, require_management=True)
        state["server_version"] = caps.get("server_version")
        run = Path(state["run_dir"])
        if not path.exists():
            run.mkdir(exist_ok=False)
        save_json(path, state)  # Identities must be durable before the first mutation.
        database = root.create_database(name, database_id=state["database_id"],
                                        limits=selected_limits, deadline=timeout)
        if (database.get("database_id") != state["database_id"] or database.get("name") != name or
                database.get("store", {}).get("store_id") != state["store_id"] or database.get("state") != "open"):
            raise ValueError("server returned a different or unavailable database identity")
    with client_for(config) as client:
        outcome = client.create_collection("hits", data.schema, collection_id=state["collection_id"], deadline=timeout)
        if outcome.outcome != "committed":
            raise Blocked("collection creation is unresolved; repeat init with the original state")
        if client.get_schema(state["collection_id"], deadline=timeout) != data.schema:
            raise ValueError("created collection schema differs from the dataset")
        state["target"] = client.get_collection(state["collection_id"], deadline=timeout)
    state["phase"] = "ready"
    save_json(path, state)
    return state


def _summary(state, journal, elapsed=0, confirmed_before=0):
    counts = journal.summary()
    result = {"status": "INCOMPLETE" if counts["pending"] else
              "FAIL" if counts["definitely_not_committed"] else "PASS",
              "confirmed_rows": counts["confirmed_rows"], "operations": counts,
              "newly_confirmed_rows": counts["confirmed_rows"] - confirmed_before,
              "elapsed_seconds": elapsed,
              "rows_per_second": (counts["confirmed_rows"] - confirmed_before) / elapsed if elapsed else None,
              "measurement_scope": "this invocation; operations/confirmed_rows are cumulative",
              "performance_eligible": state["performance_eligible"]}
    result["load_complete"] = state["load_complete"]
    result["dataset_hash"] = state["dataset_hash"]
    result["load_id"] = state["load_id"]
    return result


def _check_target(client, state, data, journal, timeout):
    info = client.get_collection(state["collection_id"], deadline=timeout)
    if client.get_schema(state["collection_id"], deadline=timeout) != data.schema:
        raise ValueError("target schema differs from the immutable run")
    receipts = [row for row in journal.operations() if row["state"] == "committed"]
    if receipts:
        last = client.find_append_outcome(state["collection_id"], receipts[-1]["operation_id"], deadline=timeout)
        if last is None or last.outcome != "committed" or last.result["snapshot_id"] != info["current_snapshot_id"]:
            raise ValueError("target changed outside the recorded load")
    elif info.get("current_snapshot_id") is not None:
        raise ValueError("target must initially be empty")


@exclusive_state(0)
def load(path, *, resume=False, timeout=30, ca_file=None):
    state = read_state(path)
    data = _dataset(state)
    if state["phase"] not in {"ready", "loading", "loaded"}:
        raise ValueError("run is not initialized for loading")
    config = _config(state, timeout, ca_file)
    run = Path(state["run_dir"])
    with client_for(config, scoped=False) as root:
        if root.get_database(state["database_id"], deadline=timeout).get("state") != "open":
            raise Blocked("benchmark database must already be open")
    with Ledger(run / "worker-000000.sqlite") as journal, client_for(config) as client:
        caps = client.capabilities(deadline=timeout)
        _backend(caps, state["test_backend"])
        _plan(state, data, caps)
        existing = {row["operation_id"]: dict(row) for row in journal.operations()}
        if existing and not resume:
            raise ValueError("load already started; use resume with the original journal")
        if any(row["attempts"] > 0 and row["state"] not in {"committed", "definitely_not_committed"}
               for row in existing.values()):
            return _summary(state, journal)
        if any(row["state"] == "definitely_not_committed" for row in existing.values()):
            return _summary(state, journal)
        _check_target(client, state, data, journal, timeout)
        state["phase"] = "loading"
        save_json(path, state)
        confirmed_before = journal.summary()["confirmed_rows"]
        started = time.perf_counter()
        for ordinal, rows in data.iter_parts():
            request = _request(state, data, ordinal, rows)
            previous = existing.get(request.operation_id)
            if previous and previous["state"] == "committed":
                if journal.request(request.operation_id).body != request.body:
                    raise ValueError("prepared request differs from its original journal")
                continue
            result = append_once(client, journal, request, deadline=timeout)
            if result["state"] != "committed":
                result = _summary(state, journal, time.perf_counter() - started, confirmed_before)
                save_json(run / "load.json", result)
                return result
        result = _summary(state, journal, time.perf_counter() - started, confirmed_before)
        if result["confirmed_rows"] != data.manifest["rows"]:
            raise ValueError("committed journal does not cover the whole dataset")
        info = client.get_collection(state["collection_id"], deadline=timeout)
        snapshot = info.get("current_snapshot_id")
        stats = client.stats(state["collection_id"], snapshot_id=snapshot, deadline=timeout)
        if int(stats["rows"]) != data.manifest["rows"]:
            raise ValueError("persisted target row count differs from the recorded dataset")
        state.update(phase="loaded", load_complete=True, snapshot_id=snapshot)
        result.update(load_complete=True, snapshot_id=snapshot)
        save_json(path, state)
        save_json(run / "load.json", result)
        return result


@exclusive_state(0)
def resolve(path, *, retry=False, timeout=30, ca_file=None):
    state = read_state(path)
    _dataset(state)
    with client_for(_config(state, timeout, ca_file)) as client, \
            Ledger(Path(state["run_dir"]) / "worker-000000.sqlite") as journal:
        resolve_ledger(client, journal, retry=retry, deadline=timeout)
        return _summary(state, journal)


@exclusive_state(0)
def verify(path, *, format="arrow", timeout=120, ca_file=None):
    state = read_state(path)
    data = _dataset(state)
    if not state["load_complete"]:
        return {"status": "INCOMPLETE", "reason": "dataset load is not complete"}
    with client_for(_config(state, timeout, ca_file)) as client:
        result = verify_ledgers(client, state["run_dir"], data.schema, state["collection_id"],
                                format=format, deadline=timeout, query={"snapshot_id": state["snapshot_id"]})
    if result["status"] == "PASS" and result["expected_rows"] != data.manifest["rows"]:
        result = {"status": "FAIL", "reason": "journal omitted source rows"}
    result.update(performance_eligible=state["performance_eligible"],
                  dataset_hash=data.digest, snapshot_id=state["snapshot_id"], load_id=state["load_id"])
    save_json(Path(state["run_dir"]) / "verify.json", result)
    return result


def status(path, *, timeout=30, ca_file=None):
    state = read_state(path)
    journal = Path(state["run_dir"]) / "worker-000000.sqlite"
    counts = None
    if journal.exists():
        with Ledger(journal) as log:
            counts = log.summary()
    with client_for(_config(state, timeout, ca_file), scoped=False) as client:
        database = client.get_database(state["database_id"], deadline=timeout)
    return {"status": "PASS" if state["load_complete"] and counts is not None and
            not counts["pending"] and not counts["definitely_not_committed"] else "INCOMPLETE",
            "phase": state["phase"], "load_complete": state["load_complete"],
            "database_id": state["database_id"], "store_id": state["store_id"],
            "collection_id": state["collection_id"], "snapshot_id": state["snapshot_id"],
            "database_state": database.get("state"), "operations": counts,
            "performance_eligible": state["performance_eligible"]}


@exclusive_state(0)
def clean(path, *, timeout=30, ca_file=None):
    state = read_state(path)
    with client_for(_config(state, timeout, ca_file), scoped=False) as client:
        result = client.drop_database(state["database_id"], store_id=state["store_id"], deadline=timeout)
    state.update(phase="dropped", load_complete=False)
    save_json(path, state)
    return {"status": "PASS", "database_id": state["database_id"], "store_id": state["store_id"],
            "drop": result}
