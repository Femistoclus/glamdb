"""Explicit target setup, immutable ordinary appends, and original-ID recovery."""

import math
from pathlib import Path
import time
from uuid import UUID, uuid4, uuid5

from glamdb_sdk import AppendOutcome, HTTPError, ProtocolError, TransportError
from glamdb_sdk.request import uuid_string

from ..ledger import Ledger
from ..runtime import client_for, error_code, validate_url
from ..write_runtime import _send_once, record_outcome
from .dataset import ClickBenchDataset
from .model import exclusive_state, read_json, save_json
from .plan import capacity_preflight, freeze_plan
from .target import KEY_ENCODING, collection_mode, prepare_request, target_schema
from .verify import hydrate_legacy_receipts, validate_journal, verify_contents


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
    collection_mode(state)
    state.setdefault("request_plan_version", 1)
    return state


def _dataset(state):
    data = ClickBenchDataset(state["dataset"])
    if data.digest != state["dataset_hash"] or data.schema.to_dict() != state["schema"]:
        raise ValueError("dataset differs from the immutable run")
    if state.get("target_schema", data.schema.to_dict()) != target_schema(data.schema, collection_mode(state)).to_dict():
        raise ValueError("target schema differs from the original source and collection mode")
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


def _plan(state, data, capabilities, *, require_management=False):
    limits = capabilities.get("limits", {})
    mode = collection_mode(state)
    schema = target_schema(data.schema, mode)
    if type(limits.get("schema_fields")) is not int or limits["schema_fields"] < len(schema.fields):
        raise Blocked("complete source/target schema is blocked by issue #161")
    features = capabilities.get("features", {})
    required = ("scan", "scan_pagination", "scan_cancel") + (("key_value", "upsert") if mode == "kv" else ("append",))
    if require_management:
        required += ("database_management",)
    if any(features.get(feature) is not True for feature in required):
        raise Blocked("server is missing a required ClickBench capability for " + mode)
    if "schema_types" in capabilities and not {f.type for f in schema.fields} <= set(capabilities["schema_types"]):
        raise Blocked("server does not support the selected complete type profile")
    total = maximum = 0
    offset = 0
    for ordinal, rows in data.iter_parts():
        operation = str(uuid5(UUID(state["load_id"]), f'{state["collection_id"]}:{data.digest}:{ordinal}'))
        request = prepare_request(state, data.schema, operation, rows, offset, -9223372036854775808)
        offset += len(rows)
        total += len(request.body)
        maximum = max(maximum, len(request.body))
    plan = {"operations": len(data.manifest["parts"]), "wire_bytes": total}
    try:
        preflight = capacity_preflight(state["limits"], plan, max_journal_bytes=state["max_journal_bytes"])
    except ValueError as error:
        raise Blocked(str(error)) from error
    return {"required_operations": plan["operations"] + 1, "request_body_bytes": total,
            "max_request_body_bytes": maximum, "capacity_preflight": preflight,
            "storage_history_capacity": "server admission under persisted history and device budgets"}


@exclusive_state(1)
def initialize(dataset, path, *, url, name, limits=None, max_journal_bytes=1024**3,
               timeout=30, ca_file=None, test_backend=None, mode=None):
    if mode is not None:
        collection_mode({"collection_mode": mode})
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
        if (any(state.get(key) != value for key, value in expected.items()) or
                (mode is not None and mode != collection_mode(state))):
            raise ValueError("init parameters differ from the original run")
        if state["phase"] != "initializing":
            return state
    else:
        database = str(uuid4())
        mode = mode or "append"
        state = {"version": 1, "kind": "clickbench", "url": url, "name": name,
                 "database_id": database, "store_id": database, "collection_id": str(uuid4()),
                 "load_id": str(uuid4()), "dataset": str(data.path), "dataset_hash": data.digest,
                 "schema": data.schema.to_dict(), "limits": selected_limits,
                 "collection_mode": mode, "target_schema": target_schema(data.schema, mode).to_dict(),
                 "key_encoding": KEY_ENCODING if mode == "kv" else None, "request_plan_version": 2,
                 "max_journal_bytes": max_journal_bytes, "test_backend": test_backend,
                 "run_dir": str(path.parent / (path.stem + "-artifacts")),
                 "phase": "initializing", "load_complete": False, "snapshot_id": None, "confirmed_rows": 0}
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
        schema = target_schema(data.schema, collection_mode(state))
        outcome = client.create_collection("hits", schema, mode=collection_mode(state),
                                           collection_id=state["collection_id"], deadline=timeout)
        if outcome.outcome != "committed":
            raise Blocked("collection creation is unresolved; repeat init with the original state")
        if client.get_schema(state["collection_id"], deadline=timeout) != schema:
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
    result["collection_mode"] = collection_mode(state)
    result["snapshot_id"] = state["snapshot_id"]
    return result


def _sync_progress(state, log):
    last = log.db.execute("SELECT outcome FROM operations WHERE state='committed' ORDER BY rowid DESC LIMIT 1").fetchone()
    import json

    state["snapshot_id"] = json.loads(last["outcome"])["result"]["snapshot_id"] if last else None
    state["confirmed_rows"] = log.summary()["confirmed_rows"]


def _restore_plan(state, data, journal):
    options = state.get("append_plan")
    if options is None:
        if state["request_plan_version"] != 1:
            return None
        # Original runs sent whole source parts within the SDK's 16 MiB wire
        # ceiling. Rebuild that mapping without splitting or changing bodies.
        options = {"batch_rows": 10000, "batch_bytes": 16 * 1024**2, "workers": 1}
    plan = freeze_plan(journal, data, state, batch_rows=options["batch_rows"],
                       batch_bytes=options["batch_bytes"], workers=options["workers"])
    state["append_plan"] = plan
    return plan


def _check_target(client, state, data, timeout):
    info = client.get_collection(state["collection_id"], deadline=timeout)
    if (client.get_schema(state["collection_id"], deadline=timeout) !=
            target_schema(data.schema, collection_mode(state)) or
            info.get("mode", "append") != collection_mode(state)):
        raise ValueError("target schema or mode differs from the immutable run")
    if info.get("current_snapshot_id") != state["snapshot_id"]:
        raise ValueError("foreign writes changed the target outside the recorded load")
    stats = client.stats(state["collection_id"], snapshot_id=state["snapshot_id"], deadline=timeout)
    if int(stats["rows"]) != state.get("confirmed_rows", 0):
        raise ValueError("target row count changed outside the recorded load")


def _write_load_report(state, result, report):
    save_json(Path(state["run_dir"]) / "load.json", result)
    if report is not None:
        from .reports import write_report

        write_report(report, result)
    return result


@exclusive_state(0)
def load(path, *, resume=False, batch_rows=None, batch_bytes=None, workers=None,
         report=None, timeout=30, ca_file=None):
    from .loader import execute_load

    overall = time.perf_counter()
    state = read_state(path)
    data = _dataset(state)
    if report is not None:
        from .reports import validate_report

        report = validate_report(report, path, data, state["run_dir"])
    if state["phase"] not in {"ready", "loading", "loaded", "verification_failed"}:
        raise ValueError("run is not initialized for loading")
    if "append_plan" in state and not resume:
        raise ValueError("load already started; use resume with the original plan")
    original = state.get("append_plan", {})
    batch_rows = original.get("batch_rows", 10000) if batch_rows is None else batch_rows
    default_bytes = 16 * 1024**2 if state["request_plan_version"] == 1 else 8 * 1024**2
    batch_bytes = original.get("batch_bytes", default_bytes) if batch_bytes is None else batch_bytes
    workers = original.get("workers", 1) if workers is None else workers
    config = _config(state, timeout, ca_file)
    with Ledger(Path(state["run_dir"]) / "worker-000000.sqlite") as journal:
        began = time.perf_counter()
        plan = freeze_plan(journal, data, state, batch_rows=batch_rows, batch_bytes=batch_bytes, workers=workers)
        state["append_plan"] = plan
        save_json(path, state)
        if state["request_plan_version"] == 1:
            with client_for(config) as client:
                hydrate_legacy_receipts(client, journal, data, state, timeout=timeout)
        validate_journal(journal, data, state)
        if journal.summary()["pending"] or journal.summary()["definitely_not_committed"]:
            return _write_load_report(state, _summary(state, journal), report)
        _sync_progress(state, journal)
        import shutil

        stored = journal.db.execute("SELECT coalesce(sum(bytes),0) FROM operations").fetchone()[0]
        remaining = max(0, plan["wire_bytes"] - stored)
        free = shutil.disk_usage(state["run_dir"]).free
        # This is a lower bound, not a reservation or a guarantee about SQLite
        # overhead/concurrent growth. Retain room for the source multiset too.
        minimum = remaining + data.manifest["converted_bytes"] + 16 * 1024**2
        if free < minimum:
            raise Blocked(f"client journal/verification disk needs at least {minimum} free bytes; found {free}")
        state["client_disk_preflight"] = {"free_bytes": free, "minimum_free_bytes": minimum,
                                           "journal_payload_remaining_bytes": remaining,
                                           "verification_source_bytes": data.manifest["converted_bytes"],
                                           "scope": "lower bound; SQLite/index overhead and external growth need headroom"}
        with client_for(config, scoped=False) as root:
            database = root.get_database(state["database_id"], deadline=timeout)
            if database.get("state") != "open":
                raise Blocked("benchmark database must already be open")
            state["effective_limits"] = database["limits"]
            try:
                state["capacity_preflight"] = capacity_preflight(
                    database["limits"], plan, max_journal_bytes=state["max_journal_bytes"])
            except ValueError as error:
                raise Blocked(str(error)) from error
        with client_for(config) as client:
            caps = client.capabilities(deadline=timeout)
            _backend(caps, state["test_backend"])
            if caps.get("store_id", state["store_id"]) != state["store_id"]:
                raise ValueError("server store identity differs from the original target")
            _check_target(client, state, data, timeout)
            state.update(phase="loading", load_complete=False)
            save_json(path, state)
            preflight = time.perf_counter() - began
            confirmed_before = journal.summary()["confirmed_rows"]
            started, cpu = time.perf_counter(), time.process_time()
            metrics = execute_load(client, journal, data, state, path, timeout=timeout)
            elapsed = time.perf_counter() - started
            result = _summary(state, journal, elapsed, confirmed_before)
            metrics.update(elapsed_seconds=elapsed, client_cpu_seconds=time.process_time() - cpu,
                           preflight_seconds=preflight, request_body_bytes=journal.summary()["confirmed_bytes"],
                           source_file_bytes=data.manifest["source"]["bytes"], converted_bytes=data.manifest["converted_bytes"],
                           journal_bytes=journal.path.stat().st_size)
            import json

            kv = {}
            if collection_mode(state) == "kv":
                for op in journal.db.execute("SELECT outcome FROM operations WHERE state='committed'"):
                    receipt = json.loads(op[0])["result"]
                    for key in ("input_rows", "rows_affected", "rows_written", "rows_removed", "data_files_added", "data_files_removed"):
                        kv[key] = kv.get(key, 0) + int(receipt[key])
                metrics["kv_mutations"] = kv
            result["metrics"] = metrics
            if journal.summary()["pending"] or journal.summary()["committed"] != plan["operations"]:
                save_json(path, state)
                return _write_load_report(state, result, report)
            _check_target(client, state, data, timeout)
            verified = time.perf_counter()
            verification = verify_contents(client, journal, data, state,
                                            format="json" if state["test_backend"] == "mock" else "arrow", timeout=max(timeout, 120))
            metrics["verification_seconds"] = time.perf_counter() - verified
            metrics["command_elapsed_seconds"] = time.perf_counter() - overall
            state.update(phase="loaded" if verification["status"] == "PASS" else "verification_failed",
                         load_complete=verification["status"] == "PASS")
            result.update(status=verification["status"], load_complete=state["load_complete"],
                          snapshot_id=state["snapshot_id"], verification=verification)
            save_json(path, state)
            _save_verification(state, data, verification)
            return _write_load_report(state, result, report)


def _save_verification(state, data, result):
    result.update(performance_eligible=state["performance_eligible"], collection_mode=collection_mode(state),
                  dataset_hash=data.digest if data is not None else state["dataset_hash"],
                  snapshot_id=state["snapshot_id"], load_id=state["load_id"])
    save_json(Path(state["run_dir"]) / "verify.json", result)


@exclusive_state(0)
def resolve(path, *, retry=False, timeout=30, ca_file=None):
    state = read_state(path)
    data = _dataset(state)
    with client_for(_config(state, timeout, ca_file)) as client, \
            Ledger(Path(state["run_dir"]) / "worker-000000.sqlite") as journal:
        if _restore_plan(state, data, journal) is None:
            return {"status": "INCOMPLETE", "reason": "no immutable load plan to resolve"}
        hydrate_legacy_receipts(client, journal, data, state, timeout=timeout)
        validate_journal(journal, data, state)
        for op in journal.operations(pending=True):
            request = journal.request(op["operation_id"])
            try:
                outcome = client.find_append_outcome(request.collection_id, request.operation_id, deadline=timeout)
                if isinstance(outcome, AppendOutcome):
                    record_outcome(journal, request, outcome, authoritative=True)
                elif outcome is None:
                    journal.mark(request.operation_id, "not_found")
                    if retry:
                        _send_once(client, journal, request, deadline=timeout)
                else:
                    journal.mark(request.operation_id, "in_progress")
            except (HTTPError, ProtocolError, TransportError) as error:
                journal.mark(request.operation_id, "transport_error", error_code=error_code(error))
        _sync_progress(state, journal)
        save_json(path, state)
        return _summary(state, journal)


@exclusive_state(0)
def verify(path, *, format="arrow", timeout=120, ca_file=None):
    state = read_state(path)
    if not state["load_complete"] and state["phase"] not in {"verification_failed", "verifying"}:
        return {"status": "INCOMPLETE", "reason": "dataset load is not complete"}
    # A failed or interrupted recheck must revoke an earlier successful result.
    state.update(load_complete=False, phase="verifying")
    save_json(path, state)
    try:
        data = _dataset(state)
        with client_for(_config(state, timeout, ca_file)) as client, \
                Ledger(Path(state["run_dir"]) / "worker-000000.sqlite") as journal:
            if _restore_plan(state, data, journal) is None:
                raise ValueError("complete verification requires the immutable load plan")
            hydrate_legacy_receipts(client, journal, data, state, timeout=timeout)
            if state["request_plan_version"] == 1:
                _sync_progress(state, journal)
            _check_target(client, state, data, timeout)
            result = verify_contents(client, journal, data, state, format=format, timeout=timeout)
    except BaseException as error:
        state["phase"] = "verification_failed"
        save_json(path, state)
        _save_verification(state, None, {"status": "FAIL", "reason": error_code(error)})
        raise
    state.update(load_complete=result["status"] == "PASS",
                 phase="loaded" if result["status"] == "PASS" else "verification_failed")
    save_json(path, state)
    _save_verification(state, data, result)
    return result


def status(path, *, timeout=30, ca_file=None):
    state = read_state(path)
    counts = None
    path = Path(state["run_dir"]) / "worker-000000.sqlite"
    if path.exists():
        with Ledger(path, readonly=True) as journal:
            counts = journal.summary()
    with client_for(_config(state, timeout, ca_file), scoped=False) as client:
        database = client.get_database(state["database_id"], deadline=timeout)
    return {"status": "PASS" if state["load_complete"] and counts is not None and
            not counts["pending"] and not counts["definitely_not_committed"] else "INCOMPLETE",
            "phase": state["phase"], "load_complete": state["load_complete"],
            "database_id": state["database_id"], "store_id": state["store_id"],
            "collection_id": state["collection_id"], "snapshot_id": state["snapshot_id"],
            "collection_mode": collection_mode(state), "database_state": database.get("state"), "operations": counts,
            "limits": database.get("limits"), "append_plan": state.get("append_plan"),
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
