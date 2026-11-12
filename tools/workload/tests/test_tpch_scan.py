"""Cursor ownership, wrong-result refusal and finite Locust reporting."""

from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from uuid import NAMESPACE_DNS, uuid5
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pyarrow as pa
import psutil
import pytest

from glamdb_sdk import Client, Field, ProtocolError, Schema
from glamdb_workload import tpch_scan
from glamdb_workload.tpch_queries import query_plan
from glamdb_workload.tpch_schema import table_schema


CID = "11111111-2222-4333-8444-555555555555"
SCHEMA = Schema([Field("l_extendedprice", "decimal", precision=15, scale=2), Field("l_discount", "decimal", precision=15, scale=2)])


def bound_plan():
    plan = query_plan(6)
    plan["stages"][0].update(collection_id=CID, snapshot_id="42")
    return plan


def metadata():
    return {"schema": SCHEMA.to_dict(), "snapshot_id": "42", "identity": {"fixed": True}}


def test_json_scan_drains_all_pages_and_returns_only_candidates():
    calls = []

    def handle(request):
        body = json.loads(request.content)
        calls.append((request.url.path, body))
        if request.url.path.endswith("/open"):
            assert body["snapshot_id"] == "42" and "limit" not in body
            return httpx.Response(200, json=dict(metadata(), cursor="first"))
        rows = [{"l_extendedprice": "100.00", "l_discount": "0.06"}]
        return httpx.Response(200, json=dict(metadata(), rows=rows, next_cursor="second" if body["cursor"] == "first" else None, profile={}))

    with Client("https://test.invalid", "not-saved", transport=httpx.MockTransport(handle)) as client:
        result = tpch_scan.execute_query(client, bound_plan(), format="json", consume="rows")
    assert result["stages"][0]["rows"] == 2
    assert result["stages"][0]["pages"] == 2
    assert result["result_verified"] is False and result["result"] is None
    assert len(calls) == 3 and not any(path.endswith("/cancel") for path, _ in calls)


def test_wrong_snapshot_is_rejected_and_unfinished_cursor_cancelled():
    cancelled = []

    def handle(request):
        if request.url.path.endswith("/cancel"):
            cancelled.append(json.loads(request.content)["cursor"])
            return httpx.Response(200, json={})
        wrong = dict(metadata(), snapshot_id="43")
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json=dict(wrong, cursor="first"))
        return httpx.Response(200, json=dict(wrong, rows=[], next_cursor="second", profile={}))

    progress = {}
    with Client("https://test.invalid", "secret", transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProtocolError, match="snapshot"):
            tpch_scan.execute_query(client, bound_plan(), format="json", consume="rows", progress=progress)
    assert cancelled == ["second"]
    assert progress["complete"] is False


def test_arrow_full_eos_and_batch_bytes_without_row_materialization():
    batch = pa.record_batch({"l_extendedprice": pa.array([Decimal("100.00")], type=pa.decimal128(15, 2)), "l_discount": pa.array([Decimal("0.06")], type=pa.decimal128(15, 2))})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    raw = sink.getvalue().to_pybytes()
    pages = []
    counter = type("Counter", (), {"pages": 0})()

    def handle(request):
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json={"cursor": "first", "snapshot_id": "42"})
        pages.append(json.loads(request.content))
        counter.pages += 1
        headers = {"content-type": "application/vnd.apache.arrow.stream", "content-length": str(len(raw)), "x-content-sha256": hashlib.sha256(raw).hexdigest(), "x-glamdb-snapshot-id": "42"}
        if len(pages) == 1:
            headers["x-glamdb-next-cursor"] = "second"
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers=headers)

    with Client("https://test.invalid", "secret", transport=httpx.MockTransport(handle)) as client:
        result = tpch_scan.execute_query(client, bound_plan(), mode="reduce", expected={"revenue": "12.00"}, page_counter=counter)
    assert result["result"] == {"revenue": "12.00"} and result["result_verified"] is True
    measured = result["stages"][0]
    assert (measured["rows"], measured["pages"], measured["arrow_batches"]) == (2, 2, 2)
    assert measured["arrow_wire_bytes"] == len(raw) * 2
    assert measured["arrow_batch_bytes"] == batch.nbytes * 2
    assert measured["integrity"] == "verified"
    assert result["client_compute_ms"] > 0 and result["result_validation_ms"] >= 0


def test_reduction_result_mismatch_is_a_failure():
    def handle(request):
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json=dict(metadata(), cursor="first"))
        return httpx.Response(200, json=dict(metadata(), rows=[], next_cursor=None, profile={}))

    progress = {}
    with Client("https://test.invalid", "secret", transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(tpch_scan.ReductionMismatch):
            tpch_scan.execute_query(client, bound_plan(), mode="reduce", format="json", consume="rows", expected={"revenue": "1.00"}, progress=progress)
    assert progress["result"] == {"revenue": None}
    assert progress["result_verified"] is False


def test_keyboard_interrupt_cancels_cursor_and_keeps_partial_work():
    cancelled = []

    def handle(request):
        body = json.loads(request.content)
        if request.url.path.endswith("/cancel"):
            cancelled.append(body["cursor"])
            return httpx.Response(200, json={})
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json=dict(metadata(), cursor="first"))
        if body["cursor"] == "second":
            raise KeyboardInterrupt()
        return httpx.Response(200, json=dict(metadata(), rows=[{"l_extendedprice": "100.00", "l_discount": "0.06"}], next_cursor="second", profile={}))

    progress = {}
    with Client("https://test.invalid", "secret", transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(KeyboardInterrupt):
            tpch_scan.execute_query(client, bound_plan(), format="json", consume="rows", progress=progress)
    assert cancelled == ["second"]
    assert progress["stages"][0]["rows"] == 1
    assert progress["stages"][0]["status"] == "cancelled"


class MetricsDouble(BaseHTTPRequestHandler):
    """Metrics/refusal fixture only; native qualification is a separate suite."""
    def log_message(self, *args): pass

    def reply(self, status, value):
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        if self.path.endswith("/open"):
            with self.server.counter_lock:
                self.server.offered += 1
                refused = self.server.refuse_every and self.server.offered % self.server.refuse_every == 0
            if refused:
                return self.reply(503, {"error": {"code": "SERVER_BUSY", "message": "metrics refusal"}, "execution_started": False})
            return self.reply(200, dict(metadata(), cursor="first"))
        if self.path.endswith("/cancel"):
            return self.reply(200, {})
        return self.reply(200, dict(metadata(), rows=[{"l_extendedprice": "100.00", "l_discount": "0.06"}], next_cursor="second" if body["cursor"] == "first" else None, profile={}))


@pytest.mark.parametrize("processes,users,warmup,refuse", [(1, 2, 1, 0), (2, 3, 0, 0), (2, 3, 0, 3)])
def test_locust_owns_finite_repetitions_and_merges_refusal_histograms(tmp_path, processes, users, warmup, refuse):
    server = ThreadingHTTPServer(("127.0.0.1", 0), MetricsDouble)
    server.counter_lock, server.offered, server.refuse_every = threading.Lock(), 0, refuse
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    report = tmp_path / "report"
    report.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        master_port = probe.getsockname()[1]
    config = dict(url=f"http://127.0.0.1:{server.server_port}", database="11111111-1111-4111-8111-111111111111", timeout=10, ca_file=None, plans=[bound_plan()], mode="reduce", format="json", consume="rows", users=users, processes=processes, warmup=warmup, repeat=2, report=str(report), reference_results={"6": {"revenue": "12.00"}}, master_host="127.0.0.1", master_port=master_port, stop_timeout=0)
    path = report / "run.json"
    path.write_text(json.dumps(config))
    try:
        process = subprocess.run([sys.executable, "-m", "glamdb_workload.tpch_locust", str(path)], capture_output=True, text=True, env=dict(os.environ, GLAMDB_TOKEN="tpch-metrics-secret"), timeout=75)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
    summary = json.loads((report / "summary.json").read_bytes())
    assert summary["status"] == ("FAIL" if refuse else "PASS"), (process.stdout, process.stderr, summary)
    assert summary["counts"]["offered_queries"] == users * 2
    assert summary["warmup"]["offered_queries"] == users * warmup
    assert len(summary["workers"]) == processes
    complete = summary["counts"]["completed_queries"]
    assert summary["stages"]["Q6/l"]["rows"] == complete * 2
    assert summary["latency_ms"]["success"]["Q6/logical"]["count"] == complete
    if refuse:
        assert summary["counts"]["rejected_queries"] == 2
        assert summary["rejections_by_code"] == {"SERVER_BUSY": 2}
        assert summary["latency_ms"]["refused"]["Q6/logical"]["count"] == 2
        assert summary["result_verified"] is False
    else:
        assert summary["result_verified"] is True
    for name in ("summary.json", "locust_stats.csv", "locust.html"):
        assert "tpch-metrics-secret" not in (report / name).read_text()


@pytest.mark.parametrize("kwargs", [{"queries": "all", "mode": "reduce"}, {"format": "json", "consume": "batches"}, {"users": 0}, {"processes": 0}, {"repeat": 0}])
def test_invalid_parameters_fail_before_any_network_or_report(tmp_path, kwargs):
    destination = tmp_path / "report"
    with pytest.raises(ValueError):
        tpch_scan.run(tmp_path / "missing-state.json", report=destination, **kwargs)
    assert not destination.exists()


@pytest.mark.parametrize("failure", [None, "mock", "store", "schema", "ops", "capacity"])
def test_preflight_binds_scoped_native_identity_and_schema(monkeypatch, failure):
    saved = {"database_id": CID, "store_id": CID, "tables": {t: {"collection_id": CID, "schema": table_schema(t, "native").to_dict()} for t in tpch_scan.TABLES}}
    dataset = SimpleNamespace(tables={t: SimpleNamespace(schema=table_schema(t, "native")) for t in tpch_scan.TABLES})
    features = dict.fromkeys(["database_management", "scan", "scan_pagination", "scan_cancel", "arrow_export"], True)
    caps = {"features": features, "mock": failure == "mock", "backend": "local", "store_id": CID if failure != "store" else "wrong", "predicate_ops": ["and", "ge", "lt", "le"], "limits": {"predicate_literals": 4 if failure == "capacity" else 100}}
    if failure == "ops": caps["predicate_ops"] = ["and"]

    class FakeClient:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def capabilities(self, **kwargs): return caps
        def get_database(self, database, **kwargs):
            return {"database_id": database, "state": "open", "store": {"store_id": CID}}
        def get_schema(self, collection, **kwargs):
            table = self.tables.pop(0)
            return Schema([Field("wrong", "string")]) if failure == "schema" else table_schema(table, "native")
        def __init__(self): self.tables = sorted(tpch_scan.TABLES)

    monkeypatch.setattr(tpch_scan, "client_for", lambda *args, **kwargs: FakeClient())
    config = dict(timeout=10, format="arrow", users=1)
    if failure is None:
        result = tpch_scan._preflight(saved, dataset, [bound_plan()], config)
        assert result["capabilities"]["store_id"] == CID
    else:
        with pytest.raises(ValueError):
            tpch_scan._preflight(saved, dataset, [bound_plan()], config)


@pytest.mark.parametrize("problem", ["incomplete", "missing-vector", "drift", "noncanonical", "duplicate-collection"])
def test_incomplete_or_damaged_snapshot_binding_is_rejected_before_dataset_access(tmp_path, problem):
    from glamdb_workload.tpch_state import write_state

    saved = dict(version=1, url="http://127.0.0.1:8080", database_id=CID, store_id=CID,
                 load_id=CID, load_complete=True, dataset=str(tmp_path / "missing-dataset"),
                 snapshots={t: "42" for t in tpch_scan.TABLES},
                 tables={t: {"snapshot_id": "42", "collection_id": str(uuid5(NAMESPACE_DNS, t))} for t in tpch_scan.TABLES})
    if problem == "incomplete": saved["load_complete"] = False
    elif problem == "missing-vector": del saved["snapshots"]["region"]
    elif problem == "drift": saved["tables"]["region"]["snapshot_id"] = "43"
    elif problem == "noncanonical":
        saved["snapshots"]["region"] = saved["tables"]["region"]["snapshot_id"] = "042"
    else: saved["tables"]["region"]["collection_id"] = saved["tables"]["nation"]["collection_id"]
    state = tmp_path / "state.json"
    write_state(state, saved)
    with pytest.raises(ValueError, match="snapshot|collection|completed"):
        tpch_scan.run(state, report=tmp_path / "report")
    assert not (tmp_path / "report").exists()


def _live_process(pid):
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _signal_process_fixture(tmp_path, behavior):
    """Use the actual controller/helper; replace only native scan preflight."""
    grandchild = tmp_path / "grandchild.py"
    grandchild.write_text("""import os, signal, sys, time
from pathlib import Path
if sys.argv[2] == 'ignore':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""")
    child = tmp_path / "child.py"
    child.write_text("""import json, os, signal, subprocess, sys, time
from pathlib import Path
behavior = sys.argv[1]
grandchild = subprocess.Popen([sys.executable, sys.argv[2], sys.argv[3], 'default' if behavior == 'graceful' else 'ignore'])
if behavior == 'stubborn':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
elif behavior == 'graceful':
    def finish(signum, frame):
        grandchild.terminate()
        grandchild.wait()
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, finish)
until = time.monotonic() + 10
while not Path(sys.argv[3]).exists() and time.monotonic() < until:
    time.sleep(0.01)
pids = Path(sys.argv[4])
temporary = pids.with_suffix('.tmp')
temporary.write_text(json.dumps({'controller': os.getppid(), 'child': os.getpid(), 'grandchild': grandchild.pid}))
temporary.replace(pids)
if behavior == 'normal-exit':
    raise SystemExit(7)
time.sleep(60)
""")
    metadata = tmp_path / "processes.json"
    grand_ready = tmp_path / "grandchild.ready"
    (tmp_path / "sitecustomize.py").write_text("""import os, subprocess, sys
from glamdb_workload import tpch_scan
def run(state, **kwargs):
    command = [sys.executable, os.environ['TPCH_SIGNAL_CHILD'], os.environ['TPCH_SIGNAL_BEHAVIOR'], os.environ['TPCH_SIGNAL_GRANDCHILD'], os.environ['TPCH_SIGNAL_READY'], os.environ['TPCH_SIGNAL_PIDS']]
    code = tpch_scan._run_owned_process(command, stdout=subprocess.DEVNULL)
    return {'status': 'PASS' if code == 0 else 'INCOMPLETE'}
tpch_scan.run = run
""")
    environment = dict(os.environ, GLAMDB_PYTHON=sys.executable,
                       PYTHONPATH=os.pathsep.join(filter(None, (str(tmp_path), os.environ.get("PYTHONPATH")))),
                       TPCH_SIGNAL_CHILD=str(child), TPCH_SIGNAL_BEHAVIOR=behavior,
                       TPCH_SIGNAL_GRANDCHILD=str(grandchild), TPCH_SIGNAL_READY=str(grand_ready),
                       TPCH_SIGNAL_PIDS=str(metadata))
    return environment, metadata


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("behavior", ["graceful", "stubborn", "leader-first"])
def test_cli_signal_reaps_owned_locust_process_group_before_bridge_deadline(tmp_path, signum, behavior):
    cli = Path(__file__).resolve().parents[3] / "build/client-standalone/glamdb-cli"
    if not cli.is_file():
        pytest.skip("build the standalone network CLI to test its actual signal bridge")
    environment, metadata = _signal_process_fixture(tmp_path, behavior)
    process = subprocess.Popen([str(cli), "bench", "tpch", "scan", "--state", str(tmp_path / "state.json"), "--report", str(tmp_path / "report")], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    owned = {}
    try:
        until = time.monotonic() + 12
        while not metadata.exists() and process.poll() is None and time.monotonic() < until:
            time.sleep(0.01)
        if not metadata.exists():
            output, error = process.communicate(timeout=2)
            pytest.fail("owned child did not start: " + output + error)
        owned = json.loads(metadata.read_text())
        started = time.monotonic()
        process.send_signal(signum)
        output, error = process.communicate(timeout=7)
        elapsed = time.monotonic() - started
        live = {name: pid for name, pid in owned.items() if _live_process(pid)}
        assert process.returncode == 128 + signum, (output, error)
        assert elapsed < 4.5 and not live, {"elapsed_seconds": elapsed, "live_processes": live, "stderr": error}
    finally:
        if owned:
            try:
                os.killpg(owned["child"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=3)


def test_owned_group_is_cleaned_when_leader_returns_before_workers(tmp_path):
    cli = Path(__file__).resolve().parents[3] / "build/client-standalone/glamdb-cli"
    if not cli.is_file():
        pytest.skip("build the standalone network CLI to test its actual controller")
    environment, metadata = _signal_process_fixture(tmp_path, "normal-exit")
    process = subprocess.Popen([str(cli), "bench", "tpch", "scan", "--state", str(tmp_path / "state.json"), "--report", str(tmp_path / "report")], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    owned = {}
    try:
        output, error = process.communicate(timeout=7)
        assert process.returncode == 3, (output, error)
        owned = json.loads(metadata.read_text())
        assert not {name: pid for name, pid in owned.items() if _live_process(pid)}
    finally:
        if owned:
            try:
                os.killpg(owned["child"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=3)


def test_owned_process_defers_launch_signals_and_ignores_repeats_during_cleanup(monkeypatch):
    launched = []
    original_popen = subprocess.Popen
    original_cleanup = tpch_scan._cleanup_owned_process
    previous_int, previous_term = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)

    def terminate(signum, frame):
        raise SystemExit(128 + signum)

    def interrupt(signum, frame):
        raise RuntimeError("SIGINT interrupted cleanup")

    def interrupt_launch(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        launched.append(process)
        # Deliver after the OS created the child but before Python receives
        # its handle. A direct unwind here loses ownership of that child.
        signal.raise_signal(signal.SIGTERM)
        return process

    def interrupt_cleanup(process):
        signal.raise_signal(signal.SIGINT)
        signal.raise_signal(signal.SIGTERM)
        return original_cleanup(process)

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, terminate)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(tpch_scan.subprocess, "Popen", interrupt_launch)
            patch.setattr(tpch_scan, "_cleanup_owned_process", interrupt_cleanup)
            with pytest.raises(SystemExit) as stopped:
                tpch_scan._run_owned_process([sys.executable, "-c", "import time; time.sleep(60)"], stdout=subprocess.DEVNULL)
        assert stopped.value.code == 143
        assert len(launched) == 1 and launched[0].poll() is not None
        assert signal.getsignal(signal.SIGINT) is interrupt
        assert signal.getsignal(signal.SIGTERM) is terminate
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)
        for process in launched:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=3)


def test_owned_process_preserves_exit_code_when_group_is_already_gone():
    assert tpch_scan._run_owned_process([sys.executable, "-c", "raise SystemExit(23)"], stdout=subprocess.DEVNULL) == 23


def test_ignored_launch_signal_does_not_displace_actionable_termination(monkeypatch):
    launched = []
    original_popen = subprocess.Popen
    previous_int, previous_term = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)

    def terminate(signum, frame):
        raise SystemExit(128 + signum)

    def interrupt_launch(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        launched.append(process)
        signal.raise_signal(signal.SIGINT)
        signal.raise_signal(signal.SIGTERM)
        return process

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, terminate)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(tpch_scan.subprocess, "Popen", interrupt_launch)
            with pytest.raises(SystemExit) as stopped:
                tpch_scan._run_owned_process([sys.executable, "-c", "import time; time.sleep(0.05)"], stdout=subprocess.DEVNULL)
        assert stopped.value.code == 143
        assert len(launched) == 1 and launched[0].poll() is not None
        assert signal.getsignal(signal.SIGINT) == signal.SIG_IGN
        assert signal.getsignal(signal.SIGTERM) is terminate
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)
        for process in launched:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=3)
