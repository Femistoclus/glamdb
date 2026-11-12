"""Finite query campaigns and real SDK cursor accounting."""

import hashlib
from contextlib import contextmanager
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import date
from dataclasses import asdict, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from glamdb_sdk import Client, HTTPError
from glamdb_workload.clickbench import reduce, run
from glamdb_workload.clickbench.queries import plan_query


CID = "11111111-2222-4333-8444-555555555555"


def arrow_payload():
    batch = pa.record_batch({"UserID": [9007199254740993]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes(), batch.nbytes


def page_headers(raw, *, following=None):
    headers = {"content-type": "application/vnd.apache.arrow.stream",
               "content-length": str(len(raw)),
               "x-content-sha256": hashlib.sha256(raw).hexdigest(),
               "x-glamdb-snapshot-id": "42"}
    if following:
        headers["x-glamdb-next-cursor"] = following
    return headers


@pytest.mark.parametrize("options", [{"users": 0}, {"processes": 0}, {"users": True},
                                    {"warmup": -1}, {"repeats": 0},
                                    {"timeout": float("nan")}, {"ids": "1,1"},
                                    {"mode": "unsupported"}, {"max_candidate_bytes": 0}])
def test_invalid_campaign_parameters_fail_before_state_or_report(tmp_path, options):
    destination = tmp_path / "queries.json"
    with pytest.raises(ValueError):
        run.campaign(tmp_path / "absent-state", tmp_path / "absent-catalog", destination, **options)
    assert not destination.exists()
    assert not destination.with_name("queries-locust").exists()


def test_existing_sibling_report_is_preserved_before_state_access(tmp_path):
    existing = tmp_path / "queries.csv"
    existing.write_text("original evidence\n")
    with pytest.raises(ValueError, match="unused"):
        run.campaign(tmp_path / "absent-state", tmp_path / "absent-catalog", tmp_path / "queries.json")
    assert existing.read_text() == "original evidence\n"


def test_report_keeps_all_csv_measurements_when_json_sample_budget_is_exhausted(tmp_path, monkeypatch):
    artifacts = tmp_path / "locust"
    artifacts.mkdir()
    samples = [dict(query_id=18, mode="scan", user=1, iteration=iteration, status="PASS",
                    warmup=False, candidate_rows=2, elapsed_seconds=0.5) for iteration in range(2)]
    (artifacts / "measurements-local.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
    result = dict(status="PASS", rows=2, collection_mode="append", dataset_hash="dataset",
                  snapshot_id="42", query_catalog_sha256="catalog", coverage=[], counts={"completed_queries": 2})
    monkeypatch.setattr(run, "INLINE_SAMPLE_BYTES", 1)
    report = tmp_path / "queries.json"
    run._write_reports(report, result, artifacts)
    persisted = json.loads(report.read_bytes())
    assert persisted["measurements"] == [] and persisted["inline_samples_complete"] is False
    assert persisted["counts"]["completed_queries"] == 2
    assert len(report.with_suffix(".csv").read_text().splitlines()) == 3
    assert persisted["measurement_files"] == [str(artifacts / "measurements-local.jsonl")]


def test_failed_stream_preserves_consumed_rows_bytes_and_cancels_cursor(tmp_path):
    raw, batch_bytes = arrow_payload()
    cancelled = []
    counter = type("Counter", (), {"pages": 0})()

    def handle(request):
        if request.url.path.endswith("/cancel"):
            cancelled.append(json.loads(request.content)["cursor"])
            return httpx.Response(200, json={})
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json={"cursor": "first", "snapshot_id": "42"})
        if json.loads(request.content)["cursor"] == "second":
            return httpx.Response(503, json={"error": {"code": "SERVER_BUSY", "message": "fixture"},
                                            "execution_started": False})
        counter.pages += 1
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers=page_headers(raw, following="second"))

    progress = {}
    with Client("https://test.invalid", "not-recorded", transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(HTTPError):
            reduce.execute_query(client, CID, plan_query("SELECT UserID FROM hits", 18, snapshot="42"),
                                 mode="scan", workspace=tmp_path, progress=progress, page_counter=counter)
    assert cancelled == ["second"]
    assert (progress["candidate_rows"], progress["batches"], progress["pages"]) == (1, 1, 1)
    assert progress["candidate_arrow_bytes"] == batch_bytes
    assert progress["arrow_wire_bytes"] == len(raw)
    assert progress["status"] == "FAIL" and progress["complete"] is False
    assert progress["logical_latency_ms"] > 0


def test_scan_counts_batches_without_sql_conversion_or_duckdb(tmp_path, monkeypatch):
    raw, batch_bytes = arrow_payload()

    def handle(request):
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json={"cursor": "first", "snapshot_id": "42"})
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers=page_headers(raw))

    def unexpected_conversion(batch):
        raise AssertionError("scan must consume the native Arrow batch directly")

    monkeypatch.setattr(reduce, "logical_batch", unexpected_conversion)
    with Client("https://test.invalid", "not-recorded", transport=httpx.MockTransport(handle)) as client:
        result = reduce.execute_query(client, CID, plan_query("SELECT UserID FROM hits", 18, snapshot="42"),
                                      mode="scan", workspace=tmp_path)
    assert result["status"] == "PASS" and result["complete"] is True
    assert result["sql_validation"] == "NOT_APPLICABLE"
    assert result["result_verified"] is False
    assert (result["candidate_rows"], result["candidate_arrow_bytes"]) == (1, batch_bytes)


def test_independent_validation_is_excluded_from_logical_latency(tmp_path):
    raw, _ = arrow_payload()

    def handle(request):
        if request.url.path.endswith("/open"):
            return httpx.Response(200, json={"cursor": "first", "snapshot_id": "42"})
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers=page_headers(raw))

    def reference():
        time.sleep(0.2)
        return pa.table({"UserID": [9007199254740993]}).to_reader()

    started = time.perf_counter()
    with Client("https://test.invalid", "not-recorded", transport=httpx.MockTransport(handle)) as client:
        result = reduce.execute_query(client, CID, plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"),
                                      mode="reduce", workspace=tmp_path, oracle_factory=reference)
    wall = time.perf_counter() - started
    assert result["status"] == "PASS" and result["result_verified"] is True
    assert result["result_validation_ms"] >= 200
    assert result["elapsed_seconds"] < wall - 0.15
    assert result["logical_latency_ms"] == pytest.approx(result["elapsed_seconds"] * 1000)


class DelayedArrowSource:
    def __init__(self, delay=0):
        self.delay = delay
        self.closed = False

    @contextmanager
    def scan_batches(self, collection, **query):
        def batches():
            yield pa.record_batch({"UserID": [1]})
            time.sleep(self.delay)
            yield pa.record_batch({"UserID": [2]})
        try:
            yield batches()
        finally:
            self.closed = True


def test_absolute_deadline_rejects_oracle_factory_that_returns_after_budget(tmp_path):
    import duckdb

    with duckdb.connect(config={"threads": 1}):
        pass
    client = DelayedArrowSource()

    def reference():
        time.sleep(0.3)
        return pa.table({"UserID": [1, 2]}).to_reader()

    progress = {}
    with pytest.raises(TimeoutError):
        reduce.execute_query(client, CID, plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"),
                             mode="reduce", workspace=tmp_path, oracle_factory=reference,
                             timeout=0.1, progress=progress)
    assert client.closed and progress["status"] == "FAIL" and not progress["result_verified"]
    assert progress["result_validation_ms"] >= 300


def test_deadline_expiring_between_batches_cannot_be_reset_by_next_sql(tmp_path):
    client = DelayedArrowSource(delay=0.3)
    plan = replace(plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"),
                   validation_sql="SELECT SUM(i) FROM range(1000000000) t(i)")
    progress = {}
    began = time.monotonic()
    with pytest.raises(TimeoutError):
        reduce.execute_query(client, CID, plan, mode="reduce", workspace=tmp_path, timeout=0.1,
                             oracle_factory=lambda: pa.table({"UserID": [1, 2]}).to_reader(), progress=progress)
    assert time.monotonic() - began < 1.5
    assert client.closed and progress["status"] == "FAIL"
    assert progress["reason"] == "TimeoutError"


def test_deadline_checks_independent_oracle_batches(tmp_path):
    client = DelayedArrowSource()

    def reference():
        schema = pa.schema([("UserID", pa.int64())])

        def batches():
            yield pa.record_batch({"UserID": [1]})
            time.sleep(0.3)
            yield pa.record_batch({"UserID": [2]})
        return pa.RecordBatchReader.from_batches(schema, batches())

    progress = {}
    with pytest.raises(TimeoutError):
        reduce.execute_query(client, CID, plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"),
                             mode="reduce", workspace=tmp_path, oracle_factory=reference,
                             timeout=0.1, progress=progress)
    assert progress["status"] == "FAIL" and progress["result_validation_ms"] >= 300


def test_active_cpu_sql_is_interrupted_with_the_remaining_query_budget(tmp_path):
    client = DelayedArrowSource()
    plan = replace(plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"),
                   validation_sql="SELECT SUM(i) FROM range(1000000000) t(i)")
    progress = {}
    began = time.monotonic()
    with pytest.raises(TimeoutError):
        reduce.execute_query(client, CID, plan, mode="reduce", workspace=tmp_path,
                             timeout=0.1, progress=progress)
    assert time.monotonic() - began < 1.5
    assert progress["status"] == "FAIL" and progress["reason"] == "TimeoutError"


def test_complete_validation_failure_preserves_unsuccessful_consumed_counters(tmp_path):
    report = tmp_path / "report"
    report.mkdir()
    config = tmp_path / "record.json"
    config.write_text(json.dumps({"report": str(report)}))
    script = """import json
from types import SimpleNamespace
from glamdb_workload.clickbench.locust import RunState
state = RunState(SimpleNamespace(runner=None))
state.record({'query_id': 18, 'mode': 'reduce', 'complete': True,
              'candidate_rows': 100, 'candidate_arrow_bytes': 800,
              'arrow_wire_bytes': 1000, 'pages': 1, 'batches': 1}, False, 'RESULT_MISMATCH')
state.finish()
print(json.dumps(state.measured))
"""
    process = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                             env=dict(os.environ, GLAMDB_CLICKBENCH_CONFIG=str(config)), timeout=15)
    assert process.returncode == 0, process.stderr
    counts = json.loads(process.stdout)["counts"]
    assert counts["failed_queries"] == 1 and counts["completed_queries"] == 0
    assert counts["candidate_rows"] == 0 and counts["partial_rows"] == 0
    assert counts["unsuccessful_candidate_rows"] == 100
    assert counts["unsuccessful_candidate_arrow_bytes"] == 800
    assert counts["unsuccessful_arrow_wire_bytes"] == 1000
    assert counts["unsuccessful_pages"] == counts["unsuccessful_batches"] == 1


def test_oracle_uses_uint16_epoch_days_without_changing_legacy_integer_dates(tmp_path):
    from tools.workload.tests.test_clickbench_dataset import source_table

    source = source_table(tmp_path / "source.parquet", rows=2)
    original = pq.read_table(source)
    native = original.set_column(original.schema.get_field_index("EventDate"), "EventDate",
                                 pa.array([15900, 15901], type=pa.uint16()))
    pq.write_table(native, source)
    with reduce.source_factory(source, 2)() as reader:
        assert reader.read_all()["EventDate"].to_pylist() == [date(2013, 7, 14), date(2013, 7, 15)]
    pq.write_table(original, source)
    with reduce.source_factory(source, 2)() as reader:
        assert reader.read_all()["EventDate"].to_pylist() == [date(2013, 7, 14)] * 2


class CampaignHTTP(BaseHTTPRequestHandler):
    """A real HTTP paging/refusal fixture, separate from native acceptance."""

    def log_message(self, *args):
        pass

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
            assert body["snapshot_id"] == "42" and body["projection"] == ["UserID"]
            with self.server.counter_lock:
                self.server.offered += 1
                refused = self.server.refuse_every and self.server.offered % self.server.refuse_every == 0
            if refused:
                return self.reply(503, {"error": {"code": "SERVER_BUSY", "message": "fixture"},
                                        "execution_started": False})
            return self.reply(200, {"cursor": "first", "snapshot_id": "42"})
        if self.path.endswith("/cancel"):
            if hasattr(self.server, "cancelled"):
                self.server.cancelled.append(body["cursor"])
            return self.reply(200, {})
        if body["cursor"] == "second" and hasattr(self.server, "second_started"):
            self.server.second_started.set()
            self.server.release.wait(10)
        raw = self.server.raw
        self.send_response(200)
        for name, value in page_headers(raw, following="second" if body["cursor"] == "first" else None).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(raw)


@pytest.mark.parametrize("processes,users,warmup,refuse,modes", [(1, 2, 1, 0, ["scan", "reduce"]),
                                                               (2, 3, 0, 0, ["scan", "reduce"]),
                                                               (2, 3, 0, 3, ["scan"])])
def test_locust_finite_rounds_merge_native_histograms_and_keep_refusals_separate(
        tmp_path, processes, users, warmup, refuse, modes):
    from tools.workload.tests.test_clickbench_dataset import source_table

    server = ThreadingHTTPServer(("127.0.0.1", 0), CampaignHTTP)
    server.counter_lock, server.offered, server.refuse_every = threading.Lock(), 0, refuse
    server.raw, batch_bytes = arrow_payload()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    report = tmp_path / "locust"
    report.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        master_port = probe.getsockname()[1]
    plan = asdict(plan_query("SELECT AVG(UserID) FROM hits", 18, snapshot="42"))
    config = dict(url=f"http://127.0.0.1:{server.server_port}", database=CID, collection=CID,
                  timeout=10, ca_file=None, plans=[dict(plan=plan, mode=mode) for mode in modes],
                  collection_mode="kv", users=users, processes=processes, warmup=warmup, repeats=2,
                  report=str(report), source=str(source_table(tmp_path / "source.parquet", rows=2)), rows=2,
                  budgets={}, master_host="127.0.0.1", master_port=master_port, stop_timeout=0)
    config_path = report / "run.json"
    config_path.write_text(json.dumps(config))
    try:
        process = subprocess.run([sys.executable, "-m", "glamdb_workload.clickbench.locust", str(config_path)],
                                 capture_output=True, text=True,
                                 env=dict(os.environ, GLAMDB_TOKEN="campaign-fixture-secret"), timeout=75)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
    summary_file = report / "summary.json"
    assert summary_file.exists(), (process.returncode, process.stdout, process.stderr)
    summary = json.loads(summary_file.read_bytes())
    assert summary["status"] == ("FAIL" if refuse else "PASS"), (process.stdout, process.stderr, summary)
    assert summary["collection_mode"] == "kv"
    assert summary["counts"]["offered_queries"] == users * 2 * len(modes)
    assert summary["warmup"]["offered_queries"] == users * warmup * len(modes)
    assert len(summary["workers"]) == processes
    counts = summary["counts"]
    complete = counts["completed_queries"]
    assert counts["candidate_rows"] == complete * 2
    assert counts["candidate_arrow_bytes"] == complete * batch_bytes * 2
    assert counts["pages"] == complete * 2
    assert counts["arrow_wire_bytes"] == complete * len(server.raw) * 2
    assert counts["offered_queries"] == sum(counts[k] for k in (
        "completed_queries", "rejected_queries", "failed_queries", "cancelled_queries"))
    assert counts["http_requests"] == complete * 3 + counts["rejected_queries"]
    assert counts["request_body_bytes"] > 0
    if refuse:
        assert counts["rejected_queries"] == 2 and counts["failed_queries"] == 0
        assert summary["rejections_by_code"] == {"SERVER_BUSY": 2}
        assert summary["latency_ms"]["refused"]["Q18/scan/logical"]["count"] == 2
        assert summary["latency_ms"]["success"]["Q18/scan/logical"]["count"] == complete
    else:
        assert counts["verified_queries"] == users * 2
        for mode in modes:
            assert summary["latency_ms"]["success"][f"Q18/{mode}/logical"]["count"] == users * 2
    for name in ("summary.json", "locust_stats.csv", "locust.html"):
        assert "campaign-fixture-secret" not in (report / name).read_text()


@pytest.mark.parametrize("problem", [None, "store", "mode", "schema", "snapshot", "predicate"])
def test_preflight_binds_store_mode_schema_snapshot_and_nested_predicates(monkeypatch, problem):
    from glamdb_workload.clickbench.schema import schema_for
    from glamdb_workload.clickbench.target import target_schema

    logical = schema_for("native")
    physical = target_schema(logical, "kv")
    saved = dict(database_id=CID, store_id=CID, collection_id=CID, collection_mode="kv",
                 snapshot_id="42", schema=logical.to_dict(), target_schema=physical.to_dict(), test_backend=None)
    features = dict.fromkeys(("database_management", "scan", "scan_pagination", "scan_cancel", "arrow_export"), True)
    caps = dict(features=features, store_id="changed" if problem == "store" else CID,
                backend="local", mock=False, predicate_ops=["not"] if problem == "predicate" else ["not", "eq"])

    def handle(request):
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(200, json=caps)
        if request.url.path == "/databases/" + CID:
            return httpx.Response(200, json={"database_id": CID, "state": "open", "store": {"store_id": CID}})
        if request.url.path.endswith("/schema"):
            return httpx.Response(200, json=logical.to_dict() if problem == "schema" else physical.to_dict())
        if request.url.path.endswith("/collections/" + CID):
            return httpx.Response(200, json={"collection_id": CID, "mode": "append" if problem == "mode" else "kv",
                                            "current_snapshot_id": "43" if problem == "snapshot" else "42"})
        raise AssertionError("unexpected route: " + request.url.path)

    monkeypatch.setattr(run, "client_for", lambda config, scoped=True: Client(
        "https://test.invalid", "not-recorded", database_id=CID if scoped else None,
        transport=httpx.MockTransport(handle)))
    plan = plan_query("SELECT UserID FROM hits WHERE NOT(UserID=4)", 18, snapshot="42", type_profile="native")
    if problem:
        with pytest.raises(ValueError):
            run._preflight(saved, SimpleNamespace(schema=logical), [plan], {"timeout": 5})
    else:
        result = run._preflight(saved, SimpleNamespace(schema=logical), [plan], {"timeout": 5})
        assert result["collection"]["mode"] == "kv"


def test_scan_campaign_writes_stem_reports_and_hardware_without_duckdb(tmp_path, monkeypatch):
    from glamdb_workload.clickbench.dataset import ClickBenchDataset, prepare
    from glamdb_workload.clickbench.model import save_json
    from tools.workload.tests.test_clickbench_dataset import source_table

    data_path = tmp_path / "data"
    prepare(source_table(tmp_path / "source.parquet", rows=2), data_path, type_profile="native")
    data = ClickBenchDataset(data_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), CampaignHTTP)
    server.counter_lock, server.offered, server.refuse_every = threading.Lock(), 0, 0
    server.raw, _ = arrow_payload()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state = tmp_path / "state.json"
    saved = dict(version=1, kind="clickbench", url=f"http://127.0.0.1:{server.server_port}",
                 database_id=CID, store_id=CID, collection_id=CID, load_id=CID, snapshot_id="42",
                 load_complete=True, dataset=str(data.path), dataset_hash=data.digest,
                 schema=data.schema.to_dict(), run_dir=str(tmp_path), test_backend=None, performance_eligible=True)
    save_json(state, saved)
    save_json(tmp_path / "verify.json", {"status": "PASS", "dataset_hash": data.digest,
                                        "load_id": CID, "snapshot_id": "42"})

    class Catalog:
        digest = "fixture-catalog"

        def __init__(self, path):
            pass

        def plan(self, identifier, *, snapshot, type_profile):
            return plan_query("SELECT UserID FROM hits", identifier, snapshot=snapshot, type_profile=type_profile)

    monkeypatch.setattr(run, "QueryCatalog", Catalog)
    monkeypatch.setattr(run, "_preflight", lambda *args: {"fixture": "native identity covered separately"})
    monkeypatch.setenv("GLAMDB_TOKEN", "campaign-fixture-secret")
    block_imports = tmp_path / "imports"
    block_imports.mkdir()
    (block_imports / "sitecustomize.py").write_text(
        "import builtins\noriginal = builtins.__import__\n"
        "def guarded(name, *args, **kwargs):\n"
        "    if name.split('.')[0] == 'duckdb':\n"
        "        raise AssertionError('scan campaign imported DuckDB')\n"
        "    return original(name, *args, **kwargs)\n"
        "builtins.__import__ = guarded\n")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(block_imports), os.environ.get("PYTHONPATH", ""))))
    report = tmp_path / "queries.json"
    try:
        result = run.campaign(state, tmp_path / "catalog", report, ids="18", mode="scan", users=2,
                              processes=1, warmup=0, repeats=2, timeout=10)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
    assert result["status"] == "PASS"
    assert result["counts"]["offered_queries"] == result["counts"]["completed_queries"] == 4
    assert len(result["measurements"]) == 4 and result["inline_samples_complete"] is True
    assert result["coverage"][0]["verified"] is True
    assert result["configuration"]["users"] == 2 and result["configuration"]["processes"] == 1
    assert result["configuration"]["hardware"]["memory_bytes"] > 0
    assert result["collection_mode"] == "append" and result["snapshot_id"] == "42"
    assert "duckdb" not in result["versions"]
    assert len(report.with_suffix(".csv").read_text().splitlines()) == 5
    for path in (report, report.with_suffix(".csv"), report.with_suffix(".md"), tmp_path / "queries-coverage.json"):
        assert "campaign-fixture-secret" not in path.read_text()


def test_interrupted_locust_cancels_cursor_and_reports_partial_work(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), CampaignHTTP)
    server.counter_lock, server.offered, server.refuse_every = threading.Lock(), 0, 0
    server.raw, batch_bytes = arrow_payload()
    server.second_started, server.release, server.cancelled = threading.Event(), threading.Event(), []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    report = tmp_path / "locust"
    report.mkdir()
    plan = asdict(plan_query("SELECT UserID FROM hits", 18, snapshot="42"))
    config = dict(url=f"http://127.0.0.1:{server.server_port}", database=CID, collection=CID,
                  timeout=15, ca_file=None, plans=[dict(plan=plan, mode="scan")],
                  users=1, processes=1, warmup=0, repeats=100, report=str(report),
                  source=None, rows=2, budgets={}, stop_timeout=0)
    config_path = report / "run.json"
    config_path.write_text(json.dumps(config))
    process = subprocess.Popen([sys.executable, "-m", "glamdb_workload.clickbench.locust", str(config_path)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True,
                               env=dict(os.environ, GLAMDB_TOKEN="campaign-fixture-secret"))
    try:
        assert server.second_started.wait(10), "second HTTP page did not start"
        process.send_signal(signal.SIGTERM)
        output, error = process.communicate(timeout=7)
        result = json.loads((report / "summary.json").read_bytes())
        assert result["status"] == "INCOMPLETE", (output, error, result)
        assert result["counts"]["cancelled_queries"] == 1
        assert result["counts"]["completed_queries"] == 0
        assert result["counts"]["partial_rows"] == 1
        assert result["counts"]["partial_arrow_bytes"] == batch_bytes
        assert server.cancelled == ["second"]
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=3)
        server.release.set()
        server.shutdown()
        server.server_close()
        thread.join(2)
