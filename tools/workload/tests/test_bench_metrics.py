"""Benchmark report fields: read modes, success/refusal split, stages, processes.

Unit tests use fakes only. The Locust runs use a tiny loopback HTTP double of
the public JSON routes; it is a fault/metrics fixture, not a server substitute.
"""

import json
import os
import socket
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest

FIELDS = [{"name": "key", "type": "string", "nullable": False}]


class FakeBatch:
    def __init__(self, rows, nbytes):
        self.num_rows = rows
        self.nbytes = nbytes
        self.materialized = 0

    def to_pylist(self):
        self.materialized += 1
        return [{"key": str(i)} for i in range(self.num_rows)]


class FakeStream:
    def __init__(self, items, bytes_read=0):
        self.items = items
        self.bytes_read = bytes_read

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        return iter(self.items)


class FakePage:
    def __init__(self, rows):
        self.rows = rows


class FakeClient:
    def __init__(self, batches=(), pages=()):
        self.batches = list(batches)
        self.pages = list(pages)

    def scan_batches(self, collection, **kwargs):
        return FakeStream(self.batches, bytes_read=1234)

    def scan_pages(self, collection, **kwargs):
        return FakeStream(self.pages)


class Counter:
    pages = 0


def progress():
    return dict(rows=0, pages=0, bytes=0, batches=0, batch_bytes=0)


def config(**kwargs):
    return dict(collection=str(uuid4()), query={}, timeout=5, **kwargs)


def test_batch_native_mode_never_materializes_python_rows():
    from glamdb_workload.runtime import consume_scan, read_mode, scan_request_name

    batches = [FakeBatch(3, 100), FakeBatch(4, 50)]
    selected = config(format="arrow", consume="batches")
    result = consume_scan(
        FakeClient(batches), selected, progress(), page_counter=Counter()
    )
    assert (result["rows"], result["batches"], result["batch_bytes"]) == (7, 2, 150)
    assert result["bytes"] == 1234
    assert all(batch.materialized == 0 for batch in batches)
    assert read_mode(selected) == "arrow-batches"
    assert scan_request_name(selected) == "scan/arrow/batches"


def test_row_modes_keep_historical_materialization_and_names():
    from glamdb_workload.runtime import consume_scan, read_mode, scan_request_name

    batches = [FakeBatch(3, 100)]
    arrow = config(format="arrow")
    result = consume_scan(
        FakeClient(batches), arrow, progress(), page_counter=Counter()
    )
    assert result["rows"] == 3 and result["batch_bytes"] == 0
    assert batches[0].materialized == 1
    assert read_mode(arrow) == "arrow-python-rows"
    assert scan_request_name(arrow) == "scan/arrow/decoded"

    plain = config(format="json")
    pages = [FakePage([{"key": "a"}] * 2), FakePage([{"key": "b"}])]
    result = consume_scan(FakeClient(pages=pages), plain, progress())
    assert (result["rows"], result["pages"]) == (3, 2)
    assert read_mode(plain) == "json-decoded-rows"
    assert scan_request_name(plain) == "scan/json/decoded"


def test_partial_progress_survives_a_failed_page():
    from glamdb_workload.runtime import consume_scan

    class Failing(FakeStream):
        def __iter__(self):
            yield FakeBatch(5, 10)
            raise RuntimeError("page failed")

    class Client(FakeClient):
        def scan_batches(self, collection, **kwargs):
            return Failing([])

    state = progress()
    with pytest.raises(RuntimeError):
        consume_scan(Client(), config(format="arrow", consume="batches"), state)
    assert state["rows"] == 5


def test_batches_require_arrow():
    from glamdb_workload.__main__ import main

    args = ["bench", "read", "--url", "http://127.0.0.1:1", "--database", str(uuid4())]
    args += ["--collection", str(uuid4()), "--query", "{}", "--report", "unused"]
    assert main(args + ["--consume", "batches"]) == 2


def test_refusal_codes_and_counter_merge():
    from glamdb_workload.runtime import failure_kind, merge_counts

    assert failure_kind("SERVER_BUSY") == "REFUSED"
    assert failure_kind("RESOURCE_EXHAUSTED") == "REFUSED"
    assert failure_kind("IO_ERROR") == "ERROR"
    assert failure_kind("transport_error") == "ERROR"
    merged = {}
    merge_counts(
        merged,
        {
            "errors": 1,
            "rejections_by_code": {"SERVER_BUSY": 2},
            "write_stages": {
                "journal": {"count": 1, "total_seconds": 0.5, "max_ms": 500}
            },
        },
    )
    merge_counts(
        merged,
        {
            "errors": 2,
            "rejections_by_code": {"SERVER_BUSY": 1, "DATABASE_BUSY": 1},
            "write_stages": {
                "journal": {"count": 2, "total_seconds": 0.25, "max_ms": 200}
            },
        },
    )
    assert merged["errors"] == 3
    assert merged["rejections_by_code"] == {"SERVER_BUSY": 3, "DATABASE_BUSY": 1}
    assert merged["write_stages"]["journal"] == {
        "count": 3,
        "total_seconds": 0.75,
        "max_ms": 500,
    }


def test_throughput_never_counts_rejections():
    from glamdb_workload.runtime import throughput_report

    reads = dict(
        offered_scans=10, completed_scans=4, consumed_rows=400, rejected_scans=5
    )
    result = throughput_report("read", reads, None, 2.0)
    assert result["successful_operations"] == 4
    assert result["successful_per_second"] == 2.0
    assert result["offered_per_second"] == 5.0
    assert result["rejected_attempts"] == 5
    assert result["other_unsuccessful_attempts"] == 1
    assert result["rejected_share"] == 0.5
    operations = dict(attempts=3, committed=1, confirmed_rows=10)
    result = throughput_report("write", dict(rejected_appends=2), operations, 1.0)
    assert result["successful_operations"] == 1
    assert result["successful_rows_per_second"] == 10


def test_success_and_refusal_percentiles_are_separate_locust_entries():
    script = """
import json
from locust.stats import RequestStats
from glamdb_workload.runtime import latency_report
stats = RequestStats()
for ms in (10, 20, 30, 40, 1000):
    stats.log_request("SDK", "scan/json/decoded", ms, 0)
for ms in (1, 2):
    stats.log_request("REFUSED", "scan/json/decoded", ms, 0)
stats.log_request("ERROR", "scan/json/decoded", 5, 0)
print(json.dumps(latency_report(stats)))
"""
    # Locust patches the interpreter on import; keep it out of the test process.
    result = subprocess.run(
        [sys.executable, "-c", script], text=True, capture_output=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    success = report["success"]["scan/json/decoded"]
    refused = report["refused"]["scan/json/decoded"]
    assert success["count"] == 5 and refused["count"] == 2
    assert report["error"]["scan/json/decoded"]["count"] == 1
    assert success["p50_ms"] == 30 and success["p99_ms"] == 1000
    assert refused["p99_ms"] == 2


def test_cpu_saturation_flag_and_stage_shares():
    from glamdb_workload.runtime import SATURATION_PERCENT, cpu_report, stage_report

    workers = [
        dict(worker="a", cpu_seconds=1, cpu_percent_one_core=SATURATION_PERCENT + 1),
        dict(worker="b", cpu_seconds=1, cpu_percent_one_core=10),
    ]
    report = cpu_report(workers)
    assert report["generator_saturated"] is True
    assert [p["saturated"] for p in report["processes"]] == [True, False]
    assert cpu_report(workers[1:])["generator_saturated"] is False
    stages = stage_report(
        {
            "input_load": {"count": 2, "total_seconds": 0.1, "max_ms": 60},
            "http_send": {"count": 2, "total_seconds": 0.3, "max_ms": 200},
            "end_to_end": {"count": 2, "total_seconds": 0.5, "max_ms": 300},
        }
    )
    assert stages["input_load"]["mean_ms"] == pytest.approx(50)
    assert stages["http_send"]["share_of_end_to_end"] == pytest.approx(0.6)
    assert "journal" not in stages


def test_append_once_reports_journal_and_send_separately(tmp_path):
    import httpx
    from glamdb_sdk import AppendRequest, Client, Field, Schema
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import append_once

    req = AppendRequest.prepare(
        str(uuid4()), str(uuid4()), Schema([Field("key", "string")]), [{"key": "x"}]
    )
    busy = {
        "error": {"code": "SERVER_BUSY", "message": "busy"},
        "execution_started": False,
    }
    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid",
        "test-token",
        transport=httpx.MockTransport(lambda r: httpx.Response(503, json=busy)),
    ) as client:
        result = append_once(client, log, req)
    assert result["error_code"] == "SERVER_BUSY"
    assert result["journal_ms"] > 0 and result["response_time_ms"] > 0


# --- Locust runs against a loopback JSON double -----------------------------


class Double(BaseHTTPRequestHandler):
    refuse_every = 3
    lock = threading.Lock()
    opened = 0
    appended = 0

    def log_message(self, *args):
        pass

    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def scan(self, rows, cursor):
        return {
            "schema": {"fields": FIELDS},
            "snapshot_id": "1",
            "identity": {"view": "fixed"},
            "profile": {},
            "rows": rows,
            "next_cursor": cursor,
        }

    def do_GET(self):
        assert self.headers["Authorization"] == "Bearer metrics-test-only"
        parts = self.path.strip("/").split("/")
        if parts == ["capabilities"]:
            features = dict.fromkeys(
                [
                    "database_management",
                    "scan",
                    "scan_pagination",
                    "scan_cancel",
                    "append",
                ],
                True,
            )
            return self.reply(200, {"api_version": "v0", "features": features})
        if parts[-1] == "schema":
            return self.reply(200, {"schema": {"fields": FIELDS}})
        if len(parts) == 2:
            return self.reply(200, {"database_id": parts[1], "state": "open"})
        return self.reply(200, {"collection_id": parts[-1], "name": "events"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        path = self.path.strip("/")
        if path.endswith("/scan/open"):
            with Double.lock:
                Double.opened += 1
                refuse = Double.opened % Double.refuse_every == 0
            if refuse and body.get("limit") != 1:
                return self.reply(
                    503,
                    {
                        "error": {"code": "SERVER_BUSY", "message": "busy"},
                        "execution_started": False,
                    },
                )
            value = self.scan([], None)
            value["cursor"] = "page-1"
            del value["rows"], value["profile"], value["next_cursor"]
            return self.reply(200, value)
        if path.endswith("/scan/cancel"):
            return self.reply(200, {})
        if path.endswith("/scan"):
            if Double.appended == 0 and body["cursor"] == "page-1" and self.empty:
                return self.reply(200, self.scan([], None))
            if body["cursor"] == "page-1":
                return self.reply(
                    200, self.scan([{"key": "a"}, {"key": "b"}], "page-2")
                )
            return self.reply(200, self.scan([{"key": "c"}], None))
        if path.endswith("/append"):
            return self.append(path.split("/")[-2], body)
        return self.reply(404, {"error": {"code": "ROUTE_NOT_FOUND", "message": "no"}})

    def append(self, collection, body):
        from glamdb_sdk import Schema
        from glamdb_sdk.request import canonical_payload_hash

        digest = canonical_payload_hash(
            collection, Schema.from_dict({"fields": FIELDS}), body["rows"]
        )
        with Double.lock:
            Double.appended += 1
            snapshot = Double.appended
        result = {
            "collection_id": collection,
            "operation_id": body["operation_id"],
            "snapshot_id": str(snapshot),
            "rows_appended": str(len(body["rows"])),
            "payload_hash": digest,
            "hash_version": 1,
            "receipt_version": 1,
            "data_files_added": "1",
            "compressed_data_bytes_added": "128",
            "parent_snapshot_id": None,
            "durability_profile": "local-delete-extra-v1",
            "snapshot_created_at_us": "1000",
        }
        return self.reply(
            200,
            {
                "outcome": "committed",
                "phase": "complete",
                "operation_id": body["operation_id"],
                "result": result,
            },
        )


@pytest.fixture
def double():
    handler = type("Handler", (Double,), {"opened": 0, "appended": 0, "empty": False})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield handler, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def bench(url, mode, run, *extra):
    args = [sys.executable, "-m", "glamdb_workload", "bench", mode, "--url", url]
    args += ["--database", str(uuid4()), "--collection", str(uuid4())]
    args += ["--report", str(run), "--stop-timeout", "1", *map(str, extra)]
    env = dict(os.environ, GLAMDB_TOKEN="metrics-test-only")
    result = subprocess.run(args, env=env, text=True, capture_output=True, timeout=120)
    summary = json.loads((run / "summary.json").read_bytes())
    for name in ("run.json", "summary.json", "locust.json", "locust_stats.csv"):
        assert "metrics-test-only" not in (run / name).read_text()
    return result, summary


def test_multiprocess_read_merges_processes_and_splits_refusals(tmp_path, double):
    handler, url = double
    query = tmp_path / "query.json"
    query.write_text("{}")
    result, summary = bench(
        url,
        "read",
        tmp_path / "run",
        "--query",
        query,
        "--users",
        4,
        "--spawn-rate",
        100,
        "--duration",
        "3s",
        "--processes",
        2,
        "--master-port",
        free_port(),
    )
    assert summary["status"] in {"PASS", "FAIL"}, (
        result.stdout,
        result.stderr,
        summary,
    )
    # Refusals are failures for PASS/FAIL, but are never counted as throughput.
    assert summary["read_mode"] == "json-decoded-rows"
    assert summary["processes"] == 2
    assert len(summary["workers"]) == 2 and summary["expected_workers"] == 2
    reads, rate = summary["reads"], summary["throughput"]
    assert reads["rejected_scans"] > 0 and reads["completed_scans"] > 0
    assert summary["rejections_by_code"]["SERVER_BUSY"] == reads["rejected_scans"]
    assert rate["successful_operations"] == reads["completed_scans"]
    assert rate["offered_attempts"] == (
        reads["completed_scans"] + reads["failed_scans"] + reads["cancelled_scans"]
    )
    assert reads["consumed_rows"] == 3 * reads["completed_scans"]
    assert reads["completed_pages"] == 2 * reads["completed_scans"]
    latency = summary["latency_ms"]
    success = latency["success"]["scan/json/decoded"]
    assert success["count"] == reads["completed_scans"]
    assert latency["refused"]["scan/json/decoded"]["count"] == reads["rejected_scans"]
    cpu = summary["client_cpu"]
    assert len(cpu["processes"]) == 2 and cpu["controller"] is not None
    assert isinstance(cpu["generator_saturated"], bool)
    assert {p["worker"] for p in cpu["processes"]} == set(summary["observed_workers"])


def test_write_reports_client_stages(tmp_path, double):
    handler, url = double
    handler.empty = True
    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({"fields": FIELDS}))
    dataset = tmp_path / "dataset"
    generated = subprocess.run(
        [
            sys.executable,
            "-m",
            "glamdb_workload",
            "generate",
            "--schema",
            str(schema),
            "--rows",
            "20",
            "--seed",
            "7",
            "--part-rows",
            "5",
            "--output",
            str(dataset),
        ],
        text=True,
        capture_output=True,
    )
    assert generated.returncode == 0, generated.stderr
    result, summary = bench(
        url,
        "write",
        tmp_path / "run",
        "--dataset",
        dataset,
        "--users",
        2,
        "--spawn-rate",
        100,
        "--duration",
        "30s",
    )
    assert summary["status"] == "PASS", (result.stdout, result.stderr, summary)
    assert summary["read_mode"] is None
    stages = summary["write_stages"]
    assert set(stages) == {
        "input_load",
        "sdk_prepare",
        "journal",
        "http_send",
        "end_to_end",
    }
    assert all(stage["count"] == 4 for stage in stages.values())
    parts = sum(
        stages[name]["total_seconds"] for name in stages if name != "end_to_end"
    )
    assert parts <= stages["end_to_end"]["total_seconds"] * 1.01
    rate = summary["throughput"]
    assert rate["successful_operations"] == rate["offered_attempts"] == 4
    assert rate["rejected_attempts"] == 0
    assert summary["latency_ms"]["success"]["append"]["count"] == 4
    assert summary["latency_ms"]["refused"] == {}
