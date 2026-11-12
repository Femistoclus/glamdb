"""Default E2E uses the compiled native server, verified TLS and isolated storage.

Usage: python native_e2e_test.py CLI SERVER [--report-root DIRECTORY]
No mock fallback is used. Cleanup drops only databases created by the test.
"""

import argparse
from contextlib import closing, contextmanager
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from uuid import uuid4

from glamdb_sdk import Client, Field, Schema

options = argparse.ArgumentParser()
options.add_argument("cli")
options.add_argument("server")
options.add_argument("--report-root", default="artifacts/workload-native")
options.add_argument(
    "--case",
    help="Run one unittest case, e.g. NativeWorkload.test_interrupt_leaves_final_reports_and_no_child",
)
ARGS = options.parse_args()
sys.argv[1:] = []
CLI, SERVER = str(Path(ARGS.cli).resolve()), str(Path(ARGS.server).resolve())
REPO = Path(__file__).resolve().parents[3]


@contextmanager
def tls_proxy(upstream_port, directory):
    cert, key = directory / "test-ca.pem", directory / "test-key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            size = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(size) if size else None
            headers = {
                k: v
                for k, v in self.headers.items()
                if k.lower() not in {"host", "connection"}
            }
            conn = http.client.HTTPConnection("127.0.0.1", upstream_port, timeout=65)
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                reply = conn.getresponse()
                content = reply.read()
                if self.command == "POST" and self.path.endswith(("/append", "/upsert")):
                    with self.server.lock:
                        self.server.append_calls += 1
                        self.server.append_requests.append({
                            "operation_id": json.loads(body)["operation_id"],
                            "body_sha256": hashlib.sha256(body).hexdigest(),
                            "path": self.path,
                        })
                        drop = self.server.drop_once
                        self.server.drop_once = False
                    if drop:
                        # Forwarded append has actually committed on the native
                        # server. Lose the response, retaining the original ID.
                        assert reply.status == 200, content
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                    if self.server.append_delay:
                        time.sleep(self.server.append_delay)
                self.send_response(reply.status)
                for name, value in reply.getheaders():
                    if name.lower() not in {
                        "connection",
                        "transfer-encoding",
                        "content-length",
                        "server",
                        "date",
                    }:
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                conn.close()

        do_GET = do_POST = forward

        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    proxy.lock, proxy.drop_once, proxy.append_calls = threading.Lock(), False, 0
    proxy.append_requests = []
    proxy.append_delay = 0
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    proxy.socket = ctx.wrap_socket(proxy.socket, server_side=True)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy, f"https://localhost:{proxy.server_port}", cert
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=3)


class NativeWorkload(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not Path(SERVER).is_file() or not Path(CLI).is_file():
            raise AssertionError(
                "build the real native server and standalone CLI before running E2E"
            )

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="glamdb-workload-",
            dir=os.environ.get("GLAMDB_WORKLOAD_TEMP_ROOT", "/tmp"),
        )
        self.addCleanup(self.temporary.cleanup)
        self.tmp = Path(self.temporary.name).resolve()
        self.reports = Path(ARGS.report_root).resolve() / (
            self._testMethodName + "-" + uuid4().hex[:8]
        )
        self.reports.mkdir(parents=True)
        self.env = dict(
            os.environ,
            GLAMDB_TOKEN="workload-e2e-test-only-" + uuid4().hex,
            GLAMDB_PYTHON=sys.executable,
        )
        self.data_dir = self.tmp / "native-data"
        self.start()
        self.addCleanup(self.stop)
        with Client(self.direct, self.env["GLAMDB_TOKEN"]) as client:
            self.db = client.create_database("workload", deadline=30)["database_id"]

    def start(self):
        self.server = subprocess.Popen(
            [
                SERVER,
                "--data-dir",
                str(self.data_dir),
                "--port",
                str(getattr(self, "fixed_port", 0)),
                "--ui-root",
                str(REPO / "ui"),
            ],
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertTrue(
            select.select([self.server.stdout], [], [], 20)[0],
            "native server startup timed out",
        )
        line = self.server.stdout.readline()
        self.assertTrue(line, "native server did not start")
        self.port = json.loads(line)["port"]
        self.direct = f"http://127.0.0.1:{self.port}"

    def stop(self):
        if self.server.poll() is None:
            self.server.terminate()
        out, err = self.server.communicate(timeout=30)
        self.assertEqual(self.server.returncode, 0, out + err)

    def collection(self, name, schema):
        cid = str(uuid4())
        with Client(
            self.direct, self.env["GLAMDB_TOKEN"], database_id=self.db
        ) as client:
            self.assertEqual(
                client.create_collection(
                    name, schema, collection_id=cid, deadline=30
                ).outcome,
                "committed",
            )
        return cid

    def cli(self, *args, expected=0, timeout=150):
        result = subprocess.run(
            [CLI, *map(str, args)],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        (self.reports / ("command-" + uuid4().hex[:8] + ".log")).write_text(
            result.stdout + result.stderr
        )
        self.assertEqual(
            result.returncode, expected, result.stdout[-3000:] + result.stderr[-5000:]
        )
        return result

    def test_maximum_width_decimal_schema_survives_restart_and_exact_retry(self):
        from decimal import Decimal

        schema = Schema([
            Field("d" * (128 - len(str(index))) + str(index), "decimal", precision=38, scale=4)
            for index in range(128)
        ])
        collection = self.collection("wide-decimal", schema)
        row = {field.name: Decimal("9007199254740993.1250") for field in schema.fields}
        with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=self.db) as client:
            request = client.prepare_append(collection, [row, row], schema=schema, operation_id=uuid4())
            outcome = client.append(request)
            self.assertEqual(outcome.outcome, "committed")
            self.assertEqual(client.scan_json(collection).rows, [row, row])
        self.fixed_port = self.port
        self.stop()
        self.start()
        with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=self.db) as client:
            self.assertEqual(client.get_schema(collection), schema)
            replay = client.append(request)
            self.assertEqual(replay.outcome, "committed")
            self.assertEqual(replay.result["snapshot_id"], outcome.result["snapshot_id"])
            self.assertEqual(client.scan_json(collection).rows, [row, row])

    def test_clickbench_prefix_all_columns_verified_sizes_and_restart(self):
        self.check_clickbench_prefix("legacy-date")

    def test_clickbench_native_date_verified_queries_sizes_and_restart(self):
        self.check_clickbench_prefix("native")

    def test_clickbench_kv_native_date_complete_lifecycle(self):
        self.check_clickbench_prefix("native", collection_mode="kv")

    def clickbench_journal(self, saved):
        """Keep exact identities, bytes, counters and receipts across process restarts."""
        snapshot = {}
        for path in sorted(Path(saved["run_dir"]).glob("worker-*.sqlite")):
            with closing(sqlite3.connect(path)) as database:
                database.row_factory = sqlite3.Row
                tables = {row[0] for row in database.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                snapshot[path.name] = {
                    name: sorted((dict(row) for row in database.execute(f"SELECT * FROM {name}")),
                                 key=lambda row: repr(sorted(row.items())))
                    for name in ("operations", "clickbench_meta", "clickbench_chunks") if name in tables
                }
        self.assertTrue(snapshot, "load must retain its original operation journal")
        return snapshot

    def check_clickbench_prefix(self, type_profile, collection_mode="append"):
        from datetime import date, datetime, timezone

        from glamdb_workload.clickbench.dataset import ClickBenchDataset
        from glamdb_workload.ledger import Ledger
        from test_clickbench_dataset import source_table

        source = source_table(self.tmp / "hits.parquet")
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pq.read_table(source)
        # Ensure the actual pinned filters have positive matches, including
        # exact int64 equality and the July DATE interval.
        for name, value in {"CounterID": 62, "UserID": 435090932899640449,
                            "DontCountHits": 0, "IsRefresh": 0, "IsLink": 1, "IsDownload": 0,
                            "TraficSourceID": -1, "RefererHash": 3594120000172545465,
                            "URLHash": 2868770270353813622}.items():
            table = table.set_column(table.schema.get_field_index(name), name, pa.array([value] * 31))
        for name, value in {"URL": "https://example.org/google", "Title": "Google Привет 💗",
                            "Referer": "https://www.example.org/страница",
                            "UTMTerm": "последний столбец 🎀"}.items():
            table = table.set_column(table.schema.get_field_index(name), name, pa.array([value.encode()] * 31))
        table = table.set_column(table.schema.get_field_index("CLID"), "CLID",
                                 pa.array([2**63 - 2 - (ordinal % 5) for ordinal in range(31)]))
        pq.write_table(table, source)
        proxy, endpoint, ca = self.enterContext(tls_proxy(self.port, self.tmp))
        def online(action, *args, expected=0):
            result = self.cli("bench", "clickbench", action, *args, "--ca-file", ca,
                              expected=expected, timeout=300)
            return json.loads(result.stdout) if result.stdout.strip() else None
        dataset, state = self.tmp / "clickbench-data", self.reports / "clickbench-state.json"
        self.cli("bench", "clickbench", "prepare", "--source", source, "--output", dataset,
                 "--type-profile", type_profile, "--part-rows", 7)
        data = ClickBenchDataset(dataset)
        self.assertEqual(len(data.schema.fields), 105)
        self.assertEqual(data.schema.fields[-1].name, "CLID")
        expected_rows = [row for _, part in data.iter_parts() for row in part]
        self.assertEqual(len(expected_rows), 31)
        selected_mode = ["--mode", collection_mode] if collection_mode == "kv" else []
        online("init", "--dataset", dataset, "--state", state,
               "--url", endpoint, "--name", "clickbench-e2e", *selected_mode)
        saved = json.loads(state.read_bytes())
        self.assertEqual(saved["collection_mode"], collection_mode)
        self.assertEqual(len(saved["schema"]["fields"]), 105)
        physical_width = 106 if collection_mode == "kv" else 105
        self.assertEqual(len(saved["target_schema"]["fields"]), physical_width)
        if collection_mode == "kv":
            self.assertEqual(saved["key_encoding"], "source-row-ordinal-decimal-v1")
            # An omitted mode keeps the saved KV target; an explicit change is refused.
            online("init", "--dataset", dataset, "--state", state,
                   "--url", endpoint, "--name", "clickbench-e2e")
            original = state.read_bytes()
            online("init", "--dataset", dataset, "--state", state,
                   "--url", endpoint, "--name", "clickbench-e2e", "--mode", "append", expected=2)
            self.assertEqual(state.read_bytes(), original)
        import httpx
        unauthorized = httpx.get(endpoint + "/storage", verify=ssl.create_default_context(cafile=str(ca)))
        self.assertEqual(unauthorized.status_code, 401)

        load_report = self.reports / "clickbench-load.json"
        proxy.drop_once = True
        interrupted = online("load", "--state", state, "--batch-rows", "3", "--batch-bytes", "1048576",
                             "--workers", "3", "--report", load_report, expected=3)
        self.assertEqual(interrupted["status"], "INCOMPLETE")
        saved = json.loads(state.read_bytes())
        original_plan = saved["append_plan"]
        self.assertEqual({key: original_plan[key] for key in ("workers", "batch_rows", "batch_bytes")},
                         {"workers": 3, "batch_rows": 3, "batch_bytes": 1048576})
        pending = self.clickbench_journal(saved)
        operations = [row for journal in pending.values() for row in journal["operations"]]
        identities = {row["operation_id"]: (row["body"], row["body_hash"], row["payload_hash"], row["kind"])
                      for row in operations}
        self.assertTrue(any(row["attempts"] and row["state"] != "committed" for row in operations))
        writes_before_resolve = proxy.append_calls
        self.fixed_port = self.port
        self.stop()
        self.start()
        self.assertEqual(online("resolve", "--state", state)["status"], "PASS")
        self.assertEqual(proxy.append_calls, writes_before_resolve, "lookup resolution must not resend a write")
        resolved = self.clickbench_journal(saved)
        for journal in resolved.values():
            for row in journal["operations"]:
                self.assertEqual((row["body"], row["body_hash"], row["payload_hash"], row["kind"]),
                                 identities[row["operation_id"]])
        loaded = online("load", "--state", state, "--resume", "--report", load_report)
        self.assertEqual(loaded["status"], "PASS")
        self.assertEqual(loaded["confirmed_rows"], 31)
        self.assertTrue(loaded["load_complete"])
        self.assertEqual(json.loads(load_report.read_bytes())["status"], "PASS")
        saved = json.loads(state.read_bytes())
        self.assertEqual(saved["append_plan"], original_plan)
        complete_journal = self.clickbench_journal(saved)
        operations = [row for journal in complete_journal.values() for row in journal["operations"]]
        self.assertEqual(len(operations), 13)  # Four 7-row parts split 3/3/1; the final 3-row part.
        self.assertEqual(sum(row["rows"] for row in operations), 31)
        self.assertTrue(all(row["state"] == "committed" and row["attempts"] == 1 for row in operations))
        self.assertEqual({row["kind"] for row in operations}, {"upsert" if collection_mode == "kv" else "append"})
        self.assertTrue(all(row["rows"] <= 3 and len(row["body"]) <= 1048576 for row in operations))
        self.assertEqual(len({row["operation_id"] for row in operations}), 13)
        for record in proxy.append_requests:
            operation = next(row for row in operations if row["operation_id"] == record["operation_id"])
            self.assertEqual(hashlib.sha256(operation["body"]).hexdigest(), record["body_sha256"])
            self.assertTrue(record["path"].endswith("/upsert" if collection_mode == "kv" else "/append"))

        receipt_results = {}
        with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=saved["database_id"]) as client:
            schema = client.get_schema(saved["collection_id"])
            self.assertEqual(len(schema.fields), physical_width)
            self.assertEqual(schema.to_dict(), saved["target_schema"])
            physical_rows = client.scan_json(saved["collection_id"], snapshot_id=saved["snapshot_id"]).rows
            if collection_mode == "kv":
                expected_physical = [dict(row, key=f"{ordinal:020d}") for ordinal, row in enumerate(expected_rows)]
                self.assertCountEqual(physical_rows, expected_physical)
                self.assertEqual({row["key"] for row in physical_rows}, {f"{ordinal:020d}" for ordinal in range(31)})
                found = client.lookup(saved["collection_id"], ["00000000000000000000", "missing",
                                      "00000000000000000030", "00000000000000000000"],
                                      snapshot_id=saved["snapshot_id"])
                self.assertEqual(found.rows, [expected_physical[0], None, expected_physical[30], expected_physical[0]])
            else:
                self.assertCountEqual(physical_rows, expected_rows)
            self.assertEqual(len(physical_rows), 31)
            self.assertEqual({row["WatchID"] for row in physical_rows}, {9007199254740993})
            self.assertEqual({row["UserID"] for row in physical_rows}, {435090932899640449})
            self.assertEqual({row["UTMTerm"] for row in physical_rows}, {"последний столбец 🎀"})
            self.assertEqual({row["CLID"] for row in physical_rows}, {2**63 - 2 - n for n in range(5)})
            expected_date = date(2013, 7, 14) if type_profile == "native" else datetime(2013, 7, 14, tzinfo=timezone.utc)
            self.assertTrue(all(type(row["EventDate"]) is type(expected_date) and row["EventDate"] == expected_date
                                for row in physical_rows))
            with client.scan_batches(saved["collection_id"], snapshot_id=saved["snapshot_id"]) as batches:
                arrow_rows = [row for batch in batches for row in batch.to_pylist()]
            self.assertCountEqual(arrow_rows, physical_rows)
            for operation in operations:
                receipt = client.find_append_outcome(saved["collection_id"], operation["operation_id"])
                self.assertEqual(receipt.outcome, "committed")
                self.assertEqual(receipt.result["payload_hash"], operation["payload_hash"])
                receipt_results[operation["operation_id"]] = receipt.result
                if collection_mode == "kv":
                    self.assertEqual(receipt.result["mutation_kind"], "upsert")
                    self.assertEqual(receipt.result["receipt_version"], 3)
                    self.assertEqual(int(receipt.result["input_rows"]), operation["rows"])
                    self.assertEqual(int(receipt.result["rows_affected"]), operation["rows"])
                    self.assertEqual(int(receipt.result["rows_written"]) - int(receipt.result["rows_removed"]),
                                     operation["rows"])
                else:
                    self.assertEqual(int(receipt.result["rows_appended"]), operation["rows"])
        for format in ("json", "arrow"):
            verification = online("verify", "--state", state, "--format", format)
            self.assertEqual(verification["status"], "PASS")
            self.assertEqual((verification["expected_rows"], verification["actual_rows"]), (31, 31))
        status = online("status", "--state", state)
        self.assertEqual(status["status"], "PASS")
        self.assertEqual(status["operations"]["confirmed_rows"], 31)
        self.assertEqual(status["snapshot_id"], saved["snapshot_id"])
        sizes = self.reports / "clickbench-sizes.json"
        owned = self.data_dir / "databases" / saved["store_id"]
        online("size", "--state", state, "--report", sizes, "--include-footers", "--owned-store-dir", owned)
        result = json.loads(sizes.read_bytes())
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(result["performance_eligible"])
        self.assertEqual(result["snapshot"]["rows"], 31)
        self.assertGreater(result["snapshot"]["parquet_files"], 0)
        self.assertGreater(result["snapshot"]["parquet_file_bytes"], 0)
        independent = sum(path.stat().st_size for path in owned.rglob("*") if path.is_file())
        self.assertEqual(result["local_filesystem"]["database_file_bytes"], independent)
        self.assertGreater(result["local_filesystem"]["catalog_file_bytes"], 0)
        self.assertIsNone(result["snapshot"]["database_file_bytes"])
        self.assertEqual(int(result["server_storage"]["database_file_bytes"]), independent)
        self.assertGreaterEqual(int(result["server_storage"]["committed_data_file_bytes"]),
                                result["snapshot"]["parquet_file_bytes"])
        self.assertTrue(sizes.with_suffix(".csv").is_file())
        self.assertTrue(sizes.with_suffix(".md").is_file())
        catalog = os.environ.get("GLAMDB_CLICKBENCH_CATALOG")
        self.assertTrue(catalog, "set GLAMDB_CLICKBENCH_CATALOG to fetched pinned query material")
        campaign = self.reports / "clickbench-queries.json"
        online("query", "--state", state, "--catalog", catalog, "--source", source,
               "--report", campaign, "--mode", "both", "--warmup", "0", "--repeats", "1")
        measured = json.loads(campaign.read_bytes())
        self.assertEqual(measured["status"], "PASS")
        self.assertEqual(len(measured["coverage"]), 86)
        self.assertTrue(all(item["verified"] for item in measured["coverage"]))
        self.assertEqual({(item["query_id"], item["mode"]) for item in measured["coverage"]},
                         {(identifier, mode) for identifier in range(1, 44) for mode in ("scan", "reduce")})
        self.assertTrue(campaign.with_suffix(".csv").is_file())
        self.assertTrue(campaign.with_suffix(".md").is_file())
        scans = self.reports / "clickbench-scans.json"
        online("scan", "--state", state, "--catalog", catalog, "--report", scans,
               "--queries", "all", "--warmup", "0", "--repeat", "1", "--users", "2", "--processes", "2")
        scanned = json.loads(scans.read_bytes())
        self.assertEqual(scanned["status"], "PASS")
        self.assertEqual(len(scanned["coverage"]), 43)
        self.assertTrue(all(item["mode"] == "scan" and item["verified"] for item in scanned["coverage"]))
        self.assertEqual(scanned["snapshot_id"], saved["snapshot_id"])
        self.assertEqual(scanned["configuration"]["users"], 2)
        self.assertEqual(scanned["configuration"]["processes"], 2)
        self.fixed_port = self.port
        self.stop()
        self.start()
        with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=saved["database_id"]) as client:
            self.assertEqual(len(client.get_schema(saved["collection_id"]).fields), physical_width)
            for operation in operations:
                recovered = client.find_append_outcome(saved["collection_id"], operation["operation_id"])
                self.assertEqual(recovered.result, receipt_results[operation["operation_id"]])
            journal_path = next(path for path in Path(saved["run_dir"]).glob("worker-*.sqlite")
                                if complete_journal[path.name]["operations"])
            with Ledger(journal_path, readonly=True) as journal:
                operation_id = complete_journal[journal_path.name]["operations"][0]["operation_id"]
                request = journal.request(operation_id)
            replay = client.upsert(request) if collection_mode == "kv" else client.append(request)
            self.assertEqual(replay.result, receipt_results[operation_id])
            self.assertEqual(int(client.stats(saved["collection_id"], snapshot_id=result["snapshot"]["snapshot_id"])["rows"]), 31)
        resumed = online("load", "--state", state, "--resume")
        self.assertEqual(resumed["status"], "PASS")
        self.assertEqual(resumed["newly_confirmed_rows"], 0)
        self.assertEqual(resumed["rows_per_second"], 0)
        self.assertEqual(self.clickbench_journal(saved), complete_journal,
                         "restart/resume changed an original UUID, request, attempt or receipt")
        self.assertEqual(json.loads(state.read_bytes())["append_plan"], original_plan)
        self.assertEqual(online("status", "--state", state)["status"], "PASS")
        online("verify", "--state", state, "--format", "arrow")
        online("clean", "--state", state)
        online("clean", "--state", state)
        self.assertFalse(owned.exists())
        self.assertEqual(self.clickbench_journal(saved), complete_journal)
        self.assertTrue((dataset / "manifest.json").is_file())
        reloaded_state = self.reports / "clickbench-reloaded-state.json"
        online("init", "--dataset", dataset, "--state", reloaded_state,
               "--url", endpoint, "--name", "clickbench-e2e-reloaded", *selected_mode)
        reloaded = json.loads(reloaded_state.read_bytes())
        self.assertNotEqual(reloaded["database_id"], saved["database_id"])
        self.assertEqual(reloaded["collection_mode"], collection_mode)
        self.assertEqual(online("load", "--state", reloaded_state)["confirmed_rows"], 31)
        for format in ("json", "arrow"):
            self.assertEqual(online("verify", "--state", reloaded_state, "--format", format)["status"], "PASS")
        online("clean", "--state", reloaded_state)
        self.assertFalse((self.data_dir / "databases" / reloaded["store_id"]).exists())

    def test_generate_write_all_pages_json_arrow_verify_restart(self):
        schema = Schema(
            [
                Field("key", "string"),
                Field("i", "int64"),
                Field("x", "float64", True),
                Field("b", "bool"),
                Field("ts", "timestamp_us_utc"),
                Field("s", "string"),
            ]
        )
        cid = self.collection("all_types", schema)
        config = {
            "schema": schema.to_dict(),
            "generation": {
                "key": {"profile": "duplicates", "distinct": 7, "prefix": "ключ-"},
                "fields": {
                    "i": {"min": 9007199254740993, "max": 9007199254740999},
                    "x": {"null_rate": 0.25},
                    "ts": {
                        "min": "2020-01-01T00:00:00.000001Z",
                        "max": "2020-01-01T00:00:00.000009Z",
                    },
                    "s": {"length": 768, "alphabet": "абcd🎀"},
                },
            },
        }
        config_file, dataset = (
            self.tmp / "schema.json",
            self.tmp / "dataset with spaces",
        )
        config_file.write_text(json.dumps(config, ensure_ascii=False))
        n = 12017
        self.cli(
            "generate",
            "--schema",
            config_file,
            "--rows",
            n,
            "--seed",
            42,
            "--part-rows",
            1000,
            "--output",
            dataset,
        )
        manifest = json.loads((dataset / "manifest.json").read_bytes())
        self.assertGreater(sum(p["bytes"] for p in manifest["parts"]), 16 * 1024**2)
        with tls_proxy(self.port, self.tmp) as (_, url, ca):
            write = self.reports / "write"
            common = [
                "--url",
                url,
                "--ca-file",
                ca,
                "--database",
                self.db,
                "--collection",
                cid,
                "--timeout",
                60,
            ]
            self.cli(
                "bench",
                "write",
                *common,
                "--dataset",
                dataset,
                "--users",
                2,
                "--spawn-rate",
                20,
                "--duration",
                "120s",
                "--report",
                write,
            )
            summary = json.loads((write / "summary.json").read_bytes())
            self.assertEqual(summary["status"], "PASS")
            self.assertEqual(summary["operations"]["confirmed_rows"], n)
            self.assertTrue(summary["dataset_fully_consumed"])
            for artifact in (
                "locust.html",
                "locust.json",
                "locust_stats.csv",
                "locust_failures.csv",
            ):
                self.assertTrue((write / artifact).is_file(), artifact)
            for format in ("json", "arrow"):
                self.cli(
                    "bench",
                    "verify",
                    "--url",
                    url,
                    "--ca-file",
                    ca,
                    "--run",
                    write,
                    "--format",
                    format,
                    "--timeout",
                    60,
                )
                self.assertEqual(
                    json.loads((write / "verify.json").read_bytes())["actual_rows"], n
                )
                query = self.tmp / "query.json"
                query.write_text("{}")
                report = self.reports / ("read-" + format)
                self.cli(
                    "bench",
                    "read",
                    *common,
                    "--query",
                    query,
                    "--format",
                    format,
                    "--users",
                    2,
                    "--spawn-rate",
                    20,
                    "--duration",
                    # Allow at least one complete large scan in Debug builds;
                    # the short shutdown grace can cancel the final scan.
                    "10s",
                    "--report",
                    report,
                )
                reads = json.loads((report / "summary.json").read_bytes())["reads"]
                self.assertGreater(reads["completed_scans"], 0)
                self.assertEqual(reads["consumed_rows"], reads["completed_scans"] * n)
                self.assertGreaterEqual(
                    reads["completed_pages"], reads["completed_scans"] * 2
                )
            # No token can appear in generated manifests, reports or journals.
            for path in self.reports.rglob("*"):
                if path.is_file():
                    self.assertNotIn(
                        self.env["GLAMDB_TOKEN"].encode(), path.read_bytes(), str(path)
                    )
        self.stop()
        self.start()
        with Client(
            self.direct, self.env["GLAMDB_TOKEN"], database_id=self.db
        ) as client:
            journal = next(write.glob("worker-*.sqlite"))
            with sqlite3.connect(journal) as db:
                operation = db.execute(
                    "SELECT operation_id FROM operations LIMIT 1"
                ).fetchone()[0]
            self.assertEqual(
                client.find_append_outcome(cid, operation).outcome, "committed"
            )

    def test_lost_response_after_native_commit_is_resolved_without_duplicate(self):
        schema = Schema([Field("key", "string")])
        cid = self.collection("lost_response", schema)
        config = self.tmp / "schema.json"
        config.write_text(json.dumps(schema.to_dict()))
        dataset = self.tmp / "dataset"
        self.cli(
            "generate",
            "--schema",
            config,
            "--rows",
            1,
            "--seed",
            7,
            "--output",
            dataset,
        )
        with tls_proxy(self.port, self.tmp) as (proxy, url, ca):
            proxy.drop_once = True
            run = self.reports / "lost-write"
            self.cli(
                "bench",
                "write",
                "--url",
                url,
                "--ca-file",
                ca,
                "--database",
                self.db,
                "--collection",
                cid,
                "--dataset",
                dataset,
                "--report",
                run,
                "--duration",
                "10s",
                expected=3,
            )
            self.assertEqual(
                json.loads((run / "summary.json").read_bytes())["operations"][
                    "pending"
                ],
                1,
            )
            self.assertEqual(proxy.append_calls, 1)
            self.cli(
                "bench",
                "verify",
                "--url",
                url,
                "--ca-file",
                ca,
                "--run",
                run,
                expected=3,
            )
            self.cli("bench", "resolve", "--url", url, "--ca-file", ca, "--run", run)
            self.assertEqual(proxy.append_calls, 1, "lookup must not resend an append")
            self.cli("bench", "verify", "--url", url, "--ca-file", ca, "--run", run)
            self.assertEqual(
                json.loads((run / "verify.json").read_bytes())["actual_rows"], 1
            )
            with Client(
                self.direct, self.env["GLAMDB_TOKEN"], database_id=self.db
            ) as client:
                client.append(
                    cid,
                    [{"key": "extra-row"}],
                    operation_id=str(uuid4()),
                    schema=schema,
                )
            self.cli(
                "bench",
                "verify",
                "--url",
                url,
                "--ca-file",
                ca,
                "--run",
                run,
                expected=1,
            )

    def test_interrupt_leaves_final_reports_and_no_child(self):
        schema = Schema([Field("key", "string")])
        cid = self.collection("interrupt_read", schema)
        query = self.tmp / "query.json"
        query.write_text("{}")
        run = self.reports / "interrupted"
        process = subprocess.Popen(
            [
                CLI,
                "bench",
                "read",
                "--url",
                self.direct,
                "--database",
                self.db,
                "--collection",
                cid,
                "--query",
                str(query),
                "--duration",
                "60s",
                "--report",
                str(run),
            ],
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            until = time.monotonic() + 20
            while time.monotonic() < until:
                stats = run / "locust_stats.csv"
                if stats.exists() and "scan/json/decoded" in stats.read_text():
                    break
                if process.poll() is not None:
                    out, err = process.communicate()
                    self.fail(out + err)
                time.sleep(0.1)
            else:
                self.fail("read workload did not produce statistics")
            process.send_signal(signal.SIGINT)
            out, err = process.communicate(timeout=10)
            (self.reports / "interrupt.log").write_text(out + err)
            self.assertEqual(process.returncode, 130, out + err)
            self.assertTrue(
                (run / "locust.html").is_file(),
                "Ctrl-C must preserve the ready Locust report",
            )
            self.assertNotEqual(
                json.loads((run / "summary.json").read_bytes()).get("reason"),
                "run has not finished",
            )
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()

    def test_distributed_workers_keep_separate_journals_and_combined_counts(self):
        schema = Schema([Field("key", "string")])
        cid = self.collection("distributed", schema)
        config = self.tmp / "schema.json"
        config.write_text(json.dumps(schema.to_dict()))
        dataset = self.tmp / "dataset"
        self.cli(
            "generate",
            "--schema",
            config,
            "--rows",
            2,
            "--seed",
            1,
            "--output",
            dataset,
        )
        run = self.reports / "distributed"
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        proxy_context = tls_proxy(self.port, self.tmp)
        proxy, url, ca = proxy_context.__enter__()
        self.addCleanup(proxy_context.__exit__, None, None, None)
        proxy.append_delay = 2
        common = [
            CLI,
            "bench",
            "write",
            "--url",
            url,
            "--ca-file",
            str(ca),
            "--database",
            self.db,
            "--collection",
            cid,
            "--dataset",
            str(dataset),
            "--report",
            str(run),
            "--users",
            "2",
            "--duration",
            "10s",
            "--spawn-rate",
            "20",
            "--master-port",
            str(port),
        ]
        processes = []
        try:
            processes.append(
                subprocess.Popen(
                    common + ["--master", "--expect-workers", "2"],
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
            until = time.monotonic() + 15
            while not (run / "run.json").exists() and time.monotonic() < until:
                if processes[0].poll() is not None:
                    self.fail(str(processes[0].communicate()))
                time.sleep(0.05)
            self.assertTrue((run / "run.json").exists())
            for _ in range(2):
                processes.append(
                    subprocess.Popen(
                        common + ["--worker"],
                        env=self.env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                )
            for index, process in enumerate(processes):
                out, err = process.communicate(timeout=35)
                (self.reports / f"node-{index}.log").write_text(out + err)
                self.assertEqual(process.returncode, 0, out + err)
            summary = json.loads((run / "summary.json").read_bytes())
            self.assertEqual(summary["status"], "PASS")
            self.assertEqual(len(summary["operations"]["journals"]), 2)
            self.assertEqual(summary["operations"]["committed"], 2)
            self.assertEqual(summary["operations"]["confirmed_rows"], 4)
            self.assertEqual(
                sum(
                    row["num_requests"]
                    for row in json.loads((run / "locust.json").read_bytes())
                ),
                2,
            )
            self.cli("bench", "verify", "--url", url, "--ca-file", ca, "--run", run)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()


if __name__ == "__main__":
    unittest.main(verbosity=2, defaultTest=ARGS.case)
