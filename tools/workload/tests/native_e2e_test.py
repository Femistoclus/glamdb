"""Default E2E uses the compiled native server, verified TLS and isolated storage.

Usage: python native_e2e_test.py CLI SERVER [--report-root DIRECTORY]
No mock fallback and no database deletion endpoint are used.
"""

import argparse
from contextlib import contextmanager
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
                if self.command == "POST" and self.path.endswith("/append"):
                    with self.server.lock:
                        self.server.append_calls += 1
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
                "0",
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
            "1s",
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
