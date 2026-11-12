"""CLI11 -> reproc -> Python lifecycle integration; receives a built CLI."""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

CLI = str(Path(sys.argv.pop(1)).resolve())


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="glamdb bridge ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        package = self.root / "modules with spaces" / "glamdb_workload"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        self.module = package / "__main__.py"
        self.schema = self.root / "schema with spaces.json"
        self.schema.write_text("{}")
        self.env = dict(
            os.environ,
            GLAMDB_PYTHON=sys.executable,
            PYTHONPATH=str(package.parent),
            GLAMDB_TOKEN="bridge-secret-never-in-argv",
        )

    def args(self):
        return [
            CLI,
            "generate",
            "--schema",
            str(self.schema),
            "--rows",
            "7",
            "--seed",
            "42",
            "--output",
            str(self.root / "output with spaces"),
        ]

    def test_child_argv_streams_and_exit_code_without_shell(self):
        self.module.write_text(
            'import json,sys\nprint(json.dumps(sys.argv[1:]))\nprint("child-error",file=sys.stderr)\nsys.exit(23)\n'
        )
        output = str(self.root / "$(touch MUST_NOT_EXIST); 🎀")
        result = subprocess.run(
            self.args()[:-1] + [output],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        args = json.loads(result.stdout)
        self.assertEqual(args[0], "generate")
        self.assertEqual(args[args.index("--output") + 1], output)
        self.assertNotIn(self.env["GLAMDB_TOKEN"], result.stdout + result.stderr)
        self.assertIn("child-error", result.stderr)
        self.assertFalse(Path("MUST_NOT_EXIST").exists())

    def test_bench_forwards_measurement_options(self):
        self.module.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
        common = ["--url", "http://127.0.0.1:1", "--database", "d", "--collection", "c"]
        common += ["--report", str(self.root / "run"), "--processes", "3"]
        for mode, extra in (
            (
                "read",
                ["--query", "q.json", "--format", "arrow", "--consume", "batches"],
            ),
            ("write", ["--dataset", "data"]),
        ):
            result = subprocess.run(
                [CLI, "bench", mode, *common, *extra],
                env=self.env,
                text=True,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            args = json.loads(result.stdout)
            self.assertEqual(args[args.index("--processes") + 1], "3")
            if mode == "read":
                self.assertEqual(args[args.index("--consume") + 1], "batches")

    def test_clickbench_commands_forward_options_without_shell_or_target_overrides(self):
        self.module.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
        path = str(self.root / "ClickBench run with spaces.json")
        for action, options in (
            ("fetch-queries", ["--output", "reference with spaces"]),
            ("query", ["--state", path, "--catalog", "reference", "--report", "queries.json",
                       "--source", "hits.parquet", "--ids", "1,20,24", "--mode", "both",
                       "--warmup", "0", "--repeats", "1", "--memory-bytes", "67108864"]),
            ("prepare", ["--source", "hits.parquet", "--output", "data", "--type-profile", "legacy-date", "--rows", "31"]),
            ("init", ["--state", path, "--dataset", "data", "--name", "hits", "--url", "https://example.org",
                      "--limits", "limits.json", "--max-journal-bytes", "1024", "--test-backend", "mock"]),
            ("load", ["--state", path, "--resume"]),
            ("resolve", ["--state", path, "--retry"]),
            ("verify", ["--state", path, "--format", "arrow"]),
            ("size", ["--state", path, "--report", "sizes.json", "--include-footers", "--owned-store-dir", "/store"]),
            ("status", ["--state", path]),
            ("clean", ["--state", path]),
        ):
            result = subprocess.run([CLI, "bench", "clickbench", action, *options], env=self.env,
                                    text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            args = json.loads(result.stdout)
            self.assertEqual(args[:3], ["bench", "clickbench", action])
            for value in options:
                self.assertIn(value, args)
            self.assertNotIn(self.env["GLAMDB_TOKEN"], result.stdout + result.stderr)

    def test_missing_python_is_actionable(self):
        self.env["GLAMDB_PYTHON"] = str(self.root / "no python")
        result = subprocess.run(
            self.args(), env=self.env, text=True, capture_output=True, timeout=10
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("GLAMDB_PYTHON", result.stderr)

    def test_tpch_clean_forwards_saved_state_path_without_identity_overrides(self):
        self.module.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
        state = str(self.root / "tpch run.json")
        result = subprocess.run(
            [CLI, "bench", "tpch", "clean", "--state", state, "--timeout", "9", "--ca-file", "ca.pem"],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), [
            "bench", "tpch", "clean", "--state", state, "--ca-file", "ca.pem", "--timeout", "9"
        ])
        self.assertNotIn(self.env["GLAMDB_TOKEN"], result.stdout + result.stderr)

    def test_standalone_drop_checks_saved_identity_and_success_result(self):
        database = "11111111-2222-4333-8444-555555555555"
        other = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        calls = []
        responses = [
            {"database_id": database, "store_id": database, "dropped": True},
            {"database_id": other, "store_id": other, "dropped": True},
        ]

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                calls.append((self.path, self.headers.get("Authorization"),
                              json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
                body = json.dumps(responses.pop(0)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            args = [CLI, "drop-database", "--url", f"http://127.0.0.1:{server.server_port}",
                    "--database", database, "--store-id", database]
            result = subprocess.run(args, env=self.env, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout),
                             {"database_id": database, "store_id": database, "dropped": True})
            result = subprocess.run(args, env=self.env, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertIn("invalid database drop response", result.stderr)
            for invalid in (other, "tpch-name"):
                result = subprocess.run(args[:-1] + [invalid], env=self.env, text=True,
                                        capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(calls, [(f"/databases/{database}/drop",
                                     "Bearer bridge-secret-never-in-argv", {"store_id": database})] * 2)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_missing_extra_is_actionable(self):
        self.module.write_text(
            'raise ModuleNotFoundError("missing Locust", name="locust")\n'
        )
        result = subprocess.run(
            self.args(), env=self.env, text=True, capture_output=True, timeout=10
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("pip install", result.stderr)

    def test_sigint_stops_child_and_reaps_it(self):
        self.module.write_text(
            "import os,signal,sys,time\nprint(os.getpid(),flush=True)\nsignal.signal(signal.SIGTERM,lambda *a: sys.exit(130))\nwhile True: time.sleep(.1)\n"
        )
        process = subprocess.Popen(
            self.args(),
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            line = process.stdout.readline()
            self.assertTrue(line.strip().isdigit(), line)
            child = int(line)
            process.send_signal(signal.SIGINT)
            _, err = process.communicate(timeout=9)
            self.assertEqual(process.returncode, 130, err)
            with self.assertRaises(ProcessLookupError):
                os.kill(child, 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()

    def test_ignored_termination_is_killed_within_timeout(self):
        self.module.write_text(
            "import os,signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nprint(os.getpid(),flush=True)\nwhile True: time.sleep(.1)\n"
        )
        process = subprocess.Popen(
            self.args(),
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            child = int(process.stdout.readline())
            began = time.monotonic()
            process.send_signal(signal.SIGINT)
            _, err = process.communicate(timeout=9)
            self.assertEqual(process.returncode, 130, err)
            self.assertLess(time.monotonic() - began, 9)
            with self.assertRaises(ProcessLookupError):
                os.kill(child, 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main()
