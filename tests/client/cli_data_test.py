"""Named CLI commands against the real native engine, never a mock server."""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest

CLI, ADMIN, SERVER = (str(Path(value).resolve()) for value in sys.argv[1:4])
del sys.argv[1:4]
STORE = "00000000-0000-0000-0000-000000000052"
COLLECTION = "00000000-0000-0000-0000-000000000053"
OPERATION = "00000000-0000-0000-0000-000000000054"
ENV = dict(os.environ, GLAMDB_TOKEN="cli-data-native-secret")
SCHEMA = {"fields": [
    {"name": "key", "type": "string", "nullable": False},
    {"name": "n", "type": "int64", "nullable": False},
    {"name": "text", "type": "string", "nullable": True},
    {"name": "at", "type": "timestamp_us_utc", "nullable": False},
]}
ROWS = [
    {"key": "duplicate", "n": "9223372036854775807", "text": "quotes \" and café", "at": "9999-12-31T23:59:59.999999Z"},
    {"key": "duplicate", "n": "-9223372036854775808", "text": None, "at": "0001-01-01T00:00:00.000001Z"},
]


class NativeCommands(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="glamdb-cli-data-", dir="/tmp")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.root = self.directory / "store with spaces"
        self.local = ["--root", str(self.root), "--store-id", STORE]
        self.run_command(ADMIN, "create-store", *self.local)

    def run_command(self, binary, *args, code=0, body=None):
        result = subprocess.run([binary, *args], env=ENV, input=body, text=True,
                                capture_output=True, timeout=20)
        self.assertEqual(result.returncode, code, (args, result.stdout, result.stderr))
        if code == 0:
            return json.loads(result.stdout)
        return result

    def body_file(self, name, value):
        target = self.directory / name
        target.write_text(json.dumps(value), encoding="utf-8")
        return str(target)

    def start_server(self, managed=False):
        log = (self.directory / "server.stderr").open("w+")
        ownership = ["--data-dir", str(self.directory / "databases")] if managed else self.local
        server = subprocess.Popen([SERVER, "serve", *ownership, "--port", "0"],
                                  env=ENV, stdout=subprocess.PIPE, stderr=log, text=True)
        self.addCleanup(log.close)
        def stop():
            if server.poll() is None:
                server.terminate()
                server.wait(timeout=15)
            server.stdout.close()
        self.addCleanup(stop)
        self.assertTrue(select.select([server.stdout], [], [], 15)[0], "server did not start")
        line = server.stdout.readline()
        if not line:
            log.seek(0)
            self.fail(log.read())
        startup = json.loads(line)
        return server, f"http://127.0.0.1:{startup['port']}"

    def create_collection(self):
        schema = self.body_file("schema.json", SCHEMA)
        return self.run_command(ADMIN, "create-collection", *self.local,
                                "--collection-id", COLLECTION, "--name", "events / café",
                                "--schema", schema)

    def test_native_local_remote_parity_and_ownership(self):
        created = self.create_collection()
        self.assertEqual(created["collection_id"], COLLECTION)
        append = self.body_file("append.json", {"operation_id": OPERATION, "rows": ROWS})
        receipt = self.run_command(ADMIN, "append", *self.local, "--collection-id", COLLECTION,
                                   "--body", append)
        self.assertEqual(receipt["outcome"], "committed")
        snapshot = receipt["result"]["snapshot_id"]
        common = ["--collection-id", COLLECTION]
        offline = {}
        for command in ("collection", "schema", "scan", "snapshots", "files"):
            offline[command] = self.run_command(ADMIN, command, *self.local, *common)
        self.assertEqual(offline["scan"]["rows"], ROWS)
        initial = self.body_file("initial.json", {"snapshot_id": "initial"})
        self.assertEqual(self.run_command(ADMIN, "scan", *self.local, *common,
                                         "--body", initial)["rows"], [])
        self.assertEqual(self.run_command(ADMIN, "operation", *self.local, *common,
                                         "--operation-id", OPERATION), receipt)
        file_id = offline["files"]["items"][0]["file_id"]
        details = self.run_command(ADMIN, "file", *self.local, *common, "--file-id", file_id)
        self.assertTrue(details["physical_file_exists"])
        self.assertEqual(details["record_count"], "2")
        server, url = self.start_server()
        for command in offline:
            with self.subTest(command=command):
                remote = self.run_command(CLI, command, "--url", url, *common)
                self.assertEqual(remote, offline[command])
        for projection in ([], ["at", "key"]):
            query = self.body_file("projection.json", {"projection": projection})
            scan = self.run_command(CLI, "scan", "--url", url, *common, "--body", query)
            self.assertEqual(scan["rows"], [{key: row[key] for key in projection} for row in ROWS])
            self.assertEqual([field["field_id"] for field in scan["schema"]["fields"]],
                             [] if not projection else [4, 1])
        self.assertEqual(self.run_command(CLI, "append", "--url", url, *common, "--body", append), receipt)
        self.assertEqual(self.run_command(CLI, "operation", "--url", url, *common,
                                         "--operation-id", OPERATION), receipt)
        self.assertEqual(self.run_command(CLI, "file", "--url", url, *common, "--file-id", file_id), details)
        resolved = self.run_command(CLI, "resolve-collection", "--url", url, "--name", "events / café")
        self.assertEqual(resolved, offline["collection"])
        self.assertEqual(self.run_command(CLI, "collections", "--url", url)["items"], [resolved])
        invalid_output = self.run_command(CLI, "append", "--url", url, *common,
                                          "--body", append, "--output", "/dev/null", code=1)
        self.assertTrue(invalid_output.stderr)
        second = self.body_file("second.json", {"operation_id": "00000000-0000-0000-0000-000000000055", "rows": ROWS})
        self.run_command(CLI, "append", "--url", url, *common, "--body", second)
        self.assertEqual(self.run_command(CLI, "append", "--url", url, *common, "--body", append), receipt)
        self.assertEqual(len(self.run_command(CLI, "scan", "--url", url, *common)["rows"]), 4)
        files = self.run_command(CLI, "files", "--url", url, *common)["items"]
        later_file = next(item["file_id"] for item in files if item["file_id"] != file_id)
        self.run_command(CLI, "file", "--url", url, *common, "--file-id", later_file,
                         "--snapshot", snapshot, code=1)
        locked = self.run_command(ADMIN, "scan", *self.local, *common, code=1)
        self.assertIn("STATE_IN_USE", locked.stderr)
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)
        self.assertEqual(self.run_command(ADMIN, "append", *self.local, *common, "--body", append), receipt)
        historical = self.body_file("historical.json", {"snapshot_id": snapshot})
        self.assertEqual(self.run_command(ADMIN, "scan", *self.local, *common,
                                         "--body", historical)["rows"], ROWS)
        self.assertEqual(self.run_command(ADMIN, "operation", *self.local, *common,
                                         "--operation-id", OPERATION), receipt)

    @unittest.skipIf(os.environ.get("GLAMDB_TEST_ADMIN_HTTP") == "0",
                     "MSan uses an ordinary network client; native admin HTTP is covered by the other profiles")
    def test_managed_database_commands_and_admin_http_mode(self):
        server, url = self.start_server(managed=True)
        database = self.run_command(CLI, "create-store", "--url", url, "--name", "named CLI")
        common = ["--database-id", database["database_id"], "--collection-id", COLLECTION]
        self.run_command(CLI, "create-collection", "--url", url, *common,
                         "--name", "events", "--schema", "-", body=json.dumps(SCHEMA))
        append = json.dumps({"operation_id": OPERATION, "rows": ROWS})
        receipt = self.run_command(CLI, "append", "--url", url, *common, "--body", "-", body=append)
        for command in ("collection", "schema", "scan", "snapshots", "files"):
            self.assertEqual(self.run_command(ADMIN, command, "--url", url, *common),
                             self.run_command(CLI, command, "--url", url, *common))
        self.assertEqual(self.run_command(ADMIN, "append", "--url", url, *common, "--body", "-", body=append), receipt)
        self.assertEqual(self.run_command(ADMIN, "operation", "--url", url, *common,
                                         "--operation-id", OPERATION), receipt)
        result = self.run_command(ADMIN, "scan", "--url", url, *common)
        self.assertEqual(result["rows"], ROWS)
        scoped = url + "/databases/" + database["database_id"]
        self.assertEqual(self.run_command(ADMIN, "schema", "--url", scoped,
                                         "--collection-id", COLLECTION), result["schema"])
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)

    def test_offline_stdout_failure_preserves_committed_operation(self):
        self.create_collection()
        body = self.body_file("append.json", {"operation_id": OPERATION, "rows": ROWS})
        common = ["--collection-id", COLLECTION]
        with open("/dev/full", "w") as sink:
            result = subprocess.run([ADMIN, "append", *self.local, *common, "--body", body],
                                    env=ENV, stdout=sink, stderr=subprocess.PIPE, text=True, timeout=20)
        self.assertEqual(result.returncode, 4, result.stderr)
        receipt = self.run_command(ADMIN, "operation", *self.local, *common, "--operation-id", OPERATION)
        self.assertEqual(receipt["outcome"], "committed")

    def test_native_scan_completes_multiple_http_pages(self):
        self.create_collection()
        rows = [{**ROWS[0], "text": "x" * 700000} for _ in range(7)]
        append = self.body_file("large.json", {"operation_id": OPERATION, "rows": rows})
        common = ["--collection-id", COLLECTION]
        self.run_command(ADMIN, "append", *self.local, *common, "--body", append)
        local = self.run_command(ADMIN, "scan", *self.local, *common)
        self.assertEqual(local["rows"], rows)
        server, url = self.start_server()
        self.assertEqual(self.run_command(CLI, "scan", "--url", url, *common), local)
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)

    def test_result_bound_counts_rows_once_across_http_pages(self):
        schema = self.body_file("large-schema.json", {"fields": [
            {"name": "key", "type": "string", "nullable": False},
            {"name": "text", "type": "string", "nullable": False},
        ]})
        common = ["--collection-id", COLLECTION]
        self.run_command(ADMIN, "create-collection", *self.local, *common,
                         "--name", "near response bound", "--schema", schema)
        for index in (1, 2):
            body = self.body_file(f"large{index}.json", {
                "operation_id": f"00000000-0000-0000-0000-{index:012d}",
                "rows": [{"key": "k", "text": "x" * 1048460}] * 8})
            self.run_command(ADMIN, "append", *self.local, *common, "--body", body)
        local = self.run_command(ADMIN, "scan", *self.local, *common)
        self.assertEqual(len(local["rows"]), 16)
        server, url = self.start_server()
        remote = self.run_command(CLI, "scan", "--url", url, *common)
        self.assertEqual(remote, local)
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)

    def test_oversized_scan_never_outputs_partial_rows(self):
        self.create_collection()
        common = ["--collection-id", COLLECTION]
        for index in (1, 2):
            body = self.body_file(f"rows{index}.json", {
                "operation_id": f"00000000-0000-0000-0000-{index:012d}", "rows": [ROWS[0]] * 5001})
            self.run_command(ADMIN, "append", *self.local, *common, "--body", body)
        result = self.run_command(ADMIN, "scan", *self.local, *common, code=1)
        self.assertEqual(result.stdout, "")
        server, url = self.start_server()
        result = self.run_command(CLI, "scan", "--url", url, *common, code=1)
        self.assertEqual(result.stdout, "")
        limited = self.body_file("limit.json", {"limit": 1})
        self.assertEqual(self.run_command(CLI, "scan", "--url", url, *common, "--body", limited)["rows"], ROWS[:1])
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)

    def test_missing_store_and_mixed_modes_do_not_create(self):
        absent = self.directory / "absent"
        result = self.run_command(ADMIN, "collections", "--root", str(absent), "--store-id", STORE, code=1)
        self.assertTrue(result.stderr)
        self.assertFalse(absent.exists())
        result = self.run_command(ADMIN, "collections", *self.local, "--url", "http://127.0.0.1:1", code=1)
        self.assertTrue(result.stderr)

    def test_invalid_append_and_conflicting_retry_are_failures(self):
        self.create_collection()
        common = ["--collection-id", COLLECTION]
        append = self.body_file("append.json", {"operation_id": OPERATION, "rows": ROWS})
        self.run_command(ADMIN, "append", *self.local, *common, "--body", append)
        conflicting = self.body_file("conflict.json", {"operation_id": OPERATION, "rows": ROWS[:1]})
        result = self.run_command(ADMIN, "append", *self.local, *common, "--body", conflicting, code=1)
        self.assertEqual(json.loads(result.stdout)["error"]["code"], "OPERATION_CONFLICT")
        self.run_command(ADMIN, "operation", *self.local, *common,
                         "--operation-id", "00000000-0000-0000-0000-000000000099", code=1)
        result = self.run_command(ADMIN, "append", *self.local, *common,
                                  "--body", "-", body='{"operation_id":"a","operation_id":"b","rows":[]}', code=1)
        self.assertTrue(result.stderr)


if __name__ == "__main__":
    unittest.main()
