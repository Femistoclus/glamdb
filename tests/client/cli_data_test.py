"""Named CLI commands against the real native engine, never a mock server."""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
import uuid

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

    def start_server(self, managed=False, max_collections=None):
        log = (self.directory / "server.stderr").open("w+")
        ownership = ["--data-dir", str(self.directory / "databases")] if managed else self.local
        if max_collections is not None:
            ownership = ownership + ['--max-collections', str(max_collections)]
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

    def test_offline_and_legacy_server_can_reopen_a_larger_profile(self):
        schema = self.body_file('profile-schema.json', SCHEMA)
        ids = [str(uuid.uuid4()) for _ in range(16)]
        for index, cid in enumerate(ids):
            self.run_command(ADMIN, 'create-collection', *self.local, '--max-collections', '16',
                             '--collection-id', cid, '--name', f'table-{index}', '--schema', schema)
        rejected = self.run_command(ADMIN, 'collections', *self.local, code=1)
        self.assertIn('CONFIG_TOO_SMALL', rejected.stderr)
        inventory = self.run_command(ADMIN, 'collections', *self.local, '--max-collections', '16')
        self.assertEqual({item['collection_id'] for item in inventory['items']}, set(ids))
        append = self.body_file('profile-append.json', {'operation_id': OPERATION, 'rows': ROWS})
        original = self.run_command(ADMIN, 'append', *self.local, '--max-collections', '16',
                                    '--collection-id', ids[8], '--body', append)
        self.assertEqual(original['outcome'], 'committed')
        _, url = self.start_server(max_collections=16)
        remote = self.run_command(CLI, 'collections', '--url', url)
        self.assertEqual({item['collection_id'] for item in remote['items']}, set(ids))
        scan = self.run_command(CLI, 'scan', '--url', url, '--collection-id', ids[8])
        self.assertEqual(scan['rows'], ROWS)

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

    def test_write_settings_local_and_remote(self):
        schema = self.body_file("schema.json", SCHEMA)
        settings = {"parquet": {"compression": {"codec": "zstd", "level": 3},
                                "columns": [{"field": "text", "compression": {"codec": "uncompressed"}}]}}
        created = self.run_command(ADMIN, "create-collection", *self.local, "--collection-id", COLLECTION,
                                   "--name", "events", "--schema", schema,
                                   "--write-settings", self.body_file("settings.json", settings))
        self.assertEqual(created["write_settings_version"], "1")
        self.assertEqual(created["write_settings"]["parquet"]["compression"], {"codec": "zstd", "level": 3})
        common = ["--collection-id", COLLECTION]
        update = self.body_file("update.json", {"parquet": {"compression": {"codec": "snappy"}}})
        updated = self.run_command(ADMIN, "write-settings", *self.local, *common,
                                   "--expected-version", "1", "--settings", update)
        self.assertEqual(updated["write_settings_version"], "2")
        # A stale expected version is a conflict, not an update.
        self.run_command(ADMIN, "write-settings", *self.local, *common,
                         "--expected-version", "1", "--settings", self.body_file("other.json", {}), code=1)
        server, url = self.start_server()
        self.assertEqual(self.run_command(CLI, "collection", "--url", url, *common), updated)
        remote = self.run_command(CLI, "write-settings", "--url", url, *common, "--expected-version", "2",
                                  "--settings", "-", body=json.dumps({"parquet": None}))
        self.assertEqual(remote["write_settings_version"], "3")
        self.assertIsNone(remote["write_settings"]["parquet"]["compression"])
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)

    def test_snapshot_archive_local_and_remote(self):
        schema = self.body_file("schema.json", SCHEMA)
        common = ["--collection-id", COLLECTION]
        create = ["create-collection", *common, "--name", "events", "--schema", schema]
        created = self.run_command(ADMIN, *create[:1], *self.local, *create[1:], "--hot-snapshots", "2")
        self.assertEqual((created["hot_snapshots"], created["hot_snapshots_defaulted"]), ("2", False))
        # Omitted: the default; "0": no archive. Both offline.
        defaulted = ["create-collection", "--collection-id", str(uuid.uuid4()), "--name", "defaulted",
                     "--schema", schema]
        info = self.run_command(ADMIN, *defaulted[:1], *self.local, *defaulted[1:])
        self.assertEqual((info["hot_snapshots"], info["hot_snapshots_defaulted"]), ("100", True))
        none = ["create-collection", "--collection-id", str(uuid.uuid4()), "--name", "none", "--schema", schema,
                "--hot-snapshots", "0"]
        info = self.run_command(ADMIN, *none[:1], *self.local, *none[1:])
        self.assertEqual((info["hot_snapshots"], info["hot_snapshots_defaulted"]), ("0", False))
        snapshots = []
        for index in range(7):
            row = dict(ROWS[0], key=f"k-{index}")
            body = self.body_file("append.json", {"operation_id": str(uuid.uuid4()), "rows": [row]})
            receipt = self.run_command(ADMIN, "append", *self.local, *common, "--body", body)
            snapshots.append(receipt["result"]["snapshot_id"])
        status = self.run_command(ADMIN, "snapshot-archive", *self.local, *common)
        self.assertEqual(status["hot_snapshots"], "2")
        self.assertGreater(int(status["archived_snapshots"]), 0)
        self.assertEqual(int(status["root_snapshots"]) + int(status["archived_snapshots"]), 7)
        lowered = self.run_command(ADMIN, "snapshot-archive", *self.local, *common, "--hot-snapshots", "1")
        self.assertEqual(lowered["hot_snapshots"], "1")
        moved = self.run_command(ADMIN, "archive-snapshots", *self.local, *common)
        self.assertEqual((moved["root_snapshots"], moved["archived_snapshots"], moved["archive_pending"]),
                         ("1", "6", "0"))
        self.assertEqual(moved["snapshots_archived"], str(int(status["root_snapshots"]) - 1))
        offline = {command: self.run_command(ADMIN, command, *self.local, *common)
                   for command in ("snapshot-archive", "snapshots", "collection")}
        self.assertEqual([item["snapshot_id"] for item in offline["snapshots"]["items"]], snapshots)
        # Usage errors never send a request.
        self.run_command(ADMIN, "snapshot-archive", *self.local, *common, "--hot-snapshots", "2", "--unset", code=2)

        server, url = self.start_server()
        for command, value in offline.items():
            with self.subTest(command=command):
                self.assertEqual(self.run_command(CLI, command, "--url", url, *common), value)
        oldest = self.body_file("oldest.json", {"snapshot_id": snapshots[0]})
        self.assertEqual([row["key"] for row in self.run_command(CLI, "scan", "--url", url, *common,
                                                                 "--body", oldest)["rows"]], ["k-0"])
        # Repeated creation must use the created value, not the current one.
        self.assertEqual(self.run_command(CLI, *create, "--url", url, "--hot-snapshots", "2"), offline["collection"])
        mismatch = self.run_command(CLI, *create, "--url", url, "--hot-snapshots", "1", code=1)
        self.assertEqual(json.loads(mismatch.stdout)["error"]["code"], "SNAPSHOT_ARCHIVE_MISMATCH")
        # Remote creation compares effective values too.
        self.assertEqual(self.run_command(CLI, *defaulted, "--url", url, "--hot-snapshots", "100")["name"],
                         "defaulted")
        mismatch = self.run_command(CLI, *none[:-2], "--url", url, code=1)
        self.assertEqual(json.loads(mismatch.stdout)["error"]["code"], "SNAPSHOT_ARCHIVE_MISMATCH")
        self.assertEqual(self.run_command(CLI, *none, "--url", url)["hot_snapshots"], "0")
        invalid = self.run_command(CLI, "snapshot-archive", "--url", url, *common, "--hot-snapshots", "-1", code=1)
        self.assertEqual(json.loads(invalid.stdout)["error"]["code"], "INVALID_ARGUMENT")
        unset = self.run_command(CLI, "snapshot-archive", "--url", url, *common, "--unset")
        self.assertEqual((unset["hot_snapshots"], unset["hot_snapshots_defaulted"]), ("0", False))
        self.assertEqual(self.run_command(CLI, "snapshot-archive", "--url", url, *common, "--unset"), unset)
        self.assertEqual(self.run_command(CLI, "snapshot-archive", "--url", url, *common, "--hot-snapshots", "0"),
                         unset)
        idle = self.run_command(CLI, "archive-snapshots", "--url", url, *common)
        self.assertEqual((idle["snapshots_archived"], idle["archive_pending"], idle["hot_snapshots"]), ("0", "0", "0"))
        self.run_command(CLI, "snapshot-archive", "--url", url, *common, "--hot-snapshots", "2", "--unset", code=2)
        server.terminate()
        self.assertEqual(server.wait(timeout=15), 0)
        # The unset setting and the archive survive restart and offline reopen.
        self.assertEqual(self.run_command(ADMIN, "snapshot-archive", *self.local, *common),
                         dict(offline["snapshot-archive"], hot_snapshots="0", archive_pending="0"))

    def test_verify_store_reproves_history_that_a_checkpoint_open_trusts(self):
        schema = self.body_file("schema.json", SCHEMA)
        # The default archive tier gives the store verified checkpoints.
        created = self.run_command(ADMIN, "create-collection", *self.local, "--collection-id", COLLECTION,
                                   "--name", "events", "--schema", schema)
        self.assertEqual(created["hot_snapshots_defaulted"], True)
        for index in range(3):
            body = self.body_file("append.json", {"operation_id": str(uuid.uuid4()),
                                                  "rows": [dict(ROWS[0], key=f"k-{index}")]})
            self.run_command(ADMIN, "append", *self.local, "--collection-id", COLLECTION, "--body", body)
        inspected = self.run_command(ADMIN, "inspect-store", *self.local)
        self.assertNotIn("verification", inspected)
        verified = self.run_command(ADMIN, "verify-store", *self.local)
        self.assertEqual(verified, dict(inspected, verification="full"))
        # Remote and data options do not apply; ownership is required.
        self.run_command(ADMIN, "verify-store", "--root", str(self.root), code=2)
        self.run_command(ADMIN, "verify-store", *self.local, "--url", "http://127.0.0.1:1", code=2)
        # A damaged footer of an older, still current data file: the checkpoint
        # open does not read it, full verification refuses the store.
        files = sorted(self.root.rglob("*.parquet"), key=lambda path: path.stat().st_mtime_ns)
        self.assertEqual(len(files), 3)
        with files[0].open("r+b") as data:
            data.seek(-4, os.SEEK_END)
            data.write(b"XXXX")
        self.run_command(ADMIN, "inspect-store", *self.local)
        refused = self.run_command(ADMIN, "verify-store", *self.local, code=1)
        self.assertIn("CORRUPTION: ", refused.stderr)
        # Detected corruption drops the checkpoints: the next default open
        # verifies fully and refuses the store as well.
        again = self.run_command(ADMIN, "inspect-store", *self.local, code=1)
        self.assertIn("CORRUPTION: ", again.stderr)

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
