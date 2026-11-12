r"""Repeatable acceptance using pinned DBGEN, the real CLI and the native server.

Run from the repository root with the workload/SDK dependencies installed:

  .venv/bin/python tools/workload/tests/tpch_native_e2e_test.py \
    build/client-standalone/glamdb-cli build/release/apps/server/glamdb \
    --dbgen /path/to/TPCH-software-code-3.0.1/dbgen/dbgen

The default generates both SF0.01 encodings through the CLI. --dataset and
--legacy-dataset can reuse validated immutable datasets. --case selects one or
more method names. All target databases are disposable; input data is retained.
Evidence never qualifies SF1/SF10 or official TPC-H compliance.
"""

import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import select
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from uuid import UUID, uuid4

from glamdb_sdk import Client, HTTPError
from glamdb_workload.tpch_dataset import SOURCE_FILES, TPCHDataset
from glamdb_workload.tpch_queries import query_plans, reference_candidates
from glamdb_workload.tpch_verify import verify_source

from tpch_native_helpers import file_digest, file_inventory, tls_proxy


REPO = Path(__file__).resolve().parents[3]
COUNTS = {
    "region": 5, "nation": 25, "supplier": 100, "customer": 1500,
    "part": 2000, "partsupp": 8000, "orders": 15000, "lineitem": 60175,
}
ROWS = sum(COUNTS.values())


def read_json(path):
    return json.loads(Path(path).read_bytes())


def journal_snapshot(state):
    """Capture identities and actual saved body bytes, independently of the SDK."""
    with sqlite3.connect(Path(state).with_suffix(".load.sqlite")) as db:
        db.row_factory = sqlite3.Row
        chunks = [dict(row) for row in db.execute("SELECT * FROM chunks ORDER BY ordinal")]
        operations = {}
        for row in db.execute("SELECT * FROM operations ORDER BY rowid"):
            value = dict(row)
            body = bytes(value.pop("body"))
            value["actual_body_sha256"] = hashlib.sha256(body).hexdigest()
            if value["actual_body_sha256"] != value["body_hash"]:
                raise AssertionError("saved append body differs from its durable checksum")
            operations[value["operation_id"]] = value
        return {"chunks": chunks, "operations": operations}


class NativeTPCHAcceptance(unittest.TestCase):
    args = None
    evidence = None
    datasets = {}
    inventories = {}
    reduction_result = None
    candidate_counts = {}

    @classmethod
    def setUpClass(cls):
        cls.args.report_root.mkdir(parents=True, exist_ok=False)
        for name in ("cli", "server", "dbgen"):
            path = getattr(cls.args, name)
            if not path.is_file() or not os.access(path, os.X_OK):
                raise AssertionError(f"{name} must be an existing compiled executable: {path}")
        cls.generator_inventory = {
            name: file_digest(cls.args.dbgen.parent / name)
            for name in (*SOURCE_FILES, "dists.dss", "dbgen")
        }
        cls.evidence.update(
            binaries={name: {"path": str(getattr(cls.args, name)), "sha256": file_digest(getattr(cls.args, name))}
                      for name in ("cli", "server", "dbgen")},
            inputs={},
        )
        for profile, supplied in (("native", cls.args.dataset), ("legacy-scaled", cls.args.legacy_dataset)):
            destination = supplied or cls.args.report_root / ("dataset-" + profile)
            if supplied is None:
                command = [str(cls.args.cli), "bench", "tpch", "generate", "--scale-factor", "0.01",
                           "--type-profile", profile, "--dbgen", str(cls.args.dbgen),
                           "--dbgen-sha256", file_digest(cls.args.dbgen), "--output", str(destination)]
                started = time.perf_counter()
                result = subprocess.run(command, capture_output=True, text=True,
                                        env=dict(os.environ, GLAMDB_PYTHON=sys.executable), timeout=300)
                (cls.args.report_root / ("generate-" + profile + ".log")).write_text(result.stdout + result.stderr)
                if result.returncode:
                    raise AssertionError(result.stdout + result.stderr)
                cls.evidence.setdefault("generation", {})[profile] = {
                    "command": command, "elapsed_seconds": time.perf_counter() - started,
                    "returncode": result.returncode,
                }
            data = TPCHDataset(destination)
            data.validate()
            if data.scale_factor != "0.01" or data.profile != profile:
                raise AssertionError("acceptance requires the explicit SF0.01 encoding")
            actual = {name: source.manifest["rows"] for name, source in data.tables.items()}
            if actual != COUNTS or data.manifest["rows"] != ROWS:
                raise AssertionError(f"pinned SF0.01 row counts differ: {actual}")
            if data.manifest["generator"]["executable_sha256"] != file_digest(cls.args.dbgen):
                raise AssertionError("reused dataset was generated by a different executable")
            logical = verify_source(data, cls.args.report_root)
            cls.datasets[profile] = data
            cls.inventories[profile] = file_inventory(destination)
            cls.evidence["inputs"][profile] = {
                "path": str(destination), "dataset_id": data.manifest["dataset_id"],
                "manifest_sha256": file_digest(destination / "manifest.json"),
                "rows": actual, "total_rows": ROWS, "jsonl_bytes": data.manifest["bytes"],
                "source_bytes": data.manifest["source_bytes"], "logical_validation": logical,
                "provenance": data.manifest["generator"], "reused": supplied is not None,
                "generation_timings": data.manifest["timings"],
            }

    @classmethod
    def tearDownClass(cls):
        for profile, data in cls.datasets.items():
            if file_inventory(data.path) != cls.inventories[profile]:
                raise AssertionError("acceptance changed an original dataset or schema manifest")
        after = {name: file_digest(cls.args.dbgen.parent / name) for name in cls.generator_inventory}
        if after != cls.generator_inventory:
            raise AssertionError("acceptance changed the pinned generator or source files")
        cls.evidence["original_inputs_unchanged"] = True

    def setUp(self):
        self.report = self.args.report_root / self._testMethodName
        self.report.mkdir()
        self.temporary = tempfile.TemporaryDirectory(
            prefix="glamdb-tpch-", dir=os.environ.get("GLAMDB_WORKLOAD_TEMP_ROOT", "/tmp"))
        self.addCleanup(self.temporary.cleanup)
        self.tmp = Path(self.temporary.name)
        self.data_dir = self.tmp / "native-data"
        self.env = dict(os.environ, GLAMDB_TOKEN="tpch-acceptance-" + uuid4().hex,
                        GLAMDB_PYTHON=sys.executable)
        self.port = 0
        self.server = None
        self.command_number = 0
        self.details = {"commands": [], "checks": []}
        self.evidence["cases"][self._testMethodName] = self.details
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        self.server_log = (self.report / "server.log").open("a")
        self.server = subprocess.Popen(
            [str(self.args.server), "--data-dir", str(self.data_dir), "--port", str(self.port),
             "--ui-root", str(REPO / "ui")], env=self.env, text=True,
            stdout=subprocess.PIPE, stderr=self.server_log)
        self.assertTrue(select.select([self.server.stdout], [], [], 30)[0], "native server startup timed out")
        line = self.server.stdout.readline()
        if not line:
            self.server.wait(timeout=10)
            self.fail("native server did not start: " + (self.report / "server.log").read_text()[-3000:])
        self.port = json.loads(line)["port"]
        self.direct = f"http://127.0.0.1:{self.port}"

    def server_usage(self):
        if self.server is None or self.server.poll() is not None:
            return
        try:
            values = Path(f"/proc/{self.server.pid}/status").read_text().splitlines()
            for row in values:
                if row.startswith("VmHWM:"):
                    peak = int(row.split()[1])
                    self.details["server_peak_rss_kib"] = max(peak, self.details.get("server_peak_rss_kib", 0))
        except FileNotFoundError:
            pass
        sizes = [p.stat() for p in self.data_dir.rglob("*") if p.is_file()]
        apparent = sum(value.st_size for value in sizes)
        allocated = sum(value.st_blocks * 512 for value in sizes)
        self.details["store_file_peak_bytes"] = max(apparent, self.details.get("store_file_peak_bytes", 0))
        self.details["store_allocated_peak_bytes"] = max(allocated, self.details.get("store_allocated_peak_bytes", 0))

    def stop(self):
        if self.server is None:
            return
        self.server_usage()
        if self.server.poll() is None:
            self.server.terminate()
        try:
            out, _ = self.server.communicate(timeout=40)
        except subprocess.TimeoutExpired:
            self.server.kill()
            self.server.communicate()
            self.server_log.close()
            self.fail("native server shutdown timed out")
        self.server_log.write(out)
        self.server_log.close()
        code = self.server.returncode
        self.server = None
        self.assertEqual(code, 0, (self.report / "server.log").read_text()[-3000:])

    def restart(self):
        previous = self.port
        self.stop()
        self.start()
        self.assertEqual(self.port, previous, "saved endpoint must survive the native restart")

    def cli(self, *arguments, expected=0, ca=None, timeout=300):
        command = [str(self.args.cli), "bench", "tpch", *map(str, arguments)]
        if ca is not None:
            command += ["--ca-file", str(ca)]
        self.command_number += 1
        prefix = self.report / f"command-{self.command_number:03d}"
        started = time.perf_counter()
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        result = subprocess.run(command, env=self.env, text=True, capture_output=True, timeout=timeout)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        metadata = {"argv": command, "returncode": result.returncode,
                    "elapsed_seconds": time.perf_counter() - started,
                    "child_cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime}
        prefix.with_suffix(".log").write_text(result.stdout + result.stderr)
        prefix.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n")
        self.details["commands"].append(metadata)
        self.server_usage()
        self.assertEqual(result.returncode, expected, result.stdout[-3000:] + result.stderr[-6000:])
        self.assertNotIn(self.env["GLAMDB_TOKEN"], result.stdout + result.stderr)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            if expected in (0, 3):
                self.fail("command did not return its JSON result: " + result.stdout[-1000:])
            return {"diagnostic": result.stdout + result.stderr}

    def init(self, profile, url, ca=None, *, label="run", dataset=None, limits=None):
        directory = self.report / label
        directory.mkdir()
        state = directory / "run.json"
        command = ["init", "--url", url, "--dataset", dataset or self.datasets[profile].path,
                   "--name", "tpch-acceptance-" + uuid4().hex[:10], "--state", state]
        if limits is not None:
            limit_path = directory / "limits.json"
            limit_path.write_text(json.dumps(limits))
            command += ["--limits", limit_path]
        result = self.cli(*command, ca=ca)
        self.assertEqual(result["status"], "PASS")
        saved = read_json(state)
        self.assertEqual(len(saved["tables"]), 8)
        self.assertIs(saved["capabilities"]["mock"], False)
        self.assertEqual(saved["capabilities"]["backend"], "local")
        return state

    def load(self, state, ca=None, *, resume=False, expected=0):
        args = ["load", "--state", state, "--workers", "3", "--batch-rows", "5000"]
        if resume:
            args.append("--resume")
        return self.cli(*args, ca=ca, expected=expected)

    def verify(self, state, format, ca=None):
        result = self.cli("verify", "--state", state, "--format", format, "--level", "full", ca=ca)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual({name: data["actual_rows"] for name, data in result["tables"].items()}, COUNTS)
        self.assertTrue(all(data["initial_empty"] and data["missing_rows"] == data["extra_rows"] == 0
                            for data in result["tables"].values()))
        return result

    def scan(self, state, ca, *, mode, format="arrow", processes=1):
        queries = "all" if mode == "scan" else "1,6"
        saved = read_json(state)
        if mode == "scan":
            profile = read_json(Path(saved["dataset"]) / "manifest.json")["type_profile"]
            data = self.datasets[profile]
            if profile not in self.candidate_counts:
                self.candidate_counts[profile] = reference_candidates(
                    data, query_plans("all", profile=profile, scale_factor=data.scale_factor))
        report = self.report / f"{state.parent.name}-{mode}-{format}"
        result = self.cli("scan", "--state", state, "--queries", queries, "--mode", mode,
                          "--format", format, "--consume", "batches" if format == "arrow" else "rows",
                          "--users", processes, "--processes", processes, "--warmup", "0", "--repeat", "1",
                          "--report", report, ca=ca)
        self.assertEqual(result["status"], "PASS")
        expected = 22 if mode == "scan" else 2
        self.assertEqual(set(result["queries"]), {str(n) for n in (range(1, 23) if mode == "scan" else (1, 6))})
        self.assertEqual(result["counts"]["completed_queries"], expected * processes)
        self.assertEqual(result["counts"]["failed_queries"], 0)
        self.assertEqual(result["result_verified"], mode == "reduce")
        self.assertEqual(result["configuration"]["snapshots"], saved["snapshots"])
        if mode == "scan":
            expected_stages = {
                f"Q{number}/{alias}": count * processes
                for number, stages in self.candidate_counts[profile].items()
                for alias, count in stages.items()
            }
            self.assertEqual({key: stage["rows"] for key, stage in result["stages"].items()}, expected_stages)
            self.assertTrue(all(stage["completed"] == processes for stage in result["stages"].values()))
            self.details["independent_candidate_stages"] = len(expected_stages)
        if mode == "reduce":
            reduced = {number: query["result"] for number, query in result["queries"].items()}
            if type(self).reduction_result is None:
                type(self).reduction_result = reduced
            self.assertEqual(reduced, type(self).reduction_result,
                             "native/legacy and JSON/Arrow reductions must give the same exact answers")
        for artifact in ("locust.html", "locust_stats.csv", "locust_failures.csv", "summary.json", "run.json"):
            self.assertTrue((report / artifact).is_file(), artifact)
        self.details.setdefault("scans", []).append({
            "mode": mode, "format": format, "processes": processes, "counts": result["counts"],
            "result_verified": result["result_verified"],
            "results": {number: query.get("result") for number, query in result["queries"].items()},
            "elapsed_seconds": result["elapsed_seconds"], "rss_peak_bytes": result["rss_peak_bytes"],
        })
        return result

    def lifecycle(self, profile, encrypted):
        context = tls_proxy(self.port, self.tmp / "tls") if encrypted else nullcontext((None, self.direct, None))
        with context as (proxy, url, ca):
            state = self.init(profile, url, ca)
            result = self.load(state, ca)
            self.assertEqual(result["operations"]["confirmed_rows"], ROWS)
            self.details["load_metrics"] = result["metrics"]
            before = journal_snapshot(state)
            self.assert_complete_profile(state, profile)
            for format in ("json", "arrow"):
                self.verify(state, format, ca)
            if profile == "native":
                for format in ("json", "arrow"):
                    self.scan(state, ca, mode="scan", format=format,
                              processes=2 if encrypted and format == "arrow" else 1)
            if profile == "native":
                self.scan(state, ca, mode="reduce", format="json")
            reduced = self.scan(state, ca, mode="reduce")
            self.assertEqual(reduced["counts"]["verified_queries"], 2)
            self.restart()
            self.cli("status", "--state", state, ca=ca)
            resumed = self.load(state, ca, resume=True)
            self.assertEqual(resumed["operations"]["attempts"], len(before["operations"]))
            self.assertEqual(resumed["metrics"]["confirmed_rows_this_run"], 0)
            self.assertEqual(resumed["metrics"]["rows_per_second"], 0)
            self.assertEqual(journal_snapshot(state), before, "restart/resume changed a UUID, body, receipt or chunk")
            self.verify(state, "arrow", ca)
            saved_bytes = state.read_bytes()
            old_id = read_json(state)["database_id"]
            self.cli("clean", "--state", state, ca=ca)
            self.assertEqual(state.read_bytes(), saved_bytes, "clean must preserve original local state")
            with Client(self.direct, self.env["GLAMDB_TOKEN"]) as root:
                with self.assertRaises(HTTPError) as missing:
                    root.get_database(old_id)
                self.assertEqual(missing.exception.status_code, 404)
            reloaded = self.init(profile, url, ca, label="reload")
            self.assertNotEqual(read_json(reloaded)["database_id"], old_id)
            self.load(reloaded, ca)
            self.assert_complete_profile(reloaded, profile)
            self.verify(reloaded, "arrow", ca)
            self.cli("clean", "--state", reloaded, ca=ca)
            if proxy is not None:
                self.assertFalse(proxy.fault_errors)
        self.details.update(type_profile=profile, transport="verified-tls" if encrypted else "http", rows=ROWS)
        self.details["checks"] += ["init", "disjoint-load", "json-arrow-full-verify", "exact-Q1-Q6",
                                   "native-restart", "immutable-resume", "clean", "reload"]

    def assert_complete_profile(self, state, profile):
        saved = read_json(state)
        self.assertIs(saved["load_complete"], True)
        self.assertEqual({name: info["confirmed_rows"] for name, info in saved["tables"].items()}, COUNTS)
        snap = journal_snapshot(state)
        self.assertEqual(sum(chunk["rows"] for chunk in snap["chunks"]), ROWS)
        self.assertEqual(len(snap["operations"]), len(snap["chunks"]))
        self.assertEqual(set(snap["operations"]), {row["operation_id"] for row in snap["chunks"]})
        self.assertEqual({row["worker"] for row in snap["chunks"]}, {0, 1, 2})
        self.assertTrue(all(UUID(row["operation_id"]).version == 5 for row in snap["chunks"]))
        self.assertTrue(all(row["state"] == "committed" for row in snap["operations"].values()))
        for name, source in self.datasets[profile].tables.items():
            self.assertEqual(saved["tables"][name]["schema"], source.schema.to_dict())
        return snap

    def test_http_native_lifecycle(self):
        self.lifecycle("native", False)

    def test_tls_native_lifecycle(self):
        self.lifecycle("native", True)

    def test_http_legacy_lifecycle(self):
        self.lifecycle("legacy-scaled", False)

    def test_tls_legacy_lifecycle(self):
        self.lifecycle("legacy-scaled", True)

    def lost_append(self, *, before_forward):
        with tls_proxy(self.port, self.tmp / "tls") as (proxy, url, ca):
            state = self.init("native", url, ca)
            if before_forward:
                proxy.drop_request_once = True
            else:
                proxy.drop_reply_once = True
            result = self.load(state, ca, expected=3)
            self.assertEqual(result["status"], "INCOMPLETE")
            self.assertEqual(result["operations"]["pending"], 1)
            self.assertEqual(result["operations"]["attempts"], 1)
            self.assertEqual(len(proxy.appends), 1, "an uncertain append must never be retried automatically")
            before = journal_snapshot(state)
            self.assertEqual(len(before["operations"]), 1)
            operation, original = next(iter(before["operations"].items()))
            self.assertIn(original["state"], ("transport_error", "commit_unknown"))
            self.assertFalse(proxy.fault_errors)
            self.assertEqual(proxy.appends[0]["forwarded"], not before_forward)
            self.restart()
            self.load(state, ca, resume=True, expected=3)
            self.assertEqual(len(proxy.appends), 1)
            readonly = self.cli("resolve", "--state", state, ca=ca, expected=3 if before_forward else 0)
            self.assertEqual(len(proxy.appends), 1, "read-only resolution resent an append")
            resolved = journal_snapshot(state)
            for field in ("operation_id", "body_hash", "actual_body_sha256", "payload_hash", "collection_id"):
                self.assertEqual(resolved["operations"][operation][field], original[field])
            if before_forward:
                self.assertEqual(readonly["operations"]["not_found"], 1)
                self.cli("resolve", "--state", state, "--retry", ca=ca)
                self.assertEqual(len(proxy.appends), 2)
                self.assertEqual(proxy.appends[0]["body_sha256"], proxy.appends[1]["body_sha256"])
                self.assertEqual(proxy.appends[0]["operation_id"], proxy.appends[1]["operation_id"])
            else:
                self.assertEqual(readonly["operations"]["committed"], 1)
            complete = self.load(state, ca, resume=True)
            self.assertEqual(complete["operations"]["confirmed_rows"], ROWS)
            after = self.assert_complete_profile(state, "native")
            self.assertEqual(after["chunks"], before["chunks"])
            self.assertEqual(after["operations"][operation]["actual_body_sha256"], original["actual_body_sha256"])
            forwarded = [request for request in proxy.appends if request["forwarded"]]
            self.assertEqual(len(forwarded), len(after["chunks"]))
            self.assertEqual(len({request["operation_id"] for request in forwarded}), len(after["chunks"]))
            self.verify(state, "arrow", ca)
            self.cli("clean", "--state", state, ca=ca)
            self.details.update(rows=ROWS, original_operation=operation,
                                append_requests=len(proxy.appends), native_appends=len(forwarded),
                                frozen_chunks=len(after["chunks"]),
                                readonly_resolve_resends=0, original_body_sha256=original["actual_body_sha256"])
            self.details["checks"] += ["exit-3-on-uncertainty", "no-auto-retry", "native-restart",
                                       "read-only-resolution", "original-UUID-body-preserved", "disjoint-resume"]

    def test_tls_lost_committed_reply(self):
        self.lost_append(before_forward=False)

    def test_tls_authoritative_not_found_requires_explicit_retry(self):
        self.lost_append(before_forward=True)

    def test_tls_lost_init_reply_preserves_original_identities(self):
        with tls_proxy(self.port, self.tmp / "tls") as (proxy, url, ca):
            for kind in ("database", "collection"):
                directory = self.report / ("lost-" + kind)
                directory.mkdir()
                state = directory / "run.json"
                command = ["init", "--url", url, "--dataset", self.datasets["native"].path,
                           "--name", "tpch-lost-init-" + uuid4().hex[:10], "--state", state]
                first_call = len(proxy.lifecycle_calls)
                proxy.drop_lifecycle_reply_once = kind
                result = self.cli(*command, ca=ca, expected=3)
                self.assertEqual(result["status"], "INCOMPLETE")
                original = read_json(state)
                calls = proxy.lifecycle_calls[first_call:]
                self.assertEqual(len(calls), 1 if kind == "database" else 2,
                                 "uncertain init must never retry automatically")
                self.assertTrue(calls[-1]["reply_dropped"])
                self.assertEqual(calls[-1]["kind"], kind)
                self.assertFalse(proxy.fault_errors)
                self.assertEqual(len(proxy.appends), 0)
                self.restart()
                self.assertEqual(self.cli(*command, ca=ca)["status"], "PASS")
                after = read_json(state)
                for field in ("database_id", "store_id", "load_id", "dataset_sha256"):
                    self.assertEqual(after[field], original[field])
                for table in COUNTS:
                    for field in ("collection_id", "name", "schema"):
                        self.assertEqual(after["tables"][table][field], original["tables"][table][field])
                    self.assertIs(after["tables"][table]["created"], True)
                calls = proxy.lifecycle_calls[first_call:]
                database_calls = [call for call in calls if call["kind"] == "database"]
                self.assertEqual(len(database_calls), 2)
                self.assertEqual(database_calls[0]["body_sha256"], database_calls[1]["body_sha256"])
                collections = [call for call in calls if call["kind"] == "collection"]
                expected_ids = {info["collection_id"] for info in original["tables"].values()}
                self.assertEqual({call["collection_id"] for call in collections}, expected_ids)
                self.assertEqual(len(collections), 8 if kind == "database" else 9)
                if kind == "collection":
                    repeated = [call for call in collections if call["collection_id"] == collections[0]["collection_id"]]
                    self.assertEqual(len(repeated), 2)
                    self.assertEqual(repeated[0]["body_sha256"], repeated[1]["body_sha256"])
                with Client(self.direct, self.env["GLAMDB_TOKEN"]) as root:
                    self.assertEqual({item["database_id"] for item in root.list_databases().items}, {after["database_id"]})
                with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=after["database_id"]) as client:
                    self.assertEqual({item["collection_id"] for item in client.list_collections().items}, expected_ids)
                    for table, info in after["tables"].items():
                        self.assertEqual(client.get_schema(info["collection_id"]).to_dict(), original["tables"][table]["schema"])
                self.cli("clean", "--state", state, ca=ca)
                self.details.setdefault("init_faults", []).append({
                    "lost_reply": kind, "automatic_retries": 0, "database_create_requests": 2,
                    "collection_create_requests": len(collections), "unique_databases": 1,
                    "unique_collections": 8, "original_ids_schema_and_body_preserved": True,
                })
        self.details["checks"] += ["init-exit-3-on-uncertainty", "init-no-auto-retry", "native-restart",
                                   "explicit-init-reuses-original-identities-and-schema"]

    def test_tls_prebind_refusal_requires_lookup_and_explicit_retry(self):
        with tls_proxy(self.port, self.tmp / "tls") as (proxy, url, ca):
            state = self.init("native", url, ca, limits={"max_owned_input_bytes": 1})
            refused = self.load(state, ca, expected=3)
            self.assertEqual(refused["status"], "INCOMPLETE")
            self.assertEqual(refused["operations"]["failure_lookup_required"], 1)
            self.assertEqual(refused["operations"]["pending"], 1)
            self.assertEqual(len(proxy.appends), 1, "an admission refusal must not be retried automatically")
            first = proxy.appends[0]
            self.assertIs(first["forwarded"], True)
            self.assertEqual(first["outcome"], "definitely_not_committed")
            self.assertEqual(first["error_code"], "RESOURCE_EXHAUSTED")
            before = journal_snapshot(state)
            self.assertEqual(len(before["operations"]), 1)
            operation, original = next(iter(before["operations"].items()))
            self.assertEqual(original["state"], "failure_lookup_required")
            saved = read_json(state)
            with Client(self.direct, self.env["GLAMDB_TOKEN"]) as root:
                root.close_database(saved["database_id"])
                reopened = root.open_database(saved["database_id"], limits={"max_owned_input_bytes": 128 * 1024**2})
                self.assertEqual(reopened["database_id"], saved["database_id"])
                self.assertEqual(reopened["store"]["store_id"], saved["store_id"])
                self.assertEqual(reopened["limits"]["max_owned_input_bytes"], 128 * 1024**2)
            with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=saved["database_id"]) as client:
                for info in saved["tables"].values():
                    current = client.stats(info["collection_id"])
                    self.assertEqual(int(current["rows"]), 0)
                    self.assertEqual(current["snapshot_id"], info["initial_snapshot"])
            lookup = self.cli("resolve", "--state", state, ca=ca, expected=3)
            self.assertEqual(lookup["operations"]["not_found"], 1)
            self.assertEqual(len(proxy.appends), 1, "read-only lookup resent a refused append")
            self.cli("resolve", "--state", state, "--retry", ca=ca)
            self.assertEqual(len(proxy.appends), 2)
            self.assertEqual(proxy.appends[0]["operation_id"], proxy.appends[1]["operation_id"])
            self.assertEqual(proxy.appends[0]["body_sha256"], proxy.appends[1]["body_sha256"])
            self.load(state, ca, resume=True)
            after = self.assert_complete_profile(state, "native")
            self.assertEqual(before["chunks"], after["chunks"])
            self.assertEqual(after["operations"][operation]["actual_body_sha256"], original["actual_body_sha256"])
            self.assertEqual(after["operations"][operation]["attempts"], 2)
            committed = [request for request in proxy.appends if request["outcome"] == "committed"]
            self.assertEqual(len(committed), len(after["chunks"]))
            self.assertEqual(len({request["operation_id"] for request in committed}), len(after["chunks"]))
            self.verify(state, "arrow", ca)
            self.cli("clean", "--state", state, ca=ca)
            self.details.update(rows=ROWS, prebind_refusals=1, readonly_resolve_resends=0,
                                explicit_operator_limit_change={"max_owned_input_bytes": 128 * 1024**2},
                                append_requests=len(proxy.appends), native_committed_appends=len(committed),
                                original_body_sha256=original["actual_body_sha256"])
        self.details["checks"] += ["native-prebind-resource-refusal", "no-auto-retry", "operator-close-open",
                                   "read-only-authoritative-not-found", "explicit-original-body-retry", "disjoint-resume"]

    def test_preflight_rejects_capacity_changed_input_and_foreign_write(self):
        with tls_proxy(self.port, self.tmp / "tls") as (proxy, url, ca):
            low = self.init("native", url, ca, label="low-capacity", limits={"max_operations": 1})
            failure = self.load(low, ca, expected=2)
            self.assertIn("max_operations", json.dumps(failure))
            self.assertEqual(len(proxy.appends), 0)
            self.cli("clean", "--state", low, ca=ca)

            # Corrupt only a disposable copy, never the generator or reusable source.
            copy = self.tmp / "changed-dataset"
            shutil.copytree(self.datasets["native"].path, copy)
            changed = self.init("native", url, ca, label="changed-input", dataset=copy)
            part = next((copy / "lineitem").glob("*.jsonl"))
            with part.open("ab") as damaged:
                damaged.write(b"\n")
            self.load(changed, ca, expected=2)
            self.assertEqual(len(proxy.appends), 0)
            self.cli("clean", "--state", changed, ca=ca)

            foreign = self.init("native", url, ca, label="foreign-write")
            saved = read_json(foreign)
            source = self.datasets["native"].tables["region"]
            with Client(self.direct, self.env["GLAMDB_TOKEN"], database_id=saved["database_id"]) as client:
                outcome = client.append(saved["tables"]["region"]["collection_id"],
                                        next(source.iter_parts())[:1], schema=source.schema,
                                        operation_id=str(uuid4()), expected_snapshot=None)
                self.assertEqual(outcome.outcome, "committed")
            failure = self.load(foreign, ca, expected=2)
            self.assertIn("foreign", json.dumps(failure))
            self.assertEqual(len(proxy.appends), 0, "preflight guards must refuse before any benchmark append")
            self.cli("clean", "--state", foreign, ca=ca)
        self.details["checks"] += ["low-operation-cap-before-send", "changed-part-before-send",
                                   "foreign-write-before-send", "original-sources-unchanged"]


class EvidenceResult(unittest.TextTestResult):
    def startTest(self, test):
        self.started = time.perf_counter()
        super().startTest(test)

    def addSuccess(self, test):
        test.details.update(status="PASS", elapsed_seconds=time.perf_counter() - self.started)
        super().addSuccess(test)

    def addFailure(self, test, err):
        test.details.update(status="FAIL", elapsed_seconds=time.perf_counter() - self.started,
                            failure=self._exc_info_to_string(err, test))
        super().addFailure(test, err)

    def addError(self, test, err):
        if hasattr(test, "details"):
            test.details.update(status="ERROR", elapsed_seconds=time.perf_counter() - self.started,
                                failure=self._exc_info_to_string(err, test))
        super().addError(test, err)


def main():
    options = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    options.add_argument("cli", type=lambda p: Path(p).resolve())
    options.add_argument("server", type=lambda p: Path(p).resolve())
    options.add_argument("--dbgen", required=True, type=lambda p: Path(p).resolve())
    options.add_argument("--dataset", type=lambda p: Path(p).resolve())
    options.add_argument("--legacy-dataset", type=lambda p: Path(p).resolve())
    options.add_argument("--report-root", type=lambda p: Path(p).resolve(),
                         default=REPO / "artifacts" / "issue157" / ("native-acceptance-" + uuid4().hex[:8]))
    options.add_argument("--evidence", type=lambda p: Path(p).resolve())
    options.add_argument("--case", action="append", help="one test method name; may be repeated")
    args = options.parse_args()
    if args.report_root.exists():
        options.error("--report-root must be a new directory to preserve earlier evidence")
    evidence = {"version": 1, "workload": "TPC-H-derived native acceptance", "scale_factor": "0.01",
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "command": [sys.executable, *sys.argv], "cases": {},
                "runtime": {"python": platform.python_version(), "platform": platform.platform(),
                            "declared_python_requirement": ">=3.13", "supported_python": sys.version_info >= (3, 13)},
                "not_run": ["SF1 full campaign", "SF10 full campaign", "official TPC-H compliance", "power-loss durability"],
                "report_root": str(args.report_root)}
    NativeTPCHAcceptance.args, NativeTPCHAcceptance.evidence = args, evidence
    loader = unittest.defaultTestLoader
    suite = (unittest.TestSuite(loader.loadTestsFromName(name, NativeTPCHAcceptance) for name in args.case)
             if args.case else loader.loadTestsFromTestCase(NativeTPCHAcceptance))
    started = time.perf_counter()
    result = unittest.TextTestRunner(verbosity=2, resultclass=EvidenceResult).run(suite)
    evidence.update(status="PASS" if result.wasSuccessful() else "FAIL", tests_run=result.testsRun,
                    failures=len(result.failures), errors=len(result.errors),
                    elapsed_seconds=time.perf_counter() - started,
                    finished_utc=datetime.now(timezone.utc).isoformat())
    destination = args.evidence or args.report_root / "acceptance.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(evidence, indent=2) + "\n")
    print("Acceptance evidence:", destination)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
