#!/usr/bin/env python3
r"""Verify real native TPC-H cursor cancellation when only the CLI is signalled.

  artifacts/issue157/venv313/bin/python tools/workload/tests/tpch_interrupt_test.py \
    build/client-standalone/glamdb-cli build/release/apps/server/glamdb \
    --dataset artifacts/issue157/sf001-native

First completes all 22 Arrow scans with two processes, then uses the same isolated
native target for SIGINT and SIGTERM. The verified TLS proxy
holds a real page request after /scan/open; cancellation must reach the native
server while that page is still held. No mock transport or patched client is used.
"""

import argparse
from datetime import datetime, timezone
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from uuid import uuid4

import psutil

from glamdb_sdk import Client, HTTPError
from glamdb_workload.tpch_dataset import TPCHDataset

import tpch_native_e2e_test as native
from tpch_native_helpers import file_digest, file_inventory, tls_proxy


REPO = Path(__file__).resolve().parents[3]


def install_barrier(proxy):
    """Specialize only the existing real TLS forwarding handler."""
    parent = proxy.RequestHandlerClass
    proxy.hold_pages = False
    proxy.page_held = threading.Event()
    proxy.release_page = threading.Event()
    proxy.held_cursors = []
    proxy.cancel_requests = []

    class HoldingPage(parent):
        def do_POST(self):
            if self.path.endswith("/scan") and self.server.hold_pages:
                size = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(size))
                with self.server.lock:
                    self.server.held_cursors.append({
                        "collection_id": self.path.split("/")[-2], "cursor": body["cursor"],
                    })
                self.server.page_held.set()
                # The driver releases this in finally. The held page cannot
                # reach EOS or explain a native cursor disappearing by itself.
                self.server.release_page.wait(15)
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.close_connection = True
                return
            if self.path.endswith("/scan/cancel"):
                with self.server.lock:
                    self.server.cancel_requests.append(self.path)
            return super().do_POST()

    proxy.RequestHandlerClass = HoldingPage


def occupancy(client):
    capabilities = client.capabilities(deadline=2)
    sessions = capabilities["scan_sessions"]
    return {
        "global": sessions["global"], "database": sessions["database"],
        "cancellations": sessions["cancellations"], "expirations": sessions["expirations"],
        "limits": {key: value for key, value in capabilities["limits"].items()
                   if key.startswith("scan_")},
    }


def owned_processes(process):
    result = []
    for child in psutil.Process(process.pid).children(recursive=True):
        try:
            with child.oneshot():
                result.append({
                    "pid": child.pid, "ppid": child.ppid(), "created": child.create_time(),
                    "pgid": os.getpgid(child.pid), "session_id": os.getsid(child.pid),
                    "command": child.cmdline(), "rss_bytes": child.memory_info().rss,
                })
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass
    return result


def retained_processes(owned):
    live, zombies = [], []
    for item in owned:
        try:
            process = psutil.Process(item["pid"])
            if process.create_time() != item["created"]:
                continue
            (zombies if process.status() == psutil.STATUS_ZOMBIE else live).append(item["pid"])
        except psutil.NoSuchProcess:
            pass
    return live, zombies


def stop_owned(process, owned):
    """Failure cleanup targets only the captured CLI hierarchy."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    live, _ = retained_processes(owned)
    for pid in live:
        try:
            psutil.Process(pid).kill()
        except psutil.NoSuchProcess:
            pass
    process.communicate(timeout=2)


def run_signal(harness, state, proxy, ca, signum):
    saved = native.read_json(state)
    name = signal.Signals(signum).name
    report = harness.report / name.lower()
    command = [str(harness.args.cli), "bench", "tpch", "scan", "--state", str(state),
               "--queries", "1", "--mode", "scan", "--format", "arrow", "--consume", "batches",
               "--users", "1", "--processes", "1", "--warmup", "0", "--repeat", "1",
               "--timeout", "20", "--report", str(report), "--ca-file", str(ca)]
    record = {"signal": name, "signal_target": "native CLI PID only", "command": command,
              "format": "arrow", "status": "RUNNING", "test_cleanup_cancellations": 0}
    proxy.page_held.clear()
    proxy.release_page.clear()
    proxy.held_cursors.clear()
    proxy.cancel_requests.clear()
    original_state, original_journal = file_digest(state), file_digest(state.with_suffix(".load.sqlite"))
    with Client(harness.direct, harness.env["GLAMDB_TOKEN"], database_id=saved["database_id"], timeout=2) as client:
        record["before"] = occupancy(client)
        assert record["before"]["database"]["live_cursors"] == 0
        proxy.hold_pages = True
        process = subprocess.Popen(command, env=harness.env, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        owned = []
        try:
            until = time.monotonic() + 30
            while not proxy.page_held.wait(0.02):
                if process.poll() is not None or time.monotonic() >= until:
                    output, error = process.communicate(timeout=2)
                    raise AssertionError("real CLI did not reach the held native page: " + output + error)
            record["held"] = occupancy(client)
            assert record["held"]["database"]["live_cursors"] > 0, record["held"]
            assert record["held"]["global"]["live_cursors"] > 0, record["held"]
            owned = owned_processes(process)
            record["owned_before_signal"] = owned
            assert len(owned) >= 2, "expected a real Python controller and Locust child"
            record["cli_pid"] = process.pid
            record["held_cursor_count"] = len(proxy.held_cursors)
            started = time.monotonic()
            process.send_signal(signum)
            output, error = process.communicate(timeout=4.5)
            record["signal_to_exit_seconds"] = time.monotonic() - started
            record["returncode"] = process.returncode
            (harness.report / (name.lower() + ".log")).write_text(output + error)
            assert harness.env["GLAMDB_TOKEN"] not in output + error
            record["after_exit"] = occupancy(client)
            record["proxy_cancel_requests"] = len(proxy.cancel_requests)
            live, zombies = retained_processes(owned)
            record["live_owned_descendants_after_exit"] = live
            record["zombie_owned_descendants_after_exit"] = zombies
            record["state_unchanged"] = file_digest(state) == original_state
            record["journal_unchanged"] = file_digest(state.with_suffix(".load.sqlite")) == original_journal
            record["report_artifacts"] = sorted(path.name for path in report.iterdir() if path.is_file())
            summary = report / "summary.json"
            assert summary.is_file(), "interrupted native scan must preserve summary.json"
            partial = native.read_json(summary)
            record["partial_summary"] = {key: partial.get(key) for key in (
                "status", "result_verified", "counts", "expected_measured_queries",
                "errors_by_code", "rejections_by_code",
            )}
            assert process.returncode == 128 + signum, "CLI did not preserve its original signal exit code"
            assert record["signal_to_exit_seconds"] < 4.5, "CLI exceeded its 4.5-second cleanup deadline"
            assert not live and not zombies, "CLI retained owned descendants after returning"
            assert record["proxy_cancel_requests"] > 0, "no cursor cancellation reached the TLS proxy"
            assert record["after_exit"]["cancellations"] > record["held"]["cancellations"], "native cancellation counter did not advance"
            assert record["after_exit"]["database"]["live_cursors"] == 0, "database retained a live native cursor"
            assert record["after_exit"]["global"]["live_cursors"] == 0, "native server retained a live cursor"
            assert record["after_exit"]["expirations"] == record["held"]["expirations"], "cursor expiration masked missing cancellation"
            assert record["state_unchanged"] and record["journal_unchanged"], "interrupted scan changed load state or journal"
            assert record["partial_summary"]["status"] == "INCOMPLETE", "cancelled scan report must remain incomplete"
            assert record["partial_summary"]["result_verified"] is False
            assert record["partial_summary"]["counts"]["offered_queries"] == 1
            assert record["partial_summary"]["counts"]["cancelled_queries"] == 1
            assert record["partial_summary"]["counts"]["completed_queries"] == 0
            record["status"] = "PASS"
        except BaseException as error:
            record.setdefault("after_exit", occupancy(client))
            record.setdefault("proxy_cancel_requests", len(proxy.cancel_requests))
            record.update(status="FAIL", failure=type(error).__name__ + ": " + str(error),
                          traceback=traceback.format_exc())
        finally:
            proxy.hold_pages = False
            proxy.release_page.set()
            stop_owned(process, owned)
            # A failed product cleanup must not leak a test cursor. Its native
            # observations above remain distinct from this explicit fallback.
            if occupancy(client)["database"]["live_cursors"]:
                for held in proxy.held_cursors:
                    try:
                        client.cancel_scan(held["collection_id"], held["cursor"], deadline=2)
                        record["test_cleanup_cancellations"] += 1
                    except HTTPError as error:
                        if error.status_code != 404:
                            raise
            record["after_test_cleanup"] = occupancy(client)
            if record["test_cleanup_cancellations"]:
                record["status"] = "FAIL"
                record.setdefault("failure", "explicit test cleanup was required to release a native cursor")
            harness.server_usage()
    return record


def main():
    options = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    options.add_argument("cli", type=lambda value: Path(value).resolve())
    options.add_argument("server", type=lambda value: Path(value).resolve())
    options.add_argument("--dataset", required=True, type=lambda value: Path(value).resolve())
    options.add_argument("--report-root", type=lambda value: Path(value).resolve(),
                         default=REPO / "artifacts/issue157" / ("native-interruption-" + uuid4().hex[:8]))
    options.add_argument("--evidence", type=lambda value: Path(value).resolve())
    args = options.parse_args()
    if args.report_root.exists():
        options.error("--report-root must be a new directory")
    if sys.version_info < (3, 13):
        options.error("this qualification requires Python >=3.13")
    args.report_root.mkdir(parents=True)
    dataset = TPCHDataset(args.dataset)
    dataset.validate()
    assert dataset.profile == "native" and dataset.scale_factor == "0.01"
    assert {name: table.manifest["rows"] for name, table in dataset.tables.items()} == native.COUNTS
    original_inputs = file_inventory(dataset.path)
    evidence = {
        "version": 1, "issue": 157, "recorded_date": "2026-11-12", "status": "RUNNING",
        "started_utc": datetime.now(timezone.utc).isoformat(), "command": [sys.executable, *sys.argv],
        "scope": "Native held-cursor cancellation through a verified TLS proxy; signals target only the real CLI.",
        "python": platform.python_version(), "platform": platform.platform(),
        "installed_versions": {name: version(name) for name in ("glamdb-sdk", "glamdb-workload", "pyarrow", "locust", "gevent", "psutil")},
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "binaries": {name: {"path": str(getattr(args, name)), "sha256": file_digest(getattr(args, name))}
                     for name in ("cli", "server")},
        "driver_sha256": file_digest(__file__),
        "workload_sources": {name: file_digest(REPO / name) for name in (
            "tools/workload/src/glamdb_workload/tpch_scan.py",
            "tools/workload/src/glamdb_workload/tpch_locust.py",
            "sdk/python/src/glamdb_sdk/client.py",
        )},
        "dataset": {"id": dataset.manifest["dataset_id"], "manifest_sha256": file_digest(dataset.path / "manifest.json"),
                    "scale_factor": "0.01", "type_profile": "native", "rows": native.ROWS,
                    "rows_by_table": native.COUNTS},
        "deadlines_seconds": {"signal_to_exit_maximum": 4.5, "cli_scan": 20, "held_page_maximum": 15},
        "cases": [],
    }
    harness = native.NativeTPCHAcceptance("test_tls_native_lifecycle")
    harness._testMethodName = "interrupt_target"
    harness.args, harness.datasets, harness.evidence = args, {"native": dataset}, {"cases": {}}
    state = None
    started = time.monotonic()
    try:
        harness.setUp()
        with tls_proxy(harness.port, harness.tmp / "tls") as (proxy, url, ca):
            install_barrier(proxy)
            state = harness.init("native", url, ca)
            loaded = harness.load(state, ca)
            assert loaded["status"] == "PASS" and loaded["operations"]["confirmed_rows"] == native.ROWS
            evidence["load_metrics"] = loaded["metrics"]
            normal = harness.scan(state, ca, mode="scan", format="arrow", processes=2)
            evidence["normal_distributed_scan"] = {
                "status": normal["status"], "mode": "scan", "format": "arrow", "processes": 2,
                "users": 2, "counts": normal["counts"], "result_verified": normal["result_verified"],
                "expected_measured_queries": normal["expected_measured_queries"],
                "expected_workers": normal["expected_workers"], "observed_workers": normal["observed_workers"],
                "independent_source_candidate_counts_verified": True,
                "stages": {name: {"rows": stage["rows"], "completed": stage["completed"]}
                           for name, stage in normal["stages"].items()},
                "elapsed_seconds": normal["elapsed_seconds"],
                "measured_elapsed_seconds": normal["measured_elapsed_seconds"],
                "rss_peak_bytes": normal["rss_peak_bytes"], "client_cpu": normal["client_cpu"],
                "report": normal["report"],
                "summary_sha256": file_digest(Path(normal["report"]) / "summary.json"),
                "locust_stop_timeout_seconds": normal["configuration"]["stop_timeout"],
            }
            assert normal["counts"]["completed_queries"] == 44
            assert len(normal["stages"]) == 86
            print("normal two-process all22 PASS", flush=True)
            for signum in (signal.SIGINT, signal.SIGTERM):
                record = run_signal(harness, state, proxy, ca, signum)
                evidence["cases"].append(record)
                print(record["signal"], record["status"], record.get("returncode"),
                      record.get("signal_to_exit_seconds"), flush=True)
            harness.cli("clean", "--state", state, ca=ca)
        evidence["status"] = "PASS" if all(item["status"] == "PASS" for item in evidence["cases"]) else "FAIL"
    except BaseException:
        evidence.update(status="FAIL", failure=traceback.format_exc())
    finally:
        evidence["cleanup_completed"] = harness.doCleanups()
        if not evidence["cleanup_completed"]:
            evidence["status"] = "FAIL"
        evidence["original_inputs_unchanged"] = file_inventory(dataset.path) == original_inputs
        evidence["resources"] = {key: value for key, value in harness.details.items() if key not in ("commands", "checks")}
        evidence["preparation_commands"] = harness.details["commands"]
        evidence.update(elapsed_seconds=time.monotonic() - started,
                        finished_utc=datetime.now(timezone.utc).isoformat())
        if not evidence["original_inputs_unchanged"]:
            evidence["status"] = "FAIL"
        destination = args.evidence or args.report_root / "interruption.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(evidence, indent=2) + "\n")
        print("Interruption evidence:", destination, flush=True)
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
