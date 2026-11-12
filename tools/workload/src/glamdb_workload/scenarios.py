"""Locust owns users, scheduling, distributed statistics and report formats."""

import hashlib
import csv
import json
import os
from pathlib import Path
import ssl
import time
from uuid import UUID, uuid5

import gevent
import httpx
import psutil
from locust import User, constant, events, task
from locust.exception import StopUser
from locust.runners import MasterRunner, WorkerRunner
from locust.html import get_html_report
from locust.stats import StatsCSV, PERCENTILES_TO_REPORT

from glamdb_sdk import Schema
from glamdb_workload.dataset import Dataset, write_json
from glamdb_workload.ledger import Ledger
from glamdb_workload.runtime import (
    add_stage,
    append_once,
    client_for,
    consume_scan,
    cpu_report,
    error_code,
    failure_kind,
    latency_report,
    merge_counts,
    read_mode,
    scan_request_name,
    stage_report,
    status_code,
    summarize,
    throughput_report,
)

STATE = None


class PageCountingTransport(httpx.BaseTransport):
    """Observe Arrow page responses; decoding and integrity remain in the SDK."""

    def __init__(self, ca_file=None):
        verify = ssl.create_default_context(cafile=ca_file) if ca_file else True
        self.inner = httpx.HTTPTransport(verify=verify, retries=0)
        self.pages = 0

    def handle_request(self, request):
        response = self.inner.handle_request(request)
        if response.status_code == 200 and response.headers.get(
            "content-type", ""
        ).startswith("application/vnd.apache.arrow.stream"):
            self.pages += 1
        return response

    def close(self):
        self.inner.close()


class RunState:
    def __init__(self, environment):
        self.environment = environment
        self.config = json.loads(
            Path(os.environ["GLAMDB_WORKLOAD_CONFIG"]).read_bytes()
        )
        self.run = Path(self.config["report"])
        self.master = isinstance(environment.runner, MasterRunner)
        self.distributed_worker = isinstance(environment.runner, WorkerRunner)
        node_id = environment.runner.client_id if self.distributed_worker else "local"
        self.worker = hashlib.sha256(node_id.encode()).hexdigest()[:16]
        self.prefix = "worker-" + self.worker
        self.schema = Schema.from_dict(self.config["target"]["schema"])
        self.log = None if self.master else Ledger(self.run / (self.prefix + ".sqlite"))
        self.dataset = (
            Dataset(self.config["dataset"])
            if self.config["mode"] == "write" and not self.master
            else None
        )
        self.parts = iter(self.dataset.iter_parts()) if self.dataset else None
        self.claimed = 0
        self.active = 0
        self.exhausted = False
        self.stopping = False
        self.started = time.perf_counter()
        self.ended = None
        self.cpu_start = psutil.Process().cpu_times()
        self.rss_peak = psutil.Process().memory_info().rss
        self.cpu_peak = None
        self.payload_bytes = 0
        self.counts = dict(
            offered_scans=0,
            completed_scans=0,
            completed_pages=0,
            consumed_rows=0,
            arrow_bytes=0,
            arrow_batches=0,
            arrow_batch_bytes=0,
            failed_scans=0,
            rejected_scans=0,
            cancelled_scans=0,
            partial_rows=0,
            offered_appends=0,
            rejected_appends=0,
            errors=0,
            rejections_by_code={},
            errors_by_code={},
            write_stages={},
        )
        self.fatal = None
        self.observed_workers = set()
        self.finished_worker_clients = set()
        self.drain_started = False
        self.drain_complete = not self.master

    def quit_when_drained(self):
        if (
            not self.active
            and (self.exhausted or self.stopping)
            and not self.distributed_worker
        ):
            gevent.spawn_later(0.05, self.environment.runner.quit)

    def worker_report(self):
        elapsed = max(0.000001, (self.ended or time.perf_counter()) - self.started)
        cpu = psutil.Process().cpu_times()
        cpu_seconds = (
            cpu.user + cpu.system - self.cpu_start.user - self.cpu_start.system
        )
        return {
            "worker": self.worker,
            "finished": self.ended is not None,
            "elapsed_seconds": elapsed,
            "counts": self.counts,
            "fatal": self.fatal,
            "claimed_parts": self.claimed,
            "total_parts": (
                len(self.dataset.manifest["parts"]) if self.dataset else None
            ),
            "cpu_seconds": cpu_seconds,
            "cpu_percent_one_core": cpu_seconds / elapsed * 100,
            "locust_cpu_peak_percent": self.cpu_peak,
            "rss_peak_bytes": self.rss_peak,
            "operations": self.log.summary() if self.log else None,
        }

    def controller_cpu(self):
        elapsed = max(0.000001, (self.ended or time.perf_counter()) - self.started)
        cpu = psutil.Process().cpu_times()
        seconds = cpu.user + cpu.system - self.cpu_start.user - self.cpu_start.system
        return {"cpu_seconds": seconds, "cpu_percent_one_core": seconds / elapsed * 100}

    def failed(self, code):
        kind = failure_kind(code)
        key = "rejections_by_code" if kind == "REFUSED" else "errors_by_code"
        self.counts[key][code] = self.counts[key].get(code, 0) + 1
        return kind

    def finish(self):
        if self.ended is None:
            self.ended = time.perf_counter()
        if not self.master:
            write_json(self.run / (self.prefix + ".json"), self.worker_report())


@events.init.add_listener
def on_init(environment, **kwargs):
    global STATE
    environment.process_exit_code = 3
    STATE = RunState(environment)


@events.test_start.add_listener
def on_start(environment, **kwargs):
    STATE.started = time.perf_counter()
    STATE.cpu_start = psutil.Process().cpu_times()


@events.usage_monitor.add_listener
def on_usage(environment, cpu_usage, memory_usage, **kwargs):
    STATE.cpu_peak = max(STATE.cpu_peak or 0, cpu_usage)
    STATE.rss_peak = max(STATE.rss_peak, memory_usage)


@events.report_to_master.add_listener
def on_report(client_id, data, **kwargs):
    if STATE is not None:
        data["glamdb_worker"] = STATE.worker
        data["glamdb_finished"] = STATE.ended is not None


@events.worker_report.add_listener
def on_worker(client_id, data, **kwargs):
    if STATE is not None and data.get("glamdb_worker"):
        STATE.observed_workers.add(data["glamdb_worker"])
        if data.get("glamdb_finished") is True:
            STATE.finished_worker_clients.add(client_id)


@events.test_stopping.add_listener
def on_stopping(environment, **kwargs):
    state = STATE
    if state is None or not state.master or state.drain_started:
        return
    state.drain_started = True
    clients = {client.id for client in environment.runner.clients.all}
    # Keep Locust's RPC/stats receivers alive while workers drain. Their native
    # quit handler sends a final standard stats report after test_stop, and our
    # report_to_master flag acknowledges the durable journal/report snapshot.
    for client_id in clients:
        environment.runner.send_message("quit", client_id=client_id)
    until = time.monotonic() + state.config["stop_timeout"] + 1
    while (
        not clients.issubset(state.finished_worker_clients) and time.monotonic() < until
    ):
        gevent.sleep(0.02)
    state.drain_complete = len(clients) == state.config[
        "expect_workers"
    ] and clients.issubset(state.finished_worker_clients)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if STATE is not None:
        STATE.finish()


@events.quitting.add_listener
def on_quitting(environment, **kwargs):
    state = STATE
    if state is None:
        return
    # Drain/interrupt users before taking a final SQLite snapshot.
    if state.master:
        with gevent.Timeout(state.config["stop_timeout"] + 1, False):
            environment.runner.stop()
    else:
        environment.runner.stop()
    state.finish()
    if state.distributed_worker:
        environment.process_exit_code = 0 if not state.fatal else 1
        return
    workers = [
        json.loads(path.read_bytes())
        for path in sorted(state.run.glob("worker-*.json"))
    ]
    operations = summarize(state.run)
    expected = state.config["expect_workers"] if state.master else 1
    counts = {}
    for worker in workers:
        merge_counts(counts, worker["counts"])
    for key, value in state.counts.items():
        counts.setdefault(key, {} if isinstance(value, dict) else 0)
    stages = counts.pop("write_stages")
    rejections = counts.pop("rejections_by_code")
    failures = counts.pop("errors_by_code")
    complete = (
        len(workers) == expected
        and len(operations["journals"]) == expected
        and all(w["finished"] for w in workers)
        and state.drain_complete
    )
    status = "PASS"
    if not complete or operations["pending"]:
        status = "INCOMPLETE"
    elif (
        counts["errors"]
        or any(w["fatal"] for w in workers)
        or operations["definitely_not_committed"]
    ):
        status = "FAIL"
    elif state.config["mode"] == "read" and not counts["completed_scans"]:
        status = "INCOMPLETE"
    elif state.config["mode"] == "write" and not operations["committed"]:
        status = "INCOMPLETE"
    elapsed = max(0.000001, state.ended - state.started)
    result = {
        "status": status,
        "run_id": state.config["run_id"],
        "mode": state.config["mode"],
        "elapsed_seconds": elapsed,
        "operations": operations,
        "reads": counts,
        "confirmed_rows_per_second": operations["confirmed_rows"] / elapsed,
        "confirmed_request_bytes_per_second": operations["confirmed_bytes"] / elapsed,
        "workers": workers,
        "expected_workers": expected,
        "observed_workers": sorted(state.observed_workers),
        "dataset_fully_consumed": (
            all(w["claimed_parts"] == w["total_parts"] for w in workers)
            if state.config["mode"] == "write"
            else None
        ),
        "measurement": state.config["measurement"],
        "read_mode": (
            read_mode(state.config) if state.config["mode"] == "read" else None
        ),
        "processes": state.config.get("processes"),
        "throughput": throughput_report(
            state.config["mode"], counts, operations, elapsed
        ),
        "rejections_by_code": rejections,
        "errors_by_code": failures,
        "latency_ms": latency_report(environment.stats),
        "write_stages": stage_report(stages),
        "client_cpu": cpu_report(
            workers, state.controller_cpu() if state.master else None
        ),
    }
    # Locust's SIGTERM shutdown skips its normal HTML-save path. Render using
    # the same upstream report implementation, including very short runs.
    html = state.run / "locust.html"
    if not html.exists():
        html.write_text(
            get_html_report(environment, show_download_link=False), encoding="utf-8"
        )
    writer = StatsCSV(environment, PERCENTILES_TO_REPORT)
    for suffix, render in (
        ("stats", writer.requests_csv),
        ("failures", writer.failures_csv),
        ("exceptions", writer.exceptions_csv),
    ):
        destination = state.run / ("locust_" + suffix + ".csv")
        temporary = destination.with_suffix(".csv.tmp")
        with temporary.open("w", encoding="utf-8", newline="") as output:
            render(csv.writer(output))
        temporary.replace(destination)
    write_json(state.run / "summary.json", result)
    environment.process_exit_code = status_code(status)


class GlamDBUser(User):
    wait_time = constant(0)

    def on_start(self):
        self.counter = (
            PageCountingTransport(STATE.config.get("ca_file"))
            if STATE.config.get("format") == "arrow"
            else None
        )
        self.client = client_for(STATE.config, transport=self.counter)

    def on_stop(self):
        self.client.close()

    @task
    def sdk_scenario(self):
        if STATE.stopping:
            raise StopUser()
        if STATE.config["mode"] == "write":
            self.write()
        else:
            self.read()

    def write(self):
        state = STATE
        began = time.perf_counter()
        try:
            rows = next(state.parts)
        except StopIteration:
            state.exhausted = True
            state.quit_when_drained()
            raise StopUser()
        except Exception as error:
            state.fatal = error_code(error)
            state.stopping = True
            state.quit_when_drained()
            raise StopUser()
        loaded = time.perf_counter()
        part = state.claimed
        state.claimed += 1
        operation = str(
            uuid5(UUID(state.config["run_id"]), state.worker + ":" + str(part))
        )
        state.active += 1
        try:
            request = self.client.prepare_append(
                state.config["collection"],
                rows,
                operation_id=operation,
                schema=state.schema,
            )
            prepared = time.perf_counter()
            if (
                state.payload_bytes + len(request.body)
                > state.config["max_journal_bytes"]
            ):
                raise ValueError("JOURNAL_BUDGET_EXCEEDED")
            state.payload_bytes += len(request.body)
            state.counts["offered_appends"] += 1
            result = append_once(
                self.client, state.log, request, deadline=state.config["timeout"]
            )
            stages = state.counts["write_stages"]
            add_stage(stages, "input_load", loaded - began)
            add_stage(stages, "sdk_prepare", prepared - loaded)
            add_stage(stages, "journal", result["journal_ms"] / 1000)
            add_stage(stages, "http_send", result["response_time_ms"] / 1000)
            add_stage(stages, "end_to_end", time.perf_counter() - began)
            failed = result["state"] != "committed"
            kind = "SDK"
            if failed:
                state.counts["errors"] += 1
                kind = state.failed(result["error_code"] or result["state"])
                if kind == "REFUSED":
                    state.counts["rejected_appends"] += 1
                if result["state"] != "definitely_not_committed":
                    state.stopping = True
            self.environment.events.request.fire(
                request_type=kind,
                name="append",
                response_time=result["response_time_ms"],
                response_length=0,
                exception=(
                    RuntimeError(result["error_code"] or result["state"])
                    if failed
                    else None
                ),
            )
        except Exception as error:
            state.fatal = error_code(error)
            state.stopping = True
        finally:
            state.active -= 1
            state.quit_when_drained()

    def read(self):
        state = STATE
        config = state.config
        progress = dict(rows=0, pages=0, bytes=0, batches=0, batch_bytes=0)
        failed = None
        kind = "SDK"
        state.counts["offered_scans"] += 1
        began = time.perf_counter()
        try:
            consume_scan(self.client, config, progress, page_counter=self.counter)
        except Exception as error:
            code = error_code(error)
            failed = RuntimeError(code)
            kind = state.failed(code)
            state.counts["errors"] += 1
            state.counts["failed_scans"] += 1
            if kind == "REFUSED":
                state.counts["rejected_scans"] += 1
            state.counts["partial_rows"] += progress["rows"]
        except BaseException:
            state.counts["cancelled_scans"] += 1
            state.counts["partial_rows"] += progress["rows"]
            raise
        finally:
            elapsed = (time.perf_counter() - began) * 1000
        if failed is None:
            state.counts["completed_scans"] += 1
            state.counts["completed_pages"] += progress["pages"]
            state.counts["consumed_rows"] += progress["rows"]
            state.counts["arrow_bytes"] += progress["bytes"]
            state.counts["arrow_batches"] += progress["batches"]
            state.counts["arrow_batch_bytes"] += progress["batch_bytes"]
        self.environment.events.request.fire(
            request_type=kind,
            name=scan_request_name(config),
            response_time=elapsed,
            response_length=progress["bytes"],
            exception=failed,
        )
