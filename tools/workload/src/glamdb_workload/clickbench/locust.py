"""Fresh Locust interpreter for finite ClickBench query rounds."""

import os

from gevent import monkey

# DuckDB deadlines must interrupt CPU-bound SQL from a real native timer thread.
monkey.patch_all(thread=False)
os.environ["LOCUST_SKIP_MONKEY_PATCH"] = "1"

import csv
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import time


def main():
    path = Path(sys.argv[1]).resolve()
    config = json.loads(path.read_bytes())
    os.environ["GLAMDB_CLICKBENCH_CONFIG"] = str(path)
    report = Path(config["report"])
    options = ["locust", "--locustfile", str(Path(__file__)), "--headless",
               "--users", str(config["users"]), "--spawn-rate", str(max(100, config["users"])),
               "--stop-timeout", str(config["stop_timeout"]), "--only-summary",
               "--loglevel", "WARNING", "--exit-code-on-error", "1"]
    workers = []
    if config.get("worker"):
        options += ["--worker", "--master-host", config["master_host"],
                    "--master-port", str(config["master_port"])]
    else:
        # Bound stalled work, including source validation between logical queries.
        rounds = config["warmup"] + config["repeats"]
        maximum = math.ceil(config["timeout"] * len(config["plans"]) * rounds *
                            math.ceil(config["users"] / config["processes"]) * 2 + 30)
        options += ["--run-time", str(maximum) + "s", "--csv", str(report / "locust"),
                    "--csv-full-history", "--html", str(report / "locust.html"),
                    "--json-file", str(report / "locust")]
        if config["processes"] > 1:
            options += ["--master", "--master-bind-host", config["master_host"],
                        "--master-bind-port", str(config["master_port"]),
                        "--expect-workers", str(config["processes"]), "--expect-workers-max-wait", "30"]
            for index in range(config["processes"]):
                worker_path = report / ("launch-process-" + str(index) + ".json")
                worker_path.write_text(json.dumps(dict(config, worker=True)), encoding="utf-8")
                workers.append(subprocess.Popen([sys.executable, "-m", "glamdb_workload.clickbench.locust",
                                                 str(worker_path)], stdin=subprocess.DEVNULL))
    sys.argv = options
    from locust.main import main as locust_main
    try:
        locust_main()
    finally:
        for process in workers:
            try:
                process.wait(config["stop_timeout"] + 5)
            except subprocess.TimeoutExpired:
                process.terminate()
        for process in workers:
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    # Locust imports this file itself; register its listeners only once.
    raise SystemExit(main())

import gevent
import psutil
from locust import User, constant, events, task
from locust.exception import StopUser
from locust.html import get_html_report
from locust.runners import MasterRunner, WorkerRunner
from locust.stats import PERCENTILES_TO_REPORT, StatsCSV

from glamdb_workload.dataset import write_json
from glamdb_workload.runtime import client_for, cpu_report, error_code, failure_kind, latency_report, merge_counts, status_code
from glamdb_workload.tpch_scan import PageCountingTransport
from glamdb_workload.clickbench.queries import QueryPlan
from glamdb_workload.clickbench.reduce import execute_query, source_factory


STATE = None
REPORT_DRAIN_SECONDS = 5
METRIC_FIELDS = ("candidate_rows", "candidate_arrow_bytes", "batches", "pages", "arrow_wire_bytes")


class CountingTransport(PageCountingTransport):
    """Count every SDK request, including refusal, replay and cancellation."""

    def __init__(self, ca_file=None):
        super().__init__(ca_file)
        self.requests, self.request_body_bytes = 0, 0

    def handle_request(self, request):
        self.requests += 1
        self.request_body_bytes += len(request.content)
        return super().handle_request(request)


def _population():
    return {"counts": dict.fromkeys(("offered_queries", "completed_queries", "failed_queries",
                                    "rejected_queries", "cancelled_queries", "verified_queries",
                                    "partial_rows", "partial_arrow_bytes", "partial_arrow_wire_bytes",
                                    "partial_pages", "partial_batches", "http_requests", "request_body_bytes",
                                    *METRIC_FIELDS, *("unsuccessful_" + field for field in METRIC_FIELDS)), 0),
            "queries": {}, "rejections_by_code": {}, "errors_by_code": {}}


class RunState:
    def __init__(self, environment):
        self.environment = environment
        self.config = json.loads(Path(os.environ["GLAMDB_CLICKBENCH_CONFIG"]).read_bytes())
        self.report = Path(self.config["report"])
        self.master = isinstance(environment.runner, MasterRunner)
        self.distributed_worker = isinstance(environment.runner, WorkerRunner)
        node = environment.runner.client_id if self.distributed_worker else "local"
        self.worker = hashlib.sha256(node.encode()).hexdigest()[:16]
        self.measured, self.warmup = _population(), _population()
        self.started, self.ended = time.perf_counter(), None
        self.measurement_started, self.measurement_ended = None, None
        self.started_users, self.finished_users = 0, 0
        self.cpu_start = psutil.Process().cpu_times()
        self.cpu_peak, self.rss_peak = None, psutil.Process().memory_info().rss
        self.observed_workers, self.finished_clients = set(), set()
        self.user_reports = {}
        self.drain_started, self.drain_complete = False, not self.master
        self.samples = None if self.master else (self.report / ("measurements-" + self.worker + ".jsonl")).open(
            "w", encoding="utf-8", buffering=1)

    def worker_report(self):
        elapsed = max(0.000001, (self.ended or time.perf_counter()) - self.started)
        cpu = psutil.Process().cpu_times()
        seconds = cpu.user + cpu.system - self.cpu_start.user - self.cpu_start.system
        return {"worker": self.worker, "finished": self.ended is not None,
                "started_users": self.started_users, "finished_users": self.finished_users,
                "measured": self.measured, "warmup": self.warmup,
                "measurement_file": str(self.report / ("measurements-" + self.worker + ".jsonl")),
                "elapsed_seconds": elapsed, "measurement_started": self.measurement_started,
                "measurement_ended": self.measurement_ended,
                "cpu_seconds": seconds, "cpu_percent_one_core": seconds / elapsed * 100,
                "locust_cpu_peak_percent": self.cpu_peak, "rss_peak_bytes": self.rss_peak}

    def finish(self):
        if self.ended is None:
            self.ended = time.perf_counter()
        if self.samples is not None:
            self.samples.close()
        if not self.master:
            write_json(self.report / ("worker-" + self.worker + ".json"), self.worker_report())

    def user_finished(self):
        if self.distributed_worker:
            self.environment.runner.send_message("clickbench_users", {
                "worker": self.worker, "finished_users": self.finished_users})
        elif self.finished_users >= self.config["users"]:
            gevent.spawn_later(0.05, self.environment.runner.quit)

    def record(self, progress, warmup, code=None, cancelled=False):
        population = self.warmup if warmup else self.measured
        counts = population["counts"]
        counts["offered_queries"] += 1
        outcome = "cancelled" if cancelled else "rejected" if code and failure_kind(code) == "REFUSED" else "failed" if code else "completed"
        counts[outcome + "_queries"] += 1
        counts["http_requests"] += progress.get("http_requests", 0)
        counts["request_body_bytes"] += progress.get("request_body_bytes", 0)
        if outcome == "completed":
            counts["verified_queries"] += int(progress.get("result_verified", False))
            for field in METRIC_FIELDS:
                counts[field] += progress.get(field) or 0
        else:
            for field in METRIC_FIELDS:
                counts["unsuccessful_" + field] += progress.get(field) or 0
            if not progress.get("complete", False):
                for partial, field in (("rows", "candidate_rows"), ("arrow_bytes", "candidate_arrow_bytes"),
                                       ("arrow_wire_bytes", "arrow_wire_bytes"), ("pages", "pages"), ("batches", "batches")):
                    counts["partial_" + partial] += progress.get(field) or 0
        if code:
            bucket = population["rejections_by_code" if outcome == "rejected" else "errors_by_code"]
            bucket[code] = bucket.get(code, 0) + 1
        query = population["queries"].setdefault(f"Q{progress['query_id']}/{progress['mode']}", {})
        merge_counts(query, {"attempts": 1, outcome: 1, "verified": int(progress.get("result_verified", False)),
                             "result_validation_ms": progress.get("result_validation_ms", 0),
                             "logical_latency": {"count": 1, "total_ms": progress.get("logical_latency_ms", 0),
                                                 "max_ms": progress.get("logical_latency_ms", 0)}})
        self.rss_peak = max(self.rss_peak, psutil.Process().memory_info().rss)
        self.samples.write(json.dumps(dict(progress, warmup=warmup, worker=self.worker), separators=(",", ":")) + "\n")


@events.init.add_listener
def on_init(environment, **kwargs):
    global STATE
    environment.process_exit_code = 3
    STATE = RunState(environment)
    if STATE.master:
        def users_report(environment, msg, **kwargs):
            STATE.user_reports[msg.data["worker"]] = msg.data["finished_users"]
            if sum(STATE.user_reports.values()) >= STATE.config["users"]:
                gevent.spawn_later(0.05, environment.runner.quit)
        environment.runner.register_message("clickbench_users", users_report)


@events.test_start.add_listener
def on_start(environment, **kwargs):
    STATE.started, STATE.cpu_start = time.perf_counter(), psutil.Process().cpu_times()


@events.usage_monitor.add_listener
def on_usage(environment, cpu_usage, memory_usage, **kwargs):
    STATE.cpu_peak = max(STATE.cpu_peak or 0, cpu_usage)
    STATE.rss_peak = max(STATE.rss_peak, memory_usage)


@events.report_to_master.add_listener
def on_report(client_id, data, **kwargs):
    if STATE is not None:
        data["clickbench_worker"] = STATE.worker
        data["clickbench_finished"] = STATE.ended is not None


@events.worker_report.add_listener
def on_worker(client_id, data, **kwargs):
    if STATE is not None and data.get("clickbench_worker"):
        STATE.observed_workers.add(data["clickbench_worker"])
        if data.get("clickbench_finished") is True:
            STATE.finished_clients.add(client_id)


@events.test_stopping.add_listener
def on_stopping(environment, **kwargs):
    state = STATE
    if state is None or not state.master or state.drain_started:
        return
    state.drain_started = True
    clients = {client.id for client in environment.runner.clients.all}
    for client in clients:
        environment.runner.send_message("quit", client_id=client)
    until = time.monotonic() + REPORT_DRAIN_SECONDS
    while not clients.issubset(state.finished_clients) and time.monotonic() < until:
        gevent.sleep(0.02)
    state.drain_complete = len(clients) == state.config["processes"] and clients.issubset(state.finished_clients)


@events.test_stop.add_listener
def on_stop(environment, **kwargs):
    if STATE is not None:
        STATE.finish()


@events.quitting.add_listener
def on_quitting(environment, **kwargs):
    state = STATE
    if state is None:
        return
    if state.master:
        with gevent.Timeout(REPORT_DRAIN_SECONDS + 1, False):
            environment.runner.stop()
    else:
        environment.runner.stop()
    state.finish()
    if state.distributed_worker:
        environment.process_exit_code = 0
        return
    workers = [json.loads(p.read_bytes()) for p in sorted(state.report.glob("worker-*.json"))]
    measured, warmup = _population(), _population()
    for worker in workers:
        merge_counts(measured, worker["measured"])
        merge_counts(warmup, worker["warmup"])
    expected = state.config["users"] * state.config["repeats"] * len(state.config["plans"])
    expected_warmup = state.config["users"] * state.config["warmup"] * len(state.config["plans"])
    complete = (len(workers) == state.config["processes"] and state.drain_complete
                and all(w["finished"] for w in workers)
                and sum(w["finished_users"] for w in workers) == state.config["users"]
                and measured["counts"]["offered_queries"] == expected
                and warmup["counts"]["offered_queries"] == expected_warmup
                and not measured["counts"]["cancelled_queries"] and not warmup["counts"]["cancelled_queries"])
    unsuccessful = sum(p["counts"][key] for p in (measured, warmup)
                       for key in ("failed_queries", "rejected_queries"))
    status = "INCOMPLETE" if not complete else "FAIL" if unsuccessful else "PASS"
    starts = [w["measurement_started"] for w in workers if w["measurement_started"] is not None]
    ends = [w["measurement_ended"] for w in workers if w["measurement_ended"] is not None]
    elapsed = max(0.000001, max(ends) - min(starts)) if starts and ends else None
    counts = measured["counts"]
    expected_verified = state.config["users"] * state.config["repeats"] * sum(
        item["mode"] == "reduce" for item in state.config["plans"])
    result = {"status": status, "collection_mode": state.config.get("collection_mode", "append"),
              "result_verified": bool(expected_verified) and status == "PASS" and counts["verified_queries"] == expected_verified,
              "counts": counts, "queries": measured["queries"],
              "warmup": dict(warmup["counts"], queries=warmup["queries"],
                             rejections_by_code=warmup["rejections_by_code"], errors_by_code=warmup["errors_by_code"]),
              "expected_measured_queries": expected, "expected_workers": state.config["processes"],
              "workers": workers, "observed_workers": sorted(state.observed_workers),
              "elapsed_seconds": state.ended - state.started, "measured_elapsed_seconds": elapsed,
              "successful_queries_per_second": counts["completed_queries"] / elapsed if elapsed else None,
              "throughput_scope": "client wall time includes independent validation between logical queries",
              "rejections_by_code": measured["rejections_by_code"], "errors_by_code": measured["errors_by_code"],
              "latency_ms": latency_report(environment.stats),
              "client_cpu": cpu_report(workers, state.worker_report() if state.master else None),
              "rss_peak_bytes": max((w["rss_peak_bytes"] for w in workers), default=0),
              "configuration": state.config, "report": str(state.report)}
    html = state.report / "locust.html"
    if not html.exists():
        html.write_text(get_html_report(environment, show_download_link=False), encoding="utf-8")
    writer = StatsCSV(environment, PERCENTILES_TO_REPORT)
    for suffix, render in (("stats", writer.requests_csv), ("failures", writer.failures_csv), ("exceptions", writer.exceptions_csv)):
        with (state.report / ("locust_" + suffix + ".csv")).open("w", encoding="utf-8", newline="") as output:
            render(csv.writer(output))
    write_json(state.report / "summary.json", result)
    environment.process_exit_code = status_code(status)


class ClickBenchUser(User):
    wait_time = constant(0)

    def on_start(self):
        state = STATE
        state.started_users += 1
        self.user = state.started_users
        self.finished_work = False
        self.counter = CountingTransport(state.config.get("ca_file"))
        self.client = client_for(state.config, transport=self.counter)
        self.oracle = source_factory(state.config["source"], state.config["rows"]) if state.config.get("source") else None
        self.rounds = ((warmup, index - rounds if warmup else index, item)
                       for warmup, rounds in ((True, state.config["warmup"]), (False, state.config["repeats"]))
                       for index in range(rounds) for item in state.config["plans"])

    def on_stop(self):
        if hasattr(self, "client"):
            self.client.close()
        if self.finished_work:
            STATE.finished_users += 1
        STATE.user_finished()

    @task
    def query(self):
        state = STATE
        try:
            warmup, iteration, item = next(self.rounds)
        except StopIteration:
            self.finished_work = True
            raise StopUser()
        plan = QueryPlan(**item["plan"])
        if not warmup and state.measurement_started is None:
            state.measurement_started = time.perf_counter()
        progress = {"query_id": plan.query_id, "mode": item["mode"], "iteration": iteration, "user": self.user}
        code, cancelled = None, False
        requests, request_bytes = self.counter.requests, self.counter.request_body_bytes
        try:
            execute_query(self.client, state.config["collection"], plan, mode=item["mode"],
                          workspace=state.report, oracle_factory=self.oracle if item["mode"] == "reduce" else None,
                          timeout=state.config["timeout"], page_counter=self.counter, progress=progress,
                          **state.config["budgets"])
            if progress["status"] != "PASS":
                code = "RESULT_MISMATCH" if progress["sql_validation"] == "FAIL" else "RESULT_NOT_VALIDATED"
        except Exception as error:
            code = error_code(error)
        except BaseException:
            cancelled = True
            raise
        finally:
            progress.update(http_requests=self.counter.requests - requests,
                            request_body_bytes=self.counter.request_body_bytes - request_bytes)
            state.record(progress, warmup, code, cancelled)
            if not warmup:
                state.measurement_ended = time.perf_counter()
                self.environment.events.request.fire(
                    request_type=failure_kind(code) if code else "ERROR" if cancelled else "SDK",
                    name=f"Q{plan.query_id}/{item['mode']}/logical",
                    response_time=progress.get("logical_latency_ms", 0),
                    response_length=progress.get("arrow_wire_bytes", 0),
                    exception=RuntimeError(code or "CANCELLED") if code or cancelled else None)
