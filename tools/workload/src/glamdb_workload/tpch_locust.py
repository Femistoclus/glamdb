"""Fresh Locust process for finite TPC-H query rounds and native reports."""

from gevent import monkey

monkey.patch_all()

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time


def main():
    path = Path(sys.argv[1]).resolve()
    config = json.loads(path.read_bytes())
    os.environ["GLAMDB_TPCH_CONFIG"] = str(path)
    report = Path(config["report"])
    options = ["locust", "--locustfile", str(Path(__file__)), "--headless",
               "--users", str(config["users"]), "--spawn-rate", str(max(100, config["users"])),
               "--stop-timeout", str(config["stop_timeout"]), "--only-summary",
               "--loglevel", "WARNING", "--exit-code-on-error", "1"]
    workers = []
    if config.get("worker"):
        options += ["--worker", "--master-host", config["master_host"], "--master-port", str(config["master_port"])]
    else:
        # A deadline bounds a stuck/failed user or worker; successful finite
        # rounds stop the native runner earlier through its normal quit path.
        maximum = math.ceil(config["timeout"] * len(config["plans"]) * (config["warmup"] + config["repeat"]) + 30)
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
                workers.append(subprocess.Popen([sys.executable, "-m", "glamdb_workload.tpch_locust", str(worker_path)], stdin=subprocess.DEVNULL))
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
    # The locustfile is imported once below under Locust's module loader.
    # Avoid registering the same global event listeners in this launcher.
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
from glamdb_workload.tpch_scan import PageCountingTransport, execute_query


STATE = None
# Worker quit sends terminal statistics after stop() fires test_stop. Keep the
# master RPC receiver alive for that acknowledgement independently of task grace.
REPORT_DRAIN_SECONDS = 5


def _population():
    return {"counts": dict.fromkeys(("offered_queries", "completed_queries", "failed_queries", "rejected_queries", "cancelled_queries", "verified_queries", "rows", "pages", "arrow_batches", "arrow_wire_bytes", "arrow_batch_bytes", "partial_rows"), 0),
            "stages": {}, "queries": {}, "rejections_by_code": {}, "errors_by_code": {}}


class RunState:
    def __init__(self, environment):
        self.environment = environment
        self.config = json.loads(Path(os.environ["GLAMDB_TPCH_CONFIG"]).read_bytes())
        self.report = Path(self.config["report"])
        self.master = isinstance(environment.runner, MasterRunner)
        self.distributed_worker = isinstance(environment.runner, WorkerRunner)
        node = environment.runner.client_id if self.distributed_worker else "local"
        self.worker = hashlib.sha256(node.encode()).hexdigest()[:16]
        self.measured, self.warmup = _population(), _population()
        self.started, self.ended = time.perf_counter(), None
        self.measurement_started, self.measurement_ended = None, None
        self.started_users, self.finished_users, self.active_users = 0, 0, 0
        self.cpu_start = psutil.Process().cpu_times()
        self.cpu_peak, self.rss_peak = None, psutil.Process().memory_info().rss
        self.observed_workers, self.finished_clients = set(), set()
        self.user_reports = {}
        self.drain_started, self.drain_complete = False, not self.master

    def worker_report(self):
        elapsed = max(0.000001, (self.ended or time.perf_counter()) - self.started)
        cpu = psutil.Process().cpu_times()
        seconds = cpu.user + cpu.system - self.cpu_start.user - self.cpu_start.system
        return {"worker": self.worker, "finished": self.ended is not None,
                "started_users": self.started_users, "finished_users": self.finished_users,
                "measured": self.measured, "warmup": self.warmup,
                "elapsed_seconds": elapsed, "measurement_started": self.measurement_started,
                "measurement_ended": self.measurement_ended,
                "cpu_seconds": seconds, "cpu_percent_one_core": seconds / elapsed * 100,
                "locust_cpu_peak_percent": self.cpu_peak, "rss_peak_bytes": self.rss_peak}

    def finish(self):
        if self.ended is None:
            self.ended = time.perf_counter()
        if not self.master:
            write_json(self.report / ("worker-" + self.worker + ".json"), self.worker_report())

    def user_finished(self):
        self.active_users -= 1
        if self.distributed_worker:
            self.environment.runner.send_message("tpch_users", {"worker": self.worker, "finished_users": self.finished_users})
        elif self.finished_users >= self.config["users"]:
            gevent.spawn_later(0.05, self.environment.runner.quit)

    def record(self, progress, warmup, code=None, cancelled=False):
        population = self.warmup if warmup else self.measured
        counts = population["counts"]
        counts["offered_queries"] += 1
        rows = sum(s["rows"] for s in progress.get("stages", []))
        if cancelled:
            counts["cancelled_queries"] += 1
            counts["partial_rows"] += rows
        elif code:
            counts["failed_queries"] += 1
            counts["partial_rows"] += rows
            key = "rejections_by_code" if failure_kind(code) == "REFUSED" else "errors_by_code"
            bucket = population[key]
            bucket[code] = bucket.get(code, 0) + 1
            if key == "rejections_by_code": counts["rejected_queries"] += 1
        else:
            counts["completed_queries"] += 1
            counts["verified_queries"] += int(progress["result_verified"])
            for field in ("rows", "pages", "arrow_batches", "arrow_wire_bytes", "arrow_batch_bytes"):
                counts[field] += sum(s[field] or 0 for s in progress["stages"])
        for stage in progress.get("stages", []):
            values = {key: stage[key] or 0 for key in ("rows", "pages", "arrow_batches", "arrow_wire_bytes", "arrow_batch_bytes")}
            values.update(attempts=1, completed=int(stage["status"] == "complete"),
                          refused=int(stage["status"] == "refused"), errors=int(stage["status"] == "error"),
                          cancelled=int(stage["status"] == "cancelled"),
                          latency={"count": 1, "total_ms": stage["latency_ms"], "max_ms": stage["latency_ms"]},
                          client_compute_ms=stage["client_compute_ms"], row_materialization_ms=stage["row_materialization_ms"])
            merge_counts(population["stages"].setdefault(f"Q{progress['query_id']}/{stage['alias']}", {}), values)
        per_query = population["queries"].setdefault(str(progress["query_id"]), {"attempts": 0, "completed": 0, "verified": 0, "client_compute_ms": 0.0, "result_validation_ms": 0.0, "logical_latency": {"count": 0, "total_ms": 0.0, "max_ms": 0}})
        merge_counts(per_query, {"attempts": 1, "completed": int(progress.get("complete", False)),
                                "verified": int(progress.get("result_verified", False)),
                                "client_compute_ms": progress.get("client_compute_ms", 0),
                                "result_validation_ms": progress.get("result_validation_ms", 0),
                                "logical_latency": {"count": 1, "total_ms": progress.get("logical_latency_ms", 0), "max_ms": progress.get("logical_latency_ms", 0)}})
        per_query["result"] = progress.get("result")


@events.init.add_listener
def on_init(environment, **kwargs):
    global STATE
    environment.process_exit_code = 3
    STATE = RunState(environment)
    if STATE.master:
        def users_report(environment, msg, **kwargs):
            state = STATE
            state.user_reports[msg.data["worker"]] = msg.data["finished_users"]
            if sum(state.user_reports.values()) >= state.config["users"]:
                gevent.spawn_later(0.05, environment.runner.quit)
        environment.runner.register_message("tpch_users", users_report)


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
        data["tpch_worker"] = STATE.worker
        data["tpch_finished"] = STATE.ended is not None


@events.worker_report.add_listener
def on_worker(client_id, data, **kwargs):
    if STATE is not None and data.get("tpch_worker"):
        STATE.observed_workers.add(data["tpch_worker"])
        if data.get("tpch_finished") is True:
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
    if state is None: return
    if state.master:
        # Leave time to record the inner drain outcome before this outer bound.
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
        for destination, source in ((measured, worker["measured"]), (warmup, worker["warmup"])):
            # Results are not additive; merge only their numeric metrics.
            for key in ("counts", "stages", "rejections_by_code", "errors_by_code"):
                merge_counts(destination[key], source[key])
            for number, query in source["queries"].items():
                result = query.pop("result", None)
                merge_counts(destination["queries"].setdefault(number, {}), query)
                destination["queries"][number]["result"] = result
    expected = state.config["users"] * state.config["repeat"] * len(state.config["plans"])
    expected_warmup = state.config["users"] * state.config["warmup"] * len(state.config["plans"])
    complete = (len(workers) == state.config["processes"] and state.drain_complete
                and all(w["finished"] for w in workers)
                and sum(w["finished_users"] for w in workers) == state.config["users"]
                and measured["counts"]["offered_queries"] == expected
                and warmup["counts"]["offered_queries"] == expected_warmup
                and not measured["counts"]["cancelled_queries"] and not warmup["counts"]["cancelled_queries"])
    status = "INCOMPLETE" if not complete else "FAIL" if measured["counts"]["failed_queries"] or warmup["counts"]["failed_queries"] else "PASS"
    starts = [w["measurement_started"] for w in workers if w["measurement_started"] is not None]
    ends = [w["measurement_ended"] for w in workers if w["measurement_ended"] is not None]
    elapsed = max(0.000001, max(ends) - min(starts)) if starts and ends else None
    counts = measured["counts"]
    result = {"status": status, "mode": state.config["mode"],
              "result_verified": state.config["mode"] == "reduce" and status == "PASS" and counts["verified_queries"] == expected,
              "counts": counts, "queries": measured["queries"], "stages": measured["stages"],
              "warmup": dict(warmup["counts"], stages=warmup["stages"], rejections_by_code=warmup["rejections_by_code"], errors_by_code=warmup["errors_by_code"]),
              "expected_measured_queries": expected, "expected_workers": state.config["processes"],
              "workers": workers, "observed_workers": sorted(state.observed_workers),
              "elapsed_seconds": state.ended - state.started, "measured_elapsed_seconds": elapsed,
              "successful_queries_per_second": counts["completed_queries"] / elapsed if elapsed else None,
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
        destination = state.report / ("locust_" + suffix + ".csv")
        with destination.open("w", encoding="utf-8", newline="") as output:
            render(csv.writer(output))
    write_json(state.report / "summary.json", result)
    environment.process_exit_code = status_code(status)


class TPCHUser(User):
    wait_time = constant(0)

    def on_start(self):
        state = STATE
        state.started_users += 1
        state.active_users += 1
        self.finished_work = False
        self.counter = PageCountingTransport(state.config.get("ca_file")) if state.config["format"] == "arrow" else None
        self.client = client_for(state.config, transport=self.counter)
        self.rounds = ((warmup, plan) for warmup, rounds in ((True, state.config["warmup"]), (False, state.config["repeat"])) for _ in range(rounds) for plan in state.config["plans"])

    def on_stop(self):
        if hasattr(self, "client"): self.client.close()
        if self.finished_work: STATE.finished_users += 1
        STATE.user_finished()

    @task
    def query(self):
        state = STATE
        try:
            warmup, plan = next(self.rounds)
        except StopIteration:
            self.finished_work = True
            raise StopUser()
        if not warmup and state.measurement_started is None:
            state.measurement_started = time.perf_counter()
        progress, code, cancelled = {}, None, False
        try:
            execute_query(self.client, plan, mode=state.config["mode"], format=state.config["format"], consume=state.config["consume"], timeout=state.config["timeout"], page_counter=self.counter, expected=state.config["reference_results"].get(str(plan["query_id"])), emit=None if warmup else self.environment.events.request.fire, progress=progress)
        except Exception as error:
            code = error_code(error)
        except BaseException:
            cancelled = True
            raise
        finally:
            state.record(progress, warmup, code, cancelled)
            if not warmup:
                state.measurement_ended = time.perf_counter()
                self.environment.events.request.fire(request_type=failure_kind(code) if code else "ERROR" if cancelled else "SDK", name=f"Q{plan['query_id']}/logical", response_time=progress.get("logical_latency_ms", 0), response_length=sum(s["arrow_wire_bytes"] for s in progress.get("stages", [])), exception=RuntimeError(code or "CANCELLED") if code or cancelled else None)
