"""Fresh process: gevent patches sockets before the SDK and HTTPX are loaded."""

from gevent import monkey

monkey.patch_all()

import json
import os
from pathlib import Path
import subprocess
import sys
import time


def spawn_workers(config, run):
    """Start `processes` fresh worker interpreters for a local master.

    Fresh interpreters rather than Locust's fork-based --processes: forked
    children are not safe with macOS system frameworks (the proxy lookup in
    HTTPX crashes after fork). Each worker joins through the ordinary
    distributed path and owns its journal and worker report.
    """
    workers = []
    for index in range(config.get("processes") or 0):
        worker = dict(config, master=False, worker=True, processes=None)
        path = run / ("launch-process-" + str(index) + ".json")
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(worker), encoding="utf-8")
        temporary.replace(path)
        workers.append(
            subprocess.Popen(
                [sys.executable, "-m", "glamdb_workload.load", str(path)],
                stdin=subprocess.DEVNULL,
            )
        )
    return workers


def reap_workers(workers, grace):
    """Workers quit after the master's final report; never leave orphans."""
    until = time.monotonic() + grace
    for process in workers:
        try:
            process.wait(max(0.0, until - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.terminate()
    for process in workers:
        try:
            process.wait(5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    config_path = Path(sys.argv[1]).resolve()
    config = json.loads(config_path.read_bytes())
    os.environ["GLAMDB_WORKLOAD_CONFIG"] = str(config_path)
    run = Path(config["report"])
    options = [
        "locust",
        "--locustfile",
        str(Path(__file__).with_name("scenarios.py")),
        "--headless",
        "--users",
        str(config["users"]),
        "--spawn-rate",
        str(config["spawn_rate"]),
        "--stop-timeout",
        str(config["stop_timeout"]),
        "--only-summary",
        "--loglevel",
        "WARNING",
        "--exit-code-on-error",
        "1",
    ]
    if config.get("worker"):
        options += [
            "--worker",
            "--master-host",
            config["master_host"],
            "--master-port",
            str(config["master_port"]),
        ]
    else:
        options += [
            "--run-time",
            config["duration"],
            "--csv",
            str(run / "locust"),
            "--csv-full-history",
            "--html",
            str(run / "locust.html"),
            "--json-file",
            str(run / "locust"),
        ]
        if config.get("master"):
            options += [
                "--master",
                "--master-bind-host",
                config["master_host"],
                "--master-bind-port",
                str(config["master_port"]),
                "--expect-workers",
                str(config["expect_workers"]),
                "--expect-workers-max-wait",
                "30",
            ]
    sys.argv = options
    from locust.main import main as locust_main

    workers = spawn_workers(config, run) if not config.get("worker") else []
    try:
        locust_main()
    finally:
        reap_workers(workers, config["stop_timeout"] + 5)


if __name__ == "__main__":
    main()
