"""Fresh process: gevent patches sockets before the SDK and HTTPX are loaded."""

from gevent import monkey

monkey.patch_all()

import json
import os
from pathlib import Path
import sys


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

    locust_main()


if __name__ == "__main__":
    main()
