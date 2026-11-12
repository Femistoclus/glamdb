#!/usr/bin/env python3
"""Run reproducible rules and native Alertmanager Telegram loopback checks."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", default=shutil.which("promtool"))
    parser.add_argument("--prometheus", default=shutil.which("prometheus"))
    parser.add_argument("--alertmanager", default=shutil.which("alertmanager"))
    parser.add_argument("--amtool", default=shutil.which("amtool"))
    args = parser.parse_args()
    for name in ("promtool", "prometheus", "alertmanager", "amtool"):
        if not getattr(args, name):
            parser.error("provide --" + name + " (official release binary or PATH)")
    env = {**os.environ, "PROMETHEUS_BIN": str(Path(args.prometheus).resolve()),
           "AMTOOL_BIN": str(Path(args.amtool).resolve()),
           "ALERTMANAGER_BIN": str(Path(args.alertmanager).resolve())}
    commands = [
        [sys.executable, "tools/alerting/generate.py", "--check"],
        [args.promtool, "check", "rules", "deploy/alerting/generated/states.yml"],
        [args.promtool, "test", "rules", "deploy/alerting/tests/states.test.yml"],
        [args.promtool, "check", "config", "deploy/alerting/external-observer/prometheus.yml"],
        [args.promtool, "test", "rules", "deploy/alerting/tests/observer.test.yml"],
        [args.amtool, "check-config", "deploy/alerting/alertmanager.yml"],
        [sys.executable, "-m", "unittest", "discover", "-s", "tools/alerting", "-p", "test_*.py", "-v"],
    ]
    for command in commands:
        subprocess.run(command, cwd=ROOT, env=env, check=True)


if __name__ == "__main__":
    main()
