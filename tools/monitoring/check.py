#!/usr/bin/env python3
"""Focused library, exposition, PromQL and optional official chart checks."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]


def run(command, **kwargs):
    print("+ " + " ".join(str(part) for part in command), flush=True)
    return subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", default=shutil.which("promtool"))
    parser.add_argument("--cxx", default=shutil.which("c++"))
    parser.add_argument("--prefix", type=Path, default=ROOT / ".deps/install")
    parser.add_argument("--helm")
    parser.add_argument("--chart", type=Path)
    args = parser.parse_args()
    if not args.promtool or not args.cxx:
        parser.error("verified promtool and C++23 compiler required")
    if bool(args.helm) != bool(args.chart):
        parser.error("--helm and --chart must be supplied together")
    prefix = args.prefix.resolve()
    core = prefix / "lib/libprometheus-cpp-core.a"
    pull = prefix / "lib/libprometheus-cpp-pull.a"
    if not core.is_file() or not pull.is_file():
        parser.error("build the locked prometheus-cpp core/pull installation first (--prefix)")
    run([sys.executable, "-m", "unittest", "discover", "-s", "tools/monitoring/tests", "-v"])
    run([args.promtool, "check", "config", "--syntax-only", "deploy/monitoring/prometheus/prometheus.yml",
         "deploy/monitoring/prometheus/prometheus-alerting.yml"])
    run([args.promtool, "check", "rules", "deploy/monitoring/prometheus/rules/recording.yml"])
    run([args.promtool, "test", "rules", "tools/monitoring/tests/rules.test.yml"])
    with tempfile.TemporaryDirectory(prefix="glamdb-monitoring-check-") as directory:
        temporary = Path(directory)
        flags = [args.cxx, "-std=c++23", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-pthread",
                 "-fsanitize=address,undefined", "-I.", "-Ipublic", "-I" + str(prefix / "include"),
                 "core/telemetry/metrics.cpp"]
        binary = temporary / "native-metrics-test"
        run([*flags, "tools/monitoring/tests/native_metrics_test.cpp", str(core), "-o", str(binary)])
        result = run([str(binary)], capture_output=True)
        # A new process must reset the process-wide domain registry.
        run([str(binary)], capture_output=True)
        run([args.promtool, "check", "metrics"], input=result.stdout)
        samples = [line for line in result.stdout.decode().splitlines() if line and not line.startswith("#")]
        print(f"Native contract: {len(samples)} fixed series, {len(result.stdout)} bytes; component only")
        endpoint = temporary / "native-metrics-endpoint"
        run([*flags, "tools/monitoring/tests/native_metrics_endpoint.cpp", str(pull), str(core), "-o", str(endpoint)])
        with subprocess.Popen([str(endpoint)], cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True) as process:
            try:
                port = int(process.stdout.readline())
                url = "http://127.0.0.1:" + str(port) + "/metrics"
                with urlopen(url, timeout=10) as response:
                    text = response.read()
                    assert response.status == 200
                    assert b"glamdb_operation_duration_seconds_sum" in text
                run([args.promtool, "check", "metrics"], input=text)
            finally:
                process.communicate("stop\n", timeout=10)
            if process.returncode:
                raise RuntimeError("library Exposer teardown failed")
        try:
            urlopen(url, timeout=1)
        except OSError:
            pass
        else:
            raise RuntimeError("endpoint still listens after application teardown")
        queries = []
        for dashboard_path in sorted((ROOT / "deploy/monitoring/grafana/dashboards").glob("*.json")):
            dashboard = json.loads(dashboard_path.read_text())
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    source = target.get("datasource", panel.get("datasource", {}))
                    if isinstance(source, dict) and source.get("type") == "loki":
                        continue
                    expression = target["expr"].replace("$__rate_interval", "5m")
                    expression = re.sub(r"\$[a-zA-Z_]+", ".*", expression)
                    queries.append({"record": "glamdb_dashboard_check_" + str(len(queries)), "expr": expression})
        dashboard_rules = temporary / "dashboard-queries.yml"
        dashboard_rules.write_text(json.dumps({"groups": [{"name": "dashboard-syntax", "rules": queries}]}))
        run([args.promtool, "check", "rules", str(dashboard_rules)])
        if args.helm:
            lock = json.loads((ROOT / "deploy/monitoring/versions.lock.json").read_text())["charts"]["kube-prometheus-stack"]
            if hashlib.sha256(args.chart.read_bytes()).hexdigest() != lock["sha256"]:
                raise RuntimeError("official chart checksum mismatch")
            run([args.helm, "lint", str(args.chart), "-f", "deploy/monitoring/kubernetes/values.yml", "--strict"])
            with (temporary / "kubernetes.yml").open("w") as output:
                run([args.helm, "template", "monitoring", str(args.chart), "--namespace", "monitoring",
                     "--kube-version", "1.36.0", "-f", "deploy/monitoring/kubernetes/values.yml"], stdout=output)
            rendered = (temporary / "kubernetes.yml").read_text()
            if "insecureSkipVerify: true" in rendered:
                raise RuntimeError("chart renders a TLS verification bypass")
            print("Official chart rendered; this is not cluster/runtime qualification")


if __name__ == "__main__":
    main()
