#!/usr/bin/env python3
"""Start task-owned native Loki/Alloy processes; validate ingestion and redaction."""
import argparse
import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

import smoke
from glamdb_logging import StructuredLogger

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_ready(url, process, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Logging service exited before becoming ready")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.25)
    raise TimeoutError("Logging service did not become ready")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loki-binary", required=True, type=Path)
    parser.add_argument("--alloy-binary", required=True, type=Path)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--exercise-outage", action="store_true",
                        help="restart the owned collector; stop the owned Loki, verify retry exhaustion and recover")
    parser.add_argument("--wal-disk-threshold", type=float, default=0.9,
                        help="test-only override for an already full host filesystem; production remains 0.9")
    parser.add_argument("--output-dir", default=ROOT / "artifacts/logging-e2e", type=Path)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    processes, streams = [], []
    with tempfile.TemporaryDirectory(prefix="glamdb-logs-e2e-", dir=output) as workspace:
        work = Path(workspace)
        logs = work / "logs"
        logs.mkdir(mode=0o700)
        (logs / "glamdb.log").touch(mode=0o600)
        config = work / "alloy-config"
        config.mkdir()
        (config / "config.alloy").write_text((ROOT / "deploy/logging/config.alloy").read_text())
        (config / "runtime-fixture.alloy").write_text('''
loki.source.file "runtime_fixture" {
  targets = [{__path__ = sys.env("GLAMDB_TEST_LOG_DIRECTORY") + "/glamdb.log",
              service = "glamdb", environment = "e2e"}]
  tail_from_end = false
  forward_to = [loki.process.application.receiver]
}
''')
        http_port, grpc_port, alloy_port = free_port(), free_port(), free_port()
        env = os.environ.copy()
        env.update({"LOKI_LISTEN_ADDRESS": "127.0.0.1", "LOKI_HTTP_PORT": str(http_port),
                    "LOKI_GRPC_PORT": str(grpc_port), "LOKI_DATA_DIR": str(work / "loki"),
                    "GLAMDB_TEST_LOG_DIRECTORY": str(logs), "GLAMDB_ENVIRONMENT": "e2e",
                    "LOKI_PUSH_URL": f"http://127.0.0.1:{http_port}/loki/api/v1/push"})
        try:
            commands = [
                ("loki", [str(args.loki_binary.resolve()), "-config.file=" + str(ROOT / "deploy/logging/loki.yaml"),
                          "-config.expand-env=true", "-ingester.wal-disk-full-threshold=" + str(args.wal_disk_threshold)]),
                ("alloy", [str(args.alloy_binary.resolve()), "run",
                           "--server.http.listen-addr=127.0.0.1:" + str(alloy_port),
                           "--storage.path=" + str(work / "alloy"), str(config)]),
            ]
            for name, command in commands:
                stream = (output / f"{name}.log").open("w")
                streams.append(stream)
                processes.append(subprocess.Popen(command, env=env, stdout=stream, stderr=subprocess.STDOUT))
            base_url = f"http://127.0.0.1:{http_port}"
            wait_ready(base_url + "/ready", processes[0])
            wait_ready(f"http://127.0.0.1:{alloy_port}/-/ready", processes[1])
            result = smoke.run(base_url, logs, args.probe)
            if args.exercise_outage:
                processes[1].terminate()
                try:
                    processes[1].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    processes[1].kill()
                    processes[1].wait(timeout=5)
                processes[1] = subprocess.Popen(commands[1][1], env=env, stdout=streams[1], stderr=subprocess.STDOUT)
                wait_ready(f"http://127.0.0.1:{alloy_port}/-/ready", processes[1])
                collector_recovered = smoke.run(base_url, logs, args.probe)
                result["collector_restart_status"] = collector_recovered["status"]
                processes[0].terminate()
                try:
                    processes[0].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    processes[0].kill()
                    processes[0].wait(timeout=5)
                with (logs / "glamdb.log").open("a") as runtime_log, contextlib.redirect_stderr(runtime_log):
                    emitter = StructuredLogger(sample_every=1)
                    for _ in range(32):
                        emitter.emit("storage_failed", operation="append", outcome="error", status_code=503)
                    emitter.close()
                dropped = 0
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    with urlopen(f"http://127.0.0.1:{alloy_port}/metrics", timeout=5) as response:
                        metrics = response.read().decode()
                    dropped = sum(float(line.rsplit(" ", 1)[1]) for line in metrics.splitlines()
                                  if line.startswith("loki_write_dropped_entries_total{"))
                    if dropped > 0:
                        break
                    time.sleep(0.25)
                assert dropped > 0, "Collector did not count retry exhaustion while Loki was unavailable"
                (output / "alloy-outage.prom").write_text(metrics)
                processes[0] = subprocess.Popen(commands[0][1], env=env, stdout=streams[0], stderr=subprocess.STDOUT)
                wait_ready(base_url + "/ready", processes[0])
                recovered = smoke.run(base_url, logs, args.probe)
                result["outage_dropped_entries"] = int(dropped)
                result["post_outage_status"] = recovered["status"]
            for name, port in (("loki", http_port), ("alloy", alloy_port)):
                with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
                    (output / f"{name}.prom").write_bytes(response.read())
            (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, sort_keys=True))
        except BaseException:
            for name in ("loki", "alloy"):
                path = output / f"{name}.log"
                if path.exists():
                    print(path.read_text()[-6000:], file=sys.stderr)
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            for stream in streams:
                stream.close()


if __name__ == "__main__":
    main()
