#!/usr/bin/env python3
"""Bounded SDK/native HTTP append timing; no mock backend or automatic retries.

Run with the repository SDK on PYTHONPATH. Each shape uses a fresh native
server/store, two warm-up appends, then --appends measured appends. Dataset
generation precedes timing; SDK preparation and the complete HTTP call are
reported separately. This is a latency comparison, not a concurrency campaign.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import select
import statistics
import subprocess
import tempfile
import time
from uuid import uuid4

from glamdb_sdk import Client, Field, Schema


def peak_rss_kib(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    return None


def dataset(fields, rows, seed):
    cycle = ("int64", "float64", "string", "bool", "timestamp_us_utc")
    specs = [Field("key", "string", False)] + [
        Field(f"f{field}", cycle[(field - 1) % len(cycle)], True)
        for field in range(1, fields)
    ]
    data = []
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    for row in range(rows):
        item = {"key": f"key-{row:08}"}
        for field, spec in enumerate(specs[1:], 1):
            value = seed * 1000003 + row * 131 + field
            item[spec.name] = None if (row + field) % 10 == 0 else {
                "int64": lambda: value - 500,
                "float64": lambda: value * 0.25,
                "string": lambda: f"value-{value % 100000}",
                "bool": lambda: value % 3 == 0,
                "timestamp_us_utc": lambda: epoch + timedelta(microseconds=1700000000000000 + value),
            }[spec.type]()
        data.append(item)
    return Schema(specs), data


def measure(server, fields, rows, appends, parent):
    inputs = [dataset(fields, rows, seed) for seed in range(appends + 2)]
    schema = inputs[0][0]
    token = "append-preparation-local-benchmark"
    with tempfile.TemporaryDirectory(prefix="append-http-", dir=parent) as temporary:
        with tempfile.TemporaryFile(mode="w+") as errors:
            process = subprocess.Popen(
                [str(server), "--data-dir", str(Path(temporary).resolve() / "data"), "--port", "0"],
                env=dict(os.environ, GLAMDB_TOKEN=token), stdout=subprocess.PIPE,
                stderr=errors, text=True,
            )
            try:
                line = bytearray()
                startup_deadline = time.monotonic() + 20
                while b"\n" not in line:
                    remaining = startup_deadline - time.monotonic()
                    if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
                        raise RuntimeError("native server did not report its port")
                    chunk = os.read(process.stdout.fileno(), 4096)
                    if not chunk:
                        raise RuntimeError("native server exited before reporting its port")
                    line.extend(chunk)
                    if len(line) > 65536:
                        raise RuntimeError("native server startup line exceeds 64 KiB")
                line = line.split(b"\n", 1)[0]
                url = f"http://127.0.0.1:{json.loads(line)['port']}"
                with Client(url, token=token) as admin:
                    database = admin.create_database("append_profile")
                with Client(url, token=token, database_id=database["database_id"]) as client:
                    collection = uuid4()
                    created = client.create_collection("events", schema, collection_id=collection)
                    if created.outcome != "committed":
                        raise RuntimeError(created.raw)
                    samples = []
                    first = None
                    for index, (_, data) in enumerate(inputs):
                        start = time.perf_counter_ns()
                        request = client.prepare_append(collection, data, operation_id=uuid4(), schema=schema)
                        prepared = time.perf_counter_ns()
                        outcome = client.append(request, deadline=60)
                        finished = time.perf_counter_ns()
                        if (outcome.outcome != "committed" or
                                outcome.result["payload_hash"] != request.payload_hash or
                                int(outcome.result["rows_appended"]) != rows):
                            raise RuntimeError(outcome.raw)
                        if first is None:
                            first = request, outcome
                        if index >= 2:
                            samples.append({"prepare_ms": (prepared - start) / 1e6,
                                            "http_ms": (finished - prepared) / 1e6,
                                            "total_ms": (finished - start) / 1e6})
                    # Resolve the original operation without creating another
                    # operation or changing its original snapshot precondition.
                    if client.append(first[0], deadline=60).raw != first[1].raw:
                        raise RuntimeError("original receipt changed on exact retry")
                    total = 0
                    with client.scan_batches(collection) as stream:
                        for batch in stream:
                            total += batch.num_rows
                    if total != rows * (appends + 2):
                        raise RuntimeError("scan did not retain every appended row")
                    return {"fields": fields, "rows": rows, "samples": samples,
                            "median_ms": {key: statistics.median(sample[key] for sample in samples)
                                          for key in samples[0]},
                            "server_peak_rss_kib": peak_rss_kib(process.pid),
                            "original_receipt_replayed": True, "scanned_rows": total}
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
                    raise
                if process.returncode != 0:
                    errors.seek(0)
                    raise RuntimeError(f"native server exited {process.returncode}: {errors.read()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--appends", type=int, default=5)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--parent", type=Path)
    args = parser.parse_args()
    if not 1 <= args.appends <= 32 or not 1 <= args.rows <= 10000:
        parser.error("appends must be 1..32 and rows must be 1..10000")
    server = args.server.resolve(strict=True)
    results = [measure(server, fields, args.rows, args.appends, args.parent) for fields in (2, 16, 64)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"mode": "sdk-native-http", "results": results}, indent=2) + "\n")
    for result in results:
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
