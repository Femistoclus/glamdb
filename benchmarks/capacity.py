#!/usr/bin/env python3
"""Measure bounded native ingestion and fresh-process recovery without raising caps."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def campaign(args):
    driver = args.driver.resolve(strict=True)
    if args.trace_syncs and shutil.which("strace") is None:
        raise RuntimeError("--trace-syncs requires strace")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    repository = Path(__file__).resolve().parents[1]

    def git(*arguments):
        return subprocess.run(["git", *arguments], cwd=repository, text=True,
                              capture_output=True, check=True, timeout=10).stdout.strip()

    environment = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "revision": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "platform": platform.platform(), "machine": platform.machine(),
        "python": platform.python_version(),
        "arguments": {key: str(value) if isinstance(value, Path) else value
                      for key, value in vars(args).items()},
        "measurement": "fresh engine/process; OS page cache retained; shared host; no power-loss qualification",
        "io_scope": "successful external-file bytes; attempted external file/directory sync calls; excludes SQLite VFS",
    }
    digest = hashlib.sha256()
    with driver.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1 << 20), b""):
            digest.update(chunk)
    environment["driver_sha256"] = digest.hexdigest()
    cache = driver.parent.parent / "CMakeCache.txt"
    if cache.is_file():
        environment["build"] = [line for line in cache.read_text().splitlines()
                                if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:",
                                                    "GLAMDB_ENABLE_", "CMAKE_PREFIX_PATH:"))]
    if sys.platform.startswith("linux"):
        environment["filesystem"] = subprocess.run(
            ["findmnt", "-T", str(output), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"],
            text=True, capture_output=True, timeout=10).stdout.strip()
    save(output / "environment.json", environment)
    config = {"operations": 4096, "snapshots": 4096,
              "warehouse_bytes": 1 << 40, "attempt_reserve_bytes": 256 << 20}
    if args.profile in ("snapshots", "combined"):
        config["snapshots"] = args.limit
    if args.profile in ("outcomes", "combined"):
        config["operations"] = args.limit
    save(output / "profile.json", config)
    ledger = []
    save(output / "ledger.json", ledger)
    attempted = 0
    segment = 0
    samples = []
    invocations = []
    append_ms = 0.0
    wall_started = time.monotonic()

    def invoke(mode, profile, count=0, name=None):
        nonlocal segment
        segment += 1
        label = name or f"{segment:03d}-{mode}"
        command = [str(driver), mode, str(output / "store"), str(profile),
                   str(output / "ledger.json"), str(count)]
        if args.trace_syncs:
            command = ["strace", "-f", "-c", "-e", "trace=fsync,fdatasync",
                       "-o", str(output / f"{label}.syncs.txt"), *command]
        with (output / "commands.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps({"command": command, "timeout_seconds": args.timeout}) + "\n")
        with (output / f"{label}.stdout.json").open("w", encoding="utf-8") as stdout, \
                (output / f"{label}.stderr.log").open("w", encoding="utf-8") as stderr:
            with subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True) as process:
                try:
                    returncode = process.wait(timeout=args.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                    raise
        if returncode != 0:
            message = (output / f"{label}.stderr.log").read_text(encoding="utf-8").strip()
            raise RuntimeError(f"{label} failed ({returncode}): {message}")
        value = json.loads((output / f"{label}.stdout.json").read_text(encoding="utf-8"))
        invocations.append(value)
        return value

    stop = None
    orphan = False
    while attempted < args.attempts:
        count = min(args.checkpoint, args.attempts - attempted)
        if args.profile == "combined" and not orphan:
            count = min(count, args.limit - 1)
        sample = invoke("segment", output / "profile.json", count)
        if not sample["opened"] or not sample["verified"]:
            raise RuntimeError(f"segment did not open and verify: {sample}")
        ledger.extend(sample.pop("receipts"))
        save(output / "ledger.json", ledger)
        attempted += sample["attempted_appends"]
        append_ms += sample["append_ms"]
        if sample["successful_appends"] != len(ledger):
            raise RuntimeError("driver receipt count differs")
        samples.append(sample)
        with (output / "checkpoints.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps(sample, ensure_ascii=False) + "\n")
        print(f"appends={len(ledger)} attempts={attempted} "
              f"rss_peak_mib={sample['rss_peak_bytes'] / (1 << 20):.1f} "
              f"reopen_ms={sample['open_ms']:.1f}", flush=True)
        stop = sample["stop"]
        if stop is not None:
            break
        if not sample["attempted_appends"]:
            raise RuntimeError("driver made no progress")
        if args.profile == "combined" and not orphan and ledger:
            pressure = invoke("pressure", output / "profile.json", name="orphan-pressure")
            orphan = pressure.get("orphan_pressure_verified") and pressure.get("verified")
            if not orphan:
                raise RuntimeError("combined physical pressure did not verify")

    reopened = invoke("verify", output / "profile.json", name="same-profile-reopen")
    if not reopened.get("opened") or not reopened.get("verified"):
        raise RuntimeError(f"same-profile reopen failed: {reopened}")
    lowered = []
    for key, value in (("operations", len(ledger) - 1),
                       ("snapshots", len(ledger) - 1),
                       ("warehouse_bytes", reopened["usage"]["warehouse_physical_bytes"] - 1)):
        if value <= 0:
            continue
        lower = dict(config)
        lower[key] = value
        path = output / f"lowered-{key}.json"
        save(path, lower)
        rejected = invoke("verify", path, name=f"lowered-{key}-reopen")
        expected_error = {"operations": "CONFIG_TOO_SMALL", "snapshots": "CORRUPTION",
                          "warehouse_bytes": "RESOURCE_EXHAUSTED"}[key]
        if rejected.get("opened") or rejected.get("error") != expected_error:
            raise RuntimeError(f"lowered {key} did not fail closed: {rejected}")
        lowered.append(key)
    final = invoke("verify", output / "profile.json", name="final-original-profile-reopen")
    if not final.get("opened") or not final.get("verified"):
        raise RuntimeError("original profile did not reopen after lowered-limit checks")
    io = {key: sum(sample["io"][key] for sample in samples) for key in samples[0]["io"]}
    process_io = {key: sum(sample["process_io"][key] for sample in samples)
                  for key in samples[0]["process_io"]}
    summary = {
        "stop_reason": "backpressure" if stop is not None else "attempt_budget",
        "stop": stop, "attempt_budget": args.attempts, "attempted_appends": attempted,
        "successful_appends": len(ledger), "rows": len(ledger) * 1000,
        "receipt_count": len(ledger), "same_profile_reopen": True,
        "lowered_limits_rejected": lowered,
        "retained_slice_verified": final["retained_slice_verified"],
        "orphan_pressure_verified": bool(orphan),
        "append_seconds": append_ms / 1000,
        "append_rows_per_second": len(ledger) * 1000 / (append_ms / 1000),
        "wall_seconds": time.monotonic() - wall_started,
        "rss_peak_bytes": max(value["rss_peak_bytes"] for value in invocations),
        "cold_process_reopen_ms": reopened["open_ms"],
        "retry_us": reopened["retry_us"], "io": io, "process_io": process_io, "usage": final["usage"],
        "qualification": "measurement only; OS cache retained; physical power-loss NOT RUN",
    }
    save(output / "summary.json", summary)
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, default=Path("build/release/benchmarks/glamdb_capacity_bench"))
    parser.add_argument("--output", required=True, type=Path, help="new directory; preserved for inspection")
    parser.add_argument("--profile", choices=("default", "snapshots", "outcomes", "combined"), default="default")
    parser.add_argument("--limit", type=positive, default=8, help="lowered boundary profile cap, at most 4096")
    parser.add_argument("--attempts", type=positive, default=5000)
    parser.add_argument("--checkpoint", type=positive, default=100)
    parser.add_argument("--timeout", type=positive, default=3600, help="seconds per native subprocess")
    parser.add_argument("--trace-syncs", action="store_true", help="strace sync calls including SQLite; adds tracing overhead")
    args = parser.parse_args()
    if args.limit > 4096 or args.attempts > 5000 or args.checkpoint > 5000:
        parser.error("caps may not exceed the default 4096 bindings/history or 5000 attempts")
    if args.profile == "combined" and args.limit < 2:
        parser.error("combined pressure needs a limit of at least 2 for logical headroom")
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        campaign(args)
    except KeyboardInterrupt:
        print("capacity campaign interrupted; evidence preserved", file=sys.stderr)
        return 130
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"capacity campaign failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
