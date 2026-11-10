#!/usr/bin/env python3
"""Run isolated P-IO cases; this is not power-loss qualification."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.qualification.environment import run_command
from probes.io.verify_trace import FOOTERS, footer_ordinals, inside, require, verify_receipt, verify_strace


CASES = ["success", *[kind + "-" + failure for kind in ("data", "manifest", "manifest-list", "metadata")
                     for failure in ("write", "short", "sync-file", "close", "sync-dir")],
         *FOOTERS, "parquet-repeat-close", "avro-repeat-close", "avro-destructor", "outside-attempt",
         "exclusive-collision", "write-eintr", "write-zero", "create-dir-sync", "flush-mask"]


def write_json(path, contents):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(contents, stream, indent=2, sort_keys=True)
        stream.write("\n")


def execute(argv, directory, timeout):
    execution = run_command(argv, timeout=timeout, output_limit=1024 * 1024)
    for name in ("stdout", "stderr"):
        with (directory / (name + ".log")).open("x", encoding="utf-8") as stream:
            stream.write(execution.pop(name))
    write_json(directory / "process.json", execution)
    return execution


def run_suite(probe, stores, output, timeout, trace=False):
    probe = Path(probe).resolve(strict=True)
    stores, output = Path(stores).resolve(), Path(output).resolve()
    require(probe.is_file(), "probe must be an existing executable file")
    require(not inside(stores, output) and not inside(output, stores), "stores and receipts must be disjoint")
    require(not stores.exists() and not output.exists(), "stores and output directories must be new")
    require(stores.parent.is_dir() and output.parent.is_dir(), "store/output parent directories must exist")
    require(math.isfinite(timeout) and timeout > 0, "timeout must be finite and positive")
    output.mkdir(mode=0o700)
    stores.mkdir(mode=0o700)
    summary = {"schema_version": 1, "probe": str(probe), "stores": str(stores),
               "started_at_utc": datetime.now(timezone.utc).isoformat(), "power_durability": "NOT_VALIDATED",
               "case_checks": "FAIL", "native_open_audit": "NOT_RUN", "p_io": "FAIL", "cases": []}
    try:
        listing = output / "list-cases"
        listing.mkdir()
        execution = execute([str(probe), "--list-cases"], listing, timeout)
        require(execution["status"] == "ok" and execution["exit_code"] == 0, "--list-cases process failed")
        cases = json.loads((listing / "stdout.log").read_text())
        require(isinstance(cases, list) and all(isinstance(case, str) for case in cases) and
                len(cases) == len(CASES) and set(cases) == set(CASES), "probe case list differs from the required 33 cases")
        ordinals = {}
        for case in CASES:
            case_output = output / case
            case_output.mkdir()
            native = case_output / "native"
            native.mkdir()
            root = stores / case
            command = [str(probe), "--case", case, "--root", str(root), "--receipt-dir", str(native)]
            if case in FOOTERS:
                require(case in ordinals, "missing successful footer calibration")
                command.extend(["--write-ordinal", str(ordinals[case])])
            if case == "success" and trace:
                summary["native_open_audit"] = "FAIL"
                summary["strace_leak_sanitizer"] = "disabled only for the ptraced success process; incompatible with ptrace"
                command = ["env", "ASAN_OPTIONS=" + os.environ.get("ASAN_OPTIONS", "") + ":detect_leaks=0",
                           "strace", "-ff", "-yy", "-ttt", "-s", "4096", "-e", "trace=%file,%desc,io_submit",
                           "-o", str(case_output / "strace"), *command]
            case_result = {"case": case, "status": "FAIL"}
            summary["cases"].append(case_result)
            try:
                execution = execute(command, case_output, timeout)
                require(execution["status"] == "ok" and execution["exit_code"] == 0,
                        f"case process failed: {execution['status']} / {execution['exit_code']}")
                _, _, events = verify_receipt(native, root, case)
                if case == "success":
                    ordinals = footer_ordinals(events)
                    summary["footer_write_ordinals"] = ordinals
                    if trace:
                        audit = verify_strace(sorted(case_output.glob("strace.*")), root, events)
                        write_json(case_output / "strace-verification.json", audit)
                        summary["native_open_audit"] = audit["status"]
                case_result["status"] = "PASS"
            except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
                case_result["error"] = str(error)
                if case == "success":
                    break
        if len(summary["cases"]) == len(CASES) and all(case["status"] == "PASS" for case in summary["cases"]):
            summary["case_checks"] = "PASS"
            summary["p_io"] = "INCOMPLETE"
            if summary["native_open_audit"] == "PASS":
                summary["p_io"] = "PASS"
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        summary["error"] = str(error)
    write_json(output / "summary.json", summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", required=True, type=Path)
    parser.add_argument("--stores", required=True, type=Path, help="new disposable stores directory")
    parser.add_argument("--output", required=True, type=Path, help="new evidence directory outside stores")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--strace", action="store_true", help="also audit native opens/writes in the success case")
    arguments = parser.parse_args(argv)
    try:
        summary = run_suite(arguments.probe, arguments.stores, arguments.output, arguments.timeout, arguments.strace)
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["case_checks"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
