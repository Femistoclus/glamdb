#!/usr/bin/env python3
"""Serial bounded runner; all commands/exits captured in commands.json."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(__file__).resolve().parent
commands = []
rerun = "--rerun" in sys.argv[1:]
ordered = [("ordered", 4, "overlap"), ("ordered", 4, "disjoint")]
ordered += [(style, size, layout) for style in ["clustered", "random", "prefix", "utf8"]
            for size in [4, 16] for layout in ["disjoint", "overlap"]]
for style, size, layout in ordered:
    case = f"{size}g-{style}-{layout}"
    result_path = root / "results" / f"{case}.json"
    if not rerun and result_path.exists() and "finished_utc" in json.loads(result_path.read_text()):
        continue
    argv = [sys.executable, str(root / "layout_experiment.py"), "case", "--style", style,
            "--gib", str(size), "--layout", layout, "--repeats", "5",
            "--scatter-repeats", "1", "--clean"]
    start = time.time()
    with (root / f"{case}.log").open("w") as log:
        proc = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                              env={**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"})
    commands.append({"argv": argv, "cwd": str(root), "exit_code": proc.returncode,
                     "started_unix_seconds": start, "elapsed_seconds": time.time() - start,
                     "log": str(root / f"{case}.log")})
    (root / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    print(json.dumps({"case": case, "exit_code": proc.returncode,
                      "elapsed_seconds": time.time() - start}), flush=True)
    if proc.returncode:
        raise SystemExit(proc.returncode)
