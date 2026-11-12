#!/usr/bin/env python3
"""Run the physical Parquet component experiment in disposable scratch."""
import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True,
                        help="absent or empty disposable directory; allow up to80GiB")
    parser.add_argument("--cxx", default="c++")
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    scratch = args.work_dir.resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    if any(scratch.iterdir()):
        parser.error("--work-dir must be absent or empty")
    for name in ["layout_experiment.py", "payload_generator.cpp", "run_matrix.py", "export_results.py"]:
        shutil.copy2(source / name, scratch / name)
    subprocess.run([args.cxx, "-O3", "-std=c++23", "-shared", "-fPIC",
                    "payload_generator.cpp", "-o", "payload_generator.so"], cwd=scratch, check=True)
    subprocess.run([sys.executable, "layout_experiment.py", "env"], cwd=scratch, check=True)
    for layout in ["disjoint", "overlap"]:
        subprocess.run([sys.executable, "layout_experiment.py", "case", "--style", "ordered",
                        "--gib", "16", "--layout", layout, "--repeats", "5",
                        "--scatter-repeats", "1", "--clean"], cwd=scratch, check=True)
    subprocess.run([sys.executable, "run_matrix.py"], cwd=scratch, check=True)
    subprocess.run([sys.executable, "export_results.py"], cwd=scratch, check=True)
    print(f"Evidence: {scratch / 'measurements.json'}")
    print(f"Summary: {scratch / 'summary.csv'}")


if __name__ == "__main__":
    main()
