#!/usr/bin/env python3

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRS = ("public", "core", "apps", "tests", "probes", "examples", "benchmarks")
STANDALONE_SOURCE_DIRS = ("probes/build_install", "probes/toolchain")
HEADERS = {".h", ".hh", ".hpp", ".hxx", ".ipp", ".tpp"}
TRANSLATION_UNITS = {".c", ".cc", ".cpp", ".cxx"}
IO_SOURCE_DIRS = ("core/io", "core/storage", "tests/unit/io", "tests/unit/storage",
                  *STANDALONE_SOURCE_DIRS)
IO_SOURCE_FILES = {"public/storage/schema.h", "tests/unit/core_test.cpp",
                   "tests/support/fault_fs.h", "tests/support/fault_fs.cpp",
                   "tests/support/durability_trace.h", "tests/support/durability_trace.cpp"}


def llvm_tool(name):
    override = os.environ.get("GLAMDB_" + name.upper().replace("-", "_"))
    executable = override or shutil.which(name + "-18") or shutil.which(name)
    if not executable:
        raise RuntimeError(f"{name} 18 is required")
    result = subprocess.run(
        [executable, "--version"], text=True, capture_output=True, check=True
    )
    match = re.search(r"\bversion\s+(\d+)\.", result.stdout + result.stderr)
    if not match or match.group(1) != "18":
        raise RuntimeError(f"{name} must be LLVM 18: {result.stdout.strip()}")
    return executable


def source_files(io_only=False):
    files = []
    for directory in SOURCE_DIRS:
        for path in (ROOT / directory).rglob("*"):
            if path.is_file() and path.suffix in HEADERS | TRANSLATION_UNITS:
                if not {"generated", "third_party"}.intersection(path.relative_to(ROOT).parts):
                    relative = path.relative_to(ROOT)
                    if (not io_only or relative.as_posix() in IO_SOURCE_FILES or
                            any(relative.is_relative_to(directory) for directory in IO_SOURCE_DIRS)):
                        files.append(path)
    return sorted(files)


def header_filter(root):
    return "^" + re.escape(str(root)) + "/(" + "|".join(SOURCE_DIRS) + ")/"


def format_sources(write, io_only=False):
    executable = llvm_tool("clang-format")
    files = source_files(io_only)
    if not files:
        raise RuntimeError("No project sources found")
    options = ["-i"] if write else ["--dry-run", "--Werror"]
    for offset in range(0, len(files), 100):
        subprocess.run(
            [executable, *options, *map(str, files[offset:offset + 100])], check=True
        )
    print(f"{'Formatted' if write else 'Checked formatting of'} {len(files)} file(s)")


def tidy_sources(build_dir, excluded_dirs=(), io_only=False, jobs=2):
    if jobs < 1:
        raise ValueError("Tidy jobs must be positive")
    executable = llvm_tool("clang-tidy")
    database = build_dir / "compile_commands.json"
    if not database.is_file():
        raise RuntimeError(f"Missing {database}; configure the project first")
    commands = json.loads(database.read_text())
    compiled = {
        (Path(entry["directory"]) / entry["file"]).resolve()
        for entry in commands
    }
    exclusions = []
    for directory in (*STANDALONE_SOURCE_DIRS, *excluded_dirs):
        path = (ROOT / directory).resolve()
        if not path.is_relative_to(ROOT.resolve()) or path == ROOT.resolve():
            raise ValueError("Tidy exclusions must be project subdirectories")
        exclusions.append(path)
    units = [path for path in source_files(io_only) if path.suffix in TRANSLATION_UNITS
             and not any(path.resolve().is_relative_to(directory) for directory in exclusions)]
    if not units:
        raise RuntimeError("No project translation units found")
    missing = [str(path.relative_to(ROOT)) for path in units if path.resolve() not in compiled]
    if missing:
        raise RuntimeError("Missing compile commands for: " + ", ".join(missing))
    def check(path):
        started = time.monotonic()
        result = subprocess.run(
            [executable, str(path), "-p", str(build_dir),
             "--config-file=" + str(ROOT / ".clang-tidy"),
             "--header-filter=" + header_filter(ROOT), "--warnings-as-errors=*"],
            text=True, capture_output=True,
        )
        return path, result, time.monotonic() - started

    failures = []
    print(f"Checking {len(units)} translation unit(s) with {jobs} worker(s)", flush=True)
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [executor.submit(check, path) for path in units]
        for completed, future in enumerate(as_completed(futures), 1):
            path, result, elapsed = future.result()
            # Group each file's diagnostics so concurrent processes cannot mix them.
            print(f"[{completed}/{len(units)}] {path.relative_to(ROOT)} ({elapsed:.1f}s)", flush=True)
            print(result.stdout, end="", flush=True)
            print(result.stderr, end="", file=sys.stderr, flush=True)
            if result.returncode:
                failures.append(str(path.relative_to(ROOT)))
    if failures:
        raise RuntimeError("clang-tidy failed for: " + ", ".join(failures))
    print(f"Checked {len(units)} translation unit(s)")


def negative_checks(build_dir):
    formatter = llvm_tool("clang-format")
    tidy = llvm_tool("clang-tidy")
    build_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="quality-negative-", dir=build_dir) as temporary:
        root = Path(temporary)
        (root / "core").mkdir()
        shutil.copyfile(ROOT / ".clang-format", root / ".clang-format")
        source = root / "main.cpp"
        source.write_text("int main(){return 0;}\n")
        result = subprocess.run(
            [formatter, "--dry-run", "--Werror", str(source)],
            text=True, capture_output=True,
        )
        if result.returncode == 0 or "-Wclang-format-violations" not in result.stderr:
            raise RuntimeError("Format negative check failed:\n" + result.stdout + result.stderr)

        header = root / "core" / "bad.h"
        header.write_text(
            "#pragma once\n"
            "inline int Dereference() {\n"
            "    int *value = nullptr;\n"
            "    return *value;\n"
            "}\n"
        )
        source.write_text('#include "core/bad.h"\nint main() { return Dereference(); }\n')
        result = subprocess.run(
            [tidy, str(source), "--config-file=" + str(ROOT / ".clang-tidy"),
             "--header-filter=" + header_filter(root), "--warnings-as-errors=*",
             "--", "-std=c++23", "-I" + str(root)],
            text=True, capture_output=True,
        )
        diagnostics = result.stdout + result.stderr
        if (result.returncode == 0 or "clang-analyzer-core.NullDereference" not in diagnostics
                or str(header) not in diagnostics):
            raise RuntimeError("Header analyzer negative check failed:\n" + diagnostics)
    print("Intentional formatting and header analyzer violations were rejected")


def main():
    parser = argparse.ArgumentParser(description="Run LLVM 18 checks on project sources.")
    parser.add_argument("command", choices=("format", "format-check", "tidy-check", "quality-negative-check"))
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build" / "local")
    parser.add_argument("--io-only", action="store_true", help="Check only standalone IO/storage sources")
    parser.add_argument("--exclude-dir", action="append", default=[],
                        help="Exclude a disabled component subtree from tidy only")
    parser.add_argument("--jobs", type=int, default=os.environ.get("GLAMDB_TIDY_JOBS", "2"),
                        help="Concurrent clang-tidy processes (default: GLAMDB_TIDY_JOBS or 2)")
    args = parser.parse_args()
    try:
        if args.command in ("format", "format-check"):
            format_sources(args.command == "format", args.io_only)
        elif args.command == "tidy-check":
            tidy_sources(args.build_dir.resolve(), args.exclude_dir, args.io_only, args.jobs)
        else:
            negative_checks(args.build_dir.resolve())
    except subprocess.CalledProcessError as error:
        print(f"Command failed with exit code {error.returncode}", file=sys.stderr)
        return error.returncode if error.returncode > 0 else 1
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
