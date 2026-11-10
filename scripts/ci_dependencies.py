#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
INPUTS = ("CMakeLists.txt", "CMakePresets.json", ".gitmodules", "third_party/lock.json",
          ".github/workflows/cpp-quality.yml", "scripts/ci_dependencies.py")
FLAGS = ("CC", "CXX", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS",
         "ARROW_THRIFT_URL", "CMAKE_TOOLCHAIN_FILE")


def cache_key(root):
    root = root.resolve()

    def output(command):
        result = subprocess.run(command, cwd=root, check=True, capture_output=True,
                                text=True, timeout=120)
        return result.stdout + result.stderr

    output(["cmake", "-P", str(root / "cmake/dependencies/VerifySubmodules.cmake")])
    paths = [root / name for name in INPUTS]
    paths.extend(path for path in (root / "cmake/dependencies").rglob("*") if path.is_file())
    inputs = {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(paths)}
    tools = {"cmake": output(["cmake", "--version"]),
             "ninja": output(["ninja", "--version"]),
             "packages": sorted(output(["dpkg-query", "--show",
                                        "--showformat=${binary:Package}=${Version}\n"]).splitlines()),
             "submodules": output(["git", "submodule", "status", "--recursive"])}
    for variable in ("CC", "CXX"):
        compiler = shlex.split(os.environ[variable])
        if not compiler:
            raise ValueError(f"{variable} must identify the dependency compiler")
        tools[variable] = [output([*compiler, option]) for option in ("--version", "-dumpmachine")]
    identity = {"system": platform.system(), "arch": platform.machine(), "prefix": str(root / ".deps/install"),
                "inputs": inputs, "tools": tools,
                "flags": {name: os.environ.get(name, "") for name in FLAGS}}
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return "deps-v1-" + hashlib.sha256(encoded).hexdigest()


def prepare(root, cache_hit):
    if cache_hit == "true":
        print("Reusing the exact dependency cache")
        return
    for path in (root / ".deps/install", root / "build/deps-local"):
        if path.exists():
            shutil.rmtree(path)
    subprocess.run(["cmake", "--preset", "deps-local", "-DGLAMDB_DEPENDENCY_JOBS=2"],
                   cwd=root, check=True)
    subprocess.run(["cmake", "--build", "--preset", "deps-local", "--parallel", "2"],
                   cwd=root, check=True)


def main():
    parser = argparse.ArgumentParser(description="Prepare the pinned CI dependency installation.")
    parser.add_argument("command", choices=("key", "build"))
    args = parser.parse_args()
    try:
        if args.command == "key":
            print(cache_key(ROOT))
        else:
            prepare(ROOT, os.environ.get("CACHE_HIT", ""))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"Dependency preparation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
