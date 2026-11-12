#!/usr/bin/env python3
"""Install the pinned official Darwin node exporter from a verified archive."""

import argparse
import hashlib
import os
from pathlib import Path
import platform
import plistlib
import shutil
import subprocess
import tarfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path,
                        default=Path.home() / ".local/share/glamdb/live/collectors")
    parser.add_argument("--no-start", action="store_true")
    args = parser.parse_args()
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        parser.error("this pinned binary requires Darwin arm64")
    expected = "35b246a34af55a8f0c3f11067f17aecf9836cfd4689bde364776b3a4ad3a84d6"
    if hashlib.sha256(args.archive.read_bytes()).hexdigest() != expected:
        parser.error("archive SHA256 differs from the pinned official release")
    runtime = args.runtime_dir.expanduser().resolve()
    binary_dir = runtime / "bin"
    binary_dir.mkdir(parents=True, exist_ok=True)
    prefix = "node_exporter-1.12.1.darwin-arm64/"
    with tarfile.open(args.archive, "r:gz") as archive:
        for name in ("node_exporter", "LICENSE", "NOTICE"):
            member = archive.getmember(prefix + name)
            if not member.isfile():
                parser.error(f"unexpected archive member: {name}")
            installed = binary_dir / name
            temporary = binary_dir / (name + ".install")
            with archive.extractfile(member) as source, temporary.open("wb") as dest:
                shutil.copyfileobj(source, dest)
            temporary.chmod(0o755 if name == "node_exporter" else 0o644)
            temporary.replace(installed)
    label = "org.glamdb.macos-exporter"
    agent = Path.home() / "Library/LaunchAgents" / (label + ".plist")
    agent.parent.mkdir(parents=True, exist_ok=True)
    configuration = {
        "Label": label,
        "ProgramArguments": [str(binary_dir / "node_exporter"),
                             "--web.listen-address=127.0.0.1:9101",
                             "--web.disable-exporter-metrics", "--no-collector.thermal"],
        "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
        "StandardOutPath": str(runtime / "macos-exporter.log"),
        "StandardErrorPath": str(runtime / "macos-exporter.log"),
        "ProcessType": "Background", "WorkingDirectory": str(runtime),
    }
    agent.write_bytes(plistlib.dumps(configuration))
    if not args.no_start:
        domain = f"gui/{os.getuid()}"
        subprocess.run(["launchctl", "bootout", domain + "/" + label],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        for _ in range(50):
            if subprocess.run(["launchctl", "print", domain + "/" + label],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("previous exporter LaunchAgent did not unload")
        subprocess.run(["launchctl", "bootstrap", domain, str(agent)], check=True)
    print(f"Installed node_exporter 1.12.1 at {binary_dir}; private listener 127.0.0.1:9101")


if __name__ == "__main__":
    main()
