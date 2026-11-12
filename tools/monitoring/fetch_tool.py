#!/usr/bin/env python3
"""Fetch a pinned upstream CLI and verify its locked SHA-256 before extraction."""

import argparse
import hashlib
import io
import json
from pathlib import Path
import platform
import tarfile
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", choices=("prometheus", "node-exporter", "kubeconform", "helm"))
    parser.add_argument("--directory", type=Path, default=ROOT / "artifacts/monitoring/bin")
    args = parser.parse_args()
    lock = json.loads((ROOT / "deploy/monitoring/versions.lock.json").read_text())["downloads"][args.tool]
    system = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    target = str(system) + "-" + str(architecture)
    if target not in lock:
        parser.error("no reviewed checksum for " + target + "; use the locked OCI image or add a verified upstream asset pin")
    name = "node_exporter" if args.tool == "node-exporter" else args.tool
    filename = (f"kubeconform-{target}.tar.gz" if name == "kubeconform" else f"{name}-{lock['version']}.{target}.tar.gz")
    if name == "helm":
        filename = f"helm-v{lock['version']}-{target}.tar.gz"
        url = lock["base_url"] + "/" + filename
    else:
        url = lock["release"].replace("/tag/", "/download/") + "/" + filename
    with urlopen(url, timeout=180) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != lock[target]["sha256"]:
        raise SystemExit("upstream archive checksum mismatch")
    expected = {"prometheus", "promtool"} if name == "prometheus" else {name}
    args.directory.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for binary in expected:
            matches = [member for member in archive.getmembers() if member.isfile() and Path(member.name).name == binary]
            if len(matches) != 1:
                raise SystemExit("archive has missing/ambiguous binary " + binary)
            # Extract only regular executable bytes; never trust archive paths or links.
            path = args.directory / binary
            path.write_bytes(archive.extractfile(matches[0]).read())
            path.chmod(0o755)
            print(path)
    print("Verified SHA256 " + digest)


if __name__ == "__main__":
    main()
