#!/usr/bin/env python3
"""Install private native Alloy and a dedicated, 1 GiB Podman Loki filesystem.

Requires an already running, explicitly selected rootful Podman machine and the
existing glamdb-observability network. Does not install or invoke Docker.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shlex
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse
from urllib.request import urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy/logging"
LABEL = "org.glamdb.alloy"


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, capture_output=True,
                          **kwargs).stdout


def wait_ready(url):
    deadline = time.monotonic() + 55
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(1)
    raise TimeoutError(f"Not ready: {url}")


def remote_file(source, destination):
    encoded = base64.b64encode(source.read_bytes()).decode()
    return f"printf %s {shlex.quote(encoded)} | base64 -d > {shlex.quote(destination)}\n"


def load_launch_agent(plist):
    domain = f"gui/{os.getuid()}"
    target = f"{domain}/{LABEL}"
    subprocess.run(["launchctl", "bootout", target], capture_output=True)
    deadline = time.monotonic() + 10
    while True:
        # bootout can return before launchd has fully removed the old job.
        present = subprocess.run(["launchctl", "print", target], capture_output=True)
        if present.returncode:
            result = subprocess.run(["launchctl", "bootstrap", domain, str(plist)],
                                    text=True, capture_output=True)
            if result.returncode == 0:
                return
            if result.returncode != 5 or time.monotonic() >= deadline:
                raise subprocess.CalledProcessError(result.returncode, result.args,
                                                    result.stdout, result.stderr)
        if time.monotonic() >= deadline:
            raise TimeoutError("Previous Alloy LaunchAgent did not unload")
        time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machine", required=True)
    parser.add_argument("--connection", required=True)
    parser.add_argument("--alloy-archive", type=Path, required=True)
    parser.add_argument("--runtime-directory", type=Path,
                        default=Path.home() / ".local/share/glamdb/live/logging")
    parser.add_argument("--log-directory", type=Path,
                        default=Path.home() / ".local/share/glamdb/live/logs")
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("The collector installation requires macOS")
    pins = json.loads((DEPLOY / "versions.lock.json").read_text())
    expected = pins["standalone_test_archives"]["alloy-darwin-arm64.zip"]["sha256"]
    digest = hashlib.sha256()
    with args.alloy_archive.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise ValueError("Alloy archive checksum differs from versions.lock.json")

    machine = json.loads(run(["podman", "machine", "inspect", args.machine]))[0]
    connections = json.loads(run(["podman", "system", "connection", "list", "--format=json"]))
    connection = next(item for item in connections if item["Name"] == args.connection)
    uri = urlparse(connection["URI"])
    if (machine["State"] != "running" or uri.hostname != "127.0.0.1"
            or uri.port != machine["SSHConfig"]["Port"] or uri.username != "root"):
        raise ValueError("Connection must select the running machine's rootful socket")
    podman = ["podman", "--connection", args.connection]
    run(podman + ["network", "inspect", "glamdb-observability"])
    image = "docker.io/grafana/loki@" + pins["images"]["grafana/loki"]["digest"]
    run(podman + ["pull", image])

    # Only a newly allocated, task-owned file is formatted. An existing file is
    # checked, never reformatted; an existing unmounted directory must be empty.
    script = """set -eu
umask 077
base=/var/lib/glamdb/logging
image=$base/loki.ext4
data=$base/loki
install -d -m 0750 "$base"
if [ ! -e "$image" ]; then
  if [ -d "$data" ]; then
    test -z "$(find "$data" -mindepth 1 -print -quit)"
  fi
  install -d -m 0000 "$data"
  fallocate -l 1073741824 "$image"
  mkfs.ext4 -q -F -m 0 "$image"
fi
test "$(stat -c %s "$image")" = 1073741824
test "$(blkid -p -s TYPE -o value "$image")" = ext4
install -d -m 0755 /etc/containers/systemd
"""
    script += remote_file(DEPLOY / "loki.yaml", "/var/lib/glamdb/logging/loki.yaml")
    script += remote_file(DEPLOY / "podman/var-lib-glamdb-logging-loki.mount",
                          "/etc/systemd/system/var-lib-glamdb-logging-loki.mount")
    script += remote_file(DEPLOY / "podman/glamdb-logging-loki.container",
                          "/etc/containers/systemd/glamdb-logging-loki.container")
    script += """chmod 0644 /var/lib/glamdb/logging/loki.yaml
systemctl daemon-reload
systemctl enable --now var-lib-glamdb-logging-loki.mount
mountpoint --quiet /var/lib/glamdb/logging/loki
chown 10001:10001 /var/lib/glamdb/logging/loki
chmod 0750 /var/lib/glamdb/logging/loki
systemctl restart glamdb-logging-loki.service
systemctl is-active --quiet glamdb-logging-loki.service
"""
    run(["podman", "machine", "ssh", args.machine, "sudo", "sh", "-s"], input=script)
    wait_ready("http://127.0.0.1:3100/ready")

    runtime = args.runtime_directory.resolve()
    config = runtime / "alloy/config"
    state = runtime / "alloy/state"
    binary = runtime / "bin/alloy"
    for path in (runtime, config, state, binary.parent, args.log_directory):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    replacement = binary.with_name(".alloy.install")
    with zipfile.ZipFile(args.alloy_archive) as archive:
        with archive.open("alloy-darwin-arm64") as source, replacement.open("wb") as target:
            shutil.copyfileobj(source, target)
    replacement.chmod(0o755)
    replacement.replace(binary)
    shutil.copyfile(DEPLOY / "config.alloy", config / "config.alloy")
    shutil.copyfile(DEPLOY / "sources/file.alloy", config / "source.alloy")
    environment = {
        "GLAMDB_LOG_DIRECTORY": str(args.log_directory.resolve()),
        "GLAMDB_ENVIRONMENT": "local",
        "LOKI_PUSH_URL": "http://127.0.0.1:3100/loki/api/v1/push",
        "GOMAXPROCS": "2",
        "GOMEMLIMIT": "192MiB",
    }
    run([str(binary), "validate", str(config)], env=dict(os.environ, **environment))
    plist = Path.home() / "Library/LaunchAgents" / f"{LABEL}.plist"
    plist.parent.mkdir(parents=True, exist_ok=True)
    definition = {
        "Label": LABEL,
        "ProgramArguments": [str(binary), "run", "--disable-reporting",
                             "--server.http.listen-addr=127.0.0.1:12345",
                             "--server.http.enable-pprof=false",
                             "--server.http.disable-support-bundle",
                             f"--storage.path={state}", str(config)],
        "EnvironmentVariables": environment,
        "WorkingDirectory": str(runtime),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ProcessType": "Background",
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
    }
    plist.write_bytes(plistlib.dumps(definition))
    plist.chmod(0o600)
    load_launch_agent(plist)
    wait_ready("http://127.0.0.1:12345/-/ready")
    print(json.dumps({"status": "ready", "loki": "http://127.0.0.1:3100",
                      "alloy": "http://127.0.0.1:12345", "runtime": str(runtime),
                      "source_path": str(args.log_directory.resolve() / "glamdb.jsonl"),
                      "loki_volume_bytes": 1073741824}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stdout, file=sys.stderr)
        print(error.stderr, file=sys.stderr)
        raise SystemExit(error.returncode) from error
