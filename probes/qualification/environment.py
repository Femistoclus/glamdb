#!/usr/bin/env python3
"""Read-only LINUX-LOCAL-1 preflight; never qualifies power-loss durability."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
MANUAL_CHECKS = [
    "Device firmware, controller and cache configuration; honest flush/FUA behavior",
    "Physical locality of every backing device, including virtual disks and NVMe fabrics",
    "Dedicated disposable volume contains no production data",
    "Catalog and warehouse use this same fixed volume",
    "ACK ledger controller has independent durable storage and power",
    "Actual engine build, loaded libraries, SQLite VFS and connection PRAGMAs",
    "Calibrated shared persistence model and real hardware power-cut campaigns",
]


def run_command(argv, timeout=10, output_limit=1024 * 1024):
    record = {"argv": argv, "status": "error", "exit_code": None,
              "stdout": "", "stderr": ""}
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True,
                                   env={**os.environ, "LC_ALL": "C", "GIT_OPTIONAL_LOCKS": "0"})
    except OSError as error:
        record["stderr"] = str(error)
        return record
    chunks = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    total = 0
    with process, selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                record["status"] = "timeout"
                break
            for key, _ in selector.select(remaining):
                data = os.read(key.fd, min(65536, output_limit - total + 1))
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                chunks[key.data].extend(data[:output_limit - total])
                total += len(data)
                if total > output_limit:
                    record["status"] = "output_limit"
                    break
            if total > output_limit:
                break
        if record["status"] in ("timeout", "output_limit"):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            record["exit_code"] = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            record["status"] = "timeout"
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            record["exit_code"] = process.wait()
        if record["status"] == "error":
            record["status"] = "ok" if record["exit_code"] == 0 else "failed"
    for name, data in chunks.items():
        record[name] = data.decode("utf-8", errors="replace")
    return record


def block_stack(devices, device_id):
    matched = []

    def visit(nodes, ancestors, depth=0):
        if not isinstance(nodes, list) or depth > 32:
            raise ValueError("invalid lsblk children or excessive block-stack depth")
        for node in nodes:
            if not isinstance(node, dict) or not re.fullmatch(r"\d+:\d+", str(node.get("maj:min", ""))):
                raise ValueError("invalid lsblk device identity")
            path = [*ancestors, node]
            if node["maj:min"] == device_id:
                matched.extend(path)
            visit(node.get("children", []), path, depth + 1)

    visit(devices, [])
    unique = {node["maj:min"]: {key: value for key, value in node.items() if key != "children"}
              for node in matched}
    if not unique or not any(node.get("type") == "disk" for node in unique.values()):
        raise ValueError("mount device has no identifiable physical disk ancestry")
    for node in unique.values():
        if not isinstance(node.get("type"), str) or node["type"] not in {
                "disk", "part", "crypt", "lvm", "mpath", "raid0", "raid1", "raid4", "raid5", "raid6", "raid10"}:
            raise ValueError("unsupported block device type")
        if node.get("ro") not in (False, 0) or type(node.get("size")) is not int or node["size"] <= 0:
            raise ValueError("block device is read-only or has unknown capacity")
        if not isinstance(node.get("path"), str) or not node["path"].startswith("/dev/"):
            raise ValueError("invalid block device path")
        transport = node.get("tran")
        if transport is not None and not isinstance(transport, str):
            raise ValueError("invalid block device transport")
        if transport in {"iscsi", "fc", "fcoe", "tcp", "rdma", "nbd"} or node["path"].startswith("/dev/nbd"):
            raise ValueError("network-backed block devices are outside the profile")
    return list(unique.values())


def collect(volume, dedicated, source_root=ROOT, runner=run_command):
    receipt = {
        "schema_version": 1,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "profile": "LINUX-LOCAL-1",
        "scope": "read-only environment preflight",
        "power_durability": "NOT_VALIDATED",
        "campaigns": "NOT_RUN",
        "dedicated_volume_operator_assertion": dedicated,
        "manual_verification_required": MANUAL_CHECKS,
        "commands": [], "observed": {}, "errors": [],
    }
    errors = receipt["errors"]

    def command(argv, decode_json=False):
        result = runner(argv)
        receipt["commands"].append(result)
        if result["status"] != "ok" or result["exit_code"] != 0:
            errors.append(f"{argv[0]} failed: {result['status']}")
            return None
        if decode_json:
            try:
                return json.loads(result["stdout"])
            except (ValueError, TypeError, RecursionError):
                errors.append(f"{argv[0]} returned invalid JSON")
                return None
        return result["stdout"].strip()

    uname = command(["uname", "-s", "-r", "-m"])
    if uname is not None:
        parts = uname.split()
        receipt["observed"]["uname"] = uname
        if len(parts) != 3 or parts[0] != "Linux" or parts[2] != "x86_64":
            errors.append("profile requires Linux x86_64")
    if not dedicated:
        errors.append("operator must assert a dedicated disposable test volume")
    try:
        volume = Path(volume).resolve(strict=True)
        if not volume.is_dir() or volume == Path("/"):
            raise ValueError("volume must be a non-root directory mountpoint")
        volume_stat = volume.stat()
        device_id = f"{os.major(volume_stat.st_dev)}:{os.minor(volume_stat.st_dev)}"
        receipt["observed"]["volume"] = str(volume)
        receipt["observed"]["device_id"] = device_id
        for name, path in (("os_root", Path("/")), ("source_root", source_root)):
            device = path.stat().st_dev
            receipt["observed"][name + "_device_id"] = f"{os.major(device)}:{os.minor(device)}"
            if device == volume_stat.st_dev:
                raise ValueError(f"{name} must be outside the disposable volume")
        mount = command(["findmnt", "--json", "--target", str(volume), "--output",
                         "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN"], decode_json=True)
        if mount is not None:
            entries = mount.get("filesystems") if isinstance(mount, dict) else None
            if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
                raise ValueError("findmnt must identify exactly one mount")
            mount = entries[0]
            receipt["observed"]["mount"] = mount
            if mount.get("target") != str(volume) or mount.get("maj:min") != device_id:
                raise ValueError("volume must be the exact mountpoint with matching device identity")
            if mount.get("fstype") != "ext4" or not isinstance(mount.get("source"), str) or not mount["source"]:
                raise ValueError("profile requires a local ext4 block-device mount")
            if "[" in mount["source"]:
                raise ValueError("bind-mounted subdirectories are not dedicated volumes")
            options = mount.get("options")
            if not isinstance(options, str):
                raise ValueError("missing mount options")
            options = set(options.split(","))
            if "rw" not in options or options & {"ro", "nobarrier", "barrier=0", "data=writeback", "data=journal"}:
                raise ValueError("mount must be rw data=ordered with barriers enabled")
            if "data=ordered" not in options:
                raise ValueError("data=ordered is not evidenced in mount options")
            devices = command(["lsblk", "--json", "--bytes", "--paths", "--tree", "--output",
                               "KNAME,PATH,TYPE,MAJ:MIN,RO,ROTA,SIZE,MODEL,SERIAL,REV,TRAN"], decode_json=True)
            if devices is not None:
                if not isinstance(devices, dict):
                    raise ValueError("invalid lsblk document")
                receipt["observed"]["block_stack"] = block_stack(devices.get("blockdevices"), device_id)
    except (OSError, ValueError) as error:
        errors.append(str(error))

    git = ["git", "-c", "core.fsmonitor=false", "-C", str(source_root)]
    revision = command([*git, "rev-parse", "HEAD"])
    status = command([*git, "status", "--porcelain=v1", "--untracked-files=normal"])
    receipt["source"] = {"revision": revision, "worktree_status": status}
    if revision is not None and not re.fullmatch(r"[0-9a-f]{40,64}", revision):
        errors.append("invalid source revision")
    try:
        with (source_root / "third_party" / "lock.json").open("rb") as lock_file:
            raw_lock = lock_file.read(1024 * 1024 + 1)
        if len(raw_lock) > 1024 * 1024:
            raise ValueError("dependency lock exceeds size bound")
        lock = json.loads(raw_lock)
        if not isinstance(lock, dict) or lock.get("schema_version") != 1:
            raise ValueError("invalid dependency lock")
        receipt["source"]["dependency_lock"] = lock
        receipt["source"]["dependency_lock_sha256"] = hashlib.sha256(raw_lock).hexdigest()
    except (OSError, ValueError, RecursionError) as error:
        errors.append(f"dependency lock: {error}")
    receipt["automatic_checks"] = "COMPATIBLE" if not errors else "INELIGIBLE"
    return receipt


def write_receipt(output, volume, contents):
    volume = Path(volume).resolve(strict=True)
    output = Path(output)
    parent = output.parent.resolve(strict=True)
    if parent == volume or volume in parent.parents:
        raise ValueError("receipt output must be outside the target volume")
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        if os.fstat(parent_fd).st_dev == volume.stat().st_dev:
            raise ValueError("receipt output must be on a different filesystem from the target volume")
        descriptor = os.open(output.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=parent_fd)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(contents)
    finally:
        os.close(parent_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume", required=True, type=Path, help="existing dedicated volume mountpoint")
    parser.add_argument("--dedicated-test-volume", action="store_true", help="assert this volume is disposable and dedicated")
    parser.add_argument("--output", type=Path, help="new receipt outside the target filesystem; default: stdout")
    arguments = parser.parse_args(argv)
    receipt = collect(arguments.volume, arguments.dedicated_test_volume)
    contents = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    try:
        if arguments.output is None:
            sys.stdout.write(contents)
        else:
            write_receipt(arguments.output, arguments.volume, contents)
    except (OSError, ValueError) as error:
        print(f"cannot write receipt: {error}", file=sys.stderr)
        return 2
    return 0 if receipt["automatic_checks"] == "COMPATIBLE" else 1


if __name__ == "__main__":
    sys.exit(main())
