import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("qualification_environment", ROOT / "probes/qualification/environment.py")
ENV = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENV)


class QualificationEnvironmentTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb preflight ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.volume = self.root / "volume"
        self.volume.mkdir()
        (self.root / "third_party").mkdir()
        self.lock_bytes = b'{"schema_version": 1, "submodules": []}\n'
        (self.root / "third_party/lock.json").write_bytes(self.lock_bytes)
        device = os.makedev(240, 1)
        self.fixture_device = device
        self.root_on_volume = False
        self.device_id = f"{os.major(device)}:{os.minor(device)}"
        self.mount = {"target": str(self.volume), "source": "UUID=fixture", "fstype": "ext4",
                      "options": "rw,relatime,data=ordered", "maj:min": self.device_id}
        self.partition = {"path": "/dev/sda1", "type": "part", "maj:min": self.device_id,
                          "ro": False, "size": 10_000}
        self.disk = {"path": "/dev/sda", "type": "disk", "maj:min": "9999:9999",
                     "ro": False, "size": 20_000, "model": "fixture", "rev": "1", "tran": "sata",
                     "children": [self.partition]}
        self.responses = {"uname": "Linux 6.8.0 x86_64", "findmnt": {"filesystems": [self.mount]},
                          "lsblk": {"blockdevices": [self.disk]}}
        self.calls = []

    def runner(self, argv):
        self.calls.append(argv)
        response = self.responses.get(argv[0], "a" * 40 if "rev-parse" in argv else "")
        if isinstance(response, dict) and "status" in response:
            return {"argv": argv, "stdout": "", "stderr": "fixture failure", "exit_code": None, **response}
        return {"argv": argv, "status": "ok", "exit_code": 0, "stderr": "",
                "stdout": json.dumps(response) if isinstance(response, (dict, list)) else response}

    def collect(self, volume=None, dedicated=True, source_root=None):
        original_stat = Path.stat

        def fixture_stat(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            if path == self.volume or self.volume in path.parents or (self.root_on_volume and path == Path("/")):
                fields = list(result)
                fields[2] = self.fixture_device
                return os.stat_result(fields)
            return result

        with mock.patch.object(Path, "stat", fixture_stat):
            return ENV.collect(volume or self.volume, dedicated, source_root or self.root, self.runner)

    def assert_ineligible(self, receipt, text):
        self.assertEqual(receipt["automatic_checks"], "INELIGIBLE")
        self.assertTrue(any(text in error for error in receipt["errors"]), receipt["errors"])
        self.assertEqual(receipt["power_durability"], "NOT_VALIDATED")

    def test_compatible_fixture_is_not_power_qualified(self):
        receipt = self.collect()
        self.assertEqual(receipt["automatic_checks"], "COMPATIBLE", receipt["errors"])
        self.assertEqual(receipt["power_durability"], "NOT_VALIDATED")
        self.assertEqual(receipt["campaigns"], "NOT_RUN")
        manual = " ".join(receipt["manual_verification_required"])
        for item in ("flush/FUA", "independent", "SQLite VFS", "firmware", "power-cut"):
            self.assertIn(item, manual)
        self.assertEqual(len(receipt["observed"]["block_stack"]), 2)
        self.assertEqual(receipt["source"]["dependency_lock_sha256"], hashlib.sha256(self.lock_bytes).hexdigest())
        self.assertEqual(receipt["source"]["revision"], "a" * 40)
        self.assertEqual([item["argv"] for item in receipt["commands"]], self.calls)
        self.assertEqual(list(self.volume.iterdir()), [])
        self.assertIn("--tree", next(call for call in self.calls if call[0] == "lsblk"))

    def test_platform_and_dedicated_assertion_are_required(self):
        for uname in ("Darwin 24.0 arm64", "Linux 6.8 aarch64", "Linux x86_64", ""):
            with self.subTest(uname=uname):
                self.responses["uname"] = uname
                self.assert_ineligible(self.collect(), "Linux x86_64")
        self.responses["uname"] = "Linux 6.8 x86_64"
        self.assert_ineligible(self.collect(dedicated=False), "dedicated")

    def test_requires_existing_exact_non_root_mountpoint(self):
        self.assert_ineligible(self.collect(Path("/")), "non-root")
        self.assert_ineligible(self.collect(self.root / "missing"), "No such file")
        regular = self.root / "file"
        regular.touch()
        self.assert_ineligible(self.collect(regular), "non-root")
        child = self.volume / "child"
        child.mkdir()
        self.assert_ineligible(self.collect(child), "exact mountpoint")
        alias = self.root / "alias"
        alias.symlink_to(self.volume, target_is_directory=True)
        self.assertEqual(self.collect(alias)["automatic_checks"], "COMPATIBLE")
        self.mount["maj:min"] = "123:456"
        self.assert_ineligible(self.collect(), "matching device identity")

    def test_mount_profile_options_fail_closed(self):
        for options in ("ro,data=ordered", "rw,data=writeback", "rw,data=journal",
                        "rw,data=ordered,nobarrier", "rw,data=ordered,barrier=0",
                        "rw", None):
            with self.subTest(options=options):
                self.mount["options"] = options
                self.assertEqual(self.collect()["automatic_checks"], "INELIGIBLE")
        self.mount["options"] = "rw,data=ordered"
        for filesystem in ("overlay", "tmpfs", "nfs", "xfs"):
            with self.subTest(filesystem=filesystem):
                self.mount["fstype"] = filesystem
                self.assert_ineligible(self.collect(), "ext4")

    def test_source_and_operating_system_must_survive_volume_cut(self):
        self.assert_ineligible(self.collect(source_root=self.volume), "source_root must be outside")
        self.root_on_volume = True
        self.assert_ineligible(self.collect(), "os_root must be outside")

    def test_block_stack_supports_nested_mapper_and_rejects_bind_mount(self):
        mapper = {"path": "/dev/mapper/test", "type": "crypt", "maj:min": self.device_id,
                  "ro": False, "size": 5_000}
        self.partition["maj:min"] = "9999:9998"
        self.partition["children"] = [mapper]
        self.mount["source"] = "/dev/mapper/test"
        receipt = self.collect()
        self.assertEqual(receipt["automatic_checks"], "COMPATIBLE", receipt["errors"])
        self.assertEqual([node["type"] for node in receipt["observed"]["block_stack"]], ["disk", "part", "crypt"])
        self.mount["source"] += "[/subvolume]"
        self.assert_ineligible(self.collect(), "bind-mounted")

    def test_unknown_or_unsafe_block_stack_is_rejected(self):
        for field, value in (("type", "loop"), ("type", []), ("tran", {}), ("ro", True), ("ro", None), ("size", 0),
                             ("size", "1000"), ("size", True), ("path", "not-a-device"), ("maj:min", "invalid"),
                             ("children", {})):
            with self.subTest(field=field, value=value):
                saved = copy.deepcopy(self.partition)
                self.partition[field] = value
                self.assertEqual(self.collect()["automatic_checks"], "INELIGIBLE")
                self.partition.clear()
                self.partition.update(saved)
        self.responses["lsblk"] = {"blockdevices": [self.partition]}
        self.assert_ineligible(self.collect(), "physical disk ancestry")
        self.responses["lsblk"] = {"blockdevices": []}
        self.assert_ineligible(self.collect(), "physical disk ancestry")

    def test_known_remote_transport_is_rejected(self):
        for transport in ("iscsi", "fc", "fcoe", "tcp", "rdma", "nbd"):
            with self.subTest(transport=transport):
                self.disk["tran"] = transport
                self.assert_ineligible(self.collect(), "network-backed")
        self.disk["tran"] = None
        self.disk["path"] = "/dev/nbd0"
        self.assert_ineligible(self.collect(), "network-backed")

    def test_command_failures_and_malformed_json_are_recorded(self):
        for tool in ("uname", "findmnt", "lsblk", "git"):
            for status in ("error", "failed", "timeout", "output_limit"):
                with self.subTest(tool=tool, status=status):
                    saved = self.responses.get(tool)
                    self.responses[tool] = {"status": status}
                    receipt = self.collect()
                    self.assert_ineligible(receipt, f"{tool} failed")
                    self.assertTrue(any(item["status"] == status for item in receipt["commands"]))
                    if saved is None:
                        self.responses.pop(tool)
                    else:
                        self.responses[tool] = saved
        for response in ("not json", "[" * 2000 + "]" * 2000, [], {}, {"filesystems": []}, {"filesystems": [self.mount, self.mount]}):
            with self.subTest(response=response):
                self.responses["findmnt"] = response
                self.assertEqual(self.collect()["automatic_checks"], "INELIGIBLE")

    def test_missing_or_invalid_dependency_lock_rejects_receipt(self):
        lock = self.root / "third_party/lock.json"
        for contents in (b"invalid", b"[]", b'{"schema_version": 2}', b"[" * 2000 + b"]" * 2000, b"x" * (1024 * 1024 + 1)):
            with self.subTest(size=len(contents)):
                lock.write_bytes(contents)
                self.assert_ineligible(self.collect(), "dependency lock")
        lock.unlink()
        self.assert_ineligible(self.collect(), "dependency lock")

    def test_output_rejects_target_subdirectory_alias_and_same_device(self):
        alias = self.root / "alias"
        alias.symlink_to(self.volume, target_is_directory=True)
        for output in (self.volume / "receipt.json", alias / "receipt.json", self.root / "receipt.json"):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, "outside|different filesystem"):
                ENV.write_receipt(output, self.volume, "{}\n")
            self.assertFalse(output.exists())

    def test_output_is_exclusive_without_creating_directories(self):
        output = self.root / "receipt.json"
        with mock.patch.object(ENV.os, "fstat", return_value=SimpleNamespace(st_dev=self.volume.stat().st_dev + 1)):
            ENV.write_receipt(output, self.volume, "receipt\n")
            self.assertEqual(output.read_text(), "receipt\n")
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                ENV.write_receipt(output, self.volume, "replacement")
            self.assertEqual(output.read_text(), "receipt\n")
            link = self.root / "link.json"
            link.symlink_to(output)
            with self.assertRaises(FileExistsError):
                ENV.write_receipt(link, self.volume, "replacement")
            with self.assertRaises(FileNotFoundError):
                ENV.write_receipt(self.root / "missing/receipt.json", self.volume, "{}")
        self.assertFalse((self.root / "missing").exists())

    def test_cli_emits_stdout_and_distinct_exit_codes(self):
        for compatible in (True, False):
            receipt = {"automatic_checks": "COMPATIBLE" if compatible else "INELIGIBLE",
                       "power_durability": "NOT_VALIDATED"}
            with mock.patch.object(ENV, "collect", return_value=receipt), mock.patch.object(ENV.sys, "stdout", new_callable=io.StringIO) as output:
                self.assertEqual(ENV.main(["--volume", str(self.volume)]), 0 if compatible else 1)
                self.assertEqual(json.loads(output.getvalue()), receipt)
        with mock.patch.object(ENV, "collect", return_value=receipt), mock.patch.object(ENV.sys, "stderr", new_callable=io.StringIO) as errors:
            self.assertEqual(ENV.main(["--volume", str(self.volume), "--output", str(self.volume / "receipt")]), 2)
            self.assertIn("cannot write receipt", errors.getvalue())


class CommandCaptureTest(unittest.TestCase):
    def test_success_and_failure_preserve_status_and_both_streams(self):
        for code in (0, 7):
            result = ENV.run_command([sys.executable, "-c", f"import sys; print('out'); print('err', file=sys.stderr); sys.exit({code})"])
            self.assertEqual(result["exit_code"], code)
            self.assertEqual(result["status"], "ok" if code == 0 else "failed")
            self.assertEqual(result["stdout"], "out\n")
            self.assertEqual(result["stderr"], "err\n")

    def test_missing_tool_timeout_and_output_limit(self):
        with mock.patch.object(ENV.subprocess, "Popen", side_effect=FileNotFoundError("missing tool")):
            self.assertEqual(ENV.run_command(["missing"])["status"], "error")
        timeout = ENV.run_command([sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.1)
        self.assertEqual(timeout["status"], "timeout")
        limited = ENV.run_command([sys.executable, "-c", "print('x' * 4096)"], output_limit=100)
        self.assertEqual(limited["status"], "output_limit")
        self.assertEqual(len(limited["stdout"]), 100)
        exact = ENV.run_command([sys.executable, "-c", "import sys; sys.stdout.write('x' * 100)"], output_limit=100)
        self.assertEqual(exact["status"], "ok")

    def test_timeout_terminates_descendant_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "survived"
            child = f"import time; from pathlib import Path; print('ready', flush=True); time.sleep(0.5); Path({str(marker)!r}).touch()"
            parent = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(60)"
            result = ENV.run_command([sys.executable, "-c", parent], timeout=0.3)
            self.assertEqual(result["status"], "timeout")
            self.assertIn("ready", result["stdout"])
            time.sleep(0.6)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
