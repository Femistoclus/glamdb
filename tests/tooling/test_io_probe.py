import errno
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from probes.io import run, verify_trace as verify


def fixture_receipt(root, directory, case):
    root, directory = Path(root), Path(directory)
    root.mkdir()
    events, files, roots, phases = [], [], [], []

    def event(op, path="", kind="Other", attempt="", size=0, error=0, references=None):
        events.append({"string_encoding": "latin1-bytes", "seq": len(events) + 1, "op": op,
                       "path": str(path).encode("utf-8").decode("latin1"), "kind": kind,
                       "attempt_id": attempt, "bytes": size, "result": error,
                       "references": references or []})

    event("OpenDir", root)
    published = case == "success" or case.endswith("-short") or case == "write-eintr"
    count = 3 if published else (0 if case == "create-dir-sync" else 2)
    for index in range(count):
        phases.append({"name": ("create-begin", "append1-begin", "append2-begin")[index], "seq": len(events)})
        attempt = verify.APPEND2 if index == 2 else str(index)
        kinds = ["Data", "Manifest", "ManifestList", "Metadata"] if index == 2 else ["Metadata"]
        for kind in kinds:
            physical = root / f"{index}-{kind}.bin"
            physical.write_bytes(b"test")
            logical = f"store://fixture/{physical.name}"
            event("CreateExclusive", physical, kind, attempt)
            if case == "write-eintr" and kind == "Data":
                event("Write", physical, kind, attempt, error=errno.EINTR)
            event("Write", physical, kind, attempt, 2)
            event("Write", physical, kind, attempt, 2)
            event("SyncFile", physical, kind, attempt)
            event("Close", physical, kind, attempt)
            event("SyncDir", root)
            files.append({"path": logical, "physical_path": str(physical), "kind": kind,
                          "attempt_id": attempt, "length": 4, "format_verified": True})
            if kind == "Metadata":
                roots.append(logical)
        event("Publish", references=[item["physical_path"] for item in files])
    if count < 2 and case != "create-dir-sync":
        phases.append({"name": "append1-begin", "seq": len(events)})
    result = {"schema_version": 1, "case": case, "status": "PASS", "message": "fixture",
              "expected_outcome": "published" if published else "rejected",
              "actual_outcome": "published" if published else "rejected", "injection_hits": 0 if case == "success" else 1,
              "first_error": 0 if published or case == "outside-attempt" else errno.EIO,
              "first_close": "EIO" if case.endswith("-repeat-close") else None,
              "second_close": "OK" if case.endswith("-repeat-close") else None,
              "published_roots": roots, "phases": phases}
    inventory = {"schema_version": 1, "published_roots": roots, "latest_root": roots[-1] if roots else None,
                 "files": files, "orphans": []}
    (directory / "result.json").write_text(json.dumps(result))
    (directory / "inventory.json").write_text(json.dumps(inventory))
    (directory / "io-events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))
    return result, inventory, events


class ReceiptTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb io probe ")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.root = self.directory / "store"
        self.native = self.directory / "native"
        self.native.mkdir()
        self.result, self.inventory, self.events = fixture_receipt(self.root, self.native, "success")

    def write(self):
        (self.native / "result.json").write_text(json.dumps(self.result))
        (self.native / "inventory.json").write_text(json.dumps(self.inventory))
        (self.native / "io-events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in self.events))

    def check(self, case="success"):
        self.write()
        return verify.verify_receipt(self.native, self.root, case)

    def test_valid_receipt_and_footer_calibration(self):
        _, _, events = self.check()
        self.assertEqual(verify.footer_ordinals(events), {case: 2 for case in verify.FOOTERS})
        for event in events:
            event["attempt_id"] = "different"
        with self.assertRaisesRegex(ValueError, "calibrate"):
            verify.footer_ordinals(events)

    def test_case_result_injection_and_close_evidence(self):
        for key, value in (("schema_version", 2), ("case", "wrong"), ("status", "FAIL"),
                           ("actual_outcome", "rejected"), ("first_error", errno.EIO), ("injection_hits", 1)):
            with self.subTest(key=key):
                saved = self.result[key]
                self.result[key] = value
                with self.assertRaises(ValueError):
                    self.check()
                self.result[key] = saved
        self.result["case"] = "data-short"
        with self.assertRaisesRegex(ValueError, "not matched"):
            self.check("data-short")

    def test_inventory_requires_verified_real_files_and_matching_roots(self):
        for key, value in (("format_verified", False), ("length", 5), ("attempt_id", "wrong"),
                           ("kind", "Data"), ("physical_path", "/outside/file")):
            with self.subTest(key=key):
                saved = self.inventory["files"][0][key]
                self.inventory["files"][0][key] = value
                with self.assertRaises(ValueError):
                    self.check()
                self.inventory["files"][0][key] = saved
        self.inventory["files"].append(self.inventory["files"][0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.check()
        self.inventory["files"].pop()
        self.result["published_roots"] = []
        with self.assertRaisesRegex(ValueError, "roots"):
            self.check()

    def test_sequence_durability_failure_and_append_mkdir_are_rejected(self):
        for operation in ("SyncFile", "Close", "SyncDir"):
            with self.subTest(operation=operation):
                event = next(event for event in self.events if event["op"] == operation)
                event["result"] = errno.EIO
                with self.assertRaisesRegex(ValueError, "unfinalized"):
                    self.check()
                event["result"] = 0
        self.events[-1]["seq"] += 1
        with self.assertRaisesRegex(ValueError, "contiguous"):
            self.check()
        self.events[-1]["seq"] -= 1
        self.events[-1]["op"] = "MakeDir"
        with self.assertRaisesRegex(ValueError, "during append"):
            self.check()

    def test_missing_malformed_and_deep_artifacts_fail_closed(self):
        artifact = self.native / "result.json"
        for contents in ("broken", "[" * 2000 + "]" * 2000):
            artifact.write_text(contents)
            with self.assertRaises(ValueError):
                verify.verify_receipt(self.native, self.root, "success")
        artifact.unlink()
        with self.assertRaises(OSError):
            verify.verify_receipt(self.native, self.root, "success")

    def test_publication_evidence_is_required_and_exact(self):
        publications = [event for event in self.events if event["op"] == "Publish"]
        publications[-1]["references"] = [entry["physical_path"] for entry in self.inventory["files"] if entry["kind"] != "Data"]
        with self.assertRaisesRegex(ValueError, "differ from reachable inventory"):
            self.check()
        publications[-1]["references"] = []
        with self.assertRaisesRegex(ValueError, "metadata root"):
            self.check()
        publications[-1]["references"] = [entry["physical_path"] for entry in self.inventory["files"][:-1]]
        with self.assertRaisesRegex(ValueError, "metadata root"):
            self.check()
        self.events = [event for event in self.events if event["op"] != "Publish"]
        for index, event in enumerate(self.events, 1):
            event["seq"] = index
        with self.assertRaisesRegex(ValueError, "publication evidence"):
            self.check()

    def test_latest_closure_excludes_historical_root_orphans(self):
        old_roots = self.inventory["files"][:2]
        self.inventory["files"] = self.inventory["files"][2:]
        self.inventory["orphans"] = [entry["physical_path"] for entry in old_roots]
        self.events[-1]["references"] = [entry["physical_path"] for entry in self.inventory["files"]]
        self.check()
        self.inventory["latest_root"] = self.inventory["published_roots"][0]
        with self.assertRaisesRegex(ValueError, "authoritative"):
            self.check()


class StraceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.trace = Path(temporary.name) / "strace.123"
        self.root = "/test/store"
        self.path = self.root + "/data/file.parquet"
        self.events = [
            {"op": "MakeDir", "path": self.root + "/data", "result": 0},
            {"op": "CreateExclusive", "path": self.path, "kind": "Data", "result": 0},
            {"op": "Write", "path": self.path, "bytes": 4, "result": 0},
            {"op": "SyncFile", "path": self.path, "result": 0},
            {"op": "SyncDir", "path": self.root + "/data", "result": 0},
            {"op": "OpenRead", "path": self.path, "result": 0},
        ]
        self.lines = [
            'execve("/probe", ["/probe"], 0x123) = 0',
            'openat(AT_FDCWD, "/lib/libc.so", O_RDONLY|O_CLOEXEC) = 3</lib/libc.so>',
            f'mkdirat(3<{self.root}>, "data", 0750) = 0',
            f'openat(3<{self.root}/data>, "file.parquet", O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, 0640) = 4<{self.path}>',
            f'write(4<{self.path}>, "data", 4) = 4',
            f'fsync(4<{self.path}>) = 0',
            f'close(4<{self.path}>) = 0',
            f'fsync(3<{self.root}/data>) = 0',
            f'openat(3<{self.root}/data>, "file.parquet", O_RDONLY|O_NOFOLLOW) = 4<{self.path}>',
            'exit_group(0) = ?',
            '+++ exited with 0 +++',
        ]

    def check(self):
        self.trace.write_text("".join(f"1000.{index:06d} {line}\n" for index, line in enumerate(self.lines)))
        return verify.verify_strace([self.trace], self.root, self.events)

    def test_registered_exclusive_creates_and_reads_pass(self):
        self.assertEqual(self.check()["status"], "PASS")
        self.lines.insert(-2, 'renameat(3</elsewhere>, "old", 3</elsewhere>, "new") = 0')
        self.assertEqual(self.check()["trace_files"], 1)
        self.assertEqual(verify.c_string(r'"/test/space\040name\x2eparquet"'), "/test/space name.parquet")
        self.assertEqual(verify.split_arguments('3</a>, "x,y", {flags=O_RDONLY, mode=0}'),
                         ['3</a>', '"x,y"', '{flags=O_RDONLY, mode=0}'])

    def test_native_open_exclusive_and_truncate_guards(self):
        original = self.lines[3]
        for replacement in (original.replace("|O_EXCL", ""), original.replace("O_EXCL", "O_TRUNC"),
                            original.replace("file.parquet", "unregistered.parquet"),
                            original.replace("O_WRONLY|O_CREAT|O_EXCL", "O_RDWR"),
                            original.replace("O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW", "0x241")):
            with self.subTest(replacement=replacement):
                self.lines[3] = replacement
                with self.assertRaises(ValueError):
                    self.check()
        self.lines[3] = original
        self.lines.insert(9, self.lines[8])
        with self.assertRaisesRegex(ValueError, "unregistered"):
            self.check()

    def test_native_append_mutations_and_untracked_writes_fail(self):
        for line in (f'mkdirat(3<{self.root}>, "new", 0750) = 0',
                     f'unlinkat(3<{self.root}>, "data/file.parquet", 0) = 0',
                     'unlinkat(3</test>, "store/data/file.parquet", 0) = 0',
                     'renameat(3</test>, "outside", 3</test>, "store/data/file.parquet") = 0',
                     'linkat(3</test>, "store/data/file.parquet", 3</test>, "outside", 0) = 0',
                     f'write(4<{self.path}>, "extra", 5) = 5',
                     'write(4, "unresolved", 10) = 10',
                     r'unlink("/test/\163tore/data/file.parquet") = 0'):
            with self.subTest(line=line):
                self.lines.insert(-2, line)
                with self.assertRaises(ValueError):
                    self.check()
                self.lines.pop(-3)

    def test_missing_incomplete_signal_and_relative_trace_fail_closed(self):
        for line in ('openat(3, "unknown", O_RDONLY) = 4', 'openat(3, "x", <unfinished ...>',
                     '--- SIGABRT {si_signo=SIGABRT} ---', '+++ exited with 1 +++'):
            with self.subTest(line=line):
                self.lines.insert(-2, line)
                with self.assertRaises(ValueError):
                    self.check()
                self.lines.pop(-3)
        self.lines.pop()
        with self.assertRaisesRegex(ValueError, "termination"):
            self.check()
        with self.assertRaisesRegex(ValueError, "missing strace"):
            verify.verify_strace([], self.root, self.events)

    def test_native_sync_order_cannot_be_replaced_by_logical_events(self):
        self.lines[4], self.lines[5] = self.lines[5], self.lines[4]
        with self.assertRaisesRegex(ValueError, "ordering"):
            self.check()
        self.lines[4], self.lines[5] = self.lines[5], self.lines[4]
        self.lines.pop(5)
        with self.assertRaisesRegex(ValueError, "missing tracked syncs"):
            self.check()

    def test_store_parent_sync_is_required_and_must_succeed(self):
        self.events.insert(0, {"op": "SyncDir", "path": "/test", "result": 0})
        sync = 'fsync(3</test>) = 0'
        self.lines.insert(2, sync)
        self.assertEqual(self.check()["status"], "PASS")
        original = self.lines.copy()
        for replacement in (None, 'fsync(3</test>) = -1 EIO (Input/output error)'):
            with self.subTest(replacement=replacement):
                self.lines = original[:2] + ([] if replacement is None else [replacement]) + original[3:]
                with self.assertRaises(ValueError):
                    self.check()
        self.events[0]["result"] = errno.EIO
        self.lines = original
        self.lines[2] = 'fsync(3</test>) = -1 EIO (Input/output error)'
        with self.assertRaises(ValueError):
            self.check()

    def test_unsupported_content_mutations_target_the_destination(self):
        target = f'4<{self.path}>'
        original = self.lines.copy()
        mutations = (
            f'fallocate({target}, FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, 0, 4) = 0',
            f'copy_file_range(5</outside>, NULL, {target}, NULL, 4, 0) = 4',
            f'splice(5<pipe:[123]>, NULL, {target}, NULL, 4, 0) = 4',
            f'sendfile({target}, 5</outside>, NULL, 4) = 4',
            f'sendfile64({target}, 5</outside>, NULL, 4) = 4',
            f'vmsplice({target}, [{{iov_base="data", iov_len=4}}], 1, 0) = 4',
            f'tee(5<pipe:[123]>, {target}, 4, 0) = 4',
            f'ioctl({target}, FICLONE, 5</outside>) = 0',
            f'mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, {target}, 0) = 0x1000',
        )
        for line in mutations:
            with self.subTest(line=line):
                self.lines = original[:-2] + [line] + original[-2:]
                with self.assertRaisesRegex(ValueError, "mutation"):
                    self.check()
        outside = (
            'fallocate(5</outside>, FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, 0, 4) = 0',
            f'copy_file_range({target}, NULL, 5</outside>, NULL, 4, 0) = 4',
            f'splice({target}, NULL, 5<pipe:[123]>, NULL, 4, 0) = 4',
            f'sendfile(5</outside>, {target}, NULL, 4) = 4',
            'vmsplice(5<pipe:[123]>, [{iov_base="data", iov_len=4}], 1, 0) = 4',
            'tee(5<pipe:[123]>, 6<pipe:[456]>, 4, 0) = 4',
            f'ioctl(5</outside>, FICLONE, {target}) = 0',
            'mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_SHARED, 5</outside>, 0) = 0x1000',
            'fsync(5</outside>) = 0',
        )
        self.lines = original[:-2] + list(outside) + original[-2:]
        self.assertEqual(self.check()["status"], "PASS")

    def test_unsupported_mutations_require_resolved_destination_descriptors(self):
        original = self.lines.copy()
        for line in ('fallocate(4, 0, 0, 4) = 0',
                     'copy_file_range(5</outside>, NULL, 4, NULL, 4, 0) = 4',
                     'splice(5<pipe:[123]>, NULL, 4, NULL, 4, 0) = 4'):
            with self.subTest(line=line):
                self.lines = original[:-2] + [line] + original[-2:]
                with self.assertRaisesRegex(ValueError, "descriptor"):
                    self.check()

    def test_asynchronous_io_is_unsupported_even_with_indirect_descriptors(self):
        original = self.lines.copy()
        for line in (
            f'io_submit(0x123, 1, [{{aio_lio_opcode=IOCB_CMD_PWRITE, aio_fildes=4<{self.path}>, '
            'aio_buf="data", aio_nbytes=4, aio_offset=0}]) = 1',
            'io_uring_setup(8, {flags=0}) = 5<anon_inode:[io_uring]>',
            f'io_uring_register(5<anon_inode:[io_uring]>, IORING_REGISTER_FILES, [4<{self.path}>], 1) = 0',
            'io_uring_enter(5<anon_inode:[io_uring]>, 1, 1, IORING_ENTER_GETEVENTS, NULL, 0) = 1',
        ):
            with self.subTest(line=line):
                self.lines = original[:-2] + [line] + original[-2:]
                with self.assertRaisesRegex(ValueError, "asynchronous"):
                    self.check()


class RunnerTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb io runner ")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.probe = self.directory / "fake-probe"
        self.mode = self.directory / "mode"
        self.mode.write_text("")
        self.probe.write_text(f'''#!{sys.executable}
import importlib.util, json, os, pathlib, signal, sys, time
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("fixtures", {str(Path(__file__).resolve())!r})
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
mode = pathlib.Path({str(self.mode)!r}).read_text()
if "--list-cases" in sys.argv:
    print(json.dumps(fixtures.run.CASES if mode != "bad-list" else ["success"]))
    sys.exit(0)
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
case = args["--case"]
if mode == "signal": os.kill(os.getpid(), signal.SIGTERM)
if mode == "timeout": time.sleep(60)
if mode == "exit": sys.exit(2)
if mode == "missing": sys.exit(0)
result, inventory, events = fixtures.fixture_receipt(args["--root"], args["--receipt-dir"], case)
if case in fixtures.verify.FOOTERS and args.get("--write-ordinal") != "2": sys.exit(1)
if mode == "unmatched" and case == "data-write":
    result["injection_hits"] = 0
    pathlib.Path(args["--receipt-dir"], "result.json").write_text(json.dumps(result))
''')
        self.probe.chmod(0o700)

    def suite(self, timeout=5):
        return run.run_suite(self.probe, self.directory / "stores", self.directory / "receipts", timeout)

    def test_fresh_process_matrix_and_calibrated_footer_arguments(self):
        result = self.suite()
        self.assertEqual(result["case_checks"], "PASS", result)
        self.assertEqual(len(result["cases"]), 33)
        self.assertEqual(result["native_open_audit"], "NOT_RUN")
        self.assertEqual(result["p_io"], "INCOMPLETE")
        self.assertEqual(result["power_durability"], "NOT_VALIDATED")
        self.assertEqual(len(list((self.directory / "stores").iterdir())), 33)
        self.assertEqual(json.loads((self.directory / "receipts/summary.json").read_text()), result)

    def test_strace_filter_captures_indirect_aio_submissions(self):
        commands = []
        execute = run.execute

        def capture(argv, directory, timeout):
            if argv[0] == "env":
                commands.append(argv)
                raise OSError("fixture tracer unavailable")
            return execute(argv, directory, timeout)

        with patch.object(run, "execute", side_effect=capture):
            result = run.run_suite(self.probe, self.directory / "stores", self.directory / "receipts", 5, trace=True)
        self.assertEqual(result["native_open_audit"], "FAIL")
        self.assertEqual(len(commands), 1)
        expression = commands[0][commands[0].index("-e") + 1]
        self.assertTrue({"%file", "%desc", "io_submit"} <= set(expression.removeprefix("trace=").split(",")))

    def test_process_failures_and_missing_artifacts_fail(self):
        self.mode.write_text("signal")
        result = self.suite()
        self.assertEqual(result["case_checks"], "FAIL")
        self.assertEqual(len(result["cases"]), 1)
        execution = json.loads((self.directory / "receipts/success/process.json").read_text())
        self.assertLess(execution["exit_code"], 0)

    def test_timeout(self):
        self.mode.write_text("timeout")
        result = self.suite(timeout=1)
        self.assertEqual(result["case_checks"], "FAIL")
        self.assertIn("timeout", result["cases"][0]["error"])

    def test_missing_receipt(self):
        self.mode.write_text("missing")
        self.assertEqual(self.suite()["case_checks"], "FAIL")

    def test_unmatched_injection_and_wrong_case_list(self):
        self.mode.write_text("unmatched")
        result = self.suite()
        self.assertEqual(result["case_checks"], "FAIL")
        self.assertEqual(next(case for case in result["cases"] if case["case"] == "data-write")["status"], "FAIL")
        self.mode.write_text("bad-list")
        result = run.run_suite(self.probe, self.directory / "stores2", self.directory / "receipts2", 5)
        self.assertIn("required 33 cases", result["error"])

    def test_existing_nested_alias_and_invalid_timeout_are_rejected_without_writes(self):
        stores = self.directory / "stores"
        for output in (stores, stores / "nested", self.directory):
            with self.subTest(output=output), self.assertRaises(ValueError):
                run.run_suite(self.probe, stores, output, 5)
            self.assertFalse(stores.exists())
        alias = self.directory / "alias"
        alias.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(ValueError):
            run.run_suite(self.probe, stores, alias / "stores/nested", 5)
        for timeout in (0, -1, float("inf"), float("nan")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                run.run_suite(self.probe, stores, self.directory / "receipts", timeout)


if __name__ == "__main__":
    unittest.main()
