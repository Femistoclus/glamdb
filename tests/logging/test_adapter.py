import contextlib
import importlib.util
import io
import json
import logging.handlers
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("glamdb_logging", ROOT / "tools/logging/glamdb_logging.py")


class AdapterTest(unittest.TestCase):
    def setUp(self):
        self.module = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(self.module)

    def test_standard_library_pipeline_projects_secrets_and_preserves_stdout(self):
        error, output = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(error), contextlib.redirect_stdout(output):
            logger = self.module.StructuredLogger(sample_every=1)
            self.assertIsInstance(logger._handler, logging.handlers.QueueHandler)
            self.assertIsInstance(logger._listener, logging.handlers.QueueListener)
            request_id = logger.new_request_id()
            self.assertTrue(logger.emit("request_completed", operation="auth", outcome="denied", request_id=request_id,
                                       password="secret-password", Authorization="Bearer secret-token", Cookie="secret",
                                       body="private-body", query="secret", path="secret", error=ValueError("secret"),
                                       org_id="password=secret", revision=7, status_code=403))
            print('{"result":"unchanged"}')
            logger.close()
        self.assertEqual(output.getvalue(), '{"result":"unchanged"}\n')
        self.assertNotIn("secret", error.getvalue())
        record = json.loads(error.getvalue())
        self.assertEqual(record["request_id"], request_id)
        self.assertEqual(record["revision"], 7)
        self.assertNotIn("org_id", record)
        self.assertFalse(set(record) & {"message", "body", "query", "path", "headers", "token", "exc_info"})

    def test_lifecycle_and_errors_bypass_success_sampling(self):
        error = io.StringIO()
        with contextlib.redirect_stderr(error):
            logger = self.module.StructuredLogger(sample_every=10, max_success_per_second=1000)
            for _ in range(100):
                logger.emit("request_completed", outcome="ok")
            logger.emit("org_updated", revision=11, org_id="example-team", database_id="production_1")
            logger.emit("runtime_started")
            logger.emit("request_completed", outcome="error")
            logger.close()
        stats = logger.snapshot()
        self.assertEqual(stats["sampled"], 90)
        self.assertEqual(stats["accepted"], 13)
        self.assertNotIn("written", stats)
        self.assertNotIn("backlog", stats)
        self.assertEqual(len(error.getvalue().splitlines()), 13)
        self.assertFalse(logger.emit("runtime_stopped"))
        logger.close()

    def test_invalid_fields_and_maximum_record_are_bounded(self):
        record = self.module.sanitize_record("password=secret", operation="token=secret", outcome="secret",
                                             request_id="secret", duration_us=-1, row_count=True, status_code=2**80)
        self.assertNotIn("secret", json.dumps(record))
        self.assertEqual(record["outcome"], "error")
        self.assertNotIn("row_count", record)
        maximum = self.module.sanitize_record("org_updated", **{key: 2**63-1 for key in self.module.NUMBERS},
                                              org_id="a"*64, database_id="b"*64)
        self.assertLess(len(json.dumps(maximum)), self.module.MAX_RECORD_BYTES)

    def test_slow_sink_queue_overload_never_waits_for_sink(self):
        entered, release = threading.Event(), threading.Event()

        class SlowStream(io.StringIO):
            def write(self, text):
                entered.set()
                release.wait(timeout=5)
                return super().write(text)

        with contextlib.redirect_stderr(SlowStream()):
            logger = self.module.StructuredLogger(capacity=2, sample_every=1)
            logger.emit("runtime_started")
            self.assertTrue(entered.wait(timeout=2))
            started = time.monotonic()
            for _ in range(10000):
                logger.emit("storage_failed", outcome="error")
            self.assertLess(time.monotonic()-started, 2)
            self.assertGreater(logger.snapshot()["dropped_full"], 0)
            self.assertLessEqual(logger._queue.qsize(), 2)
            release.set()
            logger.close()
        self.assertEqual(logger.snapshot()["submitted"], 10001)

    def test_sink_error_is_callback_count_and_never_recurses(self):
        class FailedStream(io.StringIO):
            def write(self, text):
                raise OSError("secret sink failure")
        with contextlib.redirect_stderr(FailedStream()):
            logger = self.module.StructuredLogger(sample_every=1)
            self.assertTrue(logger.emit("storage_failed", outcome="error"))
            logger.close()
        self.assertEqual(logger.snapshot()["sink_errors"], 1)
        self.assertNotIn("dropped_sink", logger.snapshot())

    def run_process_with_sink(self, sink):
        script = ("import json,sys; sys.path.insert(0," + repr(str(ROOT / "tools/logging")) + "); "
                  "from glamdb_logging import StructuredLogger; "
                  "logger=StructuredLogger(sample_every=1); "
                  "logger.emit('storage_failed',outcome='error'); logger.close(); "
                  "print(json.dumps(logger.snapshot()))")
        return subprocess.run([sys.executable, "-c", script], stdout=subprocess.PIPE,
                              stderr=sink, text=True, timeout=10)

    def test_closed_runtime_pipe_cannot_change_process_exit_status(self):
        reader, writer = os.pipe()
        os.close(reader)
        try:
            result = self.run_process_with_sink(writer)
        finally:
            os.close(writer)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["sink_errors"], 1)

    @unittest.skipUnless(Path("/dev/full").exists(), "requires the Linux ENOSPC device")
    def test_real_enospc_cannot_poison_global_stderr_or_process_exit(self):
        with open("/dev/full", "wb", buffering=0) as sink:
            result = self.run_process_with_sink(sink)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["sink_errors"], 1)

    def test_close_can_race_admission_while_object_remains_alive(self):
        with contextlib.redirect_stderr(io.StringIO()):
            logger = self.module.StructuredLogger(capacity=8)
            threads = [threading.Thread(target=lambda: [logger.emit("runtime_started") for _ in range(1000)])
                       for _ in range(4)]
            for thread in threads:
                thread.start()
            logger.close()
            for thread in threads:
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
            self.assertFalse(logger.emit("runtime_started"))


if __name__ == "__main__":
    unittest.main()
