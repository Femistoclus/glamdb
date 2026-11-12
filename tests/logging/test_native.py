"""Exercise the real pinned Quill producer, runtime pipe and callbacks."""
import json
import os
from pathlib import Path
import select
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[2]


class NativeLoggingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = os.environ.get("GLAMDB_LOG_PROBE")
        if cls.binary:
            return
        cls.binary = str(ROOT / "artifacts/logging-probe")
        (ROOT / "artifacts").mkdir(exist_ok=True)
        subprocess.run([
            os.environ.get("CXX", "c++"), "-std=c++23", "-O2", "-pthread",
            "-Wall", "-Wextra", "-Werror", "-I", str(ROOT),
            "-isystem", str(ROOT / "third_party/quill/include"),
            str(ROOT / "tests/logging/logging_probe.cpp"),
            str(ROOT / "core/telemetry/structured_log.cpp"), "-o", cls.binary,
        ], check=True, text=True)

    def run_probe(self, mode="errors", count=10):
        result = subprocess.run([self.binary, mode, "stderr", str(count)],
                                text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        events = []
        for index, line in enumerate(result.stderr.splitlines(), 1):
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                self.fail(f"{mode} probe emitted non-JSON stderr on line {index}: {line[:2048]!r}; "
                          f"stderr tail: {result.stderr[-4096:]!r}")
        return json.loads(result.stdout), events

    def test_library_json_sink_preserves_safe_domain_fields_and_stdout(self):
        stats, events = self.run_probe()
        requests = [event for event in events if event["event"] == "request_completed"]
        self.assertEqual(len(requests), 10)
        self.assertEqual(stats["accepted"], 12)
        self.assertNotIn("written", stats)
        self.assertNotIn("backlog", stats)
        self.assertEqual(len({event["request_id"] for event in requests}), 10)
        for event in requests:
            self.assertRegex(event["request_id"], r"^[a-f0-9]{32}$")
            self.assertEqual(event["schema"], "1")  # Upstream JSON sink emits named arguments as strings.
            self.assertEqual(event["operation"], "append")
            self.assertEqual(event["duration_us"], "123")
            self.assertEqual(event["row_count"], "7")
            self.assertEqual(event["level"], "error")
            self.assertFalse(set(event) & {"body", "query", "path", "headers", "token", "Authorization"})
            self.assertEqual(event["logger"], "glamdb")

    def test_success_sampling_and_rate_leave_lifecycle_and_errors_unsampled(self):
        stats, events = self.run_probe("sample", 100)
        self.assertEqual(stats["sampled"], 90)
        self.assertEqual(stats["accepted"], 12)
        self.assertEqual({event["event"] for event in events},
                         {"runtime_started", "runtime_stopped", "request_completed"})
        stats, events = self.run_probe("rate", 100)
        self.assertGreaterEqual(stats["rate_limited"], 96)
        self.assertLessEqual(sum(e["event"] == "request_completed" for e in events), 4)

    def test_maximum_typed_record_fits_fixed_output_budget(self):
        _, events = self.run_probe("maximum", 1)
        self.assertTrue(events)
        for event in events:
            self.assertLessEqual(len(json.dumps(event).encode()), 1024)

    def test_overload_uses_real_quill_admission_and_drop_notifications(self):
        stats, _ = self.run_probe("stress", 100000)
        self.assertEqual(stats["submitted"], 800002)
        self.assertEqual(stats["submitted"], stats["accepted"] + stats["dropped_full"])
        self.assertGreater(stats["dropped_full"], 0)
        self.assertGreater(stats["quill_reported_dropped"], 0)
        self.assertLessEqual(stats["quill_reported_dropped"], stats["dropped_full"])
        self.assertEqual(stats["queue_capacity_bytes"], 65536)
        self.assertLess(stats["elapsed_us"], 10000000)

    def test_producer_thread_churn_cannot_allocate_unbounded_quill_queues(self):
        stats, _ = self.run_probe("threads", 1)
        self.assertEqual(stats["producer_threads"], 16)
        self.assertEqual(stats["max_producer_threads"], 16)
        self.assertEqual(stats["dropped_unregistered"], 49)
        self.assertEqual(stats["accepted"], 17)

    def test_blocked_runtime_pipe_does_not_block_operation_path(self):
        # Fill the fixed queue and blocked pipe with bounded sanitizer work.
        count = 10000
        process = subprocess.Popen([self.binary, "blocked", "stderr", str(count)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            readable, _, _ = select.select([process.stdout], [], [], 10)
            self.assertTrue(readable, "Producer did not finish while stderr remained blocked")
            stats = json.loads(process.stdout.readline())
            self.assertEqual(stats["submitted"], count + 2)
            self.assertGreater(stats["dropped_full"], 0)
            self.assertLess(stats["elapsed_us"], 10000000)
            # Releasing the runtime pipe permits library-owned shutdown to drain.
            _, output = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0)
            self.assertTrue(output)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()
            process.stderr.close()

    def test_closed_runtime_pipe_reports_real_sink_error_without_sigpipe_exit(self):
        process = subprocess.Popen([self.binary, "errors", "stderr", "1000"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.stderr.close()
        try:
            stats = json.loads(process.stdout.read())
            process.wait(timeout=10)
            self.assertEqual(process.returncode, 0)
            self.assertGreater(stats["sink_errors"], 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()

    def test_backend_reopen_preserves_process_stats_after_logger_destruction(self):
        stats, events = self.run_probe("reopen", 1)
        self.assertEqual(stats["process_accepted"], stats["accepted"] + 1)
        self.assertEqual(len(events), 4)

    def test_shutdown_closes_admission_before_library_objects_are_destroyed(self):
        stats, _ = self.run_probe("shutdown-race", 10000)
        self.assertGreater(stats["dropped_stopped"], 0)
        self.assertEqual(stats["submitted"], stats["accepted"] + stats["dropped_full"] +
                         stats["dropped_unregistered"] + stats["dropped_stopped"])


if __name__ == "__main__":
    unittest.main()
