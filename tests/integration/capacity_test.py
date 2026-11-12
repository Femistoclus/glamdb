"""Small real-engine campaigns; the 5,000-attempt measurement stays manual."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


DRIVER = Path(sys.argv.pop(1)).resolve()
RUNNER = Path(__file__).resolve().parents[2] / "benchmarks" / "capacity.py"


class CapacityCampaignTest(unittest.TestCase):
    def run_campaign(self, output, *arguments, expected=0):
        process = subprocess.Popen(
            [sys.executable, str(RUNNER), "--driver", str(DRIVER),
             "--output", str(output), "--timeout", "30", *arguments],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=90)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            process.terminate()
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
            raise
        self.assertEqual(process.returncode, expected, stdout + stderr)

    def test_aggregate_history_and_outcome_boundaries_reopen(self):
        for profile in ("snapshots", "outcomes", "combined"):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "campaign"
                self.run_campaign(output, "--profile", profile, "--limit", "3",
                                  "--attempts", "8", "--checkpoint", "2")
                summary = json.loads((output / "summary.json").read_text())
                self.assertEqual(summary["stop_reason"], "backpressure")
                self.assertEqual(summary["successful_appends"], 3)
                self.assertEqual(summary["attempted_appends"], 4)
                self.assertEqual(summary["rows"], 3000)
                self.assertTrue(summary["same_profile_reopen"])
                self.assertTrue(summary["lowered_limits_rejected"])
                self.assertEqual(summary["receipt_count"], 3)
                samples = [json.loads(line) for line in (output / "checkpoints.jsonl").read_text().splitlines()]
                self.assertEqual([sample["successful_appends"] for sample in samples], [2, 3])
                self.assertGreater(samples[-1]["io"]["file_syncs"], 0)
                self.assertGreater(samples[-1]["usage"]["snapshots"], 0)
                self.assertGreater(samples[-1]["rss_peak_bytes"], 0)
                self.assertTrue(summary["retained_slice_verified"])
                if profile == "combined":
                    self.assertTrue(summary["orphan_pressure_verified"])
                    pressure = json.loads((output / "orphan-pressure.stdout.json").read_text())
                    self.assertEqual(pressure["successful_appends"], 2,
                                     "physical probe needs unused snapshot and operation slots")

    def test_attempt_budget_is_distinct_from_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "campaign"
            self.run_campaign(output, "--attempts", "3", "--checkpoint", "2")
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["stop_reason"], "attempt_budget")
            self.assertEqual(summary["successful_appends"], 3)
            self.assertTrue(summary["same_profile_reopen"])
            self.assertEqual(summary["rows"], 3000)
            before = (output / "summary.json").read_bytes()
            self.run_campaign(output, "--attempts", "1", expected=1)
            self.assertEqual((output / "summary.json").read_bytes(), before)

    def test_invalid_counts_do_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "campaign"
            self.run_campaign(output, "--checkpoint", "0", expected=2)
            self.assertFalse(output.exists())

    def test_sigterm_stops_native_process(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "campaign"
            driver = Path(directory) / "sleeping-driver"
            driver.write_text(
                f"#!{sys.executable}\nimport os, pathlib, sys, time\n"
                "pathlib.Path(sys.argv[2]).parent.joinpath('native.pid').write_text(str(os.getpid()))\n"
                "time.sleep(120)\n")
            driver.chmod(0o755)
            process = subprocess.Popen([sys.executable, str(RUNNER), "--driver", str(driver),
                                        "--output", str(output), "--attempts", "1"],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            pid = None
            try:
                deadline = time.monotonic() + 10
                while not (output / "native.pid").exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue((output / "native.pid").exists())
                pid = int((output / "native.pid").read_text())
                process.terminate()
                process.communicate(timeout=10)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                self.assertFalse((output / "summary.json").exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_pressure_peak_is_in_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "campaign"
            driver = Path(directory) / "reporting-driver"
            driver.write_text(
                f"#!{sys.executable}\nimport json, subprocess, sys\n"
                f"result = subprocess.run([{str(DRIVER)!r}, *sys.argv[1:]], capture_output=True, text=True)\n"
                "if result.returncode: sys.stderr.write(result.stderr); sys.exit(result.returncode)\n"
                "value = json.loads(result.stdout)\n"
                "if sys.argv[1] == 'pressure': value['rss_peak_bytes'] = 1099511627776\n"
                "print(json.dumps(value))\n")
            driver.chmod(0o755)
            self.run_campaign(output, "--driver", str(driver), "--profile", "combined",
                              "--limit", "3", "--attempts", "8", "--checkpoint", "2")
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual(summary["rss_peak_bytes"], 1099511627776)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    unittest.main()
