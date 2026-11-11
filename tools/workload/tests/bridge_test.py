"""CLI11 -> reproc -> Python lifecycle integration; receives a built CLI."""

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest

CLI = str(Path(sys.argv.pop(1)).resolve())


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="glamdb bridge ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        package = self.root / "modules with spaces" / "glamdb_workload"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        self.module = package / "__main__.py"
        self.schema = self.root / "schema with spaces.json"
        self.schema.write_text("{}")
        self.env = dict(
            os.environ,
            GLAMDB_PYTHON=sys.executable,
            PYTHONPATH=str(package.parent),
            GLAMDB_TOKEN="bridge-secret-never-in-argv",
        )

    def args(self):
        return [
            CLI,
            "generate",
            "--schema",
            str(self.schema),
            "--rows",
            "7",
            "--seed",
            "42",
            "--output",
            str(self.root / "output with spaces"),
        ]

    def test_child_argv_streams_and_exit_code_without_shell(self):
        self.module.write_text(
            'import json,sys\nprint(json.dumps(sys.argv[1:]))\nprint("child-error",file=sys.stderr)\nsys.exit(23)\n'
        )
        output = str(self.root / "$(touch MUST_NOT_EXIST); 🎀")
        result = subprocess.run(
            self.args()[:-1] + [output],
            env=self.env,
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        args = json.loads(result.stdout)
        self.assertEqual(args[0], "generate")
        self.assertEqual(args[args.index("--output") + 1], output)
        self.assertNotIn(self.env["GLAMDB_TOKEN"], result.stdout + result.stderr)
        self.assertIn("child-error", result.stderr)
        self.assertFalse(Path("MUST_NOT_EXIST").exists())

    def test_missing_python_is_actionable(self):
        self.env["GLAMDB_PYTHON"] = str(self.root / "no python")
        result = subprocess.run(
            self.args(), env=self.env, text=True, capture_output=True, timeout=10
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("GLAMDB_PYTHON", result.stderr)

    def test_missing_extra_is_actionable(self):
        self.module.write_text(
            'raise ModuleNotFoundError("missing Locust", name="locust")\n'
        )
        result = subprocess.run(
            self.args(), env=self.env, text=True, capture_output=True, timeout=10
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("pip install", result.stderr)

    def test_sigint_stops_child_and_reaps_it(self):
        self.module.write_text(
            "import os,signal,sys,time\nprint(os.getpid(),flush=True)\nsignal.signal(signal.SIGTERM,lambda *a: sys.exit(130))\nwhile True: time.sleep(.1)\n"
        )
        process = subprocess.Popen(
            self.args(),
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            line = process.stdout.readline()
            self.assertTrue(line.strip().isdigit(), line)
            child = int(line)
            process.send_signal(signal.SIGINT)
            _, err = process.communicate(timeout=9)
            self.assertEqual(process.returncode, 130, err)
            with self.assertRaises(ProcessLookupError):
                os.kill(child, 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()

    def test_ignored_termination_is_killed_within_timeout(self):
        self.module.write_text(
            "import os,signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nprint(os.getpid(),flush=True)\nwhile True: time.sleep(.1)\n"
        )
        process = subprocess.Popen(
            self.args(),
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            child = int(process.stdout.readline())
            began = time.monotonic()
            process.send_signal(signal.SIGINT)
            _, err = process.communicate(timeout=9)
            self.assertEqual(process.returncode, 130, err)
            self.assertLess(time.monotonic() - began, 9)
            with self.assertRaises(ProcessLookupError):
                os.kill(child, 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main()
