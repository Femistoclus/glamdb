"""Native CI must select real clients/browser and fail missing prerequisites."""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class NativeAcceptanceRunner(unittest.TestCase):
    def test_native_ci_requirements_do_not_duplicate_shared_lock_pins(self):
        def pinned_packages(path):
            packages = set()
            for raw_line in path.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith(("#", "-r", "--")) or "==" not in line:
                    continue
                packages.add(line.split("==", 1)[0].lower())
            return packages

        shared_packages = pinned_packages(ROOT / "requirements-dev.txt")
        native_packages = pinned_packages(ROOT / "requirements-native-ci.txt")

        self.assertFalse(
            shared_packages & native_packages,
            "native CI requirements must not repin packages from requirements-dev.txt",
        )

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='native CI ')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        (self.root / 'scripts').mkdir()
        (self.root / 'bin').mkdir()
        for script in ('ci-native.sh', 'check-native-http.sh'):
            shutil.copyfile(ROOT / 'scripts' / script, self.root / 'scripts' / script)
        self.log = self.root / 'commands.jsonl'
        self.env = dict(os.environ, PATH=str(self.root / 'bin') + os.pathsep + os.environ['PATH'])

    def fake(self, name, result=0):
        executable = self.root / 'bin' / name
        executable.write_text(
            '#!' + sys.executable + '\nimport json, os, sys\n'
            f'with open({str(self.log)!r}, "a") as output:\n'
            '    print(json.dumps({"args": sys.argv, "server": os.getenv("GLAMDB_NATIVE_SERVER"), '
            '"required": os.getenv("GLAMDB_REQUIRE_NATIVE_CLIENTS"), '
            '"schemas": os.getenv("GLAMDB_VALIDATE_SCHEMAS"), '
            '"backend": os.getenv("GLAMDB_TEST_BACKEND"), '
            '"e2e_python": os.getenv("GLAMDB_E2E_PYTHON")}), file=output)\n'
            f'raise SystemExit({result})\n')
        executable.chmod(0o755)
        return executable

    def records(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def run_script(self, script, *args):
        return subprocess.run(['bash', str(self.root / 'scripts' / script), *args], cwd=self.root,
                              env=self.env, capture_output=True, text=True, timeout=15)

    def test_native_browser_receives_real_server_and_standalone_client(self):
        python = self.fake('python')
        self.env['GLAMDB_NATIVE_PYTHON'] = str(python)
        self.fake('npm')
        server = self.root / 'build/apps/server/glamdb'
        server.parent.mkdir(parents=True)
        server.touch()
        server.chmod(0o755)
        client = self.root / 'standalone/glamdb-cli'
        client.parent.mkdir()
        client.touch()
        client.chmod(0o755)
        result = self.run_script('check-native-http.sh', str(server.parents[2]), str(client))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        records = self.records()
        self.assertEqual(len(records), 5)
        self.assertEqual(records[0]['args'][1:], ['-c', 'import glamdb_sdk, httpx, pyarrow, jsonschema, pytest, fastavro'])
        self.assertEqual(records[1]['args'][1:], ['tests/client/cli_network_test.py', str(client), str(server)])
        self.assertEqual(records[2]['args'][1:], ['tools/workload/tests/native_e2e_test.py', str(client), str(server),
                                               '--report-root', 'artifacts/native-normal/workload'])
        self.assertEqual(records[3]['args'][1:], ['-m', 'pytest', 'tests/integration/test_network_sdk.py',
                                               '-q', '--junitxml=artifacts/native-normal/sdk-conformance-junit.xml'])
        self.assertEqual(Path(records[4]['args'][0]).name, 'npm')
        self.assertEqual(records[4]['args'][1:], ['--prefix', 'ui', 'run', 'test:e2e'])
        for record in records[1:]:
            self.assertEqual(record['server'], str(server))
            self.assertEqual(record['required'], '1')
            self.assertEqual(record['schemas'], '1')
            self.assertEqual(record['e2e_python'], str(python))
            self.assertEqual(record['backend'], 'native')

    def test_missing_client_dependency_fails_before_ctest(self):
        self.env['GLAMDB_NATIVE_PYTHON'] = str(self.fake('python', 23))
        self.fake('ctest')
        result = self.run_script('ci-native.sh', 'asan', 'test')
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertEqual(len(self.records()), 1)
        self.assertIn('import glamdb_sdk, glamdb_workload.tpch, httpx, pyarrow, jsonschema', self.records()[0]['args'])

    def test_missing_workload_dependency_fails_before_ctest(self):
        modules = self.root / 'modules'
        modules.mkdir()
        for name in ('glamdb_sdk', 'httpx', 'pyarrow', 'jsonschema'):
            (modules / (name + '.py')).touch()
        python = self.root / 'bin/python'
        python.write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' -S "$@"\n')
        python.chmod(0o755)
        self.env.update(GLAMDB_NATIVE_PYTHON=str(python), PYTHONPATH=str(modules))
        self.fake('ctest')
        result = self.run_script('ci-native.sh', 'ubsan', 'test')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("No module named 'glamdb_workload'", result.stdout + result.stderr)
        self.assertFalse(self.log.exists(), 'CTest must not start without the cleanup dependency')

    def test_msan_external_client_does_not_inherit_sanitized_toolchain(self):
        self.fake('cmake')
        result = self.run_script('ci-native.sh', 'msan', 'build')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        commands = [record['args'] for record in self.records()]
        self.assertEqual(commands[0][1:3], ['-S', 'apps/cli'])
        self.assertFalse(any('TOOLCHAIN' in argument for argument in commands[0]))
        self.assertIn('-DGLAMDB_TEST_CLI_EXECUTABLE=' + str(self.root / 'build/ci-msan-client/glamdb-cli'), commands[2])
        self.assertIn('-DGLAMDB_BUILD_ADMIN=ON', commands[2])
        self.assertTrue(any(argument.startswith('-DPython3_EXECUTABLE=') for argument in commands[2]))

    def test_native_flags_reach_ctest_for_every_profile(self):
        self.env['GLAMDB_NATIVE_PYTHON'] = str(self.fake('python'))
        self.fake('ctest')
        self.fake('cmake')
        for profile in ('normal', 'asan', 'ubsan', 'msan', 'tsan', 'lsan'):
            with self.subTest(profile=profile):
                self.log.write_text('')
                result = self.run_script('ci-native.sh', profile, 'test')
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                records = self.records()
                ctests = [record for record in records if Path(record['args'][0]).name == 'ctest']
                self.assertTrue(ctests)
                for ctest in ctests:
                    self.assertEqual(Path(ctest['args'][0]).name, 'ctest')
                    self.assertEqual(ctest['required'], '1')
                    self.assertEqual(ctest['schemas'], '1')
                systems = [record for record in records if record['args'][1:2] == ['tools/acceptance/run.py']]
                self.assertEqual(len(systems), 1 if profile == 'normal' else 0)
                for system in systems:
                    self.assertNotIn('--no-browser', system['args'])
                    self.assertEqual(system['required'], '1')
                    self.assertEqual(system['schemas'], '1')

    def test_cli_failure_blocks_browser_in_acceptance_runner(self):
        self.test_native_browser_receives_real_server_and_standalone_client()
        self.log.write_text('')
        python = self.root / 'bin/python'
        script = python.read_text().replace('raise SystemExit(0)', 'raise SystemExit(29 if len(sys.argv) > 1 and sys.argv[1].endswith("cli_network_test.py") else 0)')
        python.write_text(script)
        result = self.run_script('check-native-http.sh', str(self.root / 'build'), str(self.root / 'standalone/glamdb-cli'))
        self.assertEqual(result.returncode, 29, result.stdout + result.stderr)
        self.assertEqual(len(self.records()), 2)

    def test_workload_failure_blocks_browser_in_acceptance_runner(self):
        self.test_native_browser_receives_real_server_and_standalone_client()
        self.log.write_text('')
        python = self.root / 'bin/python'
        script = python.read_text().replace('raise SystemExit(0)', 'raise SystemExit(31 if len(sys.argv) > 1 and sys.argv[1].endswith("tools/workload/tests/native_e2e_test.py") else 0)')
        python.write_text(script)
        result = self.run_script('check-native-http.sh', str(self.root / 'build'), str(self.root / 'standalone/glamdb-cli'))
        self.assertEqual(result.returncode, 31, result.stdout + result.stderr)
        records = self.records()
        self.assertEqual(len(records), 3)
        self.assertEqual(records[-1]['args'][1], 'tools/workload/tests/native_e2e_test.py')

    def test_sdk_conformance_failure_blocks_browser_in_acceptance_runner(self):
        self.test_native_browser_receives_real_server_and_standalone_client()
        self.log.write_text('')
        python = self.root / 'bin/python'
        python.write_text(python.read_text().replace('raise SystemExit(0)',
            'raise SystemExit(37 if "pytest" in sys.argv else 0)'))
        result = self.run_script('check-native-http.sh', str(self.root / 'build'), str(self.root / 'standalone/glamdb-cli'))
        self.assertEqual(result.returncode, 37, result.stdout + result.stderr)
        self.assertEqual(len(self.records()), 4)


if __name__ == '__main__':
    unittest.main()
