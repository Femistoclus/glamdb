import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DockerRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="glamdb runner ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for directory in ("scripts", "docker/dev", "third_party/googletest", "bin"):
            (self.root / directory).mkdir(parents=True)
        self.runner = self.root / "scripts/test-in-docker.sh"
        shutil.copyfile(ROOT / "scripts/test-in-docker.sh", self.runner)
        shutil.copyfile(ROOT / "docker/dev/Dockerfile", self.root / "docker/dev/Dockerfile")
        (self.root / "third_party/googletest/CMakeLists.txt").touch()
        self.log = self.root / "docker.jsonl"
        fake = self.root / "bin/docker"
        fake.write_text(
            f"#!{sys.executable}\n"
            "import json, os, subprocess, sys\n"
            "with open(os.environ['DOCKER_TEST_LOG'], 'a') as log:\n"
            "    json.dump({'args': sys.argv[1:], "
            "'stdin': sys.stdin.read() if sys.argv[1] == 'build' else ''}, log)\n"
            "    log.write('\\n')\n"
            "if sys.argv[1] == 'run' and os.environ.get('DOCKER_TEST_EXECUTE'):\n"
            "    sys.exit(subprocess.run(sys.argv[sys.argv.index('sh', 2):]).returncode)\n"
            "sys.exit(int(os.environ.get('DOCKER_TEST_' + sys.argv[1].upper() + '_EXIT', '0')))\n"
        )
        fake.chmod(0o755)

    def run_script(self, *args, **environment):
        return subprocess.run(
            ["sh", str(self.runner), *args],
            cwd=self.root / "bin",
            env={
                **os.environ,
                "PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"],
                "DOCKER_TEST_LOG": str(self.log),
                **environment,
            },
            text=True,
            capture_output=True,
            timeout=10,
        )

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_profiles_use_read_only_sources_and_temporary_builds(self):
        for arguments, profile in (((), "local"), (("release",), "release"),
                                   (("asan-ubsan",), "asan-ubsan")):
            with self.subTest(profile=profile):
                self.log.unlink(missing_ok=True)
                result = self.run_script(*arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                build, run = self.calls()
                self.assertEqual(build["args"], ["build", "--tag", "glamdb-dev:clang18", "-"])
                self.assertEqual(build["stdin"], (self.root / "docker/dev/Dockerfile").read_text())
                self.assertEqual(run["args"][:9], [
                    "run", "--rm", "--network", "none", "--volume", f"{self.root}:/src:ro",
                    "--workdir", "/src", "glamdb-dev:clang18",
                ])
                self.assertEqual(run["args"][-2:], ["sh", profile])
                self.assertEqual(run["args"][9:12], ["sh", "-eu", "-c"])
                commands = run["args"][12]
                self.assertIn("-B /build", commands)
                self.assertIn("-DGLAMDB_IO_ONLY=ON", commands)
                self.assertIn("--no-tests=error -L unit", commands)
                self.assertIn("--target format-check tidy-check quality-negative-check", commands)

    def test_invalid_arguments_do_not_contact_docker(self):
        for arguments in (("deps-local",), ("local", "extra"), ("",)):
            with self.subTest(arguments=arguments):
                self.assertEqual(self.run_script(*arguments).returncode, 2)
                self.assertFalse(self.log.exists())

    def test_missing_submodule_explains_fix_without_contacting_docker(self):
        (self.root / "third_party/googletest/CMakeLists.txt").unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 1)
        self.assertIn("git submodule update --init third_party/googletest", result.stderr)
        self.assertFalse(self.log.exists())

    def test_failed_image_build_prevents_test_run(self):
        self.assertEqual(self.run_script(DOCKER_TEST_BUILD_EXIT="17").returncode, 17)
        self.assertEqual(len(self.calls()), 1)

    def test_container_failure_is_returned(self):
        self.assertEqual(self.run_script(DOCKER_TEST_RUN_EXIT="23").returncode, 23)
        self.assertEqual(len(self.calls()), 2)

    def prepare_dependencies(self):
        self.runner = self.root / "scripts/build-deps-in-docker.sh"
        shutil.copyfile(ROOT / "scripts/build-deps-in-docker.sh", self.runner)
        (self.root / "docker/deps").mkdir()
        shutil.copyfile(ROOT / "docker/deps/Dockerfile", self.root / "docker/deps/Dockerfile")
        for directory in ("arrow/cpp", "iceberg-cpp", "prometheus-cpp", "prometheus-cpp/3rdparty/civetweb"):
            path = self.root / "third_party" / directory
            path.mkdir(parents=True)
            (path / "CMakeLists.txt").touch()

    def test_dependencies_keep_build_and_install_in_dedicated_volume(self):
        self.prepare_dependencies()
        result = self.run_script(GLAMDB_DEPS_VOLUME="test-deps-volume")
        self.assertEqual(result.returncode, 0, result.stderr)
        dev, deps, run = self.calls()
        self.assertEqual(dev["args"], ["build", "--tag", "glamdb-dev:clang18", "-"])
        self.assertEqual(deps["args"], ["build", "--tag", "glamdb-deps:clang18", "-"])
        self.assertEqual(deps["stdin"], (self.root / "docker/deps/Dockerfile").read_text())
        self.assertIn(f"{self.root}:/src:ro", run["args"])
        self.assertIn("test-deps-volume:/deps", run["args"])
        self.assertIn("CMAKE_BUILD_PARALLEL_LEVEL=2", run["args"])
        self.assertIn("--name", run["args"])
        self.assertIn("-DGLAMDB_DEPENDENCY_PREFIX=/deps/install", run["args"][-1])
        self.assertIn("--build /deps/build --parallel 1", run["args"][-1])

    def test_dependencies_reject_missing_sources_and_arguments(self):
        self.prepare_dependencies()
        self.assertEqual(self.run_script("unknown").returncode, 2)
        for directory in ("arrow/cpp", "iceberg-cpp", "googletest", "prometheus-cpp", "prometheus-cpp/3rdparty/civetweb"):
            with self.subTest(dependency=directory):
                source = self.root / "third_party" / directory / "CMakeLists.txt"
                source.unlink()
                result = self.run_script()
                self.assertEqual(result.returncode, 1)
                self.assertIn("git submodule update --init --recursive third_party/" + directory.split("/")[0],
                              result.stderr)
                self.assertFalse(self.log.exists())
                source.touch()

    def test_dependency_failures_are_reported(self):
        self.prepare_dependencies()
        self.assertEqual(self.run_script(DOCKER_TEST_BUILD_EXIT="17").returncode, 17)
        self.assertEqual(len(self.calls()), 1)
        self.log.unlink()
        self.assertEqual(self.run_script(DOCKER_TEST_RUN_EXIT="23").returncode, 23)
        self.assertEqual(len(self.calls()), 3)

    def prepare_installed_probe(self):
        self.runner = self.root / "scripts/test-installed-deps-in-docker.sh"
        shutil.copyfile(ROOT / "scripts/test-installed-deps-in-docker.sh", self.runner)
        shutil.rmtree(self.root / "third_party")

    def test_installed_probe_mounts_only_consumer_and_install_prefix(self):
        self.prepare_installed_probe()
        result = self.run_script(GLAMDB_DEPS_VOLUME="test-deps-volume")
        self.assertEqual(result.returncode, 0, result.stderr)
        (run,) = self.calls()
        self.assertEqual(run["args"][:4], ["run", "--rm", "--network", "none"])
        self.assertIn(f"{self.root}/probes/build_install:/consumer-src:ro", run["args"])
        self.assertIn("type=volume,src=test-deps-volume,dst=/prefix,readonly,volume-subpath=install",
                      run["args"])
        self.assertNotIn(f"{self.root}:/src:ro", run["args"])
        self.assertIn("-DCMAKE_PREFIX_PATH=/prefix", run["args"][-1])
        self.assertIn("-L build-install", run["args"][-1])
        self.assertIn("clang-tidy-18 /consumer-src/main.cpp -p /build", run["args"][-1])

    def test_installed_probe_rejects_arguments_and_preserves_failure(self):
        self.prepare_installed_probe()
        self.assertEqual(self.run_script("extra").returncode, 2)
        self.assertFalse(self.log.exists())
        self.assertEqual(self.run_script(DOCKER_TEST_RUN_EXIT="23").returncode, 23)
        self.assertEqual(len(self.calls()), 1)

    def prepare_storage(self):
        self.runner = self.root / "scripts/test-storage-in-docker.sh"
        shutil.copyfile(ROOT / "scripts/test-storage-in-docker.sh", self.runner)

    def test_storage_profiles_reuse_read_only_installed_prefix(self):
        self.prepare_storage()
        for arguments, profile in (((), "local"), (("release",), "release"),
                                   (("asan-ubsan",), "asan-ubsan")):
            with self.subTest(profile=profile):
                self.log.unlink(missing_ok=True)
                result = self.run_script(*arguments, GLAMDB_DEPS_VOLUME="test-deps-volume")
                self.assertEqual(result.returncode, 0, result.stderr)
                (run,) = self.calls()
                self.assertEqual(run["args"][:6], [
                    "run", "--rm", "--network", "none", "--volume", f"{self.root}:/src:ro",
                ])
                self.assertIn("type=volume,src=test-deps-volume,dst=/prefix,readonly,volume-subpath=install",
                              run["args"])
                self.assertIn("glamdb-deps:clang18", run["args"])
                self.assertEqual(run["args"][-2:], ["sh", profile])
                commands = run["args"][-3]
                for argument in ("-B /build", "-DGLAMDB_BUILD_STORAGE=ON", "-DGLAMDB_IO_ONLY=OFF",
                                 "-DCMAKE_PREFIX_PATH=/prefix", "--parallel 2",
                                 "-DCMAKE_EXE_LINKER_FLAGS=-static-libstdc++"):
                    self.assertIn(argument, commands)
                self.assertNotIn("ASAN_OPTIONS", commands)

    def test_storage_rejects_invalid_inputs_and_preserves_failure(self):
        self.prepare_storage()
        for arguments in (("deps-local",), ("local", "extra"), ("",)):
            with self.subTest(arguments=arguments):
                self.assertEqual(self.run_script(*arguments).returncode, 2)
                self.assertFalse(self.log.exists())
        self.assertEqual(self.run_script(DOCKER_TEST_RUN_EXIT="23").returncode, 23)
        self.assertEqual(len(self.calls()), 1)
        self.log.unlink()
        (self.root / "third_party/googletest/CMakeLists.txt").unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 1)
        self.assertIn("git submodule update --init third_party/googletest", result.stderr)
        self.assertFalse(self.log.exists())

    def check_failure_boundaries(self, profile="local", annotations=True):
        checks_log = self.root / "checks.jsonl"
        for tool in ("cmake", "ctest"):
            executable = self.root / "bin" / tool
            executable.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "path = Path(os.environ['CHECKS_TEST_LOG'])\n"
                "calls = path.read_text().splitlines() if path.exists() else []\n"
                "with path.open('a') as log:\n"
                "    log.write(json.dumps({'args': [Path(sys.argv[0]).name, *sys.argv[1:]], "
                "'flags': os.environ.get('CXXFLAGS', '')}) + '\\n')\n"
                "sys.exit(31 if len(calls) + 1 == int(os.environ['CHECKS_TEST_FAIL_AT']) else 0)\n"
            )
            executable.chmod(0o755)
        for failure in range(6):
            with self.subTest(failure=failure):
                checks_log.unlink(missing_ok=True)
                result = self.run_script(
                    profile,
                    DOCKER_TEST_EXECUTE="1", CHECKS_TEST_LOG=str(checks_log),
                    CHECKS_TEST_FAIL_AT=str(failure),
                    CXXFLAGS="-stdlib=libc++",
                )
                self.assertEqual(result.returncode, 31 if failure else 0, result.stderr)
                entries = [json.loads(line) for line in checks_log.read_text().splitlines()]
                for entry in entries:
                    expected = "-stdlib=libc++" + ("" if annotations else " -D_LIBCPP_HAS_NO_ASAN")
                    self.assertEqual(entry["flags"], expected)
                calls = [entry["args"] for entry in entries]
                self.assertEqual(len(calls), failure or 5)
                if not failure:
                    self.assertEqual([call[0] for call in calls],
                                     ["cmake", "cmake", "ctest", "ctest", "cmake"])
                    self.assertIn("unit", calls[2])
                    self.assertNotIn("-L", calls[3])

    def test_checks_stop_at_first_failure(self):
        self.check_failure_boundaries()

    def test_storage_checks_stop_at_first_failure(self):
        self.prepare_storage()
        for profile in ("local", "release", "asan-ubsan"):
            with self.subTest(profile=profile):
                self.check_failure_boundaries(profile, annotations=profile != "asan-ubsan")


if __name__ == "__main__":
    unittest.main()
