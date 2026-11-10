import contextlib
import importlib.util
import io
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci_dependencies.py"
spec = importlib.util.spec_from_file_location("ci_dependencies", SCRIPT)
dependencies = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dependencies)


class DependencyCacheTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb dependency cache ")
        self.addCleanup(temporary.cleanup)
        self.parent = Path(temporary.name).resolve()
        self.root = self.parent / "checkout"
        self.inputs = (
            "CMakeLists.txt", "CMakePresets.json", ".gitmodules", "third_party/lock.json",
            ".github/workflows/cpp-quality.yml", "scripts/ci_dependencies.py",
            "cmake/dependencies/CMakeLists.txt", "cmake/dependencies/VerifySubmodules.cmake",
            "cmake/dependencies/sqlite/CMakeLists.txt",
        )
        for name in self.inputs:
            self.write(name, name + "\n")
        environment = patch.dict(os.environ, {
            "CC": "clang-18", "CXX": "clang++-18", "CXXFLAGS": "-stdlib=libc++",
        }, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        for name, value in (("system", "Linux"), ("machine", "x86_64")):
            replacement = patch.object(dependencies.platform, name, return_value=value)
            replacement.start()
            self.addCleanup(replacement.stop)
        self.outputs = {
            "pins": "verified\n", "c-version": "clang version 18.1.3\n",
            "cxx-version": "clang version 18.1.3\n", "c-target": "x86_64-linux-gnu\n",
            "cxx-target": "x86_64-linux-gnu\n", "cmake": "cmake version 3.28.3\n",
            "ninja": "1.11.1\n", "packages": "libc++-18-dev=18.1.3\nzlib1g-dev=1.3\n",
            "submodules": " 0123456789abcdef third_party/arrow\n",
        }
        runner = patch.object(dependencies.subprocess, "run", side_effect=self.run_command)
        self.run = runner.start()
        self.addCleanup(runner.stop)

    def write(self, name, contents):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def run_command(self, command, **kwargs):
        command = [str(argument) for argument in command]
        if command[0] == "cmake":
            label = "pins" if "-P" in command else "cmake"
        elif command[0] == "ninja":
            label = "ninja"
        elif command[0] == "dpkg-query":
            label = "packages"
        elif command[0] == "git":
            label = "submodules"
        else:
            compiler = "cxx" if "++" in command[0] else "c"
            label = compiler + ("-target" if command[-1] == "-dumpmachine" else "-version")
        return subprocess.CompletedProcess(command, 0, self.outputs[label], "")

    def key(self, root=None):
        return dependencies.cache_key(root or self.root)

    def test_key_is_stable_and_ignores_project_sources_and_generated_outputs(self):
        expected = self.key()
        self.assertIsInstance(expected, str)
        self.assertTrue(expected)
        self.assertLessEqual(len(expected), 512)
        for name in ("core/storage/writer.cpp", "tests/unit/new_test.cpp", "docs/README.md",
                     ".deps/install/lib/libarrow.a", "build/deps-local/CMakeCache.txt"):
            self.write(name, "unrelated change\n")
        self.assertEqual(self.key(), expected)
        self.assertEqual(self.key(), expected)

    def test_pins_are_verified_before_observing_tools(self):
        self.key()
        first = self.run.call_args_list[0]
        self.assertEqual(first.args[0], ["cmake", "-P", str(self.root / "cmake/dependencies/VerifySubmodules.cmake")])
        self.assertEqual(Path(first.kwargs["cwd"]), self.root)
        self.assertTrue(first.kwargs["check"])
        self.assertTrue(first.kwargs["text"])
        self.assertTrue(first.kwargs.get("capture_output") or
                        first.kwargs.get("stdout") == subprocess.PIPE)

    def test_changed_pins_recipes_and_workflow_invalidate_the_key(self):
        expected = self.key()
        for name in self.inputs:
            with self.subTest(name=name):
                path = self.root / name
                previous = path.read_bytes()
                path.write_bytes(previous + b"changed\n")
                self.assertNotEqual(self.key(), expected)
                path.write_bytes(previous)
        self.write("cmake/dependencies/nested/new-recipe.txt", "new dependency recipe\n")
        self.assertNotEqual(self.key(), expected)

    def test_flags_and_compiler_selection_invalidate_the_key(self):
        expected = self.key()
        for name in ("CC", "CXX", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS",
                     "ARROW_THRIFT_URL", "CMAKE_TOOLCHAIN_FILE"):
            with self.subTest(name=name), patch.dict(os.environ, {name: "changed"}):
                self.assertNotEqual(self.key(), expected)

    def test_actual_tool_package_and_submodule_versions_invalidate_the_key(self):
        expected = self.key()
        for name in self.outputs.keys() - {"pins"}:
            with self.subTest(name=name):
                previous = self.outputs[name]
                self.outputs[name] += "new version\n"
                self.assertNotEqual(self.key(), expected)
                self.outputs[name] = previous

    def test_platform_architecture_and_install_location_invalidate_the_key(self):
        expected = self.key()
        with patch.object(dependencies.platform, "system", return_value="Darwin"):
            self.assertNotEqual(self.key(), expected)
        with patch.object(dependencies.platform, "machine", return_value="aarch64"):
            self.assertNotEqual(self.key(), expected)
        copied = self.parent / "different checkout"
        shutil.copytree(self.root, copied)
        self.assertNotEqual(self.key(copied), expected)

    def test_quoted_compiler_paths_and_arguments_are_not_executed_by_a_shell(self):
        with patch.dict(os.environ, {
            "CC": "'/opt/LLVM 18/clang' --target=x86_64-linux-gnu",
            "CXX": "'/opt/LLVM 18/clang++' --target=x86_64-linux-gnu",
        }):
            self.key()
        commands = [list(call.args[0]) for call in self.run.call_args_list]
        for compiler in ("clang", "clang++"):
            for option in ("--version", "-dumpmachine"):
                self.assertIn([f"/opt/LLVM 18/{compiler}", "--target=x86_64-linux-gnu", option], commands)
        self.assertTrue(all(not call.kwargs.get("shell", False) for call in self.run.call_args_list))

    def test_pin_verification_failure_cannot_return_a_key_or_probe_more_tools(self):
        self.run.side_effect = subprocess.CalledProcessError(7, ["cmake", "-P"], "mismatched pins", "")
        with self.assertRaises(subprocess.CalledProcessError):
            self.key()
        self.assertEqual(self.run.call_count, 1)

    def test_missing_recipe_and_unavailable_tool_fail_closed(self):
        (self.root / "third_party/lock.json").unlink()
        with self.assertRaises(OSError):
            self.key()
        self.run.side_effect = OSError("compiler missing")
        with self.assertRaises(OSError):
            self.key()

    def seed_generated_paths(self):
        self.write(".deps/install/stale", "old prefix")
        self.write("build/deps-local/stale", "old build")
        self.write(".deps/keep", "keep dependencies")
        self.write("build/local/keep", "keep project build")
        self.write("core/keep.cpp", "keep source")

    def test_exact_hit_preserves_prefix_and_does_not_run_a_build(self):
        self.seed_generated_paths()
        dependencies.prepare(self.root, "true")
        self.run.assert_not_called()
        self.assertEqual((self.root / ".deps/install/stale").read_text(), "old prefix")
        self.assertEqual((self.root / "build/deps-local/stale").read_text(), "old build")

    def test_every_nonexact_hit_cleans_only_generated_dependency_paths_before_build(self):
        def check_clean(command, **kwargs):
            self.assertFalse((self.root / ".deps/install").exists())
            self.assertFalse((self.root / "build/deps-local").exists())
            for name in (".deps/keep", "build/local/keep", "core/keep.cpp"):
                self.assertTrue((self.root / name).is_file())
            self.assertEqual(Path(kwargs["cwd"]), self.root)
            self.assertTrue(kwargs["check"])
            return subprocess.CompletedProcess(command, 0)

        self.run.side_effect = check_clean
        for cache_hit in ("false", "", "True", "true\n", "false "):
            with self.subTest(cache_hit=cache_hit):
                self.seed_generated_paths()
                self.run.reset_mock()
                dependencies.prepare(self.root, cache_hit)
                commands = [list(call.args[0]) for call in self.run.call_args_list]
                self.assertEqual(commands, [
                    ["cmake", "--preset", "deps-local", "-DGLAMDB_DEPENDENCY_JOBS=2"],
                    ["cmake", "--build", "--preset", "deps-local", "--parallel", "2"],
                ])

    def test_configure_and_build_errors_propagate_without_running_later_steps(self):
        for failed_stage in (0, 1):
            with self.subTest(stage=failed_stage):
                self.seed_generated_paths()
                self.run.reset_mock()
                failure = subprocess.CalledProcessError(23, "cmake")
                self.run.side_effect = [subprocess.CompletedProcess("cmake", 0)] * failed_stage + [failure]
                with self.assertRaises(subprocess.CalledProcessError):
                    dependencies.prepare(self.root, "false")
                self.assertEqual(self.run.call_count, failed_stage + 1)
                self.assertFalse((self.root / ".deps/install/stale").exists())
                self.assertTrue((self.root / "core/keep.cpp").exists())

    def test_cli_returns_nonzero_for_process_and_os_errors(self):
        for error in (OSError("tool missing"), subprocess.CalledProcessError(9, "cmake")):
            with self.subTest(error=type(error).__name__), patch.object(sys, "argv", [str(SCRIPT), "key"]), \
                    contextlib.redirect_stderr(io.StringIO()) as stderr:
                self.run.side_effect = error
                with self.assertRaises(SystemExit) as exited:
                    runpy.run_path(str(SCRIPT), run_name="__main__")
                self.assertNotEqual(exited.exception.code, 0)
                self.assertTrue(stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
