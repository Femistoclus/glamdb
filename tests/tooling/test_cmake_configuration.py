import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CmakeConfigurationTest(unittest.TestCase):
    cmake = os.environ.get("GLAMDB_TEST_CMAKE", "cmake")
    generator = os.environ.get("GLAMDB_TEST_GENERATOR", "Ninja")
    c_compiler = os.environ.get("CC", "clang-18")
    cxx_compiler = os.environ.get("CXX", "clang++-18")

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb cmake ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.build = self.root / "build"
        shutil.copyfile(ROOT / "CMakeLists.txt", self.source / "CMakeLists.txt")
        for directory in ("core", "apps", "cmake", "tests", "scripts"):
            shutil.copytree(ROOT / directory, self.source / directory,
                            ignore=shutil.ignore_patterns("__pycache__"))

    def run_command(self, *command):
        return subprocess.run(command, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=60)

    def assert_success(self, *command):
        result = self.run_command(*command)
        self.assertEqual(result.returncode, 0, result.stdout)

    def configure(self, *options):
        toolchain = os.environ.get("GLAMDB_TEST_TOOLCHAIN")
        return self.run_command(
            self.cmake, "-S", str(self.source), "-B", str(self.build),
            "-G", self.generator, f"-DCMAKE_C_COMPILER={shutil.which(self.c_compiler) or self.c_compiler}",
            f"-DCMAKE_CXX_COMPILER={shutil.which(self.cxx_compiler) or self.cxx_compiler}", "-DCMAKE_BUILD_TYPE=Debug", "-DGLAMDB_IO_ONLY=ON",
            *([f"-DCMAKE_TOOLCHAIN_FILE={toolchain}"] if toolchain else []),
            *options,
        )

    def test_build_without_tests_needs_no_googletest(self):
        result = self.configure("-DBUILD_TESTING=OFF", "-DBUILD_SHARED_LIBS=ON")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_success(self.cmake, "--build", str(self.build), "--config", "Debug")
        self.assertEqual(len(list(self.build.rglob("libglamdb_io.a"))), 1)
        self.assertFalse(list(self.build.rglob("libglamdb_core.a")))
        self.assertFalse(list(self.build.rglob("glamdb")))

    def test_enabled_tests_explain_missing_googletest(self):
        result = self.configure("-DBUILD_TESTING=ON")
        self.assertNotEqual(result.returncode, 0)
        diagnostic = " ".join(result.stdout.split())
        self.assertIn("git submodule update --init third_party/googletest", diagnostic)
        self.assertIn("-DBUILD_TESTING=OFF", diagnostic)

    def test_new_nested_native_test_gets_a_compile_command(self):
        dependency = self.source / "third_party/googletest"
        dependency.parent.mkdir()
        dependency.symlink_to(ROOT / "third_party/googletest", target_is_directory=True)
        added = self.source / "tests/unit/io/nested/new_test.cpp"
        added.parent.mkdir()
        added.write_text('#include <gtest/gtest.h>\nTEST(Discovery, NewFile) { EXPECT_EQ(1, 1); }\n')
        result = self.configure("-DBUILD_TESTING=ON", "-DCMAKE_CXX_SCAN_FOR_MODULES=OFF")
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = json.loads((self.build / "compile_commands.json").read_text())
        self.assertIn(str(added), {entry["file"] for entry in commands})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for option in ("cmake", "generator", "c-compiler", "cxx-compiler"):
        parser.add_argument("--" + option, required=True)
    arguments, unittest_arguments = parser.parse_known_args()
    for name, value in vars(arguments).items():
        setattr(CmakeConfigurationTest, name, value)
    unittest.main(argv=[sys.argv[0], *unittest_arguments])
