import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CmakeConfigurationTest(unittest.TestCase):
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
        return self.run_command(
            self.cmake, "-S", str(self.source), "-B", str(self.build),
            "-G", self.generator, f"-DCMAKE_C_COMPILER={self.c_compiler}",
            f"-DCMAKE_CXX_COMPILER={self.cxx_compiler}", "-DCMAKE_BUILD_TYPE=Debug", "-DGLAMDB_IO_ONLY=ON",
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for option in ("cmake", "generator", "c-compiler", "cxx-compiler"):
        parser.add_argument("--" + option, required=True)
    arguments, unittest_arguments = parser.parse_known_args()
    for name, value in vars(arguments).items():
        setattr(CmakeConfigurationTest, name, value)
    unittest.main(argv=[sys.argv[0], *unittest_arguments])
