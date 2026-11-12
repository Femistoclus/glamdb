import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


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
        for directory in ("core", "apps", "cmake", "tests", "scripts", "public"):
            shutil.copytree(ROOT / directory, self.source / directory,
                            ignore=shutil.ignore_patterns("__pycache__"))

        dependency = self.source / "third_party/quill"
        dependency.parent.mkdir()
        dependency.symlink_to(ROOT / "third_party/quill", target_is_directory=True)

    def run_command(self, *command):
        return subprocess.run(command, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=60)

    def assert_success(self, *command):
        result = self.run_command(*command)
        self.assertEqual(result.returncode, 0, result.stdout)

    @staticmethod
    def build_flag_options(cxx_only=False):
        names = ("CXX",) if cxx_only else ("C", "CXX")
        options = [f"-DCMAKE_{name}_FLAGS={os.environ[f'GLAMDB_TEST_{name}_FLAGS']}"
                   for name in names if f"GLAMDB_TEST_{name}_FLAGS" in os.environ]
        if "GLAMDB_TEST_EXE_LINKER_FLAGS" in os.environ:
            options.append(f"-DCMAKE_EXE_LINKER_FLAGS={os.environ['GLAMDB_TEST_EXE_LINKER_FLAGS']}")
        return options

    def configure(self, *options):
        toolchain = os.environ.get("GLAMDB_TEST_TOOLCHAIN")
        return self.run_command(
            self.cmake, "-S", str(self.source), "-B", str(self.build),
            "-G", self.generator, f"-DCMAKE_C_COMPILER={shutil.which(self.c_compiler) or self.c_compiler}",
            f"-DCMAKE_CXX_COMPILER={shutil.which(self.cxx_compiler) or self.cxx_compiler}", "-DCMAKE_BUILD_TYPE=Debug", "-DGLAMDB_IO_ONLY=ON",
            *([f"-DCMAKE_TOOLCHAIN_FILE={toolchain}"] if toolchain else []),
            *self.build_flag_options(),
            *options,
        )

    def test_build_without_tests_needs_no_googletest(self):
        result = self.configure("-DBUILD_TESTING=OFF", "-DBUILD_SHARED_LIBS=ON")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_success(self.cmake, "--build", str(self.build), "--config", "Debug")
        self.assertEqual(len(list(self.build.rglob("libglamdb_io.a"))), 1)
        self.assertFalse(list(self.build.rglob("libglamdb_core.a")))
        self.assertFalse(list(self.build.rglob("glamdb")))

    def test_nested_configure_preserves_active_language_flags(self):
        c_flags = " ".join(filter(None, (
            os.environ.get("GLAMDB_TEST_C_FLAGS"), "-Wno-unused-parameter",
        )))
        cxx_flags = " ".join(filter(None, (
            os.environ.get("GLAMDB_TEST_CXX_FLAGS"), "-Wno-unused-variable",
        )))
        with mock.patch.dict(os.environ, {
            "GLAMDB_TEST_C_FLAGS": c_flags,
            "GLAMDB_TEST_CXX_FLAGS": cxx_flags,
        }):
            result = self.configure("-DBUILD_TESTING=OFF")
        self.assertEqual(result.returncode, 0, result.stdout)
        cache = (self.build / "CMakeCache.txt").read_text()
        self.assertIn(f"CMAKE_C_FLAGS:STRING={c_flags}", cache)
        self.assertIn(f"CMAKE_CXX_FLAGS:STRING={cxx_flags}", cache)

    def test_nested_configure_preserves_active_executable_linker_flags(self):
        linker_flags = " ".join(filter(None, (
            os.environ.get("GLAMDB_TEST_EXE_LINKER_FLAGS"), "-Wl,--as-needed",
        )))
        with mock.patch.dict(os.environ, {
            "GLAMDB_TEST_EXE_LINKER_FLAGS": linker_flags,
        }):
            result = self.configure("-DBUILD_TESTING=OFF")
        self.assertEqual(result.returncode, 0, result.stdout)
        cache = (self.build / "CMakeCache.txt").read_text()
        self.assertIn(f"CMAKE_EXE_LINKER_FLAGS:STRING={linker_flags}", cache)

    def test_missing_logging_dependency_explains_initialization(self):
        (self.source / "third_party/quill").unlink()
        result = self.configure("-DBUILD_TESTING=OFF")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("git submodule update --init third_party/quill", " ".join(result.stdout.split()))

    def test_installed_logging_archive_needs_no_quill_target(self):
        install = self.root / "install"
        with (self.source / "CMakeLists.txt").open("a") as output:
            output.write('''
set_target_properties(glamdb_logging PROPERTIES EXPORT_NAME logging)
install(TARGETS glamdb_logging EXPORT LoggingTargets ARCHIVE DESTINATION lib)
install(EXPORT LoggingTargets NAMESPACE logging:: DESTINATION lib/cmake/logging)
''')
        result = self.configure("-DBUILD_TESTING=OFF", f"-DCMAKE_INSTALL_PREFIX={install}")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_success(self.cmake, "--build", str(self.build), "--target", "glamdb_logging")
        self.assert_success(self.cmake, "--install", str(self.build))
        consumer = self.root / "consumer"
        consumer.mkdir()
        (consumer / "main.cpp").write_text('''#include "core/telemetry/structured_log.h"
int main()
{
    using namespace NGlamDB::NTelemetry;
    auto logger = TStructuredLogger::Open();
    if (!logger || !(**logger).TryLog({.Event = ELogEvent::RuntimeStarted})) {
        return 1;
    }
    (**logger).Shutdown();
    return GetProcessLogStats().Accepted == 1 ? 0 : 2;
}
''')
        (consumer / "CMakeLists.txt").write_text(f'''cmake_minimum_required(VERSION 3.28)
project(InstalledLogging LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(Threads REQUIRED)
include("{install.as_posix()}/lib/cmake/logging/LoggingTargets.cmake")
add_executable(consumer main.cpp)
target_include_directories(consumer PRIVATE "{self.source.as_posix()}")
target_link_libraries(consumer PRIVATE logging::logging)
''')
        consumer_build = self.root / "consumer-build"
        toolchain = os.environ.get("GLAMDB_TEST_TOOLCHAIN")
        self.assert_success(
            self.cmake, "-S", str(consumer), "-B", str(consumer_build), "-G", self.generator,
            f"-DCMAKE_CXX_COMPILER={shutil.which(self.cxx_compiler) or self.cxx_compiler}",
            "-DCMAKE_CXX_SCAN_FOR_MODULES=OFF",
            *([f"-DCMAKE_TOOLCHAIN_FILE={toolchain}"] if toolchain else []),
            *self.build_flag_options(cxx_only=True),
        )
        self.assert_success(self.cmake, "--build", str(consumer_build))
        self.assert_success(str(consumer_build / "consumer"))

    def test_enabled_tests_explain_missing_googletest(self):
        result = self.configure("-DBUILD_TESTING=ON")
        self.assertNotEqual(result.returncode, 0)
        diagnostic = " ".join(result.stdout.split())
        self.assertIn("git submodule update --init third_party/googletest", diagnostic)
        self.assertIn("-DBUILD_TESTING=OFF", diagnostic)

    def test_new_nested_native_test_gets_a_compile_command(self):
        dependency = self.source / "third_party/googletest"
        dependency.parent.mkdir(exist_ok=True)
        dependency.symlink_to(ROOT / "third_party/googletest", target_is_directory=True)
        added = self.source / "tests/unit/io/nested/new_test.cpp"
        added.parent.mkdir()
        added.write_text('#include <gtest/gtest.h>\nTEST(Discovery, NewFile) { EXPECT_EQ(1, 1); }\n')
        result = self.configure("-DBUILD_TESTING=ON", "-DCMAKE_CXX_SCAN_FOR_MODULES=OFF")
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = json.loads((self.build / "compile_commands.json").read_text())
        self.assertIn(str(added), {entry["file"] for entry in commands})

    def test_storage_off_configures_core_without_storage_targets(self):
        for name in ("cli11", "asio", "json", "reproc"):
            (self.source / "third_party" / name).symlink_to(ROOT / "third_party" / name, target_is_directory=True)
        # Keep source preparation covered even without the optional installed prefix.
        prefix = os.environ.get("GLAMDB_TEST_PREFIX")
        if not prefix or not any((Path(path) / library / "cmake/Arrow").is_dir()
                                 for path in prefix.split(";") for library in ("lib", "lib64")):
            self.skipTest("the pinned dependency prefix is not configured")
        result = self.configure("-DGLAMDB_IO_ONLY=OFF", "-DGLAMDB_BUILD_STORAGE=OFF", "-DBUILD_TESTING=OFF",
                                f"-DCMAKE_PREFIX_PATH={prefix}", "-DCMAKE_CXX_SCAN_FOR_MODULES=OFF")
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = json.loads((self.build / "compile_commands.json").read_text())
        files = {Path(entry["file"]).relative_to(self.source).as_posix(): entry for entry in commands}
        self.assertIn("core/engine/engine.cpp", files)
        self.assertNotIn("GLAMDB_HAS_STORAGE", files["core/engine/engine.cpp"]["command"])
        for absent in ("core/backend/local_storage_backend.cpp", "core/scan/scan_cursor.cpp",
                       "core/storage/data_file_writer.cpp"):
            self.assertNotIn(absent, files)
        self.assertNotIn("glamdb_storage", (self.build / "build.ninja").read_text())

    def test_io_probe_requires_storage(self):
        result = self.configure("-DBUILD_TESTING=OFF", "-DGLAMDB_BUILD_IO_PROBE=ON")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GLAMDB_BUILD_IO_PROBE requires GLAMDB_BUILD_STORAGE=ON", result.stdout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for option in ("cmake", "generator", "c-compiler", "cxx-compiler"):
        parser.add_argument("--" + option, required=True)
    arguments, unittest_arguments = parser.parse_known_args()
    for name, value in vars(arguments).items():
        setattr(CmakeConfigurationTest, name, value)
    unittest.main(argv=[sys.argv[0], *unittest_arguments])
