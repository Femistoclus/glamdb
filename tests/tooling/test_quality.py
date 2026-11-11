import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import subprocess
import sys
import textwrap
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("quality", ROOT / "scripts/quality.py")
quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)


class QualityClassificationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="glamdb quality ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.build = self.root / "build"
        self.build.mkdir()
        self.core = self.add_source("core/engine.cpp")
        self.probe = self.add_source("probes/build_install/nested/main.cpp")
        self.toolchain = self.add_source("probes/toolchain/libcxx_asan.cpp")
        (self.build / "compile_commands.json").write_text(json.dumps([
            {"directory": str(self.root), "file": str(self.core)},
        ]))
        self.root_patch = patch.object(quality, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)

    def add_source(self, path):
        source = self.root / path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("int main() { return 0; }\n")
        return source

    def test_root_tidy_excludes_standalone_but_format_includes_it(self):
        with patch.object(quality, "llvm_tool", side_effect=lambda name: name), \
                patch.object(quality.subprocess, "run",
                             return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            quality.tidy_sources(self.build)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[0][1], str(self.core))
            run.reset_mock()
            quality.format_sources(False)
            self.assertEqual(run.call_count, 1)
            self.assertIn(str(self.probe), run.call_args.args[0])
            self.assertIn(str(self.toolchain), run.call_args.args[0])
            self.assertIn(str(self.core), run.call_args.args[0])

    def test_other_sources_still_require_compile_commands(self):
        for path in ("core/missing.cpp", "probes/other/main.cpp",
                     "probes/build_install_extra/main.cpp", "probes/toolchain_extra/main.cpp"):
            with self.subTest(path=path):
                source = self.add_source(path)
                with patch.object(quality, "llvm_tool", return_value="clang-tidy"), \
                        patch.object(quality.subprocess, "run",
                             return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
                    with self.assertRaisesRegex(RuntimeError, "Missing compile commands"):
                        quality.tidy_sources(self.build)
                    run.assert_not_called()
                source.unlink()

    def test_only_explicitly_disabled_component_is_excluded(self):
        storage = self.add_source("core/storage/adapter.cpp")
        with patch.object(quality, "llvm_tool", return_value="clang-tidy"), \
                patch.object(quality.subprocess, "run",
                             return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            with self.assertRaisesRegex(RuntimeError, "Missing compile commands"):
                quality.tidy_sources(self.build)
            run.assert_not_called()
            quality.tidy_sources(self.build, ["core/storage"])
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[0][1], str(self.core))
            self.assertIn(storage, quality.source_files())

    def test_exclusion_cannot_escape_or_hide_entire_project(self):
        with patch.object(quality, "llvm_tool", return_value="clang-tidy"):
            for excluded in (".", "..", "core/../.."):
                with self.subTest(excluded=excluded):
                    with self.assertRaisesRegex(ValueError, "project subdirectories"):
                        quality.tidy_sources(self.build, [excluded])

    def test_io_only_keeps_io_sources_without_requiring_engine_commands(self):
        io = self.add_source("core/io/local_fs.cpp")
        support = self.add_source("tests/support/fault_fs.cpp")
        probe = self.add_source("probes/io/main.cpp")
        storage = self.add_source("core/storage/adapter.cpp")
        self.add_source("tests/unit/coordinator_test.cpp")
        self.add_source("tests/support/fake_storage.cpp")
        (self.build / "compile_commands.json").write_text(json.dumps([
            {"directory": str(self.root), "file": str(path)} for path in (io, support, probe)
        ]))
        with patch.object(quality, "llvm_tool", return_value="clang-tidy"), \
                patch.object(quality.subprocess, "run",
                             return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            quality.tidy_sources(self.build, ["core/storage"], io_only=True)
            self.assertEqual({call.args[0][1] for call in run.call_args_list}, {str(io), str(support), str(probe)})
            run.reset_mock()
            quality.tidy_sources(self.build, ["core/storage", "probes/io"], io_only=True)
            self.assertEqual({call.args[0][1] for call in run.call_args_list}, {str(io), str(support)})
        self.assertIn(storage, quality.source_files(io_only=True))
        self.assertNotIn(self.core, quality.source_files(io_only=True))


class ParallelTidyTest(unittest.TestCase):
    def test_parallel_checks_keep_all_failure_diagnostics(self):
        # A sequential implementation cannot pass the two-process rendezvous.
        with tempfile.TemporaryDirectory(prefix="parallel-tidy-") as temporary:
            root = Path(temporary)
            (root / "core").mkdir()
            sources = [root / "core" / name for name in ("good.cpp", "bad.cpp")]
            for source in sources:
                source.write_text("int main() { return 0; }\n")
            (root / "compile_commands.json").write_text(json.dumps([
                {"directory": str(root), "file": str(source)} for source in sources
            ]))
            executable = root / "tidy"
            executable.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""\
                from pathlib import Path
                import sys
                import time
                source = Path(sys.argv[1])
                source.with_suffix('.started').touch()
                deadline = time.monotonic() + 5
                while len(list(source.parent.glob('*.started'))) < 2:
                    if time.monotonic() > deadline:
                        print('checks did not overlap', file=sys.stderr)
                        sys.exit(3)
                    time.sleep(0.01)
                print('checked ' + source.name)
                if source.name == 'bad.cpp':
                    print('bad.cpp: analyzer diagnostic', file=sys.stderr)
                    sys.exit(1)
                """))
            executable.chmod(0o755)
            output = io.StringIO()
            with patch.object(quality, "ROOT", root), \
                    patch.object(quality, "llvm_tool", return_value=str(executable)), \
                    contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                with self.assertRaises(RuntimeError):
                    quality.tidy_sources(root)
            self.assertIn("checked good.cpp", output.getvalue())
            self.assertIn("checked bad.cpp", output.getvalue())
            self.assertIn("bad.cpp: analyzer diagnostic", output.getvalue())
            self.assertNotIn("checks did not overlap", output.getvalue())


@unittest.skipUnless(shutil.which("cmake"), "CMake is required")
class OptionalAdminQualityTest(unittest.TestCase):
    def test_configured_tidy_checks_admin_only_when_enabled(self):
        with tempfile.TemporaryDirectory(prefix="optional-admin-quality-") as temporary:
            root = Path(temporary)
            (root / "scripts").mkdir()
            shutil.copyfile(ROOT / "scripts/quality.py", root / "scripts/quality.py")
            for name in ("core/main.cpp", "apps/admin/main.cpp"):
                source = root / name
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("int main() { return 0; }\n")
            (root / "CMakeLists.txt").write_text(textwrap.dedent(f"""\
                cmake_minimum_required(VERSION 3.28)
                project(OptionalAdminQuality LANGUAGES CXX)
                set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
                option(GLAMDB_BUILD_ADMIN "Offline administration" OFF)
                set(GLAMDB_BUILD_STORAGE ON)
                include("{ROOT / 'cmake/Quality.cmake'}")
                add_executable(core core/main.cpp)
                if(GLAMDB_BUILD_ADMIN)
                    add_executable(admin apps/admin/main.cpp)
                endif()
                """))
            executable = root / "tidy"
            executable.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""\
                from pathlib import Path
                import sys
                if '--version' in sys.argv:
                    print('clang-tidy version 18.0.0')
                else:
                    Path(sys.argv[1]).with_suffix('.checked').touch()
                """))
            executable.chmod(0o755)
            for enabled in (False, True):
                with self.subTest(admin=enabled):
                    build = root / ("enabled" if enabled else "disabled")
                    configured = subprocess.run(["cmake", "-S", str(root), "-B", str(build),
                        "-DGLAMDB_BUILD_ADMIN=" + ("ON" if enabled else "OFF")],
                        text=True, capture_output=True)
                    self.assertEqual(configured.returncode, 0, configured.stdout + configured.stderr)
                    checked = subprocess.run(["cmake", "--build", str(build), "--target", "tidy-check"],
                        env=dict(os.environ, GLAMDB_CLANG_TIDY=str(executable)), text=True, capture_output=True)
                    self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
                    self.assertTrue((root / "core/main.checked").exists())
                    self.assertEqual((root / "apps/admin/main.checked").exists(), enabled)


@unittest.skipUnless(shutil.which("clang-tidy-18"), "LLVM 18 is required")
class VendorAnalyzerExceptionTest(unittest.TestCase):
    def test_vendor_exception_keeps_other_checks_and_other_translation_units(self):
        with tempfile.TemporaryDirectory(prefix="vendor-analyzer-") as temporary:
            root = Path(temporary)
            shutil.copyfile(ROOT / ".clang-tidy", root / ".clang-tidy")
            client = root / "apps/client/http.cpp"
            core = root / "core/main.cpp"
            vendor = root / "third_party/resolver.h"
            for source in (client, core, vendor):
                source.parent.mkdir(parents=True, exist_ok=True)
            # Same intentional destructor cleanup pattern as pinned Asio 1.36.
            virtual_cleanup = ('struct Resolver { virtual ~Resolver() { shutdown(); } '
                               'virtual void shutdown() {} };\n')
            vendor.write_text(virtual_cleanup)
            client.write_text('#include "third_party/resolver.h"\nint main() { Resolver resolver; }\n')
            core.write_text('int main() { return 0; }\n')
            (root / "compile_commands.json").write_text(json.dumps([
                {"directory": str(root), "file": str(source), "arguments": [
                    "clang++", "-std=c++23", "-I" + str(root), "-c", str(source)]}
                for source in (client, core)
            ]))
            output = io.StringIO()
            with patch.object(quality, "ROOT", root), \
                    contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                try:
                    quality.tidy_sources(root)
                except RuntimeError:
                    self.fail(output.getvalue())
                # An unrelated memory defect in this same client remains fatal.
                client.write_text('int main() { int* pointer = nullptr; return *pointer; }\n')
                with self.assertRaises(RuntimeError):
                    quality.tidy_sources(root)
                self.assertIn("clang-analyzer-core.NullDereference", output.getvalue())
                client.write_text('int main() { return 0; }\n')
                core.write_text(virtual_cleanup + 'int main() { Resolver resolver; }\n')
                with self.assertRaises(RuntimeError):
                    quality.tidy_sources(root)
                self.assertIn("clang-analyzer-optin.cplusplus.VirtualCall", output.getvalue())


if __name__ == "__main__":
    unittest.main()
