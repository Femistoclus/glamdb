import contextlib
import importlib.util
import io
import json
from pathlib import Path
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
        storage = self.add_source("core/storage/adapter.cpp")
        self.add_source("tests/unit/coordinator_test.cpp")
        self.add_source("tests/support/fake_storage.cpp")
        (self.build / "compile_commands.json").write_text(json.dumps([
            {"directory": str(self.root), "file": str(path)} for path in (io, support)
        ]))
        with patch.object(quality, "llvm_tool", return_value="clang-tidy"), \
                patch.object(quality.subprocess, "run",
                             return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            quality.tidy_sources(self.build, ["core/storage"], io_only=True)
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


if __name__ == "__main__":
    unittest.main()
