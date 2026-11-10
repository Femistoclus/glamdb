"""Validate the actual static artifact used for public publication."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/build-public-demo.py"


def test_static_package_contains_only_ui_and_supplied_artwork():
    parent = ROOT / "artifacts"
    parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="package-test-", dir=parent) as temporary:
        output = Path(temporary) / "site"
        command = [sys.executable, str(SCRIPT), "--output", str(output)]
        subprocess.run(command, check=True, capture_output=True)
        subprocess.run(command, check=True, capture_output=True)  # safe rebuild
        html = (output / "index.html").read_text()
        assert 'data-public-demo="true"' in html
        assert 'href="assets/styles.css"' in html
        assert (output / ".nojekyll").is_file()
        assert len(json.loads((output / "config/demo-data.json").read_text())) == 2
        assert not any(p.name in {"token", "server.log", ".env", "node_modules", "tests", ".git"} for p in output.rglob("*"))
        for name, digest in [
            ("wordmark", "a2222b67971f388fe2948c18ec9ca9f098a3d25c83a4b3652c10fbfd8eca038e"),
            ("mark", "4656feea9c748658ab75b4b36244ccb6822c1869e10cf029e872d13724a7e010"),
        ]:
            assert hashlib.sha256((output / f"assets/brand/{name}.png").read_bytes()).hexdigest() == digest


def test_packager_refuses_existing_unmarked_directory_and_source_root():
    (ROOT / "artifacts").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="package-guard-", dir=ROOT / "artifacts") as temporary:
        path = Path(temporary)
        (path / "keep.txt").write_text("keep me")
        result = subprocess.run([sys.executable, str(SCRIPT), "--output", str(path)], capture_output=True)
        assert result.returncode != 0
        assert (path / "keep.txt").read_text() == "keep me"
    result = subprocess.run([sys.executable, str(SCRIPT), "--output", str(ROOT)], capture_output=True)
    assert result.returncode != 0
