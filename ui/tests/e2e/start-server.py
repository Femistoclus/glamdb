"""Start an explicitly disposable mock backend for browser acceptance tests."""
import os
from pathlib import Path
import shutil
import sys

root = Path(__file__).resolve().parents[3]
artifacts = root / "artifacts" / "e2e"
artifacts.mkdir(parents=True, exist_ok=True)
theme = artifacts / "theme.json"
shutil.copyfile(root / "ui" / "config" / "theme.json", theme)
env = dict(os.environ)
env["PYTHONPATH"] = os.pathsep.join(str(root / item) for item in (
    "tools/verification/src", "apps/mock_server/src", "sdk/python/src"))
env["GLAMDB_TOKEN"] = env.get("GLAMDB_E2E_TOKEN", "e2e-only-local-token")
os.execve(sys.executable, [sys.executable, "-m", "glamdb_mock", "--ui-dir", str(root / "ui"),
    "--theme-file", str(theme), "--port", env.get("GLAMDB_E2E_PORT", "8766"),
    "--seed-demo", "--enable-development-faults"], env)
