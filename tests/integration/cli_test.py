import json
from pathlib import Path
import subprocess
import sys
import tempfile


binary = sys.argv[1]
store_id = "00000000-0000-0000-0000-000000000001"


def run(*args):
    return subprocess.run([binary, *args], text=True, capture_output=True, timeout=15)


assert "create-store" in run("--help").stdout
with tempfile.TemporaryDirectory(prefix="glamdb-cli-", dir="/tmp") as directory:
    root = Path(directory) / "store"
    missing = run("inspect-store", "--root", str(root), "--store-id", store_id)
    assert missing.returncode != 0
    assert not root.exists()
    created = run("create-store", "--root", str(root), "--store-id", store_id)
    assert created.returncode == 0, created.stderr
    assert json.loads(created.stdout)["store_id"] == store_id
    assert (root / "catalog.sqlite").is_file()
    assert (root / "store.ready").is_file()
    assert run("create-store", "--root", str(root), "--store-id", store_id).returncode != 0
    opened = run("inspect-store", "--root", str(root), "--store-id", store_id)
    assert opened.returncode == 0, opened.stderr
    assert json.loads(opened.stdout) == json.loads(created.stdout)
    wrong = run("inspect-store", "--root", str(root), "--store-id",
                "00000000-0000-0000-0000-000000000002")
    assert wrong.returncode != 0
    assert "STORE_IDENTITY_MISMATCH" in wrong.stderr
print("CLI create/open/identity/no-create: PASS")
