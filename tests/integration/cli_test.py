import json
from pathlib import Path
import subprocess
import sys
import tempfile


binary = sys.argv[1]
store_id = "00000000-0000-0000-0000-000000000001"


def run(*args):
    return subprocess.run([binary, *args], text=True, capture_output=True, timeout=15)


for args in ((), ("--help",), ("-h",)):
    help_result = run(*args)
    assert help_result.returncode == 0, help_result.stderr
    assert "create-store" in help_result.stdout
    assert "inspect-store" in help_result.stdout

with tempfile.TemporaryDirectory(prefix="glamdb-cli-", dir="/tmp") as directory:
    root = Path(directory) / "store with spaces"
    for command in ("create-store", "inspect-store"):
        for options in (
            ("--help",),
            ("-h",),
            ("--root", str(root), "--store-id", store_id, "--help"),
        ):
            help_result = run(command, *options)
            assert help_result.returncode == 0, help_result.stderr
            assert "--root" in help_result.stdout
            assert "--store-id" in help_result.stdout
            assert not root.exists()
        for options in (
            (),
            ("--root", str(root)),
            ("--store-id", store_id),
            ("--root", str(root), "--store-id"),
            ("--root", str(root), "--store-id", store_id, "--unknown"),
            ("--root", str(root), "--root", str(root), "--store-id", store_id),
            ("--root", str(root), "--store-id", store_id, "--store-id", store_id),
            ("--root", str(root), "--store-id", store_id, "extra"),
            ("--root", str(root), "--store-id", store_id, "inspect-store"),
        ):
            invalid = run(command, *options)
            assert invalid.returncode == 2, (command, options, invalid)
            assert invalid.stderr, (command, options)
            assert not root.exists()
    for args in (("unknown",), ("--root", str(root), "--store-id", store_id)):
        invalid = run(*args)
        assert invalid.returncode == 2, invalid
        assert invalid.stderr
        assert not root.exists()
    missing = run("inspect-store", "--root", str(root), "--store-id", store_id)
    assert missing.returncode == 1, missing.stderr
    assert not root.exists()
    created = run("create-store", "--root", str(root), "--store-id", store_id)
    assert created.returncode == 0, created.stderr
    assert json.loads(created.stdout)["store_id"] == store_id
    assert (root / "catalog.sqlite").is_file()
    assert (root / "store.ready").is_file()
    assert run("create-store", "--root", str(root), "--store-id", store_id).returncode == 1
    opened = run("inspect-store", "--root", str(root), "--store-id", store_id)
    assert opened.returncode == 0, opened.stderr
    assert json.loads(opened.stdout) == json.loads(created.stdout)
    for options in (
        ("--store-id", store_id, "--root", str(root)),
        (f"--root={root}", f"--store-id={store_id}"),
    ):
        opened = run("inspect-store", *options)
        assert opened.returncode == 0, opened.stderr
        assert json.loads(opened.stdout) == json.loads(created.stdout)
    wrong = run("inspect-store", "--root", str(root), "--store-id",
                "00000000-0000-0000-0000-000000000002")
    assert wrong.returncode == 1, wrong.stderr
    assert "STORE_IDENTITY_MISMATCH" in wrong.stderr
print("CLI parsing/help/create/open/identity/no-create: PASS")
