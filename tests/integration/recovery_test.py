import json
from pathlib import Path
import subprocess
import sys
import tempfile


binary = sys.argv[1]
with tempfile.TemporaryDirectory(prefix="glamdb-recovery-", dir="/tmp") as directory:
    root = Path(directory) / "store"

    def run(mode, expected=0):
        result = subprocess.run([binary, mode, str(root)], text=True, capture_output=True, timeout=20)
        assert result.returncode == expected, (mode, result.returncode, result.stdout, result.stderr)
        return result.stdout.strip()

    original = run("create")
    assert json.loads(original)["outcome"] == "committed"
    assert run("read") == original
    run("bound-crash", 77)
    interrupted = json.loads(run("lookup-bound"))
    assert interrupted["outcome"] == "definitely_not_committed"
    assert interrupted["error"]["code"] == "INTERRUPTED"
    assert run("lookup-bound") == json.dumps(interrupted, separators=(",", ":"))
    assert run("read") == original
    run("pointer-crash", 78)
    interrupted_pointer = json.loads(run("lookup-pointer"))
    assert interrupted_pointer["outcome"] == "definitely_not_committed"
    assert interrupted_pointer["error"]["code"] == "INTERRUPTED"
    assert run("read") == original
    run("hot-journal", 79)
    journal = root / "catalog.sqlite-journal"
    assert journal.is_file() and journal.stat().st_size > 512
    assert run("read") == original
    holder = subprocess.Popen([binary, "hold", str(root)], text=True,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert holder.stdout.readline().strip() == "locked"
        locked = subprocess.run([binary, "read", str(root)], text=True, capture_output=True, timeout=10)
        assert locked.returncode == 1 and "STATE_IN_USE" in locked.stderr
    finally:
        holder.communicate("\n", timeout=10)
    assert holder.returncode == 0
    assert run("read") == original
print("Fresh-process retry, interrupted binding/pointer, hot journal, process lock: PASS")
