"""The browser harness must reap its native child before removing test storage."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[2]


def launch_wrapper(tmp_path, executable):
    return subprocess.Popen([sys.executable, "ui/tests/e2e/start-native.py"], cwd=ROOT,
        env=dict(os.environ, GLAMDB_NATIVE_SERVER=str(executable), GLAMDB_E2E_TEMP_ROOT=str(tmp_path),
                 GLAMDB_E2E_STATE_FILE=str(tmp_path / "server.json")),
        start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def reap_test_processes(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.communicate(timeout=5)


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_native_browser_signal_reaps_a_child_that_ignores_term(tmp_path, stop_signal):
    executable = tmp_path / "resistant-native"
    child_pid = tmp_path / "native.pid"
    executable.write_text(
        f"#!{sys.executable}\nimport os, signal, time\nfrom pathlib import Path\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"ready = Path({str(child_pid)!r})\n"
        "ready.with_suffix('.tmp').write_text(str(os.getpid()))\n"
        "ready.with_suffix('.tmp').replace(ready)\n"
        "time.sleep(60)\n")
    executable.chmod(0o755)
    process = launch_wrapper(tmp_path, executable)
    try:
        deadline = time.monotonic() + 3
        while not child_pid.exists() and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(0.01)
        assert child_pid.exists(), "native child never started"
        pid = int(child_pid.read_text())
        process.send_signal(stop_signal)
        stdout, stderr = process.communicate(timeout=8)
        assert process.returncode == 0, stdout + stderr
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert not (tmp_path / "server.json").exists()
        assert not list(tmp_path.glob("glamdb-browser-*"))
    finally:
        reap_test_processes(process)


@pytest.mark.parametrize("exit_code", [0, 23])
def test_native_browser_preserves_child_exit_status_and_cleans_storage(tmp_path, exit_code):
    executable = tmp_path / "exiting-native"
    executable.write_text(f"#!/bin/sh\nexit {exit_code}\n")
    executable.chmod(0o755)
    process = launch_wrapper(tmp_path, executable)
    try:
        stdout, stderr = process.communicate(timeout=3)
        assert process.returncode == exit_code, stdout + stderr
        assert not (tmp_path / "server.json").exists()
        assert not list(tmp_path.glob("glamdb-browser-*"))
    finally:
        reap_test_processes(process)


def test_native_browser_startup_failure_removes_state_and_storage(tmp_path):
    process = launch_wrapper(tmp_path, tmp_path / "missing-native")
    try:
        process.communicate(timeout=3)
        assert process.returncode != 0
        assert not (tmp_path / "server.json").exists()
        assert not list(tmp_path.glob("glamdb-browser-*"))
    finally:
        reap_test_processes(process)
