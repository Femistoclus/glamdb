"""Selection and startup failures must fail closed before any client scenario."""

import pytest

from .helpers import backends
from .helpers.backends import NativeHTTPServer, assert_backend_identity, client_server, selected_backend


@pytest.mark.parametrize("backend", ["mock", "native"])
def test_explicit_backend_wins_over_an_inherited_executable(monkeypatch, backend):
    monkeypatch.setenv("GLAMDB_TEST_BACKEND", backend)
    monkeypatch.setenv("GLAMDB_NATIVE_SERVER", "/missing/native-server")
    assert selected_backend() == backend


def test_invalid_backend_does_not_default_to_mock(monkeypatch, tmp_path):
    monkeypatch.setenv("GLAMDB_TEST_BACKEND", "nativ")
    with pytest.raises(RuntimeError, match="GLAMDB_TEST_BACKEND"):
        with client_server(tmp_path):
            pytest.fail("invalid selection launched a backend")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("executable", [None, "/missing/native-server"])
def test_native_selection_without_executable_never_starts_a_mock(monkeypatch, tmp_path, executable):
    monkeypatch.setenv("GLAMDB_TEST_BACKEND", "native")
    monkeypatch.delenv("GLAMDB_NATIVE_SERVER", raising=False)
    if executable is not None:
        monkeypatch.setenv("GLAMDB_NATIVE_SERVER", executable)
    with pytest.raises(RuntimeError, match="native server"):
        with client_server(tmp_path):
            pytest.fail("native selection launched a mock")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("capabilities", [
    {"backend": "mock", "mock": True, "durability_validation": "designed"},
    {"backend": "local", "mock": True, "durability_validation": "designed"},
    {"backend": "local", "mock": False, "durability_validation": "qualified"},
])
def test_native_identity_rejects_mock_or_unqualified_claims(capabilities):
    with pytest.raises(AssertionError):
        assert_backend_identity(capabilities, "native")


def test_early_native_exit_is_reaped_without_fallback(monkeypatch, tmp_path):
    executable = tmp_path / "exits-before-readiness"
    executable.write_text("#!/bin/sh\nexit 19\n")
    executable.chmod(0o755)
    monkeypatch.setenv("GLAMDB_NATIVE_SERVER", str(executable))
    server = NativeHTTPServer(tmp_path)
    with pytest.raises(RuntimeError, match="native server exited before readiness"):
        with server:
            pytest.fail("failed native process was accepted")
    assert server.process is None
    assert not (tmp_path / "ui").exists()


def test_partial_native_readiness_obeys_deadline_and_reaps_child(monkeypatch, tmp_path):
    executable = tmp_path / "partial-readiness"
    executable.write_text("#!/usr/bin/python3\nimport time\nprint('{\"port\":', end='', flush=True)\ntime.sleep(2)\n")
    executable.chmod(0o755)
    monkeypatch.setenv("GLAMDB_NATIVE_SERVER", str(executable))
    monkeypatch.setattr(backends, "READINESS_TIMEOUT", 0.1, raising=False)
    server = NativeHTTPServer(tmp_path)
    with pytest.raises(RuntimeError, match="readiness deadline"):
        with server:
            pytest.fail("partial readiness was accepted")
    assert server.process is None
