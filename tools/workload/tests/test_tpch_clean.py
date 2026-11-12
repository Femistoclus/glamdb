"""Saved-state cleanup exercises the CLI adapter, SDK and actual HTTP boundary."""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from glamdb_workload.__main__ import main

DATABASE = "11111111-2222-4333-8444-555555555555"
OTHER = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
RESULT = {"database_id": DATABASE, "store_id": DATABASE, "dropped": True}


@pytest.fixture
def endpoint():
    requests = []
    responses = [RESULT]

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append((self.path, self.headers.get("Authorization"),
                             json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            response = responses.pop(0) if len(responses) > 1 else responses[0]
            if response is None:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests, responses
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def state_file(tmp_path, base_url, **extra):
    state = tmp_path / "tpch run.json"
    state.write_text(json.dumps({"version": 1, "url": base_url, "database_id": DATABASE,
                                 "store_id": DATABASE, "tables": {"lineitem": OTHER}} | extra))
    return state


def clean(state):
    return main(["bench", "tpch", "clean", "--state", str(state)])


def test_clean_repeats_only_saved_identity_and_preserves_state(endpoint, tmp_path, monkeypatch, capsys):
    url, requests, _ = endpoint
    state = state_file(tmp_path, url)
    original = state.read_bytes()
    monkeypatch.setenv("GLAMDB_TOKEN", "environment-only-secret")
    for _ in range(2):
        assert clean(state) == 0
        output = capsys.readouterr()
        assert json.loads(output.out) == RESULT
        assert output.err == ""
        assert state.read_bytes() == original
    assert requests == [(f"/databases/{DATABASE}/drop", "Bearer environment-only-secret",
                         {"store_id": DATABASE})] * 2


def test_clean_lost_reply_is_ambiguous_and_manual_retry_uses_same_state(endpoint, tmp_path, monkeypatch, capsys):
    url, requests, responses = endpoint
    responses[:] = [None, RESULT]
    state = state_file(tmp_path, url)
    original = state.read_bytes()
    monkeypatch.setenv("GLAMDB_TOKEN", "environment-only-secret")
    assert clean(state) != 0
    output = capsys.readouterr()
    assert output.out == ""
    assert "TRANSPORT_ERROR" in output.err
    assert len(requests) == 1
    assert state.read_bytes() == original
    assert clean(state) == 0
    assert json.loads(capsys.readouterr().out) == RESULT
    assert requests[0] == requests[1]
    assert state.read_bytes() == original


@pytest.mark.parametrize("reply", [RESULT | {"database_id": OTHER}, RESULT | {"dropped": False}, {}])
def test_clean_does_not_claim_success_for_invalid_reply(endpoint, tmp_path, monkeypatch, capsys, reply):
    url, requests, responses = endpoint
    responses[:] = [reply]
    state = state_file(tmp_path, url)
    original = state.read_bytes()
    monkeypatch.setenv("GLAMDB_TOKEN", "environment-only-secret")
    assert clean(state) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "PROTOCOL_ERROR" in output.err
    assert "environment-only-secret" not in output.err
    assert len(requests) == 1
    assert state.read_bytes() == original


@pytest.mark.parametrize("extra", [
    {"version": 2}, {"version": True}, {"url": "http://example.org"},
    {"url": "https://secret:password@example.org"}, {"url": None},
    {"database_id": "tpch"}, {"store_id": OTHER}, {"store_id": None},
    {"token": "state-secret"}, {"access_token": "state-secret"},
])
def test_malformed_clean_state_fails_before_mutation(endpoint, tmp_path, monkeypatch, capsys, extra):
    url, requests, _ = endpoint
    state = state_file(tmp_path, url, **extra)
    original = state.read_bytes()
    monkeypatch.setenv("GLAMDB_TOKEN", "environment-only-secret")
    assert clean(state) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert requests == []
    assert state.read_bytes() == original
    assert "state-secret" not in output.err
    assert "password" not in output.err


@pytest.mark.parametrize("body", ["[]", "{}", "{", '{"version":1,"version":2}',
                                  '{"version":1,"url":NaN}'])
def test_invalid_clean_state_json_fails_before_mutation(endpoint, tmp_path, monkeypatch, capsys, body):
    _, requests, _ = endpoint
    state = tmp_path / "run.json"
    state.write_text(body)
    monkeypatch.setenv("GLAMDB_TOKEN", "environment-only-secret")
    assert clean(state) == 2
    assert capsys.readouterr().out == ""
    assert requests == []
    assert state.read_text() == body


def test_clean_refuses_an_active_benchmark_run_before_remote_drop(endpoint, tmp_path, monkeypatch, capsys):
    from glamdb_workload.tpch_state import run_lock
    url, requests, _ = endpoint
    state = state_file(tmp_path, url)
    monkeypatch.setenv('GLAMDB_TOKEN', 'environment-only-secret')
    with run_lock(state):
        assert clean(state) == 2
    assert requests == []
    assert 'locked' in capsys.readouterr().err
