"""Explicit fault injection through the real SDK transport seam."""

import json
from uuid import uuid4

import httpx
import pytest
from glamdb_sdk import AppendRequest, Client, Field, Schema

SCHEMA = Schema([Field("key", "string")])


def prepared():
    return AppendRequest.prepare(
        str(uuid4()), str(uuid4()), SCHEMA, [{"key": "same payload 🎀"}]
    )


def committed(req):
    return {
        "outcome": "committed",
        "phase": "complete",
        "operation_id": req.operation_id,
        "result": {
            "collection_id": req.collection_id,
            "operation_id": req.operation_id,
            "snapshot_id": "1",
            "rows_appended": "1",
            "payload_hash": req.payload_hash,
            "hash_version": 1,
            "receipt_version": 1,
            "snapshot_created_at_us": "1",
        },
    }


def test_lost_commit_response_then_lookup_never_resends(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import append_once, resolve_ledger

    req = prepared()
    calls = []

    def fault(request):
        calls.append((request.method, request.content))
        if request.method == "POST":
            # A committed server response exists but the network loses it.
            raise httpx.ReadError("lost response after commit")
        return httpx.Response(200, json=committed(req))

    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid", "test-token", transport=httpx.MockTransport(fault)
    ) as client:
        result = append_once(client, log, req)
        assert result["state"] == "transport_error"
        assert log.summary()["pending"] == 1
        resolve_ledger(client, log)
        assert log.summary()["committed"] == 1
        assert log.summary()["attempts"] == 1
        assert [method for method, _ in calls] == ["POST", "GET"]
        assert calls[0][1] == req.body


@pytest.mark.parametrize("kind", ["unknown", "conflict", "timeout"])
def test_unknown_conflict_and_timeout_stay_pending_without_hidden_retry(tmp_path, kind):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import append_once

    req = prepared()
    calls = []

    def fault(request):
        calls.append(request)
        if kind == "timeout":
            raise httpx.ReadTimeout("test timeout")
        if kind == "conflict":
            return httpx.Response(
                409,
                json={
                    "outcome": "definitely_not_committed",
                    "error": {"code": "OPERATION_CONFLICT", "message": "conflict"},
                },
            )
        return httpx.Response(
            503,
            json={
                "outcome": "commit_unknown",
                "recovery_required": True,
                "error": {"code": "IO_ERROR", "message": "uncertain"},
            },
        )

    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid", "test-token", transport=httpx.MockTransport(fault)
    ) as client:
        result = append_once(client, log, req)
        assert (
            result["state"]
            == {
                "unknown": "commit_unknown",
                "conflict": "conflict",
                "timeout": "transport_error",
            }[kind]
        )
        assert log.summary()["pending"] == 1
        assert log.summary()["confirmed_rows"] == 0
        assert len(calls) == 1


def test_retry_requires_explicit_action_and_original_payload(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import resolve_ledger

    req = prepared()
    calls = []

    def fault(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(
                404,
                json={"error": {"code": "OPERATION_NOT_FOUND", "message": "not bound"}},
            )
        assert request.content == req.body
        return httpx.Response(200, json=committed(req))

    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid", "test-token", transport=httpx.MockTransport(fault)
    ) as client:
        log.prepare(req)
        resolve_ledger(client, log)
        assert [r.method for r in calls] == ["GET"]
        assert log.summary()["pending"] == 1
        resolve_ledger(client, log, retry=True)
        assert [r.method for r in calls] == ["GET", "GET", "POST"]
        assert log.summary()["committed"] == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org",
        "https://user:secret@example.org",
        "https://example.org/?token=secret",
        "file:///tmp/file",
    ],
)
def test_unsafe_endpoint_rejected(url):
    from glamdb_workload.runtime import validate_url

    with pytest.raises(ValueError):
        validate_url(url)


def test_result_codes():
    from glamdb_workload.runtime import status_code

    assert status_code("PASS") == 0
    assert status_code("FAIL") == 1
    assert status_code("INCOMPLETE") == 3


def test_resolve_after_crash_does_not_require_finished_worker_report(
    tmp_path, monkeypatch
):
    from glamdb_workload import runtime
    from glamdb_workload.__main__ import _bench, parser
    from glamdb_workload.ledger import Ledger

    req = prepared()
    config = {
        "url": "https://test.invalid",
        "mode": "write",
        "database": str(uuid4()),
        "collection": req.collection_id,
        "expect_workers": 1,
        "timeout": 1,
    }
    (tmp_path / "run.json").write_text(json.dumps(config))
    with Ledger(tmp_path / "worker-crashed.sqlite") as log:
        log.prepare(req)
    monkeypatch.setattr(
        runtime,
        "client_for",
        lambda config: Client(
            config["url"],
            "test-token",
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json=committed(req))
            ),
        ),
    )
    args = parser().parse_args(
        ["bench", "resolve", "--url", config["url"], "--run", str(tmp_path)]
    )
    assert _bench(args) == 0
    with Ledger(tmp_path / "worker-crashed.sqlite") as log:
        assert log.summary()["committed"] == 1


@pytest.mark.parametrize("mismatch", ["hash", "rows"])
def test_lookup_cannot_confirm_a_different_payload_after_conflict(tmp_path, mismatch):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import resolve_ledger

    req = prepared()
    other = committed(req)
    if mismatch == "hash":
        other["result"]["payload_hash"] = "f" * 64
    else:
        other["result"]["rows_appended"] = "2"
    calls = []

    def lookup(request):
        calls.append(request.method)
        return httpx.Response(200, json=other)

    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid", "test-token", transport=httpx.MockTransport(lookup)
    ) as client:
        log.prepare(req)
        log.mark(req.operation_id, "conflict", error_code="OPERATION_CONFLICT")
        resolve_ledger(client, log, retry=True)
        assert log.summary()["committed"] == 0
        assert log.summary()["pending"] == 1
        assert (
            log.summary()["conflict" if mismatch == "hash" else "protocol_error"] == 1
        )
        assert calls == ["GET"]


@pytest.mark.parametrize(
    "body",
    [b'{"error":{"code":"IO_ERROR","message":"unknown write"}}', b"invalid response"],
)
def test_sdk_unknown_write_errors_remain_commit_unknown(tmp_path, body):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import append_once

    req = prepared()
    with Ledger(tmp_path / "worker.sqlite") as log, Client(
        "https://test.invalid",
        "test-token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                500, content=body, headers={"Content-Type": "application/json"}
            )
        ),
    ) as client:
        result = append_once(client, log, req)
        assert result["state"] == "commit_unknown"
        assert log.summary()["commit_unknown"] == 1
        assert log.summary()["pending"] == 1
