import json
from uuid import uuid4

import pytest
from glamdb_sdk import AppendRequest, Field, Schema


def request():
    return AppendRequest.prepare(
        str(uuid4()),
        str(uuid4()),
        Schema([Field("key", "string")]),
        [{"key": "🎀"}, {"key": "🎀"}],
    )


def test_prepare_survives_restart_and_exact_retry_bytes(tmp_path):
    from glamdb_workload.ledger import Ledger

    req = request()
    path = tmp_path / "worker.sqlite"
    with Ledger(path) as log:
        log.prepare(req)
    with Ledger(path) as log:
        assert log.summary()["pending"] == 1
        saved = log.request(req.operation_id)
        assert saved == req and saved.body == req.body
        assert log.summary()["confirmed_rows"] == 0


def test_unique_terminal_count_and_conflict_never_overwrites(tmp_path):
    from glamdb_workload.ledger import Ledger

    req = request()
    with Ledger(tmp_path / "worker.sqlite") as log:
        log.prepare(req)
        log.mark(req.operation_id, "commit_unknown")
        assert log.summary()["pending"] == 1
        log.mark(req.operation_id, "committed")
        log.mark(req.operation_id, "committed")
        summary = log.summary()
        assert summary["committed"] == 1 and summary["confirmed_rows"] == 2
        assert summary["confirmed_bytes"] == len(req.body)
        with pytest.raises(ValueError):
            log.mark(req.operation_id, "definitely_not_committed")
        bad = AppendRequest(req.collection_id, req.operation_id, "0" * 64, b"bad")
        with pytest.raises(ValueError):
            log.prepare(bad)
        assert log.request(req.operation_id) == req


def test_broken_local_payload_detected(tmp_path):
    from glamdb_workload.ledger import Ledger

    req = request()
    with Ledger(tmp_path / "worker.sqlite") as log:
        log.prepare(req)
        log.db.execute("UPDATE operations SET body=?", (b"corrupt",))
        log.db.commit()
        with pytest.raises(ValueError, match="hash"):
            log.request(req.operation_id)


def test_worker_summary_counts_shared_operation_once(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import summarize

    req = request()
    for name in ("worker-a.sqlite", "worker-b.sqlite"):
        with Ledger(tmp_path / name) as log:
            log.prepare(req)
            log.attempted(req.operation_id)
            log.mark(req.operation_id, "committed")
    summary = summarize(tmp_path)
    assert summary["committed"] == 1 and summary["confirmed_rows"] == 2
    assert summary["attempts"] == 2


def test_pending_report_retains_ids_but_not_payload(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.runtime import summarize

    req = request()
    with Ledger(tmp_path / "worker-a.sqlite") as log:
        log.prepare(req)
        log.mark(req.operation_id, "transport_error")
    summary = summarize(tmp_path)
    path = tmp_path / summary["pending_operations_file"]
    row = json.loads(path.read_text())
    assert row["operation_id"] == req.operation_id
    assert row["state"] == "transport_error"
    assert "body" not in row and "rows" not in row
