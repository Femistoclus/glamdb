from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from glamdb_sdk import AppendRequest, Field, Schema


class ScanDouble:
    """Explicit small scan fixture; native E2E uses the server process instead."""

    def __init__(self, schema, pages):
        self.schema, self.pages, self.closed = schema, pages, False

    @contextmanager
    def scan_pages(self, *args, **kwargs):
        try:
            yield iter(SimpleNamespace(rows=rows) for rows in self.pages)
        finally:
            self.closed = True


def test_unordered_verification_retains_duplicate_multiplicity(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.verification import verify_ledgers

    schema = Schema([Field("key", "string"), Field("n", "int64", True)])
    cid = str(uuid4())
    rows = [
        {"key": "🎀", "n": 9007199254740993},
        {"key": "🎀", "n": 9007199254740993},
        {"key": "a", "n": None},
    ]
    req = AppendRequest.prepare(cid, str(uuid4()), schema, rows)
    with Ledger(tmp_path / "worker-a.sqlite") as log:
        log.prepare(req)
        log.mark(req.operation_id, "committed")
    client = ScanDouble(schema, [[rows[2]], rows[:2]])
    result = verify_ledgers(client, tmp_path, schema, cid)
    assert (
        result["status"] == "PASS"
        and result["expected_rows"] == result["actual_rows"] == 3
    )
    assert client.closed
    client = ScanDouble(schema, [[rows[2]], rows[:1]])
    result = verify_ledgers(client, tmp_path, schema, cid)
    assert result["status"] == "FAIL" and result["missing_rows"] == 1


def test_pending_is_incomplete_and_does_not_claim_failure_or_scan(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.verification import verify_ledgers

    schema = Schema([Field("key", "string")])
    cid = str(uuid4())
    req = AppendRequest.prepare(cid, str(uuid4()), schema, [{"key": "a"}])
    with Ledger(tmp_path / "worker-a.sqlite") as log:
        log.prepare(req)
    client = ScanDouble(schema, [])
    result = verify_ledgers(client, tmp_path, schema, cid)
    assert result["status"] == "INCOMPLETE" and not client.closed


def test_verification_counts_one_committed_id_across_two_journals_once(tmp_path):
    from glamdb_workload.ledger import Ledger
    from glamdb_workload.verification import verify_ledgers

    schema = Schema([Field("key", "string")])
    cid = str(uuid4())
    rows = [{"key": "one server append"}]
    req = AppendRequest.prepare(cid, str(uuid4()), schema, rows)
    for name in ("worker-a.sqlite", "worker-b.sqlite"):
        with Ledger(tmp_path / name) as log:
            log.prepare(req)
            log.mark(req.operation_id, "committed")
    result = verify_ledgers(ScanDouble(schema, [rows]), tmp_path, schema, cid)
    assert result["status"] == "PASS"
    assert result["expected_rows"] == result["actual_rows"] == 1
