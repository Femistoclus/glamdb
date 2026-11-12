"""Independent source multiset and immutable request/receipt verification."""

import json
from pathlib import Path
import sqlite3
import tempfile

from glamdb_sdk import AppendOutcome
from glamdb_sdk.request import ANY

from ..verification import _row_key, scan_chunks
from ..write_runtime import _receipt_matches_rows, record_outcome
from .plan import request_for
from .target import collection_mode, target_rows, target_schema


def hydrate_legacy_receipts(client, log, data, saved, *, timeout):
    """Old append journals omitted result details; restore them by lookup only."""
    if saved.get("request_plan_version") != 1:
        return
    for op in log.db.execute("SELECT * FROM operations WHERE state='committed' ORDER BY rowid"):
        if "result" in json.loads(op["outcome"] or "{}"):
            continue
        chunk = log.db.execute("SELECT * FROM clickbench_chunks WHERE operation_id=?",
                               (op["operation_id"],)).fetchone()
        if chunk is None:
            raise ValueError("legacy journal contains an operation outside its source plan")
        request = log.request(op["operation_id"])
        if request != request_for(data, saved, chunk):
            raise ValueError("legacy request differs from its immutable source")
        outcome = client.find_append_outcome(request.collection_id, request.operation_id, deadline=timeout)
        if (not isinstance(outcome, AppendOutcome) or outcome.outcome != "committed" or
                outcome.operation_id != request.operation_id or outcome.result.get("payload_hash") != request.payload_hash or
                not _receipt_matches_rows(request, outcome.result)):
            raise ValueError("legacy committed request has no matching authoritative receipt")
        record_outcome(log, request, outcome, authoritative=True)


def validate_journal(log, data, saved, *, client=None, timeout=30):
    """Rebuild each saved body from its exact source range before any retry."""
    previous_snapshot = None
    ordinal = 0
    pending = False
    for op in log.db.execute("SELECT * FROM operations ORDER BY rowid"):
        chunk = log.db.execute("SELECT * FROM clickbench_chunks WHERE operation_id=?",
                               (op["operation_id"],)).fetchone()
        if chunk is None:
            raise ValueError("journal contains an operation outside the immutable load plan")
        if pending or chunk["ordinal"] != ordinal:
            raise ValueError("journal source request order differs from the immutable load plan")
        request = log.request(op["operation_id"])
        # Payload hashes deliberately exclude preconditions. Derive the exact
        # expected body independently from preceding committed receipts.
        snapshot = ANY if saved.get("request_plan_version") == 1 else previous_snapshot
        rebuilt = request_for(data, saved, chunk, snapshot)
        if (request != rebuilt or request.payload_hash != chunk["payload_hash"] or
                op["rows"] != chunk["rows"] or op["bytes"] != len(request.body)):
            raise ValueError("journal body or accounting differs from its immutable source request")
        if op["state"] == "committed":
            receipt = json.loads(op["outcome"] or "{}")
            if not _receipt_matches_rows(request, receipt.get("result") or {}):
                raise ValueError("journal committed receipt differs from the source rows")
            if client is not None:
                actual = client.find_append_outcome(request.collection_id, request.operation_id, deadline=timeout)
                if (not isinstance(actual, AppendOutcome) or actual.outcome != "committed" or
                        actual.operation_id != request.operation_id or actual.result != receipt["result"]):
                    raise ValueError("authoritative receipt differs from the immutable journal")
            previous_snapshot = receipt["result"]["snapshot_id"]
        else:
            pending = True
        ordinal += 1


def verify_contents(client, log, data, saved, *, format="arrow", timeout=120):
    if format not in ("json", "arrow"):
        raise ValueError("verification format must be json or arrow")
    totals = log.summary()
    plan = saved["append_plan"]
    if (totals["pending"] or totals["definitely_not_committed"] or
            totals["committed"] != plan["operations"] or totals["confirmed_rows"] != data.manifest["rows"]):
        return {"status": "INCOMPLETE", "reason": "complete source/receipt inventory is required",
                "operations": totals}
    validate_journal(log, data, saved, client=client, timeout=timeout)
    mode = collection_mode(saved)
    schema = target_schema(data.schema, mode)
    if client.get_schema(saved["collection_id"], deadline=timeout) != schema:
        raise ValueError("target schema differs from the immutable source/mode")
    expected = actual = extra = 0
    with tempfile.TemporaryDirectory(prefix="verify-source-", dir=saved["run_dir"]) as temporary:
        db = sqlite3.connect(Path(temporary) / "multiset.sqlite")
        try:
            db.execute("PRAGMA cache_size=-8192")
            db.execute("PRAGMA temp_store=FILE")
            db.execute("PRAGMA journal_mode=OFF")
            db.execute("CREATE TABLE counts (row BLOB PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID")
            for _, rows in data.iter_parts():
                keyed = target_rows(rows, expected, mode)
                with db:
                    db.executemany("INSERT INTO counts VALUES (?,1) ON CONFLICT(row) DO UPDATE SET n=n+1",
                                   ((_row_key(schema, row),) for row in keyed))
                expected += len(rows)
            for rows in scan_chunks(client, saved["collection_id"], format=format, deadline=timeout,
                                    query={"snapshot_id": saved["snapshot_id"]}):
                with db:
                    for row in rows:
                        actual += 1
                        if not db.execute("UPDATE counts SET n=n-1 WHERE row=? AND n>0",
                                          (_row_key(schema, row),)).rowcount:
                            extra += 1
            missing = db.execute("SELECT coalesce(sum(n),0) FROM counts").fetchone()[0]
        finally:
            db.close()
    return {"status": "PASS" if not missing and not extra and expected == data.manifest["rows"] else "FAIL",
            "expected_rows": expected, "actual_rows": actual, "missing_rows": missing, "extra_rows": extra,
            "format": format, "operations": totals, "collection_mode": mode,
            "source_columns": len(data.schema.fields), "physical_columns": len(schema.fields),
            "verification_scope": "complete source multiset, derived keys and every original request/receipt"}
