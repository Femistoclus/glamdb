"""Bounded unordered comparison, including duplicate multiplicities."""

import json
import sqlite3
import tempfile
from pathlib import Path

from glamdb_sdk.codec import encode_rows

from .ledger import Ledger
from .runtime import summarize


def _row_key(schema, row):
    return json.dumps(
        encode_rows(schema, [row])[0],
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def scan_chunks(client, collection_id, *, format="json", query=None, deadline=None):
    if format == "json":
        with client.scan_pages(
            collection_id, **(query or {}), deadline=deadline
        ) as pages:
            for page in pages:
                yield page.rows
    else:
        with client.scan_batches(
            collection_id, **(query or {}), deadline=deadline
        ) as batches:
            for batch in batches:
                yield batch.to_pylist()


def verify_ledgers(client, run, schema, collection_id, *, format="json", deadline=None):
    totals = summarize(run)
    if not totals["journals"] or totals["pending"]:
        return {
            "status": "INCOMPLETE",
            "reason": "missing journal or unresolved operations",
            "operations": totals,
        }
    expected = actual = extra = 0
    with tempfile.TemporaryDirectory(prefix="verify-", dir=run) as temporary:
        db = sqlite3.connect(Path(temporary) / "counts.sqlite")
        try:
            db.execute("PRAGMA cache_size=-2048")
            db.execute("PRAGMA temp_store=FILE")
            db.execute(
                "CREATE TABLE counts (row BLOB PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID"
            )
            db.execute("CREATE TABLE included_ops (id TEXT PRIMARY KEY) WITHOUT ROWID")
            for name in totals["journals"]:
                with Ledger(Path(run) / name) as log:
                    for op in log.operations():
                        if op["state"] != "committed":
                            continue
                        req = log.request(op["operation_id"])
                        if req.collection_id != collection_id:
                            raise ValueError("journal collection differs from the run")
                        with db:
                            included = db.execute(
                                "INSERT INTO included_ops VALUES (?) ON CONFLICT(id) DO NOTHING",
                                (req.operation_id,),
                            )
                            if not included.rowcount:
                                continue
                            for row in req.to_dict()["rows"]:
                                db.execute(
                                    "INSERT INTO counts VALUES (?,1) ON CONFLICT(row) DO UPDATE SET n=n+1",
                                    (_row_key(schema, row),),
                                )
                                expected += 1
            for chunk in scan_chunks(
                client, collection_id, format=format, deadline=deadline
            ):
                with db:
                    for row in chunk:
                        actual += 1
                        changed = db.execute(
                            "UPDATE counts SET n=n-1 WHERE row=? AND n>0",
                            (_row_key(schema, row),),
                        )
                        if not changed.rowcount:
                            extra += 1
            missing = db.execute("SELECT coalesce(sum(n),0) FROM counts").fetchone()[0]
        finally:
            db.close()
    failed = bool(missing or extra or totals["definitely_not_committed"])
    return {
        "status": "FAIL" if failed else "PASS",
        "expected_rows": expected,
        "actual_rows": actual,
        "missing_rows": missing,
        "extra_rows": extra,
        "operations": totals,
        "format": format,
    }
