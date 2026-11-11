"""One durable SQLite journal per Locust worker; exact prepared SDK bodies."""

import hashlib
import json
import sqlite3
from pathlib import Path

from glamdb_sdk import AppendRequest

STATES = (
    "prepared",
    "committed",
    "definitely_not_committed",
    "commit_unknown",
    "transport_error",
    "protocol_error",
    "conflict",
    "in_progress",
    "not_found",
)
TERMINAL = {"committed", "definitely_not_committed"}


class Ledger:
    def __init__(self, path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=EXTRA")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS operations (
                operation_id TEXT PRIMARY KEY, collection_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL, body BLOB NOT NULL, body_hash TEXT NOT NULL,
                rows INTEGER NOT NULL, bytes INTEGER NOT NULL,
                state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                error_code TEXT, outcome TEXT);
        """)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    def prepare(self, request):
        digest = hashlib.sha256(request.body).hexdigest()
        old = self.db.execute(
            "SELECT * FROM operations WHERE operation_id=?", (request.operation_id,)
        ).fetchone()
        if old is not None:
            if (old["body_hash"], old["payload_hash"], old["collection_id"]) != (
                digest,
                request.payload_hash,
                request.collection_id,
            ):
                raise ValueError("operation identity conflicts with immutable payload")
            return
        rows = json.loads(request.body)["rows"]
        with self.db:
            self.db.execute(
                "INSERT INTO operations VALUES (?,?,?,?,?,?,?, ?,0,NULL,NULL)",
                (
                    request.operation_id,
                    request.collection_id,
                    request.payload_hash,
                    request.body,
                    digest,
                    len(rows),
                    len(request.body),
                    "prepared",
                ),
            )

    def request(self, operation_id):
        row = self.db.execute(
            "SELECT * FROM operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if row is None:
            raise ValueError("unknown journal operation")
        if hashlib.sha256(row["body"]).hexdigest() != row["body_hash"]:
            raise ValueError("journal payload hash mismatch")
        return AppendRequest(
            row["collection_id"], row["operation_id"], row["payload_hash"], row["body"]
        )

    def attempted(self, operation_id):
        with self.db:
            self.db.execute(
                "UPDATE operations SET attempts=attempts+1 WHERE operation_id=?",
                (operation_id,),
            )

    def mark(self, operation_id, state, *, error_code=None, outcome=None):
        if state not in STATES:
            raise ValueError("unsupported operation state")
        old = self.db.execute(
            "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if old is None or (old["state"] in TERMINAL and state != old["state"]):
            raise ValueError("terminal operation cannot be replaced")
        with self.db:
            self.db.execute(
                "UPDATE operations SET state=?,error_code=?,outcome=? WHERE operation_id=?",
                (
                    state,
                    error_code,
                    None if outcome is None else json.dumps(outcome),
                    operation_id,
                ),
            )

    def operations(self, *, pending=False):
        where = (
            "WHERE state NOT IN ('committed','definitely_not_committed')"
            if pending
            else ""
        )
        return self.db.execute(
            "SELECT operation_id,state,rows,bytes,attempts,error_code FROM operations "
            + where
            + " ORDER BY rowid"
        )

    def summary(self):
        summary = dict.fromkeys(STATES, 0)
        summary.update(
            unique_operations=0,
            attempts=0,
            confirmed_rows=0,
            confirmed_bytes=0,
            pending=0,
        )
        for row in self.db.execute(
            "SELECT state,count(*) n,sum(rows) rows,sum(bytes) bytes,sum(attempts) attempts FROM operations GROUP BY state"
        ):
            summary[row["state"]] = row["n"]
            summary["unique_operations"] += row["n"]
            summary["attempts"] += row["attempts"]
            if row["state"] == "committed":
                summary["confirmed_rows"] += row["rows"]
                summary["confirmed_bytes"] += row["bytes"]
            elif row["state"] not in TERMINAL:
                summary["pending"] += row["n"]
        return summary
