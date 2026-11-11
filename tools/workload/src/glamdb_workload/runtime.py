"""SDK scenarios and conservative write outcome accounting."""

import hashlib
import hmac
import ipaddress
import json
import os
import ssl
import sqlite3
import tempfile
import time
from importlib.metadata import version
from pathlib import Path
from urllib.parse import urlsplit

from glamdb_sdk import (
    AppendOutcome,
    Client,
    HTTPError,
    OutcomeUnknownError,
    ProtocolError,
    TransportError,
)

from .ledger import Ledger, STATES


def validate_url(url):
    parts = urlsplit(url)
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ValueError(
            "use an HTTP(S) base URL without credentials, query or fragment"
        )
    if parts.scheme == "http":
        try:
            local = ipaddress.ip_address(parts.hostname).is_loopback
        except ValueError:
            local = parts.hostname == "localhost"
        if not local:
            raise ValueError(
                "remote endpoints require verified HTTPS; HTTP is loopback-only"
            )
    return url.rstrip("/")


def client_for(config, *, scoped=True, transport=None):
    token = os.environ.get("GLAMDB_TOKEN")
    if not token:
        raise ValueError("set GLAMDB_TOKEN in the environment")
    verify = (
        ssl.create_default_context(cafile=config["ca_file"])
        if config.get("ca_file")
        else True
    )
    return Client(
        validate_url(config["url"]),
        token,
        verify=verify,
        timeout=config["timeout"],
        database_id=config["database"] if scoped else None,
        transport=transport,
    )


def status_code(status):
    return {"PASS": 0, "FAIL": 1, "INCOMPLETE": 3}[status]


def error_code(error):
    if isinstance(error, HTTPError):
        code = error.detail.code
        # Keep diagnostics bounded and never include response/request text.
        return (
            code
            if code.isascii() and code.replace("_", "").isalnum() and len(code) <= 64
            else "HTTP_ERROR"
        )
    if isinstance(error, TransportError):
        return "TRANSPORT_ERROR"
    if isinstance(error, ProtocolError):
        return "PROTOCOL_ERROR"
    return type(error).__name__


def _save_outcome(log, request, outcome):
    code = outcome.error.code if outcome.error else None
    state = outcome.outcome
    if state == "committed":
        if not hmac.compare_digest(
            outcome.result["payload_hash"].lower(), request.payload_hash.lower()
        ):
            state, code = "conflict", "OPERATION_CONFLICT"
        elif int(outcome.result["rows_appended"]) != len(request.to_dict()["rows"]):
            state, code = "protocol_error", "RECEIPT_ROW_COUNT_MISMATCH"
    log.mark(
        request.operation_id,
        state,
        error_code=code,
        outcome={
            "server_outcome": outcome.outcome,
            "phase": outcome.phase,
            "operation_id": request.operation_id,
        },
    )
    return state, code


def append_once(client, log, request, *, deadline=None):
    """Journal before sending; one SDK call and no automatic append retry."""
    log.prepare(request)
    log.attempted(request.operation_id)
    started = time.perf_counter()
    try:
        outcome = client.append(request, deadline=deadline)
    except (TransportError, ProtocolError, HTTPError) as error:
        elapsed = (time.perf_counter() - started) * 1000
        code = error_code(error)
        state = (
            "commit_unknown"
            if isinstance(error, OutcomeUnknownError)
            else (
                "conflict"
                if code in {"OPERATION_CONFLICT", "IDEMPOTENCY_CONFLICT"}
                else (
                    "transport_error"
                    if isinstance(error, TransportError)
                    else "protocol_error"
                )
            )
        )
        log.mark(request.operation_id, state, error_code=code)
        return {"state": state, "error_code": code, "response_time_ms": elapsed}
    except BaseException:
        log.mark(request.operation_id, "transport_error", error_code="INTERRUPTED")
        raise
    elapsed = (time.perf_counter() - started) * 1000
    state, code = _save_outcome(log, request, outcome)
    return {
        "state": state,
        "error_code": code,
        "response_time_ms": elapsed,
    }


def resolve_ledger(client, log, *, retry=False, deadline=None):
    """Look up pending IDs. Only --retry resends, with verified original bytes."""
    for row in log.operations(pending=True):
        operation_id = row["operation_id"]
        request = log.request(operation_id)
        try:
            outcome = client.find_append_outcome(
                request.collection_id, operation_id, deadline=deadline
            )
        except (TransportError, ProtocolError, HTTPError) as error:
            log.mark(
                operation_id,
                (
                    "transport_error"
                    if isinstance(error, TransportError)
                    else "protocol_error"
                ),
                error_code=error_code(error),
            )
            continue
        if isinstance(outcome, AppendOutcome):
            _save_outcome(log, request, outcome)
            # A known terminal outcome is never re-sent. Unknown still requires
            # operator recovery; lookup is not an automatic reconciliation.
        elif outcome is None:
            log.mark(operation_id, "not_found")
            if retry:
                append_once(client, log, request, deadline=deadline)
        else:
            log.mark(operation_id, "in_progress")
    return log.summary()


def preflight(config, *, dataset=None):
    with client_for(config, scoped=False) as root:
        capabilities = root.capabilities(deadline=config["timeout"])
        database = root.get_database(config["database"], deadline=config["timeout"])
    features = capabilities.get("features", {})
    required = ["database_management", "scan", "scan_pagination", "scan_cancel"]
    if config["mode"] == "write":
        required.append("append")
    if config.get("format") == "arrow":
        required.append("arrow_export")
    if any(features.get(key) is not True for key in required):
        raise ValueError("server does not advertise the required scenario capabilities")
    if database.get("state") != "open":
        raise ValueError(
            "database must already be open; no automatic open or reconciliation"
        )
    with client_for(config) as client:
        collection = client.get_collection(
            config["collection"], deadline=config["timeout"]
        )
        schema = client.get_schema(config["collection"], deadline=config["timeout"])
        if dataset is not None and schema != dataset.schema:
            raise ValueError("dataset schema differs from the collection schema")
        if config["mode"] == "write" and not config.get("worker"):
            with client.scan_pages(
                config["collection"], limit=1, deadline=config["timeout"]
            ) as pages:
                if any(page.rows for page in pages):
                    raise ValueError(
                        "write benchmark requires a dedicated empty collection for exact verification"
                    )
    query = config.get("query", {})
    if set(query) - {"projection", "predicate", "limit", "snapshot_id"}:
        raise ValueError(
            "unsupported query option; use SDK projection/predicate/limit/snapshot_id"
        )
    predicate = query.get("predicate")
    if predicate is not None and predicate.get("op") not in capabilities.get(
        "predicate_ops", []
    ):
        raise ValueError("predicate is not advertised by this server")
    return {
        "capabilities": capabilities,
        "database": database,
        "collection": collection,
        "schema": schema.to_dict(),
        "server_version": capabilities.get("server_version"),
        "sdk_version": version("glamdb-sdk"),
        "sdk_client_sha256": hashlib.sha256(
            Path(
                __import__("glamdb_sdk.client", fromlist=["Client"]).__file__
            ).read_bytes()
        ).hexdigest(),
    }


def summarize(run):
    """Aggregate counts, never worker percentiles; the latter belong to Locust."""
    result = dict.fromkeys(
        (
            *STATES,
            "unique_operations",
            "attempts",
            "confirmed_rows",
            "confirmed_bytes",
            "pending",
        ),
        0,
    )
    journals = sorted(Path(run).glob("worker-*.sqlite"))
    with tempfile.TemporaryDirectory(prefix="summary-", dir=run) as temporary:
        db = sqlite3.connect(Path(temporary) / "unique.sqlite")
        try:
            db.execute("PRAGMA cache_size=-2048")
            db.execute(
                "CREATE TABLE ops (id TEXT PRIMARY KEY, collection TEXT, hash TEXT, body_hash TEXT, rows INTEGER, bytes INTEGER, state TEXT, attempts INTEGER)"
            )
            for path in journals:
                with Ledger(path) as log:
                    for row in log.db.execute(
                        "SELECT operation_id,collection_id,payload_hash,body_hash,rows,bytes,state,attempts FROM operations"
                    ):
                        previous = db.execute(
                            "SELECT * FROM ops WHERE id=?", (row["operation_id"],)
                        ).fetchone()
                        values = tuple(row)
                        if previous is None:
                            db.execute(
                                "INSERT INTO ops VALUES (?,?,?,?,?,?,?,?)", values
                            )
                        else:
                            if previous[1:6] != values[1:6]:
                                raise ValueError(
                                    "operation identity differs between worker journals"
                                )
                            if previous[6] in {"committed", "definitely_not_committed"}:
                                if (
                                    values[6]
                                    in {"committed", "definitely_not_committed"}
                                    and previous[6] != values[6]
                                ):
                                    raise ValueError(
                                        "contradictory terminal operation outcomes"
                                    )
                                state = previous[6]
                            else:
                                state = values[6]
                            db.execute(
                                "UPDATE ops SET state=?,attempts=attempts+? WHERE id=?",
                                (state, values[7], values[0]),
                            )
            for state, count, rows, size, attempts in db.execute(
                "SELECT state,count(*),sum(rows),sum(bytes),sum(attempts) FROM ops GROUP BY state"
            ):
                result[state] = count
                result["unique_operations"] += count
                result["attempts"] += attempts
                if state == "committed":
                    result["confirmed_rows"] += rows
                    result["confirmed_bytes"] += size
                elif state != "definitely_not_committed":
                    result["pending"] += count
            pending = Path(run) / "pending.jsonl"
            temporary_pending = pending.with_suffix(".jsonl.tmp")
            with temporary_pending.open("w", encoding="utf-8") as output:
                for operation_id, collection, state in db.execute(
                    "SELECT id,collection,state FROM ops WHERE state NOT IN ('committed','definitely_not_committed') ORDER BY id"
                ):
                    output.write(
                        json.dumps(
                            {
                                "operation_id": operation_id,
                                "collection_id": collection,
                                "state": state,
                            }
                        )
                        + "\n"
                    )
            temporary_pending.replace(pending)
        finally:
            db.close()
    result["journals"] = [p.name for p in journals]
    result["pending_operations_file"] = "pending.jsonl"
    return result
