"""Bounded immutable source-to-request mappings in the existing SQLite journal."""

import hashlib
import json
from uuid import UUID, uuid5

from glamdb_sdk.errors import ValidationError
from glamdb_sdk.request import ANY, json_bytes

from .target import KEY_ENCODING, collection_mode, prepare_request


def _digest(log):
    digest = hashlib.sha256()
    for row in log.db.execute("SELECT * FROM clickbench_chunks ORDER BY ordinal"):
        digest.update(json_bytes(tuple(row)) + b"\n")
    return digest.hexdigest()


def request_for(data, saved, chunk, snapshot=ANY):
    rows = data.read_part(chunk["part_ordinal"])
    start, end = chunk["start_row"], chunk["end_row"]
    if not 0 <= start < end <= len(rows) or end - start != chunk["rows"]:
        raise ValueError("immutable plan range differs from its source part")
    if saved.get("request_plan_version", 2) == 1:
        snapshot = ANY
    return prepare_request(saved, data.schema, chunk["operation_id"], rows[start:end],
                           chunk["row_offset"], snapshot)


def freeze_plan(log, data, saved, *, batch_rows=10000, batch_bytes=8 * 1024**2, workers=1):
    if type(batch_rows) is not int or not 1 <= batch_rows <= 10000:
        raise ValueError("batch_rows must be between one and 10000")
    if type(batch_bytes) is not int or not 1 <= batch_bytes <= 16 * 1024**2:
        raise ValueError("batch_bytes must be between one and 16 MiB")
    if type(workers) is not int or not 1 <= workers <= 64:
        raise ValueError("workers must be between one and 64")
    log.db.executescript("""
        CREATE TABLE IF NOT EXISTS clickbench_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS clickbench_chunks (
            ordinal INTEGER PRIMARY KEY, part_ordinal INTEGER NOT NULL,
            start_row INTEGER NOT NULL, end_row INTEGER NOT NULL, row_offset INTEGER NOT NULL,
            operation_id TEXT UNIQUE NOT NULL, rows INTEGER NOT NULL,
            wire_bytes INTEGER NOT NULL, payload_hash TEXT NOT NULL, worker INTEGER NOT NULL);
    """)
    config = {"dataset_hash": data.digest, "load_id": saved["load_id"],
              "collection_id": saved["collection_id"], "collection_mode": collection_mode(saved),
              "request_plan_version": saved.get("request_plan_version", 2),
              "batch_rows": batch_rows, "batch_bytes": batch_bytes, "workers": workers}
    if collection_mode(saved) == "kv":
        config["key_encoding"] = KEY_ENCODING
    old = log.db.execute("SELECT value FROM clickbench_meta WHERE key='plan'").fetchone()
    if old is not None:
        previous = json.loads(old[0])
        if previous["config"] != config or previous["summary"]["sha256"] != _digest(log):
            raise ValueError("load plan differs from its immutable original configuration or chunks")
        if saved.get("append_plan", previous["summary"]) != previous["summary"]:
            raise ValueError("load plan differs from the original run state")
        # Check unloaded parts too: resuming must not publish a valid prefix
        # before discovering that a later source part changed.
        for _ in data.iter_parts():
            pass
        return previous["summary"]
    summary = dict(config, rows=0, operations=0, wire_bytes=0, max_request_body_bytes=0)
    offset = 0
    with log.db:
        for part, rows in data.iter_parts():
            start = 0
            while start < len(rows):
                end = min(len(rows), start + batch_rows)
                while True:
                    legacy = config["request_plan_version"] == 1
                    identity = (f'{saved["collection_id"]}:{data.digest}:{part}' if legacy else
                                f'{saved["collection_id"]}:{data.digest}:{part}:{start}:{end}')
                    operation = str(uuid5(UUID(saved["load_id"]), identity))
                    try:
                        request = prepare_request(saved, data.schema, operation, rows[start:end],
                                                  offset + start, ANY if legacy else -9223372036854775808)
                        if len(request.body) <= batch_bytes:
                            break
                    except ValidationError:
                        if end - start == 1:
                            raise
                    if legacy:
                        raise ValueError("legacy run requests cannot be split; retain original part boundaries")
                    if end - start == 1:
                        raise ValueError("one source row exceeds the frozen request byte budget")
                    end = start + max(1, (end - start) // 2)
                ordinal = summary["operations"]
                log.db.execute("INSERT INTO clickbench_chunks VALUES (?,?,?,?,?,?,?,?,?,?)",
                               (ordinal, part, start, end, offset + start, operation, end - start,
                                len(request.body), request.payload_hash, ordinal % workers))
                summary["operations"] += 1
                summary["rows"] += end - start
                summary["wire_bytes"] += len(request.body)
                summary["max_request_body_bytes"] = max(summary["max_request_body_bytes"], len(request.body))
                start = end
            offset += len(rows)
        summary["sha256"] = _digest(log)
        log.db.execute("INSERT INTO clickbench_meta VALUES ('plan',?)",
                       (json.dumps({"config": config, "summary": summary}),))
    return summary


def capacity_preflight(limits, plan, *, max_journal_bytes):
    """Count lower bounds; compressed physical bytes remain server admission."""
    operations = plan["operations"] + 1  # Initial collection creation is permanent.
    for name, needed, default in (("max_collections", 1, 8), ("max_operations", operations, 4096)):
        value = limits.get(name, default)
        if type(value) is not int or value < needed:
            raise ValueError(f"insufficient {name}: requires at least {needed}; see #156")
    history = limits.get("history", {})
    snapshots = plan["operations"]
    for name, needed, default in (("max_snapshots", snapshots, 4096),
                                  ("max_data_files", snapshots, 16384),
                                  ("max_manifests", snapshots, 4096),
                                  ("max_recovery_workspace_bytes", 8192 * (1 + 3 * snapshots), 256 * 1024**2)):
        value = history.get(name, default)
        if type(value) is not int or value < needed:
            raise ValueError(f"insufficient history.{name}: requires at least {needed}; see #156")
    if plan["wire_bytes"] > max_journal_bytes:
        raise ValueError(f'journal payload budget requires at least {plan["wire_bytes"]} bytes, excluding SQLite overhead')
    return {"operations_required": operations, "snapshots_required": snapshots,
            "recovery_workspace_bytes_required_at_least": 8192 * (1 + 3 * snapshots),
            "journal_payload_bytes_required": plan["wire_bytes"],
            "physical_byte_admission": "server enforced; compressed bytes cannot be predicted exactly"}
