"""Disposable synthetic state. No warehouse, catalog, filesystem writes or durability."""

import base64
import copy
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass, field
from threading import RLock
from uuid import UUID, uuid4, uuid5

from glamdb_verify import append_hash, canonical_append, normalize_rows, normalize_schema, scan_rows


MOCK = {"backend": "mock", "mock": True, "durability_validation": "designed"}
HEALTH = {
    'state': 'ready', 'read_available': True, 'write_ready': True,
    'storage_failure_latched': False, 'durability_qualified': False, **MOCK,
}
CAPABILITIES = {
    'health': HEALTH,
    "scan_pagination": False,
    **MOCK, "mode": "append", "key_unique": False, "cow": False,
    "deletes": False, "compaction": False, "async_admission": False,
    "page_pruning": False, "bloom_pruning": False, "file_pruning": False,
    "row_group_pruning": False, "snapshots": True, "arrow_ipc": True,
    "storage": "disposable_in_memory", "production_writer": False,
}


class MockError(Exception):
    def __init__(self, status, code, message, retry_hint=None):
        super().__init__(message)
        self.status = status
        self.detail = {"code": code, "message": message}
        if retry_hint is not None:
            self.detail["retry_hint"] = retry_hint


def uuid_value(value, label):
    if not isinstance(value, str):
        raise MockError(422, "INVALID_ARGUMENT", f"{label} must be a UUID string")
    try:
        return str(UUID(value))
    except ValueError as error:
        raise MockError(422, "INVALID_ARGUMENT", f"{label} must be a UUID string") from error


def snapshot_value(value):
    if value is None or value == "initial":
        return None
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and value.isascii() and value.lstrip("-").isdigit():
        parsed = int(value)
        if str(parsed) != value:
            raise MockError(422, "INVALID_ARGUMENT", "snapshot_id must be a canonical decimal int64")
    else:
        raise MockError(422, "INVALID_ARGUMENT", "snapshot_id must be a decimal int64 or null")
    if not -(2**63) <= parsed < 2**63:
        raise MockError(422, "INVALID_ARGUMENT", "snapshot_id is outside int64")
    return str(parsed)


@dataclass
class Operation:
    payload_hash: str
    rows: list
    response: dict
    status: int


@dataclass
class Collection:
    collection_id: str
    name: str
    schema: dict
    creation_sequence: int
    table_uuid: str
    snapshots: list = field(default_factory=list)
    operations: dict = field(default_factory=dict)
    pending_operation: str | None = None

    @property
    def current_snapshot_id(self):
        return self.snapshots[-1]["snapshot_id"] if self.snapshots else None

    def info(self):
        return {
            "collection_id": self.collection_id, "name": self.name,
            "table_uuid": self.table_uuid, "schema": copy.deepcopy(self.schema),
            "mode": "append", "creation_sequence": self.creation_sequence,
            "current_snapshot_id": self.current_snapshot_id, **MOCK,
        }


class MockStore:
    def __init__(self, token, clock=time.time, cursor_ttl=900, max_rows=100_000, max_state_bytes=64 * 1024 * 1024):
        self.lock = RLock()
        self.collections = {}
        self.store_id = str(uuid4())
        self.issuer = str(uuid4())
        # The bearer belongs to the caller; it must not also be a derivable
        # cursor-signing key. This key and issuer disappear with mock state.
        self.secret = secrets.token_bytes(32)
        self.clock = clock
        self.cursor_ttl = cursor_ttl
        self.max_rows = max_rows
        self.max_state_bytes = max_state_bytes
        self.state_bytes = 0
        self.next_snapshot = 1

    def collection(self, collection_id):
        collection_id = uuid_value(collection_id, "collection_id")
        if collection_id not in self.collections:
            raise MockError(404, "NOT_FOUND", "collection does not exist")
        return self.collections[collection_id]

    def create(self, body):
        if set(body) - {"collection_id", "name", "schema", "mode"}:
            raise MockError(422, "INVALID_ARGUMENT", "unknown create field")
        cid = uuid_value(body.get("collection_id"), "collection_id")
        name = body.get("name")
        if not isinstance(name, str) or not name or len(name.encode()) > 128 or "\0" in name:
            raise MockError(422, "INVALID_ARGUMENT", "name must contain 1..128 UTF-8 bytes without NUL")
        if body.get("mode", "append") != "append":
            raise MockError(422, "UNSUPPORTED_OPERATION", "only append collections are available")
        schema = normalize_schema(body.get("schema"))
        if cid in self.collections:
            existing = self.collections[cid]
            if existing.name == name and existing.schema == schema:
                return existing.info(), False
            raise MockError(409, "ALREADY_EXISTS", "collection UUID is already bound to different metadata")
        if any(item.name == name for item in self.collections.values()):
            raise MockError(409, "ALREADY_EXISTS", "exact collection name is already in use")
        if len(self.collections) >= 8:
            raise MockError(429, "RESOURCE_EXHAUSTED", "mock collection limit reached")
        reservation = 4096 + len(json.dumps(schema).encode())
        if self.state_bytes + reservation > self.max_state_bytes:
            raise MockError(429, "RESOURCE_EXHAUSTED", "disposable mock metadata budget reached")
        item = Collection(cid, name, schema, len(self.collections) + 1, str(uuid5(UUID(self.store_id), cid)))
        self.collections[cid] = item
        self.state_bytes += reservation
        return item.info(), True

    def view(self, collection, selector="latest"):
        if selector == "latest":
            selector = collection.current_snapshot_id
        else:
            selector = snapshot_value(selector)
        if selector is None:
            return None, []
        for snapshot in collection.snapshots:
            if snapshot["snapshot_id"] == selector:
                return snapshot, snapshot["rows"]
        raise MockError(404, "SNAPSHOT_NOT_FOUND", "snapshot does not belong to this collection")

    def identity(self, collection, snapshot):
        sid = snapshot["snapshot_id"] if snapshot else None
        return {
            "store_id": self.store_id, "collection_id": collection.collection_id,
            "table_uuid": collection.table_uuid, "current_snapshot_id": sid,
            "snapshot_id": sid, "schema_id": collection.schema.get("schema_id", 0),
            "spec_id": 0,
            "metadata_location": f"mock://{self.store_id}/{collection.collection_id}/metadata/{sid or 'initial'}.json",
            **MOCK,
        }

    def _unknown(self, collection, operation_id, digest, base, published=None):
        error = {"code": "RECOVERY_REQUIRED", "message": "development scenario awaits explicit mock resolution",
                 "retry_hint": "SameIdAfterReconcile"}
        return {
            "outcome": "commit_unknown", "phase": "reconciling",
            "operation_id": operation_id, "recovery_required": True, **MOCK,
            "error": error,
            "recovery": {
                "store_id": self.store_id, "collection_id": collection.collection_id,
                "operation_id": operation_id, "payload_hash": digest, "hash_version": 1,
                "base": base, "attempt_id": str(uuid4()),
                "candidate_snapshot_id": published["snapshot_id"] if published else None,
                "candidate_metadata_location": published["identity"]["metadata_location"] if published else None,
                "first_error": dict(error), "publication_started": published is not None, "lookup_blocked": False,
            },
        }

    def _failure(self, operation_id, code, message):
        return {
            "outcome": "definitely_not_committed", "phase": "before_publication",
            "operation_id": operation_id, "recovery_required": False, **MOCK,
            "error": {"code": code, "message": message, "retry_hint": "new_operation_id_for_new_attempt"},
        }

    def _commit(self, collection, operation_id, digest, rows):
        previous = collection.snapshots[-1] if collection.snapshots else None
        old_rows = previous["rows"] if previous else []
        sid = str(self.next_snapshot)
        self.next_snapshot += 1
        created_at = str(int(self.clock() * 1_000_000))
        snapshot = {
            "snapshot_id": sid, "parent_id": previous["snapshot_id"] if previous else None,
            "created_at_us": created_at, "operation_id": operation_id,
            "rows_added": len(rows), "rows": old_rows + rows,
        }
        collection.snapshots.append(snapshot)
        result = {
            "collection_id": collection.collection_id, "operation_id": operation_id,
            "snapshot_id": sid, "payload_hash": digest, "hash_version": 1,
            "receipt_version": 1, "rows_appended": len(rows), "rows_total": len(snapshot["rows"]),
            "snapshot_created_at_us": created_at, "identity": self.identity(collection, snapshot),
            "durable": False, **MOCK,
        }
        response = {"outcome": "committed", "phase": "mock_publication", "result": result, "operation_id": operation_id, **MOCK}
        collection.operations[operation_id] = Operation(digest, [], response, 200)
        return response, 200

    def append(self, collection, body, scenario=None):
        if set(body) - {"operation_id", "rows", "expected_snapshot"}:
            raise MockError(422, "INVALID_ARGUMENT", "unknown append field")
        operation_id = uuid_value(body.get("operation_id"), "operation_id")
        rows = normalize_rows(collection.schema, body.get("rows"))
        digest = append_hash(collection.schema, collection.collection_id, rows)
        existing = collection.operations.get(operation_id)
        if existing is not None:
            if not hmac.compare_digest(existing.payload_hash, digest):
                raise MockError(409, "IDEMPOTENCY_CONFLICT", "operation_id is bound to a different canonical payload")
            return copy.deepcopy(existing.response), existing.status
        if collection.pending_operation:
            raise MockError(503, "RECOVERY_REQUIRED", "mock writer is blocked by an unknown operation", "resolve_original_operation")
        if len(collection.operations) >= 4096 or len(collection.snapshots) >= 4096:
            raise MockError(429, "RESOURCE_EXHAUSTED", "retained mock operation/snapshot limit reached")
        # Conservative admission estimates include owned values, Python row
        # containers, retained view pointer arrays and a terminal outcome reserve.
        # They are development ceilings, not a qualified RSS accounting profile.
        operation_reservation = 4096
        if self.state_bytes + operation_reservation > self.max_state_bytes:
            raise MockError(429, "RESOURCE_EXHAUSTED", "disposable mock operation budget reached")
        if "expected_snapshot" in body and snapshot_value(body["expected_snapshot"]) != collection.current_snapshot_id:
            response = self._failure(operation_id, "SNAPSHOT_CONFLICT", "expected snapshot does not match the current view")
            collection.operations[operation_id] = Operation(digest, [], response, 409)
            self.state_bytes += operation_reservation
            return response, 409
        count = len(collection.snapshots[-1]["rows"]) if collection.snapshots else 0
        row_reservation = len(canonical_append(collection.schema, collection.collection_id, rows)) * 4 + len(rows) * 512 + (count + len(rows)) * 8
        if count + len(rows) > self.max_rows or self.state_bytes + operation_reservation + row_reservation > self.max_state_bytes:
            response = self._failure(operation_id, "RESOURCE_EXHAUSTED", "disposable mock row or estimated memory limit reached")
            collection.operations[operation_id] = Operation(digest, [], response, 429)
            self.state_bytes += operation_reservation
            return response, 429
        if scenario == "definitely_not_committed":
            response = self._failure(operation_id, "MOCK_FAILURE", "explicit development failure before mock publication")
            collection.operations[operation_id] = Operation(digest, [], response, 503)
            self.state_bytes += operation_reservation
            return response, 503
        self.state_bytes += operation_reservation + row_reservation
        base = self.identity(collection, collection.snapshots[-1] if collection.snapshots else None)
        if scenario == "commit_unknown":
            response = self._unknown(collection, operation_id, digest, base)
            collection.operations[operation_id] = Operation(digest, rows, response, 503)
            collection.pending_operation = operation_id
            return response, 503
        response, status = self._commit(collection, operation_id, digest, rows)
        if scenario == "lost_response":
            return self._unknown(collection, operation_id, digest, base, response["result"]), 503
        return response, status

    def resolve(self, collection, operation_id, outcome):
        operation_id = uuid_value(operation_id, "operation_id")
        operation = collection.operations.get(operation_id)
        if operation is None:
            raise MockError(404, "OPERATION_NOT_FOUND", "operation does not exist")
        if operation.response.get("outcome") != "commit_unknown":
            return copy.deepcopy(operation.response), operation.status
        if outcome == "committed":
            result = self._commit(collection, operation_id, operation.payload_hash, operation.rows)
        elif outcome == "definitely_not_committed":
            response = self._failure(operation_id, "MOCK_FAILURE", "development resolution proves no mock publication")
            operation.response, operation.status = response, 503
            operation.rows = []
            result = response, 503
        else:
            raise MockError(422, "INVALID_ARGUMENT", "resolution outcome must be committed or definitely_not_committed")
        collection.pending_operation = None
        return result

    def scan(self, collection, body):
        if set(body) - {"projection", "predicate", "snapshot_id", "limit"}:
            raise MockError(422, "INVALID_ARGUMENT", "unknown scan field")
        snapshot, rows = self.view(collection, body.get("snapshot_id", "latest"))
        projection = body.get("projection")
        if projection is None:
            projection = [item["name"] for item in collection.schema["fields"]]
        # Validate the projected schema separately, then retain complete selected
        # rows for the shared wire codec, which accepts fixed collection schemas.
        scan_rows(collection.schema, [], projection, body.get("predicate"), 0)
        output = scan_rows(collection.schema, rows, None, body.get("predicate"), body.get("limit"))
        by_name = {item["name"]: item for item in collection.schema["fields"]}
        schema = {"schema_id": collection.schema.get("schema_id", 0), "fields": [copy.deepcopy(by_name[name]) for name in projection]}
        return snapshot, rows, output, schema

    def cursor_for(self, cursor, *, kind, collection_id, params):
        if not cursor:
            return None
        payload = self.decode_cursor(cursor)
        if payload["kind"] != kind or payload["collection_id"] != collection_id or payload["params"] != params:
            raise MockError(400, "CURSOR_MISMATCH", "cursor parameters or collection do not match")
        return payload

    def page(self, items, *, kind, collection_id, params, identity=None, watermark=None, cursor=None):
        size = params["page_size"]
        position = 0
        issued = self.clock()
        if cursor:
            payload = self.cursor_for(cursor, kind=kind, collection_id=collection_id, params=params)
            identity, watermark, position = payload["identity"], payload["watermark"], payload["position"]
            issued = payload["issued"]
        if watermark is not None:
            items = [item for item in items if item["creation_sequence"] <= watermark]
        if identity is not None and kind in {"files", "snapshots"}:
            # Membership is limited by the previously resolved snapshot identity.
            sid = identity["snapshot_id"]
            maximum = int(sid) if sid is not None else 0
            items = [item for item in items if int(item.get("snapshot_id", "0")) <= maximum]
        selected = items[position:position + size]
        next_cursor = None
        if position + size < len(items):
            payload = {
                "v": 1, "issuer": self.issuer, "issued": issued, "kind": kind,
                "collection_id": collection_id, "params": params, "identity": identity,
                "watermark": watermark, "position": position + size,
            }
            encoded = base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).rstrip(b"=")
            signature = base64.urlsafe_b64encode(hmac.digest(self.secret, encoded, "sha256")).rstrip(b"=")
            next_cursor = (encoded + b"." + signature).decode()
        result = {"items": selected, "next_cursor": next_cursor, **MOCK}
        if identity is not None:
            result["identity"] = identity
        return result

    def decode_cursor(self, token):
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError
            encoded, signature = token.encode("ascii").split(b".")
            value = json.loads(base64.urlsafe_b64decode(encoded + b"=" * (-len(encoded) % 4)))
            if not isinstance(value, dict) or set(value) != {"v", "issuer", "issued", "kind", "collection_id", "params", "identity", "watermark", "position"}:
                raise ValueError
            if type(value["v"]) is not int or value["v"] != 1 or type(value["position"]) is not int or value["position"] < 0:
                raise ValueError
            if type(value["issued"]) not in (int, float) or not isinstance(value["issuer"], str):
                raise ValueError
            UUID(value["issuer"])
            if value["issuer"] != self.issuer:
                # A prior disposable issuer cannot be authenticated with the
                # new process key and can never resume a view in this store.
                raise MockError(400, "CURSOR_EXPIRED", "cursor belongs to a prior mock process", "restart_pagination")
            expected = base64.urlsafe_b64encode(hmac.digest(self.secret, encoded, "sha256")).rstrip(b"=")
            if not hmac.compare_digest(signature, expected):
                raise ValueError
        except (ValueError, KeyError, UnicodeError, TypeError, RecursionError) as error:
            raise MockError(400, "INVALID_ARGUMENT", "invalid authenticated pagination cursor") from error
        if self.clock() - value["issued"] > self.cursor_ttl:
            raise MockError(400, "CURSOR_EXPIRED", "cursor expired or belongs to a prior mock process", "restart_pagination")
        return value
