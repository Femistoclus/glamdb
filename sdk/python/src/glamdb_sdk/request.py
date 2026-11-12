"""Immutable append bodies bind retry identity before any POST."""

from dataclasses import dataclass
import hashlib
import json
import struct
from typing import Any, Iterable, Mapping
from uuid import UUID

from .codec import Schema, _canonical_row_size, row_plan, timestamp_us
from .errors import ValidationError
from .models import ANY

APPEND_ROW_LIMIT = 10000
APPEND_CANONICAL_LIMIT = 16 * 1024**2


def uuid_string(value: str | UUID, label: str) -> str:
    try:
        if not isinstance(value, (str, UUID)):
            raise ValueError()
        return str(UUID(str(value)))
    except (ValueError, AttributeError) as exc:
        raise ValidationError(f"{label} must be a UUID generated and retained by the caller") from exc


def json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValidationError("request is not valid finite UTF-8 JSON") from exc


def schema_fingerprint(schema: Schema) -> bytes:
    return row_plan(schema).fingerprint


def canonical_payload_hash(collection_id: str, schema: Schema, rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(b"COLUMNAR-APPEND\0" + struct.pack("<H", 1) + UUID(collection_id).bytes)
    digest.update(schema_fingerprint(schema) + struct.pack("<Q", len(rows)))
    size = 0
    ordered = sorted(schema.fields, key=lambda value: value.field_id)
    for row in rows:
        row_size = _canonical_row_size(schema, row)
        for field in ordered:
            value = row[field.name]
            if value is None:
                chunk = b"\0"
            elif field.type == "string":
                encoded = value.encode("utf-8")
                chunk = b"\1" + struct.pack("<I", len(encoded)) + encoded
            elif field.type == "bool":
                chunk = bytes([1, int(value)])
            elif field.type == "int64":
                chunk = b"\1" + struct.pack("<q", int(value))
            elif field.type == "timestamp_us_utc":
                chunk = b"\1" + struct.pack("<q", timestamp_us(value))
            else:
                chunk = b"\1" + struct.pack("<d", value)
            digest.update(chunk)
        if row_size > 2 * 1024**2:
            raise ValidationError("append row exceeds 2 MiB canonical decoded bytes")
        size += row_size
        if size > 16 * 1024**2:
            raise ValidationError("append exceeds 16 MiB canonical decoded bytes")
    return digest.hexdigest()


def _payload_digest(collection_id: str, fingerprint: bytes, row_count: int) -> Any:
    digest = hashlib.sha256()
    digest.update(b"COLUMNAR-APPEND\0" + struct.pack("<H", 1) + UUID(collection_id).bytes
                  + fingerprint + struct.pack("<Q", row_count))
    return digest


@dataclass(frozen=True)
class AppendRequest:
    collection_id: str
    operation_id: str
    payload_hash: str
    body: bytes

    @classmethod
    def prepare(cls, collection_id: str | UUID, operation_id: str | UUID, schema: Schema,
                rows: Iterable[Mapping[str, Any]], expected_snapshot: Any = ANY) -> "AppendRequest":
        from .codec import int64

        collection_id = uuid_string(collection_id, "collection_id")
        operation_id = uuid_string(operation_id, "operation_id")
        plan = row_plan(schema)
        # Bound iteration before normalization so an unbounded generator cannot
        # allocate an unbounded list. The public append profile is 10,000 rows.
        # Each row is validated once; its canonical bytes feed both the decoded
        # byte budget and the payload hash, and its wire object feeds the body.
        sized = isinstance(rows, (list, tuple)) and 0 < len(rows) <= APPEND_ROW_LIMIT
        expected_rows = len(rows) if sized else 0
        digest = _payload_digest(collection_id, plan.fingerprint, expected_rows) if sized else None
        pending: list[bytes] = []  # canonical rows, bounded by the 16 MiB budget
        owned = []
        canonical_bytes = 0
        encode_row = plan.encode_row
        for row in rows:
            if len(owned) == APPEND_ROW_LIMIT:
                raise ValidationError("append supports at most 10,000 rows")
            encoded, canonical = encode_row(row)
            canonical_bytes += len(canonical)
            if canonical_bytes > APPEND_CANONICAL_LIMIT:
                raise ValidationError("append exceeds 16 MiB canonical decoded bytes")
            owned.append(encoded)
            if digest is not None:
                digest.update(canonical)
            else:
                pending.append(canonical)
        if not owned:
            raise ValidationError("append requires at least one row")
        if digest is None:
            # The row count precedes row bytes in the preimage, so unsized
            # inputs hash only after iteration has finished.
            digest = _payload_digest(collection_id, plan.fingerprint, len(owned))
            for canonical in pending:
                digest.update(canonical)
            del pending
            payload_hash = digest.hexdigest()
        elif len(owned) != expected_rows:
            # The sequence changed length while being read; hash what was owned.
            payload_hash = canonical_payload_hash(collection_id, schema, owned)
        else:
            payload_hash = digest.hexdigest()
        value = {"operation_id": operation_id, "rows": owned}
        if expected_snapshot is not ANY:
            value["expected_snapshot"] = None if expected_snapshot is None else str(int64(expected_snapshot))
        return cls(collection_id, operation_id, payload_hash, json_bytes(value))

    def to_dict(self) -> dict[str, Any]:
        """Return a detached view; editing it cannot change a retry body."""
        return json.loads(self.body)
