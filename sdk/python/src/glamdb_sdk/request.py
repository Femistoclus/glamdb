"""Immutable append bodies bind retry identity before any POST."""

from dataclasses import dataclass
import hashlib
import json
import struct
from typing import Any, Iterable, Mapping
from uuid import UUID

from .codec import Schema, _canonical_row_size, encode_rows, timestamp_us
from .errors import ValidationError
from .models import ANY

_TAGS = {"string": 1, "bool": 2, "int64": 3, "float64": 4, "timestamp_us_utc": 5}


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
    digest = hashlib.sha256()
    digest.update(b"COLUMNAR-SCHEMA\0" + struct.pack("<HH", 1, len(schema.fields)))
    for field in sorted(schema.fields, key=lambda value: value.field_id):
        name = field.name.encode("utf-8")
        digest.update(struct.pack("<IH", field.field_id, len(name)) + name)
        digest.update(bytes([_TAGS[field.type], int(field.nullable)]))
    return digest.digest()


def canonical_payload_hash(collection_id: str, schema: Schema, rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(b"COLUMNAR-APPEND\0" + struct.pack("<H", 1) + UUID(collection_id).bytes)
    digest.update(schema_fingerprint(schema) + struct.pack("<Q", len(rows)))
    size = 0
    for row in rows:
        row_size = _canonical_row_size(schema, row)
        for field in sorted(schema.fields, key=lambda value: value.field_id):
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
        # Bound iteration before normalization so an unbounded generator cannot
        # allocate an unbounded list. The public append profile is 10,000 rows.
        owned = []
        canonical_bytes = 0
        for row in rows:
            if len(owned) == 10000:
                raise ValidationError("append supports at most 10,000 rows")
            encoded = encode_rows(schema, [row])[0]
            row_size = _canonical_row_size(schema, encoded)
            if row_size > 2 * 1024**2:
                raise ValidationError("append row exceeds 2 MiB canonical decoded bytes")
            canonical_bytes += row_size
            if canonical_bytes > 16 * 1024**2:
                raise ValidationError("append exceeds 16 MiB canonical decoded bytes")
            owned.append(encoded)
        if not owned:
            raise ValidationError("append requires at least one row")
        payload_hash = canonical_payload_hash(collection_id, schema, owned)
        value = {"operation_id": operation_id, "rows": owned}
        if expected_snapshot is not ANY:
            value["expected_snapshot"] = None if expected_snapshot is None else str(int64(expected_snapshot))
        return cls(collection_id, operation_id, payload_hash, json_bytes(value))

    def to_dict(self) -> dict[str, Any]:
        """Return a detached view; editing it cannot change a retry body."""
        return json.loads(self.body)
