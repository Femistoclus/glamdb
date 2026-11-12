"""Frozen pre-#105 append preparation, kept only as a differential oracle.

This is a copy (comments trimmed, logic unchanged) of the scalar, row-encoding, hashing and preparation
code from ``glamdb_sdk.codec`` and ``glamdb_sdk.request`` before the
single-pass preparation plan. It deliberately imports nothing from those
modules except the exception type, so later SDK changes cannot drift into the
oracle. Do not optimize or "fix" this file; it defines the expected bytes.
It is not collected by pytest (no ``test_`` prefix) and is loaded by path.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import struct
from typing import Any, Iterable, Mapping
from uuid import UUID

from glamdb_sdk.errors import ValidationError

MIN_TIMESTAMP_US = -62135596800000000
MAX_TIMESTAMP_US = 253402300799999999
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_DECIMAL = re.compile(r"(?:0|-[1-9][0-9]*|[1-9][0-9]*)\Z", re.ASCII)
_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,6}))?(Z|[+-][0-9]{2}:[0-9]{2})\Z", re.ASCII)
_TAGS = {"string": 1, "bool": 2, "int64": 3, "float64": 4, "timestamp_us_utc": 5}
_ANY = object()


def _utf8(value: str) -> bytes:
    try:
        return value.encode("utf-8")
    except UnicodeError as exc:
        raise ValidationError("string is not valid UTF-8") from exc


def int64(value: Any) -> int:
    if isinstance(value, str) and _DECIMAL.fullmatch(value):
        if len(value) > 20:
            raise ValidationError("int64 outside signed range")
        value = int(value)
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise ValidationError("expected signed int64 or canonical decimal string")
    return value


def timestamp_us(value: Any) -> int:
    if isinstance(value, str):
        match = _TIMESTAMP.fullmatch(value)
        if match is None:
            raise ValidationError("timestamp requires RFC3339 with timezone and at most six fractional digits")
        year, month, day, hour, minute, second = map(int, match.groups()[:6])
        fraction = int((match[7] or "").ljust(6, "0"))
        zone = match[8]
        offset = 0
        if zone != "Z":
            hours, minutes = int(zone[1:3]), int(zone[4:6])
            if hours > 23 or minutes > 59:
                raise ValidationError("invalid timestamp UTC offset")
            offset = (hours * 60 + minutes) * (1 if zone[0] == "+" else -1)
        try:
            local = datetime(year, month, day, hour, minute, second, fraction, tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValidationError("invalid timestamp date or time") from exc
        delta = local - _EPOCH
        value = delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds
        value -= offset * 60000000
    elif isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValidationError("naive datetime is not allowed")
        delta = value.replace(tzinfo=timezone.utc) - _EPOCH
        offset = value.utcoffset()
        value = (delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds
                 - offset.days * 86400000000 - offset.seconds * 1000000 - offset.microseconds)
    else:
        value = int64(value)
    if not MIN_TIMESTAMP_US <= value <= MAX_TIMESTAMP_US:
        raise ValidationError("timestamp UTC value is outside years 0001 through 9999")
    return value


def format_timestamp(value: Any) -> str:
    moment = _EPOCH + timedelta(microseconds=timestamp_us(value))
    return (f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}T"
            f"{moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}.{moment.microsecond:06d}Z")


def encode_scalar(field, value: Any) -> Any:
    if value is None:
        if not field.nullable:
            raise ValidationError(f"required field {field.name} cannot be NULL")
        return None
    if field.type == "string":
        if not isinstance(value, str):
            raise ValidationError(f"field {field.name} requires a string")
        maximum = 1024 if field.name == "key" else 1024 * 1024
        if len(_utf8(value)) > maximum:
            raise ValidationError(f"field {field.name} exceeds UTF-8 byte limit")
        return value
    if field.type == "bool":
        if type(value) is not bool:
            raise ValidationError(f"field {field.name} requires bool")
        return value
    if field.type == "int64":
        return str(int64(value))
    if field.type == "timestamp_us_utc":
        return format_timestamp(value)
    if field.type == "float64":
        if type(value) not in (int, float):
            raise ValidationError(f"field {field.name} requires a finite number")
        try:
            value = float(value)
        except (OverflowError, ValueError) as exc:
            raise ValidationError(f"field {field.name} requires a finite float64") from exc
        if not math.isfinite(value):
            raise ValidationError(f"field {field.name} requires a finite float64")
        return 0.0 if value == 0 else value
    raise ValidationError(f"unsupported field type: {field.type}")


def _canonical_row_size(schema, row: Mapping[str, Any]) -> int:
    size = 0
    for field in schema.fields:
        value = row[field.name]
        size += 1
        if value is not None:
            size += (4 + len(_utf8(value)) if field.type == "string"
                     else 1 if field.type == "bool" else 8)
    return size


def encode_rows(schema, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    names = {f.name for f in schema.fields}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) - names:
            raise ValidationError("row must be an object with only schema fields")
        encoded = {field.name: encode_scalar(field, row.get(field.name)) for field in schema.fields}
        size = _canonical_row_size(schema, encoded)
        if size > 2 * 1024 * 1024:
            raise ValidationError("row exceeds 2 MiB canonical decoded bytes")
        result.append(encoded)
    return result


def uuid_string(value, label: str) -> str:
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


def schema_fingerprint(schema) -> bytes:
    digest = hashlib.sha256()
    digest.update(b"COLUMNAR-SCHEMA\0" + struct.pack("<HH", 1, len(schema.fields)))
    for field in sorted(schema.fields, key=lambda value: value.field_id):
        name = field.name.encode("utf-8")
        digest.update(struct.pack("<IH", field.field_id, len(name)) + name)
        digest.update(bytes([_TAGS[field.type], int(field.nullable)]))
    return digest.digest()


def canonical_payload_hash(collection_id: str, schema, rows: list[dict[str, Any]]) -> str:
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


def prepare(collection_id, operation_id, schema, rows: Iterable[Mapping[str, Any]],
            expected_snapshot: Any = _ANY) -> tuple[str, str, str, bytes]:
    """Old ``AppendRequest.prepare``; returns (collection_id, operation_id, payload_hash, body).

    Pass ``glamdb_sdk.models.ANY`` (or omit) for no snapshot precondition.
    """
    from glamdb_sdk.models import ANY

    collection_id = uuid_string(collection_id, "collection_id")
    operation_id = uuid_string(operation_id, "operation_id")
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
    if expected_snapshot is not _ANY and expected_snapshot is not ANY:
        value["expected_snapshot"] = None if expected_snapshot is None else str(int64(expected_snapshot))
    return collection_id, operation_id, payload_hash, json_bytes(value)
