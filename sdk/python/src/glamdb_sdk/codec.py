"""Exact v0 scalars. No engine or verification-tool code is imported here."""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import math
import re
from typing import Any, Iterable, Mapping

from .errors import ValidationError

MIN_TIMESTAMP_US = -62135596800000000
MAX_TIMESTAMP_US = 253402300799999999
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_DECIMAL = re.compile(r"(?:0|-[1-9][0-9]*|[1-9][0-9]*)\Z", re.ASCII)
_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,6}))?(Z|[+-][0-9]{2}:[0-9]{2})\Z", re.ASCII)
_TYPES = {"string", "bool", "int64", "float64", "timestamp_us_utc"}


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    nullable: bool = False
    field_id: int | None = None


@dataclass(frozen=True, init=False)
class Schema:
    fields: tuple[Field, ...]

    def __init__(self, fields: Iterable[Field], *, allow_empty: bool = False):
        values = tuple(fields)
        if not (0 if allow_empty else 1) <= len(values) <= 64:
            raise ValidationError("schema must have 1 through 64 fields")
        names, ids = set(), set()
        assigned = []
        for index, field in enumerate(values, 1):
            if not isinstance(field, Field):
                raise ValidationError("schema entries must be Field values")
            if not isinstance(field.name, str) or not field.name or len(_utf8(field.name)) > 128:
                raise ValidationError("field name must have 1 through 128 UTF-8 bytes")
            if field.name in names or field.type not in _TYPES or type(field.nullable) is not bool:
                raise ValidationError("duplicate field name, unsupported type, or invalid nullable flag")
            field_id = index if field.field_id is None else field.field_id
            if type(field_id) is not int or not 1 <= field_id <= 2**31-1 or field_id in ids:
                raise ValidationError("field IDs must be unique positive int32 values")
            if field.name == "key" and (field.type != "string" or field.nullable):
                raise ValidationError("key must be a required string")
            names.add(field.name)
            ids.add(field_id)
            assigned.append(replace(field, field_id=field_id))
        object.__setattr__(self, "fields", tuple(assigned))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], *, allow_empty: bool = False) -> "Schema":
        try:
            fields = value["fields"]
            if not isinstance(fields, list):
                raise ValidationError("schema fields must be an array")
            return cls((Field(item["name"], item["type"], item["nullable"], item.get("field_id"))
                        for item in fields), allow_empty=allow_empty)
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValidationError("invalid schema object") from exc

    def to_dict(self) -> dict[str, Any]:
        return {"fields": [dict(field_id=f.field_id, name=f.name, type=f.type, nullable=f.nullable)
                           for f in self.fields]}

    def field(self, name: str) -> Field:
        for field in self.fields:
            if field.name == name:
                return field
        raise ValidationError(f"unknown field: {name}")


def _utf8(value: str) -> bytes:
    try:
        return value.encode("utf-8")
    except UnicodeError as exc:
        raise ValidationError("string is not valid UTF-8") from exc


def int64(value: Any) -> int:
    if isinstance(value, str) and _DECIMAL.fullmatch(value):
        # Avoid constructing enormous integers from untrusted decimal input.
        if len(value) > 20:
            raise ValidationError("int64 outside signed range")
        value = int(value)
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise ValidationError("expected signed int64 or canonical decimal string")
    return value


def timestamp_us(value: Any) -> int:
    if isinstance(value, str) and not _DECIMAL.fullmatch(value):
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
        # Compute offset arithmetically so overflow at year boundaries is rejected
        # after UTC normalization rather than rounded by datetime.timestamp().
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


def encode_scalar(field: Field, value: Any) -> Any:
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


def _canonical_row_size(schema: Schema, row: Mapping[str, Any]) -> int:
    """Count decoded canonical bytes independently of scalar wire formatting."""
    size = 0
    for field in schema.fields:
        value = row[field.name]
        size += 1
        if value is not None:
            size += (4 + len(_utf8(value)) if field.type == "string"
                     else 1 if field.type == "bool" else 8)
    return size


def encode_rows(schema: Schema, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
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


def decode_rows(schema: Schema, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    raw_rows = list(rows)
    for row in raw_rows:
        if not isinstance(row, Mapping):
            raise ValidationError("wire row must be an object")
        for field in schema.fields:
            value = row.get(field.name)
            if value is None:
                continue
            if field.type == "int64" and not isinstance(value, str):
                raise ValidationError("wire int64 must be a decimal string")
            if field.type == "timestamp_us_utc" and (not isinstance(value, str) or value != format_timestamp(value)):
                raise ValidationError("wire timestamp must be six-digit UTC RFC3339 text")
    encoded = encode_rows(schema, raw_rows)
    for row in encoded:
        for field in schema.fields:
            value = row[field.name]
            if value is None:
                continue
            if field.type == "int64":
                row[field.name] = int(value)
            elif field.type == "timestamp_us_utc":
                row[field.name] = _EPOCH + timedelta(microseconds=timestamp_us(value))
    return encoded
