"""C-TYPES validation using only exact integer/calendar arithmetic."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
import math
import re

MIN_TIMESTAMP_US = -62135596800000000
MAX_TIMESTAMP_US = 253402300799999999
TYPE_IDS = {"string": 1, "bool": 2, "int64": 3, "float64": 4, "timestamp_us_utc": 5, "decimal": 6, "date": 7}
_EPOCH = dt.datetime(1970, 1, 1)
_TIMESTAMP = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,6}))?(Z|[+-][0-9]{2}:[0-9]{2})")
_DECIMAL = re.compile(r"-?[0-9]+")
_FIXED = re.compile(r"(-?)([0-9]+)(?:\.([0-9]+))?")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def _utf8(value: str, label: str) -> bytes:
    try:
        return value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} must contain valid UTF-8") from exc


def _timestamp_range(value: int) -> int:
    if type(value) is not int or not MIN_TIMESTAMP_US <= value <= MAX_TIMESTAMP_US:
        raise ValueError("timestamp must be integer UTC microseconds in years 0001..9999")
    return value


def parse_timestamp(value: str) -> int:
    """Parse RFC3339 with mandatory zone, 0..6 fractional digits and no rounding."""
    if not isinstance(value, str) or (match := _TIMESTAMP.fullmatch(value)) is None:
        raise ValueError("timestamp requires RFC3339 with a four-digit year and explicit zone")
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    fraction, zone = match.groups()[6:]
    try:
        local = dt.datetime(year, month, day, hour, minute, second, int((fraction or "").ljust(6, "0")))
    except ValueError as exc:
        raise ValueError("invalid timestamp calendar value") from exc
    offset = 0
    if zone != "Z":
        offset_hour, offset_minute = int(zone[1:3]), int(zone[4:6])
        if offset_hour > 23 or offset_minute > 59:
            raise ValueError("invalid UTC offset")
        offset = (offset_hour * 60 + offset_minute) * 60000000
        if zone[0] == "-":
            offset = -offset
    elapsed = local - _EPOCH
    # Subtract offset before validating UTC range. Do not construct out-of-range datetime.
    return _timestamp_range((elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds - offset)


def format_timestamp(us: int) -> str:
    _timestamp_range(us)
    value = _EPOCH + dt.timedelta(microseconds=us)
    return f"{value.year:04d}-{value.month:02d}-{value.day:02d}T{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}Z"


def decimal_coefficient(field: dict, value) -> int:
    """Independently parse a coefficient, rejecting every inexact rescale."""
    precision, scale = field.get("precision"), field.get("scale")
    if type(precision) is not int or not 1 <= precision <= 38 or type(scale) is not int or not 0 <= scale <= precision:
        raise ValueError("invalid decimal precision or scale")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("decimal must be finite")
        sign, digits, exponent = value.as_tuple()
        plain_length = sign + len(digits) + exponent if exponent >= 0 else sign + 1 + max(len(digits), 1 - exponent)
        if plain_length > 128:
            raise ValueError("decimal input exceeds 128 characters")
        value = format(value, "f")
    if not isinstance(value, str) or len(value) > 128 or (match := _FIXED.fullmatch(value)) is None:
        raise ValueError("decimal requires plain decimal text or Decimal within 128 characters")
    sign, integer, fraction = match.groups()
    fraction = fraction or ""
    if len(fraction) > scale and fraction[scale:].strip("0"):
        raise ValueError("decimal cannot rescale without rounding")
    coefficient = int(integer) * 10 ** scale + int((fraction[:scale] + "0" * scale)[:scale] or "0")
    if coefficient >= 10 ** precision:
        raise ValueError("decimal exceeds precision")
    return -coefficient if sign else coefficient


def decimal_text(field: dict, value) -> str:
    coefficient = decimal_coefficient(field, value)
    scale = field["scale"]
    digits = str(abs(coefficient)).zfill(scale + 1)
    if scale:
        digits = digits[:-scale] + "." + digits[-scale:]
    return ("-" if coefficient < 0 else "") + digits


def parse_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        raise ValueError("date excludes datetime values")
    if isinstance(value, str):
        if not _DATE.fullmatch(value):
            raise ValueError("date requires strict YYYY-MM-DD text")
        try:
            value = dt.date(*map(int, value.split("-")))
        except ValueError as exc:
            raise ValueError("invalid calendar date") from exc
    if not isinstance(value, dt.date):
        raise ValueError("date or strict YYYY-MM-DD text required")
    return value


def normalize_schema(schema: dict) -> dict:
    if not isinstance(schema, dict) or set(schema) - {"fields", "schema_id"}:
        raise ValueError("schema must contain fields and optional schema_id")
    fields = schema.get("fields")
    schema_id = schema.get("schema_id", 0)
    if type(schema_id) is not int or not 0 <= schema_id <= 2147483647:
        raise ValueError("schema_id must be a nonnegative int32")
    if not isinstance(fields, list) or not 1 <= len(fields) <= 128:
        raise ValueError("schema requires 1..128 fields")
    output, names = [], set()
    for index, field in enumerate(fields, 1):
        if not isinstance(field, dict) or set(field) - {"field_id", "name", "type", "nullable", "precision", "scale"}:
            raise ValueError("invalid field specification")
        name, kind, nullable = field.get("name"), field.get("type"), field.get("nullable", False)
        if not isinstance(name, str) or not name or "\0" in name or len(_utf8(name, "field name")) > 128 or name in names:
            raise ValueError("field names must be unique, nonempty and at most 128 UTF-8 bytes")
        if not isinstance(kind, str) or kind not in TYPE_IDS or type(nullable) is not bool:
            raise ValueError("invalid field type or nullable flag")
        parameters = {}
        if kind == "decimal":
            precision, scale = field.get("precision"), field.get("scale")
            if type(precision) is not int or not 1 <= precision <= 38 or type(scale) is not int or not 0 <= scale <= precision:
                raise ValueError("decimal requires precision 1..38 and scale 0..precision")
            parameters = {"precision": precision, "scale": scale}
        elif "precision" in field or "scale" in field:
            raise ValueError("precision and scale are forbidden for other types")
        field_id = field.get("field_id", index)
        if type(field_id) is not int or field_id != index:
            raise ValueError("fixed-schema field IDs must be assigned in order starting at 1")
        if name == "key" and (kind != "string" or nullable):
            raise ValueError("key must be a required string")
        names.add(name)
        output.append({"field_id": index, "name": name, "type": kind, "nullable": nullable, **parameters})
    return {"schema_id": schema_id, "fields": output}


def normalize_value(field: dict, value, *, literal: bool = False):
    """Normalize a row value; predicate NULL literals are allowed for all fields."""
    if value is None:
        if not field["nullable"] and not literal:
            raise ValueError(f"required field {field['name']} cannot be NULL")
        return None
    kind = field["type"]
    if kind == "string":
        if not isinstance(value, str):
            raise ValueError("string value required")
        cap = 1024 if field["name"] == "key" else 1048576
        if len(_utf8(value, field["name"])) > cap:
            raise ValueError(f"string exceeds {cap} UTF-8 bytes")
        return value
    if kind == "bool":
        if type(value) is not bool:
            raise ValueError("bool value required")
        return value
    if kind == "int64":
        if isinstance(value, str) and _DECIMAL.fullmatch(value):
            # Bound parsing work and avoid Python's decimal-digit conversion limit.
            significant = value.lstrip("-").lstrip("0")
            if len(significant) > 19:
                raise ValueError("int64 out of range")
            value = int(value)
        if type(value) is not int or not -(1 << 63) <= value < (1 << 63):
            raise ValueError("signed int64 value or decimal string required")
        return value
    if kind == "float64":
        if type(value) not in (int, float):
            raise ValueError("finite numeric float64 value required")
        try:
            value = float(value)
        except OverflowError as exc:
            raise ValueError("float64 out of range") from exc
        if not math.isfinite(value):
            raise ValueError("NaN and infinity are unsupported")
        return 0.0 if value == 0 else value
    if kind == "decimal":
        return Decimal(decimal_text(field, value))
    if kind == "date":
        return parse_date(value)
    if isinstance(value, str):
        return parse_timestamp(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("naive datetime is forbidden")
        try:
            elapsed = value.astimezone(dt.timezone.utc).replace(tzinfo=None) - _EPOCH
        except (ValueError, OverflowError) as exc:
            raise ValueError("timestamp UTC value out of range") from exc
        return _timestamp_range((elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds)
    return _timestamp_range(value)


def _value_size(field: dict, value) -> int:
    if value is None:
        return 1
    if field["type"] == "string":
        return 5 + len(value.encode("utf-8"))
    if field["type"] == "decimal":
        return 17
    if field["type"] == "date":
        return 5
    return 2 if field["type"] == "bool" else 9


def normalize_rows(schema: dict, rows: list[dict]) -> list[dict]:
    """Return owned native rows; append-wide caps are applied by canonical_append."""
    schema = normalize_schema(schema)
    if not isinstance(rows, list):
        raise ValueError("rows must be a list of objects")
    fields, output = schema["fields"], []
    names = {field["name"] for field in fields}
    for row in rows:
        if not isinstance(row, dict) or set(row) - names:
            raise ValueError("rows must be objects without unknown fields")
        normalized = {field["name"]: normalize_value(field, row.get(field["name"])) for field in fields}
        if sum(_value_size(field, normalized[field["name"]]) for field in fields) > 2097152:
            raise ValueError("row exceeds 2 MiB of canonical decoded values")
        output.append(normalized)
    return output


def wire_rows(schema: dict, rows: list[dict]) -> list[dict]:
    schema = normalize_schema(schema)
    output = normalize_rows(schema, rows)
    for row in output:
        for field in schema["fields"]:
            value = row[field["name"]]
            if value is not None:
                if field["type"] == "int64":
                    row[field["name"]] = str(value)
                elif field["type"] == "timestamp_us_utc":
                    row[field["name"]] = format_timestamp(value)
                elif field["type"] == "decimal":
                    row[field["name"]] = decimal_text(field, value)
                elif field["type"] == "date":
                    row[field["name"]] = value.isoformat()
    return output
