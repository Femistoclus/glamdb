"""Exact v0 scalars. No engine or verification-tool code is imported here."""

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
import hashlib
import math
import re
import struct
from typing import Any, Iterable, Mapping

from .errors import ValidationError

MAX_SCHEMA_FIELDS = 128

MIN_TIMESTAMP_US = -62135596800000000
MAX_TIMESTAMP_US = 253402300799999999
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NAIVE_EPOCH = datetime(1970, 1, 1)
_DATE_EPOCH = date(1970, 1, 1)
MIN_DATE_DAYS = -719162
MAX_DATE_DAYS = 2932896
_DECIMAL = re.compile(r"(?:0|-[1-9][0-9]*|[1-9][0-9]*)\Z", re.ASCII)
_FIXED_DECIMAL = re.compile(r"-?[0-9]+(?:\.[0-9]+)?\Z", re.ASCII)
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z", re.ASCII)
_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,6}))?(Z|[+-][0-9]{2}:[0-9]{2})\Z", re.ASCII)
_TYPES = {"string", "bool", "int64", "float64", "timestamp_us_utc", "decimal", "date"}


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    nullable: bool = False
    field_id: int | None = None
    precision: int | None = None
    scale: int | None = None


@dataclass(frozen=True, init=False)
class Schema:
    fields: tuple[Field, ...]

    def __init__(self, fields: Iterable[Field], *, allow_empty: bool = False):
        values = tuple(fields)
        if not (0 if allow_empty else 1) <= len(values) <= MAX_SCHEMA_FIELDS:
            raise ValidationError(f"schema must have 1 through {MAX_SCHEMA_FIELDS} fields")
        names, ids = set(), set()
        assigned = []
        for index, field in enumerate(values, 1):
            if not isinstance(field, Field):
                raise ValidationError("schema entries must be Field values")
            if not isinstance(field.name, str) or not field.name or "\0" in field.name or len(_utf8(field.name)) > 128:
                raise ValidationError("field name must have 1 through 128 UTF-8 bytes")
            if field.name in names or field.type not in _TYPES or type(field.nullable) is not bool:
                raise ValidationError("duplicate field name, unsupported type, or invalid nullable flag")
            if field.type == "decimal":
                _decimal_parameters(field.precision, field.scale)
            elif field.precision is not None or field.scale is not None:
                raise ValidationError("precision and scale are only valid for decimal fields")
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
            assigned = []
            for item in fields:
                if item["type"] != "decimal" and ("precision" in item or "scale" in item):
                    raise ValidationError("precision and scale are only valid for decimal fields")
                assigned.append(Field(item["name"], item["type"], item["nullable"], item.get("field_id"),
                                      item.get("precision"), item.get("scale")))
            return cls(assigned, allow_empty=allow_empty)
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValidationError("invalid schema object") from exc

    def to_dict(self) -> dict[str, Any]:
        return {"fields": [dict(field_id=f.field_id, name=f.name, type=f.type, nullable=f.nullable)
                           | ({"precision": f.precision, "scale": f.scale} if f.type == "decimal" else {})
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


def _decimal_parameters(precision: Any, scale: Any) -> None:
    if type(precision) is not int or not 1 <= precision <= 38 or type(scale) is not int or not 0 <= scale <= precision:
        raise ValidationError("decimal requires integer precision 1..38 and scale 0..precision")


def decimal_parts(value: Any, precision: int, scale: int) -> tuple[int, str]:
    """Return an exact signed coefficient and fixed-scale text, without rounding."""
    _decimal_parameters(precision, scale)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValidationError("decimal must be finite plain decimal text or Decimal")
        sign, digits, exponent = value.as_tuple()
        length = sign + (len(digits) + exponent if exponent >= 0 else max(len(digits), 1 - exponent) + 1)
        if length > 128:
            raise ValidationError("decimal input exceeds 128 characters")
        # Fixed formatting is exact and independent of decimal.getcontext().
        # Decimal's own str() uses scientific notation for small values.
        value = format(value, "f")
    if not isinstance(value, str) or len(value) > 128 or _FIXED_DECIMAL.fullmatch(value) is None:
        raise ValidationError("decimal requires plain decimal text or Decimal of at most 128 characters")
    negative = value.startswith("-")
    whole, _, fraction = value.lstrip("-").partition(".")
    if any(digit != "0" for digit in fraction[scale:]):
        raise ValidationError("decimal cannot be rescaled exactly")
    digits = (whole + fraction[:scale].ljust(scale, "0")).lstrip("0") or "0"
    if len(digits) > precision:
        raise ValidationError("decimal exceeds declared precision")
    coefficient = int(digits)
    if negative and coefficient:
        coefficient = -coefficient
    return coefficient, format_decimal_coefficient(coefficient, scale)


def format_decimal_coefficient(coefficient: int, scale: int) -> str:
    """Format a validated coefficient without using the Decimal context."""
    digits = str(abs(coefficient)).rjust(scale + 1, "0")
    text = digits if scale == 0 else digits[:-scale] + "." + digits[-scale:]
    return "-" + text if coefficient < 0 else text


def date_days(value: Any) -> int:
    if isinstance(value, str):
        if _DATE.fullmatch(value) is None:
            raise ValidationError("date requires strict YYYY-MM-DD text or a date")
        try:
            value = date.fromisoformat(value)
        except ValueError as exc:
            raise ValidationError("invalid calendar date in years 0001 through 9999") from exc
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValidationError("date requires strict YYYY-MM-DD text or a date excluding datetime")
    return (value - _DATE_EPOCH).days


def format_date(value: Any) -> str:
    return (_DATE_EPOCH + timedelta(days=date_days(value))).isoformat()


def timestamp_us(value: Any) -> int:
    # Text is always RFC3339; exact integer microseconds must be passed as int.
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


def _format_naive_utc(moment: datetime) -> str:
    # Naive isoformat zero-pads the year to four digits; it omits the fraction
    # only when it is zero.
    text = moment.isoformat()
    return text + "Z" if moment.microsecond else text + ".000000Z"


def _format_us(micros: int) -> str:
    """Format validated UTC microseconds as six-digit RFC3339 ``...Z`` text."""
    return _format_naive_utc(_NAIVE_EPOCH + timedelta(0, 0, micros))


def format_timestamp(value: Any) -> str:
    return _format_us(timestamp_us(value))


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
    if field.type == "decimal":
        return decimal_parts(value, field.precision, field.scale)[1]
    if field.type == "date":
        return format_date(value)
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
                     else 1 if field.type == "bool" else 16 if field.type == "decimal"
                     else 4 if field.type == "date" else 8)
    return size


ROW_CANONICAL_LIMIT = 2 * 1024 * 1024
_TAGS = {"string": 1, "bool": 2, "int64": 3, "float64": 4, "timestamp_us_utc": 5, "decimal": 6, "date": 7}
_KIND_STRING, _KIND_BOOL, _KIND_INT64, _KIND_TIMESTAMP = 1, 2, 3, 5
_KIND_DECIMAL, _KIND_DATE = 6, 7
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1
# Canonical v1 scalar encodings: NULL is one 0x00 byte; a present value is
# 0x01 followed by its payload, so a field's canonical size is its chunk length.
_NULL_CHUNK = b"\0"
_TRUE_CHUNK = b"\1\1"
_FALSE_CHUNK = b"\1\0"
_PACK_INT = struct.Struct("<Bq").pack
_PACK_FLOAT = struct.Struct("<Bd").pack
_PACK_LEN = struct.Struct("<BI").pack
_PACK_DATE = struct.Struct("<Bi").pack
_CANONICAL_TIMESTAMP_MATCH = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z", re.ASCII).fullmatch
_FROM_ISO = datetime.fromisoformat
_ONE_US = timedelta(microseconds=1)
_ISFINITE = math.isfinite


def _canonical_decimal(text: str) -> int | None:
    """Return int64 for canonical decimal text (as ``int64`` accepts), else None."""
    try:
        number = int(text)
    except ValueError:
        return None
    # int() also accepts signs, whitespace, underscores and non-ASCII digits;
    # the round trip admits only the canonical ASCII form _DECIMAL matches.
    if _INT64_MIN <= number <= _INT64_MAX and str(number) == text:
        return number
    return None


def _timestamp_fast(value: Any) -> tuple[int, str] | None:
    """Return (micros, wire text) for plainly valid timestamp input, else None.

    Accepts only inputs whose result is unambiguous: six-digit UTC ``...Z``
    text, in-range int, or an aware datetime in a stdlib fixed-offset zone.
    Callers send everything else (including every invalid value) through
    ``timestamp_us``, which owns all checks and error messages.
    """
    kind = type(value)
    if kind is str:
        # Hour 24 is excluded explicitly: some Python versions' fromisoformat
        # read "24:00" as next-day midnight, which timestamp_us rejects.
        if _CANONICAL_TIMESTAMP_MATCH(value) is not None and value[11:13] < "24":
            try:
                micros = (_FROM_ISO(value[:26]) - _NAIVE_EPOCH) // _ONE_US
            except ValueError:  # e.g. day 31 of a 30-day month, year 0000
                return None
            return micros, value  # already canonical wire text
    elif kind is int:
        if MIN_TIMESTAMP_US <= value <= MAX_TIMESTAMP_US:
            return value, _format_us(value)
    elif kind is datetime and type(value.tzinfo) is timezone:
        # Shift wall time to naive UTC; naive datetimes span exactly years
        # 0001..9999, so results outside the timestamp range overflow here.
        try:
            moment = (value - value.utcoffset()).replace(tzinfo=None)
        except OverflowError:
            return None
        return (moment - _NAIVE_EPOCH) // _ONE_US, _format_naive_utc(moment)
    return None


class _RowPlan:
    """Immutable per-schema preparation plan.

    Holds only values copied out of the schema (never caller rows or hashes
    over caller buffers), so sharing one plan between requests is safe.
    """

    __slots__ = ("fields", "names", "order", "fingerprint", "wire_template", "chunk_template",
                 "strings", "int64s", "float64s", "bools", "timestamps", "decimals", "dates", "parameters")

    def __init__(self, key: tuple[tuple[str, str, bool, int, int | None, int | None], ...]):
        # Declaration order defines wire-object key order and error order;
        # field-ID order defines canonical hash order.
        self.fields = tuple((name, _TAGS[kind], nullable, 1024 if name == "key" else 1024 * 1024)
                            for name, kind, nullable, *_ in key)
        self.parameters = {name: (precision, scale) for name, kind, _, _, precision, scale in key if kind == "decimal"}
        self.names = frozenset(name for name, *_ in key)
        order = tuple(sorted(range(len(key)), key=lambda index: key[index][3]))
        self.order = None if order == tuple(range(len(key))) else order
        digest = hashlib.sha256()
        version = 2 if any(kind in ("decimal", "date") for _, kind, *_ in key) else 1
        digest.update(b"COLUMNAR-SCHEMA\0" + struct.pack("<HH", version, len(key)))
        for index in order:
            name, kind, nullable, field_id, precision, scale = key[index]
            encoded = name.encode("utf-8")
            digest.update(struct.pack("<IH", field_id, len(encoded)) + encoded)
            digest.update(bytes([_TAGS[kind], int(nullable)]))
            if kind == "decimal":
                digest.update(bytes([precision, scale]))
        self.fingerprint = digest.digest()
        # Grouped fast path: fields of one type with their field-ID-order slot.
        self.wire_template = dict.fromkeys(name for name, *_ in key)
        self.chunk_template = [_NULL_CHUNK] * len(key)
        slot = {index: rank for rank, index in enumerate(order)}
        groups: dict[str, list] = {kind: [] for kind in _TAGS}
        for index, (name, kind, nullable, maximum) in enumerate(self.fields):
            groups[key[index][1]].append((name, slot[index], nullable, maximum))
        self.strings = tuple(groups["string"])
        self.int64s = tuple(groups["int64"])
        self.float64s = tuple(groups["float64"])
        self.bools = tuple(groups["bool"])
        self.timestamps = tuple(groups["timestamp_us_utc"])
        self.decimals = tuple(groups["decimal"])
        self.dates = tuple(groups["date"])

    def encode_row(self, row: Any) -> tuple[dict[str, Any], bytes]:
        """Validate one caller row once; return its wire object and canonical bytes.

        Errors, their messages and the order in which they are raised match
        ``encode_scalar`` applied field by field in declaration order. The
        returned canonical bytes are the row's v1 hash preimage (fields in
        field-ID order); their length is the row's canonical decoded size.
        """
        if not (type(row) is dict or isinstance(row, Mapping)) or not self.names.issuperset(row):
            raise ValidationError("row must be an object with only schema fields")
        result = self._encode_plain(row) if type(row) is dict else None
        if result is None:
            result = self._encode_ordered(row)
        if len(result[1]) > ROW_CANONICAL_LIMIT:
            raise ValidationError("row exceeds 2 MiB canonical decoded bytes")
        return result

    def _encode_plain(self, row: dict) -> tuple[dict[str, Any], bytes] | None:
        """Fast path for a plain dict whose values are exact, plainly valid types.

        Visits fields grouped by type, so it returns None on the first value it
        does not accept outright; the caller then re-encodes the whole row in
        declaration order, which raises the same first error as before. dict.get
        on a plain dict has no side effects, so the second visit sees the same
        values. Absent and NULL values keep the templates' None/0x00 entries.
        """
        get = row.get
        wire = self.wire_template.copy()
        chunks = self.chunk_template.copy()
        try:
            for name, slot, nullable, maximum in self.strings:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                elif type(value) is str:
                    encoded = value.encode("utf-8")
                    if len(encoded) > maximum:
                        return None
                    wire[name] = value
                    chunks[slot] = _PACK_LEN(1, len(encoded)) + encoded
                else:
                    return None
            for name, slot, nullable, _ in self.int64s:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                elif type(value) is int and _INT64_MIN <= value <= _INT64_MAX:
                    wire[name] = str(value)
                    chunks[slot] = _PACK_INT(1, value)
                elif type(value) is str and len(value) <= 20 and (number := _canonical_decimal(value)) is not None:
                    wire[name] = value
                    chunks[slot] = _PACK_INT(1, number)
                else:
                    return None
            for name, slot, nullable, _ in self.float64s:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                elif type(value) is float and _ISFINITE(value):
                    if value == 0:
                        value = 0.0
                    wire[name] = value
                    chunks[slot] = _PACK_FLOAT(1, value)
                else:
                    return None
            for name, slot, nullable, _ in self.bools:
                value = get(name)
                if value is True:
                    wire[name] = True
                    chunks[slot] = _TRUE_CHUNK
                elif value is False:
                    wire[name] = False
                    chunks[slot] = _FALSE_CHUNK
                elif value is not None or not nullable:
                    return None
            for name, slot, nullable, _ in self.timestamps:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                elif (converted := _timestamp_fast(value)) is not None:
                    wire[name] = converted[1]
                    chunks[slot] = _PACK_INT(1, converted[0])
                else:
                    return None
            for name, slot, nullable, _ in self.decimals:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                else:
                    coefficient, wire[name] = decimal_parts(value, *self.parameters[name])
                    chunks[slot] = b"\1" + coefficient.to_bytes(16, "little", signed=True)
            for name, slot, nullable, _ in self.dates:
                value = get(name)
                if value is None:
                    if not nullable:
                        return None
                else:
                    days = date_days(value)
                    wire[name] = (_DATE_EPOCH + timedelta(days=days)).isoformat()
                    chunks[slot] = _PACK_DATE(1, days)
        except Exception:  # noqa: BLE001 - the ordered path owns every error
            return None
        return wire, b"".join(chunks)

    def _encode_ordered(self, row: Any) -> tuple[dict[str, Any], bytes]:
        get = row.get
        wire: dict[str, Any] = {}
        chunks: list[bytes] = []
        append = chunks.append
        for name, kind, nullable, maximum in self.fields:
            value = get(name)
            if value is None:
                if not nullable:
                    raise ValidationError(f"required field {name} cannot be NULL")
                wire[name] = None
                append(_NULL_CHUNK)
            elif kind == _KIND_STRING:
                if not isinstance(value, str):
                    raise ValidationError(f"field {name} requires a string")
                try:
                    encoded = value.encode("utf-8")
                except UnicodeError as exc:
                    raise ValidationError("string is not valid UTF-8") from exc
                if len(encoded) > maximum:
                    raise ValidationError(f"field {name} exceeds UTF-8 byte limit")
                wire[name] = value
                append(_PACK_LEN(1, len(encoded)) + encoded)
            elif kind == _KIND_INT64:
                number = int64(value)
                wire[name] = str(number)
                append(_PACK_INT(1, number))
            elif kind == _KIND_TIMESTAMP:
                # Convert to microseconds once; hash bytes and wire text both
                # derive from them instead of formatting and re-parsing text.
                converted = _timestamp_fast(value)
                if converted is None:
                    micros = timestamp_us(value)
                    converted = micros, _format_us(micros)
                wire[name] = converted[1]
                append(_PACK_INT(1, converted[0]))
            elif kind == _KIND_BOOL:
                if type(value) is not bool:
                    raise ValidationError(f"field {name} requires bool")
                wire[name] = value
                append(_TRUE_CHUNK if value else _FALSE_CHUNK)
            elif kind == _KIND_DECIMAL:
                coefficient, wire[name] = decimal_parts(value, *self.parameters[name])
                append(b"\1" + coefficient.to_bytes(16, "little", signed=True))
            elif kind == _KIND_DATE:
                days = date_days(value)
                wire[name] = (_DATE_EPOCH + timedelta(days=days)).isoformat()
                append(_PACK_DATE(1, days))
            else:
                if type(value) not in (int, float):
                    raise ValidationError(f"field {name} requires a finite number")
                try:
                    value = float(value)
                except (OverflowError, ValueError) as exc:
                    raise ValidationError(f"field {name} requires a finite float64") from exc
                if not math.isfinite(value):
                    raise ValidationError(f"field {name} requires a finite float64")
                value = 0.0 if value == 0 else value
                wire[name] = value
                append(_PACK_FLOAT(1, value))
        order = self.order
        return wire, b"".join(chunks if order is None else [chunks[index] for index in order])


@lru_cache(maxsize=128)
def _compile(key: tuple[tuple[str, str, bool, int, int | None, int | None], ...]) -> _RowPlan:
    return _RowPlan(key)


def row_plan(schema: Schema) -> _RowPlan:
    """Return the cached preparation plan for the schema's current field values.

    The cache key is a value snapshot of every field, not object identity, so
    equal schemas share a plan and a plan can never describe other fields.
    """
    return _compile(tuple((f.name, f.type, f.nullable, f.field_id, f.precision, f.scale) for f in schema.fields))


def encode_rows(schema: Schema, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    encode_row = row_plan(schema).encode_row
    return [encode_row(row)[0] for row in rows]


def decode_rows(schema: Schema, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Validate and decode wire rows in one pass: each value is parsed once."""
    names = {f.name for f in schema.fields}
    result = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) - names:
            raise ValidationError("wire row must be an object with only schema fields")
        decoded: dict[str, Any] = {}
        sized: dict[str, Any] = {}
        for field in schema.fields:
            value = row.get(field.name)
            if value is None:
                # Missing and NULL alike: encode_scalar enforces nullability.
                decoded[field.name] = sized[field.name] = encode_scalar(field, None)
            elif field.type == "int64":
                if not isinstance(value, str):
                    raise ValidationError("wire int64 must be a decimal string")
                decoded[field.name] = int64(value)
                sized[field.name] = 0
            elif field.type == "timestamp_us_utc":
                if not isinstance(value, str):
                    raise ValidationError("wire timestamp must be six-digit UTC RFC3339 text")
                micros = timestamp_us(value)
                if format_timestamp(micros) != value:
                    raise ValidationError("wire timestamp must be six-digit UTC RFC3339 text")
                decoded[field.name] = _EPOCH + timedelta(microseconds=micros)
                sized[field.name] = 0
            elif field.type == "decimal":
                if not isinstance(value, str) or decimal_parts(value, field.precision, field.scale)[1] != value:
                    raise ValidationError("wire decimal must be canonical fixed-scale text")
                decoded[field.name] = Decimal(value)
                sized[field.name] = 0
            elif field.type == "date":
                if not isinstance(value, str):
                    raise ValidationError("wire date must be strict YYYY-MM-DD text")
                decoded[field.name] = _DATE_EPOCH + timedelta(days=date_days(value))
                sized[field.name] = 0
            else:
                decoded[field.name] = sized[field.name] = encode_scalar(field, value)
        if _canonical_row_size(schema, sized) > 2 * 1024 * 1024:
            raise ValidationError("row exceeds 2 MiB canonical decoded bytes")
        result.append(decoded)
    return result
