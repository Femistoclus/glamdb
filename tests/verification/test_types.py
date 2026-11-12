import datetime
import math

import pytest


SCHEMA = {"fields": [
    {"name": "key", "type": "string", "nullable": False},
    {"name": "n", "type": "int64", "nullable": False},
    {"name": "t", "type": "timestamp_us_utc", "nullable": False},
    {"name": "f", "type": "float64", "nullable": True},
    {"name": "b", "type": "bool", "nullable": True},
]}


@pytest.mark.parametrize("text,us", [
    ("0001-01-01T00:00:00Z", -62135596800000000),
    ("9999-12-31T23:59:59.999999Z", 253402300799999999),
    ("1969-12-31T23:59:59.999999Z", -1),
    ("1970-01-01T00:00:00.000001Z", 1),
    ("1970-01-01T01:30:00+01:30", 0),
    ("1969-12-31T22:30:00-01:30", 0),
    ("2000-02-29T12:34:56.1Z", 951827696100000),
])
def test_timestamp_exact_microseconds(verify, text, us):
    assert verify.parse_timestamp(text) == us


@pytest.mark.parametrize("text", [
    "0000-01-01T00:00:00Z", "10000-01-01T00:00:00Z",
    "0001-01-01T00:00:00+00:01", "9999-12-31T23:59:59.999999-00:01",
    "2000-02-30T00:00:00Z", "1970-01-01T00:00:60Z",
    "1970-01-01T00:00:00.1234567Z", "1970-01-01T00:00:00",
    "1970-01-01T00:00:00+24:00", "1970-01-01T00:00:00+00:60",
    "1970-01-01T00:00:00.Z", "1970-1-01T00:00:00Z",
    "1970-01-01 00:00:00Z", "1970-01-01T00:00:00Z\n",
])
def test_timestamp_rejects_invalid_without_rounding(verify, text):
    with pytest.raises(ValueError):
        verify.parse_timestamp(text)


@pytest.mark.parametrize("us,text", [
    (-62135596800000000, "0001-01-01T00:00:00.000000Z"),
    (253402300799999999, "9999-12-31T23:59:59.999999Z"),
    (-1, "1969-12-31T23:59:59.999999Z"),
    (0, "1970-01-01T00:00:00.000000Z"),
])
def test_timestamp_formats_four_digit_year_and_six_digits(verify, us, text):
    assert verify.format_timestamp(us) == text


@pytest.mark.parametrize("us", [-62135596800000001, 253402300800000000, True, 1.0])
def test_timestamp_format_range_and_type(verify, us):
    with pytest.raises(ValueError):
        verify.format_timestamp(us)


def test_schema_stable_ids_and_owned_normalized_rows(verify):
    schema = verify.normalize_schema(SCHEMA)
    assert schema["schema_id"] == 0
    assert [f["field_id"] for f in schema["fields"]] == [1, 2, 3, 4, 5]
    rows = [{"key": "dup", "n": "9007199254740993", "t": "1969-12-31T23:59:59.999999Z", "f": -0.0}] * 2
    normalized = verify.normalize_rows(schema, rows)
    assert normalized == [{"key": "dup", "n": 9007199254740993, "t": -1, "f": 0.0, "b": None}] * 2
    assert math.copysign(1, normalized[0]["f"]) == 1
    rows[0]["key"] = "changed"
    assert normalized[0]["key"] == "dup"
    normalized[0]["key"] = "first"
    assert normalized[1]["key"] == "dup"


def test_wire_rows_preserve_integer_precision(verify):
    row = {"key": "x", "n": 9223372036854775807, "t": -1, "f": 0.25, "b": True}
    assert verify.wire_rows(SCHEMA, [row]) == [{"key": "x", "n": "9223372036854775807", "t": "1969-12-31T23:59:59.999999Z", "f": 0.25, "b": True}]


def test_aware_datetime_normalization_uses_exact_us(verify):
    zone = datetime.timezone(datetime.timedelta(hours=1))
    row = {"key": "x", "n": 1, "t": datetime.datetime(1970, 1, 1, 1, 0, 0, 1, tzinfo=zone)}
    assert verify.normalize_rows(SCHEMA, [row])[0]["t"] == 1
    row["t"] = datetime.datetime(1, 1, 1, tzinfo=zone)
    with pytest.raises(ValueError):
        verify.normalize_rows(SCHEMA, [row])


@pytest.mark.parametrize("schema", [
    {"fields": []}, {"fields": [{"name": "x", "type": "int32", "nullable": False}]},
    {"fields": [{"name": "x", "type": "bool", "nullable": "false"}]},
    {"fields": [{"name": "key", "type": "string", "nullable": True}]},
    {"fields": [{"name": "key", "type": "int64", "nullable": False}]},
    {"fields": [{"name": "x", "type": "bool", "nullable": False, "field_id": 2}]},
    {"fields": [{"name": "x", "type": "bool", "nullable": False}] * 2},
    {"fields": [{"name": "é" * 65, "type": "bool", "nullable": False}]},
    {"fields": [{"name": str(i), "type": "bool", "nullable": False} for i in range(129)]},
])
def test_invalid_schema_rejected(verify, schema):
    with pytest.raises(ValueError):
        verify.normalize_schema(schema)


@pytest.mark.parametrize("change", [
    {"n": 9223372036854775808}, {"n": -9223372036854775809}, {"n": True}, {"n": 1.0}, {"n": "1.0"},
    {"n": None}, {"t": 253402300800000000}, {"t": datetime.datetime(1970, 1, 1)},
    {"b": 1}, {"f": float("nan")}, {"f": float("inf")}, {"key": "é" * 513},
    {"key": "\ud800"}, {"unknown": 1},
])
def test_invalid_rows_rejected(verify, change):
    row = {"key": "x", "n": 1, "t": 0}
    row.update(change)
    with pytest.raises(ValueError):
        verify.normalize_rows(SCHEMA, [row])


def test_string_and_row_limits(verify):
    schema = {"fields": [{"name": name, "type": "string", "nullable": False} for name in ["a", "b", "c"]]}
    with pytest.raises(ValueError):
        verify.normalize_rows(schema, [{"a": "x" * (1048576 + 1), "b": "", "c": ""}])
    with pytest.raises(ValueError):
        verify.normalize_rows(schema, [{"a": "x" * 1048576, "b": "x" * 1048576, "c": "x"}])
