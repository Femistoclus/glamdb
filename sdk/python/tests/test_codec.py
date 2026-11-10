"""Literal expectations protect the SDK's public wire conversion boundary."""

from datetime import datetime, timezone
import math

import pytest

from glamdb_sdk import Field, Schema, ValidationError
from glamdb_sdk.codec import decode_rows, encode_rows, format_timestamp, timestamp_us


def test_int64_keeps_bits_beyond_javascript_integer_precision():
    schema = Schema([Field("n", "int64", False)])
    assert encode_rows(schema, [{"n": 9007199254740993}]) == [{"n": "9007199254740993"}]
    assert decode_rows(schema, [{"n": "9007199254740993"}]) == [{"n": 9007199254740993}]


@pytest.mark.parametrize("value", [True, 1.0, 2**63, -(2**63)-1, "1.0", "01"])
def test_int64_rejects_bool_coercion_and_out_of_range(value):
    with pytest.raises(ValidationError):
        encode_rows(Schema([Field("n", "int64", False)]), [{"n": value}])


@pytest.mark.parametrize("text,want", [
    ("0001-01-01T00:00:00Z", -62135596800000000),
    ("9999-12-31T23:59:59.999999Z", 253402300799999999),
    ("1969-12-31T23:59:59.999999Z", -1),
    ("1970-01-01T01:00:00.123+01:00", 123000),
    ("1970-01-01T00:00:00-00:00", 0),
])
def test_timestamp_offsets_and_year_bounds_without_float_rounding(text, want):
    assert timestamp_us(text) == want


@pytest.mark.parametrize("value", [
    "0001-01-01T00:00:00+00:01", "9999-12-31T23:59:59.999999-00:01",
    "10000-01-01T00:00:00Z", "0000-01-01T00:00:00Z", "2020-01-01T00:00:60Z",
    "2020-01-01T00:00:00.0000001Z", "2020-01-01", "2020-01-01T00:00:00",
    "2020-01-01T00:00:00+24:00", datetime(2020, 1, 1), True,
    -62135596800000001, 253402300800000000,
])
def test_timestamp_rejects_invalid_or_naive_values(value):
    with pytest.raises(ValidationError):
        timestamp_us(value)


def test_timestamp_wire_is_six_digit_utc_and_decodes_to_aware_datetime():
    schema = Schema([Field("t", "timestamp_us_utc", False)])
    assert format_timestamp(-62135596800000000) == "0001-01-01T00:00:00.000000Z"
    assert format_timestamp(253402300799999999) == "9999-12-31T23:59:59.999999Z"
    assert encode_rows(schema, [{"t": -1}]) == [{"t": "1969-12-31T23:59:59.999999Z"}]
    assert decode_rows(schema, [{"t": "1970-01-01T00:00:00.000001Z"}]) == [
        {"t": datetime(1970, 1, 1, microsecond=1, tzinfo=timezone.utc)}
    ]


def test_nullable_missing_values_and_negative_zero_are_normalized():
    schema = Schema([Field("key", "string", False), Field("v", "float64", True)])
    assert encode_rows(schema, [{"key": "a"}]) == [{"key": "a", "v": None}]
    value = encode_rows(schema, [{"key": "a", "v": -0.0}])[0]["v"]
    assert value == 0.0 and math.copysign(1, value) == 1


@pytest.mark.parametrize("row", [{}, {"key": None}, {"key": "x", "other": 1}, {"key": 1}])
def test_required_unknown_and_mismatched_fields_are_rejected(row):
    with pytest.raises(ValidationError):
        encode_rows(Schema([Field("key", "string", False)]), [row])


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True])
def test_float_values_must_be_finite_and_not_bool(value):
    with pytest.raises(ValidationError):
        encode_rows(Schema([Field("f", "float64", False)]), [{"f": value}])


def test_bool_cannot_be_supplied_as_integer():
    with pytest.raises(ValidationError):
        encode_rows(Schema([Field("b", "bool", False)]), [{"b": 1}])


@pytest.mark.parametrize("fields", [[], [Field("x", "unknown", False)],
    [Field("x", "int64", False), Field("x", "string", True)],
    [Field("key", "string", True)], [Field("key", "int64", False)]])
def test_invalid_schema_is_rejected(fields):
    with pytest.raises(ValidationError):
        Schema(fields)


def test_schema_assigns_and_preserves_stable_ids():
    schema = Schema.from_dict({"fields": [
        {"field_id": 4, "name": "x", "type": "int64", "nullable": False},
        {"field_id": 8, "name": "y", "type": "string", "nullable": True},
    ]})
    assert [field.field_id for field in schema.fields] == [4, 8]
    assert Schema([Field("x", "int64", False)]).to_dict() == {
        "fields": [{"field_id": 1, "name": "x", "type": "int64", "nullable": False}]}


@pytest.mark.parametrize("value", [0, "0", "1970-01-01T00:00:00Z", "1970-01-01T01:00:00.000000+01:00"])
def test_timestamp_output_wire_requires_canonical_six_digit_utc_text(value):
    with pytest.raises(ValidationError):
        decode_rows(Schema([Field("t", "timestamp_us_utc", False)]), [{"t": value}])


@pytest.mark.parametrize("kind,value,wire,decoded,second_bytes", [
    ("int64", 9223372036854775807, "9223372036854775807", 9223372036854775807, 1048008),
    ("int64", -9223372036854775808, "-9223372036854775808", -9223372036854775808, 1048008),
    ("int64", 0, "0", 0, 1048008),
    ("float64", 1.5, 1.5, 1.5, 1048008),
    ("timestamp_us_utc", 253402300799999999, "9999-12-31T23:59:59.999999Z",
     datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc), 1048008),
    ("bool", True, True, True, 1048442),
    ("bool", False, False, False, 1048442),
    ("int64", None, None, None, 1048504),
])
@pytest.mark.parametrize("from_wire", [False, True], ids=["input", "wire"])
@pytest.mark.parametrize("extra", [0, 1], ids=["exact-limit", "one-byte-over"])
def test_row_limit_counts_canonical_decoded_values(kind, value, wire, decoded, second_bytes,
                                                  from_wire, extra):
    # Two string presence/length markers plus 62 scalar encodings make exactly
    # 2 MiB before extra. Each scalar costs 9 bytes, bool 2, or NULL 1.
    schema = Schema([Field("a", "string"), Field("b", "string")] + [
        Field(f"v{index}", kind, value is None) for index in range(62)])
    strings = {"a": "x" * 1048576, "b": "é" * (second_bytes // 2) + "x" * extra}
    row = strings | {f"v{index}": wire if from_wire else value for index in range(62)}
    convert = decode_rows if from_wire else encode_rows
    if extra:
        with pytest.raises(ValidationError, match="2 MiB"):
            convert(schema, [row])
    else:
        expected = strings | {f"v{index}": decoded if from_wire else wire for index in range(62)}
        assert convert(schema, [row]) == [expected]
