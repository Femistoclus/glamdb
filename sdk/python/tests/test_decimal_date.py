"""Exact decimal/date contracts, independent of binary floating point."""

from collections import UserDict
from datetime import date, datetime
from decimal import Decimal, localcontext
import json
from pathlib import Path

import pytest

from glamdb_sdk import AppendRequest, Field, Schema, ValidationError, col
from glamdb_sdk.codec import _canonical_row_size, decode_rows, encode_rows, row_plan
from glamdb_sdk.request import canonical_payload_hash, schema_fingerprint

CID = "00112233-4455-6677-8899-aabbccddeeff"
OP = "10112233-4455-6677-8899-aabbccddeeff"


def money(precision=8, scale=2, nullable=False):
    return Schema([Field("v", "decimal", nullable, None, precision, scale)])


@pytest.mark.parametrize("value,want", [
    ("00012.3", "12.30"), ("-000.0000", "0.00"), ("1.23000", "1.23"),
    ("0", "0.00"), ("-999999.99", "-999999.99"),
    (Decimal("12.3000"), "12.30"), (Decimal("-0"), "0.00"),
    (Decimal("1E+2"), "100.00"),
])
def test_decimal_normalizes_without_rounding(value, want):
    schema = money()
    assert encode_rows(schema, [{"v": value}]) == [{"v": want}]
    decoded = decode_rows(schema, [{"v": want}])[0]["v"]
    assert type(decoded) is Decimal and str(decoded) == want
    assert row_plan(schema).encode_row({"v": value}) == row_plan(schema).encode_row(UserDict(v=value))


@pytest.mark.parametrize("value", [
    "1.231", "1000000.00", "-1000000.00", "1e2", "+1", " 1", "1 ",
    ".1", "1.", "NaN", "Infinity", "１", "1_0", "0" * 129,
    1, True, 1.25, Decimal("NaN"), Decimal("Infinity"), Decimal("1E+1000000000"),
])
def test_decimal_rejects_lossy_or_nonplain_inputs(value):
    with pytest.raises(ValidationError):
        encode_rows(money(), [{"v": value}])


def test_decimal_38_digits_ignore_active_decimal_context_and_preserve_cache_parameters():
    schema = money(38, 2)
    text = "999999999999999999999999999999999999.99"
    with localcontext() as context:
        context.prec = 3
        assert encode_rows(schema, [{"v": Decimal(text)}]) == [{"v": text}]
        assert str(decode_rows(schema, [{"v": text}])[0]["v"]) == text
    assert row_plan(money(8, 2)) is not row_plan(money(8, 3))
    assert schema_fingerprint(money(8, 2)) != schema_fingerprint(money(8, 3))
    assert encode_rows(money(2, 2), [{"v": "0.99"}]) == [{"v": "0.99"}]
    assert encode_rows(money(1, 0), [{"v": "-9.000"}]) == [{"v": "-9"}]
    with pytest.raises(ValidationError):
        encode_rows(money(2, 2), [{"v": "1.00"}])
    tiny = money(38, 38)
    assert encode_rows(tiny, [{"v": Decimal("1E-38")}]) == [{"v": "0." + "0" * 37 + "1"}]
    assert encode_rows(tiny, decode_rows(tiny, [{"v": "0." + "0" * 38}])) == [{"v": "0." + "0" * 38}]


@pytest.mark.parametrize("kind,params", [
    ("decimal", {}), ("decimal", {"precision": 8}), ("decimal", {"scale": 2}),
    ("decimal", {"precision": 0, "scale": 0}), ("decimal", {"precision": 39, "scale": 0}),
    ("decimal", {"precision": 8, "scale": -1}), ("decimal", {"precision": 8, "scale": 9}),
    ("decimal", {"precision": True, "scale": 0}), ("decimal", {"precision": 8, "scale": False}),
    ("decimal", {"precision": 8.0, "scale": 2}),
    ("int64", {"precision": 8}), ("date", {"scale": 0}), ("string", {"scale": 0}),
])
def test_schema_rejects_missing_invalid_or_unrelated_decimal_parameters(kind, params):
    with pytest.raises(ValidationError):
        Schema.from_dict({"fields": [{"name": "v", "type": kind, "nullable": True, **params}]})


@pytest.mark.parametrize("kind", ["int64", "date", "string"])
def test_wire_schema_rejects_explicit_null_decimal_parameters_on_other_types(kind):
    with pytest.raises(ValidationError):
        Schema.from_dict({"fields": [{"name": "v", "type": kind, "nullable": False,
                                      "precision": None, "scale": None}]})


def test_schema_parameters_roundtrip_and_old_positional_fields_remain_compatible():
    schema = Schema([Field("old", "int64", True, 7), Field("v", "decimal", False, 9, 8, 2),
                     Field("d", "date", True, 11)])
    assert schema.to_dict() == {"fields": [
        {"field_id": 7, "name": "old", "type": "int64", "nullable": True},
        {"field_id": 9, "name": "v", "type": "decimal", "nullable": False, "precision": 8, "scale": 2},
        {"field_id": 11, "name": "d", "type": "date", "nullable": True},
    ]}
    assert Schema.from_dict(schema.to_dict()) == schema


@pytest.mark.parametrize("value,want", [
    (date(1, 1, 1), "0001-01-01"), (date(9999, 12, 31), "9999-12-31"),
    ("2000-02-29", "2000-02-29"), ("1969-12-31", "1969-12-31"),
])
def test_date_calendar_boundaries_roundtrip(value, want):
    schema = Schema([Field("d", "date")])
    assert encode_rows(schema, [{"d": value}]) == [{"d": want}]
    decoded = decode_rows(schema, [{"d": want}])[0]["d"]
    assert type(decoded) is date and decoded.isoformat() == want


@pytest.mark.parametrize("value", [
    "0000-01-01", "10000-01-01", "1900-02-29", "2000-02-30", "2026-1-01",
    "20260101", "2026-W01-1", "2026-01-01T00:00:00Z", "2026-01-01 ", "２０２６-01-01",
    datetime(2026, 1, 1), 0, True,
])
def test_date_rejects_noncalendar_and_datetime_inputs(value):
    with pytest.raises(ValidationError):
        encode_rows(Schema([Field("d", "date")]), [{"d": value}])


@pytest.mark.parametrize("value", ["01.20", "1.2", "-0.00", "1.200", Decimal("1.20"), 1.2])
def test_decimal_wire_requires_fixed_scale_canonical_text(value):
    with pytest.raises(ValidationError):
        decode_rows(money(), [{"v": value}])


def test_decimal_date_nulls_sizes_and_typed_predicates():
    schema = Schema([Field("v", "decimal", True, 1, 8, 2), Field("d", "date", True)])
    assert encode_rows(schema, [{}]) == [{"v": None, "d": None}]
    row = {"v": "12.30", "d": "1970-01-01"}
    assert _canonical_row_size(schema, row) == 22
    assert len(row_plan(schema).encode_row(row)[1]) == 22
    assert _canonical_row_size(schema, {"v": None, "d": None}) == 2
    assert col("v", "decimal", precision=8, scale=2).eq("00012.300").to_dict() == {
        "op": "eq", "field": "v", "value": {"type": "decimal", "precision": 8, "scale": 2, "value": "12.30"}}
    assert col("v", "decimal", precision=8, scale=2).in_([None, Decimal("1")]).to_dict()["values"] == [
        {"type": "decimal", "precision": 8, "scale": 2, "value": None},
        {"type": "decimal", "precision": 8, "scale": 2, "value": "1.00"}]
    assert col("d", "date").eq(date(2026, 11, 12)).to_dict() == {
        "op": "eq", "field": "d", "value": {"type": "date", "value": "2026-11-12"}}


def test_mixed_literal_golden_hash_is_shared_without_changing_old_vectors():
    root = Path(__file__).resolve().parents[3] / "fixtures" / "golden"
    for name in ("int64-null-duplicates", "all-types-boundaries", "decimal-date-boundaries"):
        vector = json.loads((root / f"{name}.json").read_text())
        schema = Schema.from_dict(vector["schema"])
        request = AppendRequest.prepare(CID, OP, schema, vector["rows"])
        assert schema_fingerprint(schema).hex() == vector["schema_fingerprint"]
        assert request.payload_hash == vector["payload_sha256"]
        assert canonical_payload_hash(CID, schema, request.to_dict()["rows"]) == vector["payload_sha256"]
        assert bytes.fromhex(vector["canonical_hex"])[74:] == b"".join(
            row_plan(schema).encode_row(row)[1] for row in vector["rows"])


@pytest.mark.parametrize("kind,value,second_bytes", [
    ("decimal", "12.30", 1047512), ("date", "0001-01-01", 1048256),
])
@pytest.mark.parametrize("extra", [0, 1], ids=["exact-limit", "one-byte-over"])
def test_decimal_date_row_limits_count_fixed_binary_payload(kind, value, second_bytes, extra):
    fields = [Field("a", "string"), Field("b", "string")] + [
        Field(f"v{i}", kind, False, i + 3, 8 if kind == "decimal" else None, 2 if kind == "decimal" else None)
        for i in range(62)]
    schema = Schema(fields)
    row = {"a": "x" * 1048576, "b": "x" * (second_bytes + extra)} | {f"v{i}": value for i in range(62)}
    if extra:
        with pytest.raises(ValidationError):
            encode_rows(schema, [row])
        with pytest.raises(ValidationError):
            decode_rows(schema, [row])
    else:
        assert _canonical_row_size(schema, encode_rows(schema, [row])[0]) == 2097152
        assert len(decode_rows(schema, [row])) == 1
