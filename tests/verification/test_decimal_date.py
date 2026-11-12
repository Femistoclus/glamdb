"""Independent decimal/date normalization and typed predicate expectations."""

from datetime import date, datetime
from decimal import Decimal, localcontext
import json
from pathlib import Path

import pytest


SCHEMA = {"fields": [
    {"name": "v", "type": "decimal", "nullable": True, "precision": 38, "scale": 2},
    {"name": "d", "type": "date", "nullable": True},
]}


def test_exact_decimal_date_normalization_and_wire(verify):
    with localcontext() as context:
        context.prec = 3
        rows = verify.normalize_rows(SCHEMA, [{"v": "-000.0000", "d": "0001-01-01"},
            {"v": Decimal("999999999999999999999999999999999999.99"), "d": date(9999, 12, 31)}, {}])
    assert rows == [{"v": Decimal("0.00"), "d": date(1, 1, 1)},
                    {"v": Decimal("999999999999999999999999999999999999.99"), "d": date(9999, 12, 31)},
                    {"v": None, "d": None}]
    assert verify.wire_rows(SCHEMA, rows) == [
        {"v": "0.00", "d": "0001-01-01"},
        {"v": "999999999999999999999999999999999999.99", "d": "9999-12-31"},
        {"v": None, "d": None}]


@pytest.mark.parametrize("value", ["1.001", "1e2", "+1", " 1", "1.", ".1", "NaN", "0" * 129,
    "1000000000000000000000000000000000000.00", 1, 1.2, True, Decimal("1E+1000000000"), Decimal("NaN")])
def test_decimal_rejects_lossy_overflow_and_nonplain_input(verify, value):
    with pytest.raises(ValueError):
        verify.normalize_rows(SCHEMA, [{"v": value}])


@pytest.mark.parametrize("value", ["1900-02-29", "0000-01-01", "10000-01-01", "2026-1-01",
    "20260101", "2026-01-01T00:00:00Z", "2026-01-01\n", datetime(2026, 1, 1), 0])
def test_date_requires_strict_calendar_date(verify, value):
    with pytest.raises(ValueError):
        verify.normalize_rows(SCHEMA, [{"d": value}])


@pytest.mark.parametrize("kind,params", [
    ("decimal", {}), ("decimal", {"precision": 8}), ("decimal", {"precision": True, "scale": 0}),
    ("decimal", {"precision": 39, "scale": 0}), ("decimal", {"precision": 8, "scale": 9}),
    ("decimal", {"precision": 8, "scale": -1}), ("decimal", {"precision": 8, "scale": False}),
    ("date", {"precision": 8}), ("int64", {"scale": None}),
])
def test_schema_decimal_parameters_are_required_and_type_specific(verify, kind, params):
    with pytest.raises(ValueError):
        verify.normalize_schema({"fields": [{"name": "v", "type": kind, **params}]})


def test_decimal_predicates_use_exact_typed_parameters_and_null_logic(verify):
    rows = [{"v": "12.30", "d": "2000-02-29"}, {"v": "12.31", "d": "1970-01-01"}, {}]
    value = {"type": "decimal", "precision": 38, "scale": 2, "value": "12.3000"}
    assert verify.scan_rows(SCHEMA, rows, ["d"], {"op": "eq", "field": "v", "value": value}) == [{"d": date(2000, 2, 29)}]
    assert verify.scan_rows(SCHEMA, rows, ["d"], {"op": "in", "field": "v", "values": [value, None]}) == [{"d": date(2000, 2, 29)}]
    assert verify.scan_rows(SCHEMA, rows, ["v"], {"op": "eq", "field": "d", "value": {"type": "date", "value": "1970-01-01"}}) == [{"v": Decimal("12.31")}]
    typed_null = {**value, "value": None}
    assert verify.scan_rows(SCHEMA, rows, None, {"op": "eq", "field": "v", "value": typed_null}) == []
    for invalid in ("12.30", {"type": "decimal", "value": "12.30"}, {**value, "scale": 3},
                    {**value, "precision": 37}, {**value, "precision": True}, {**value, "extra": 1},
                    {"type": "date", "value": "2000-01-01"}):
        with pytest.raises(ValueError):
            verify.scan_rows(SCHEMA, [], None, {"op": "eq", "field": "v", "value": invalid}, 0)


def test_parquet_decoded_decimal_and_date_values_match_literal_hash(verify, tmp_path):
    pa = pytest.importorskip("pyarrow")
    parquet = pytest.importorskip("pyarrow.parquet")
    fixture = json.loads((Path(__file__).resolve().parents[2] /
                          "fixtures/golden/decimal-date-boundaries.json").read_text())
    stored = tmp_path / "data.parquet"
    arrow_schema = pa.schema([pa.field("key", pa.string(), nullable=False),
                              pa.field("amount", pa.decimal128(38, 2)), pa.field("day", pa.date32())])
    parquet.write_table(pa.Table.from_pylist(verify.normalize_rows(fixture["schema"], fixture["rows"]),
                                            schema=arrow_schema), stored)
    decoded = parquet.read_table(stored).to_pylist()
    assert verify.append_hash(fixture["schema"], fixture["collection_id"], decoded) == "e3dce310f241d4479f7bf5ed508ebf2a5661e2cc6424cc910658bea647ca0144"
    assert verify.scan_rows(fixture["schema"], decoded, ["key"], {
        "op": "lt", "field": "day", "value": {"type": "date", "value": "1970-01-01"}}) == [{"key": "min"}]


@pytest.mark.parametrize('kind,value', [
    ('string', 'text'), ('bool', True), ('int64', '1'), ('float64', 1.0),
    ('timestamp_us_utc', '1970-01-01T00:00:00.000000Z'),
])
def test_legacy_predicates_reject_typed_objects_like_native_codec(verify, kind, value):
    schema = {'fields': [{'name': 'v', 'type': kind, 'nullable': True}]}
    with pytest.raises(ValueError):
        verify.scan_rows(schema, [], None, {
            'op': 'eq', 'field': 'v', 'value': {'type': kind, 'value': value}})
