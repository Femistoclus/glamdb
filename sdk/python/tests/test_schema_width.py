"""Wide schemas retain exact values and stable IDs across SDK boundaries."""

import pytest

from glamdb_sdk import Field, Schema, ValidationError, canonical_write_settings
from glamdb_sdk.codec import decode_rows, encode_rows


@pytest.mark.parametrize("width", [65, 105, 128])
def test_wide_schema_roundtrip_keeps_ids_duplicates_and_late_int64(width):
    names = ["WatchID"] + [f"c{index}" for index in range(1, width - 1)] + ["CLID"]
    schema = Schema([Field(name, "int64", True) for name in names])
    assert [field.field_id for field in schema.fields] == list(range(1, width + 1))
    assert Schema.from_dict(schema.to_dict()) == schema
    row = {name: None if index % 7 == 0 else 9007199254740993 + index
           for index, name in enumerate(names)}
    row["CLID"] = 9223372036854775807
    wire = encode_rows(schema, [row, row])
    assert wire[0]["CLID"] == "9223372036854775807"
    assert decode_rows(schema, wire) == [row, row]


@pytest.mark.parametrize("width", [129, 256])
def test_schema_width_has_a_finite_upper_bound(width):
    with pytest.raises(ValidationError):
        Schema([Field(f"c{index}", "int64") for index in range(width)])


@pytest.mark.parametrize("width", [105, 128])
def test_write_settings_cover_every_wide_schema_column(width):
    schema = Schema([Field(f"c{index}", "int64") for index in range(width)])
    columns = [{"field": field.name, "compression": {"codec": "snappy"}}
               for field in reversed(schema.fields)]
    result = canonical_write_settings({"parquet": {"columns": columns}}, schema)
    assert [item["field"] for item in result["parquet"]["columns"]] == [f.name for f in schema.fields]


def test_write_settings_reject_more_than_the_schema_width_limit_without_schema():
    columns = [{"field": f"c{index}", "compression": {"codec": "snappy"}}
               for index in range(129)]
    with pytest.raises(ValidationError):
        canonical_write_settings({"parquet": {"columns": columns}})
