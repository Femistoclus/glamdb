"""The independent oracle accepts the same bounded scalar schema domain."""

import pytest

from glamdb_verify.types import normalize_schema, normalize_rows
from glamdb_verify.canonical import schema_fingerprint


@pytest.mark.parametrize("width", [65, 105, 128])
def test_wide_oracle_preserves_late_fields_and_duplicate_rows(width):
    schema = {"fields": [{"name": f"c{index}", "type": "int64", "nullable": False}
                         for index in range(width)]}
    normalized = normalize_schema(schema)
    assert [field["field_id"] for field in normalized["fields"]] == list(range(1, width + 1))
    row = {f"c{index}": 9007199254740993 + index for index in range(width)}
    assert normalize_rows(schema, [row, row]) == [row, row]
    assert len(schema_fingerprint(schema)) == 32


def test_oracle_rejects_a_schema_over_the_width_limit():
    schema = {"fields": [{"name": f"c{index}", "type": "int64", "nullable": False}
                         for index in range(129)]}
    with pytest.raises(ValueError):
        normalize_schema(schema)
