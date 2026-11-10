"""Expected writer/manifest inputs; these tests do not read physical manifests."""

import json
from pathlib import Path

import pytest

from glamdb_verify import normalize_schema, scan_rows


FIXTURE = json.loads((Path(__file__).resolve().parents[2] /
                      "fixtures/metadata/file-metrics.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["name"])
def test_expected_full_key_metrics_and_matches_are_independent_of_footer(case):
    schema = normalize_schema(FIXTURE["schema"])
    assert FIXTURE["physical_manifest_validated"] is False
    assert schema["fields"][0]["field_id"] == case["expected_key_field_id"]
    keys = [row["key"].encode("utf-8") for row in case["rows"]]
    assert len(keys) == case["expected_manifest_record_count"]
    assert min(keys) == case["expected_key_bounds"]["lower"].encode("utf-8")
    assert max(keys) == case["expected_key_bounds"]["upper"].encode("utf-8")
    assert sum(row["value"] is None for row in case["rows"]) == case["expected_value_null_count"]
    actual = scan_rows(schema, case["rows"], ["key"], case["predicate"])
    assert [row["key"] for row in actual] == case["expected_keys"]
    if case["footer_key_bounds"]:
        assert case["footer_key_bounds"]["truncated"] is True
        assert case["footer_key_bounds"]["upper"].encode() < max(keys)
    if case["name"].startswith("utf8-key"):
        assert max(map(len, keys)) == 1024
