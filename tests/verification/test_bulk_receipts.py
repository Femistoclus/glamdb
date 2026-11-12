"""Literal receipt profiles shared with native serialization, SDK and UI checks."""

import copy
import json
from pathlib import Path

import pytest

from glamdb_verify import validate_receipt


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = json.loads((ROOT / "fixtures/http/outcomes-v0.json").read_text())
CASES = {case["name"]: case for case in FIXTURE["cases"]}
COMMITTED = [case for case in FIXTURE["cases"] if case["response"]["outcome"] == "committed"]


@pytest.mark.parametrize("case", COMMITTED, ids=lambda case: case["name"])
def test_shared_committed_receipts_validate_without_changing_identity(case):
    receipt = case["response"]["result"]
    normalized = validate_receipt(receipt)
    assert normalized["operation_id"] == receipt["operation_id"]
    assert normalized["receipt_version"] == receipt["receipt_version"]
    assert normalized["rows_appended"] == int(receipt["rows_appended"])


@pytest.mark.parametrize("invalid", FIXTURE["invalid_receipts"], ids=lambda case: case["name"])
def test_shared_invalid_receipts_are_rejected(invalid):
    receipt = copy.deepcopy(CASES[invalid["case"]]["response"]["result"])
    field = invalid.get("remove", invalid.get("path")).split("/")[-1]
    if "remove" in invalid:
        del receipt[field]
    else:
        receipt[field] = invalid["value"]
    with pytest.raises(ValueError):
        validate_receipt(receipt)


@pytest.mark.parametrize("rows,files,size", [
    (1, 1, 1), (100000, 1, 33554432), (25600000, 256, 8589934592),
])
def test_bulk_receipt_profile_accepts_exact_bounds(rows, files, size):
    receipt = CASES["bulk_committed"]["response"]["result"] | {
        "rows_appended": str(rows), "data_files_added": str(files),
        "compressed_data_bytes_added": str(size),
    }
    assert validate_receipt(receipt)["compressed_data_bytes_added"] == size


@pytest.mark.parametrize("case", COMMITTED, ids=lambda case: case["name"])
def test_native_wire_schema_retains_required_fields_and_supports_bulk(case):
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((ROOT / "fixtures/http/v0.schema.json").read_text())
    validator = jsonschema.Draft202012Validator(schema | {"$ref": "#/$defs/receipt"})
    receipt = case["response"]["result"]
    validator.validate(receipt)
    assert not validator.is_valid(receipt | {"receipt_version": 3})
    for field in ("data_files_added", "compressed_data_bytes_added", "durability_profile", "parent_snapshot_id"):
        partial = dict(receipt)
        del partial[field]
        assert not validator.is_valid(partial)
