"""The independent oracle and SDK accept no schema or receipt that the core validators reject.

The core rejects NUL in field names (ValidateSchema) and committed receipts with
compressed_data_bytes_added outside 1..32 MiB, parent_snapshot_id == snapshot_id or a
durability_profile over 128 bytes (ValidateOutcome).
"""
import pytest

from glamdb_verify import normalize_schema, validate_receipt

RECEIPT = {
    "collection_id": "00112233-4455-6677-8899-aabbccddeeff",
    "operation_id": "10112233-4455-6677-8899-aabbccddeeff",
    "payload_hash": "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7",
    "hash_version": 1, "snapshot_id": "9007199254740993", "parent_snapshot_id": "9007199254740992",
    "rows_appended": "3", "data_files_added": "1", "compressed_data_bytes_added": "120",
    "snapshot_created_at_us": "1700000000000000", "durability_profile": "local-delete-extra-v1",
    "receipt_version": 1,
}


def test_oracle_rejects_nul_in_field_name_like_core():
    with pytest.raises(ValueError):
        normalize_schema({"fields": [{"name": "key", "type": "string", "nullable": False},
                                     {"name": "a\x00b", "type": "string", "nullable": True}]})


def test_sdk_schema_rejects_nul_in_field_name_like_core():
    sdk = pytest.importorskip("glamdb_sdk")
    with pytest.raises(sdk.ValidationError):
        sdk.Schema([sdk.Field("a\x00b", "string", True)])


def test_valid_receipt_is_accepted():
    assert validate_receipt(RECEIPT)["snapshot_id"] == "9007199254740993"


@pytest.mark.parametrize("field,value", [
    ("compressed_data_bytes_added", "0"),
    ("compressed_data_bytes_added", str(32 * 1024 * 1024 + 1)),
    ("parent_snapshot_id", "9007199254740993"),
    ("durability_profile", "p" * 129),
])
def test_receipt_check_rejects_what_core_validate_outcome_rejects(field, value):
    with pytest.raises(ValueError):
        validate_receipt(RECEIPT | {field: value})
