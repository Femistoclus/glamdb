"""Timestamp text is always RFC3339 with a timezone.

Digit-only text such as "20260101" is rejected instead of being read as epoch microseconds;
exact integer microseconds are passed as int.
"""
import pytest

from glamdb_sdk import Field, Schema, ValidationError, col
from glamdb_sdk.codec import encode_scalar
from glamdb_sdk.request import AppendRequest

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
FIELD = Field("at", "timestamp_us_utc", True)
SCHEMA = Schema([Field("key", "string", False), FIELD])


@pytest.mark.parametrize("text", ["20260101", "2026", "1700000000000000", "0", "-1"])
def test_digit_only_text_is_rejected_for_append_values(text):
    with pytest.raises(ValidationError):
        encode_scalar(FIELD, text)
    with pytest.raises(ValidationError):
        AppendRequest.prepare(CID, OP, SCHEMA, [{"key": "k", "at": text}])


@pytest.mark.parametrize("text", ["20260101", "1700000000000000"])
def test_digit_only_text_is_rejected_for_predicate_literals(text):
    with pytest.raises(ValidationError):
        col("at", "timestamp_us_utc").ge(text)


def test_documented_timestamp_inputs_still_work():
    assert encode_scalar(FIELD, 1_700_000_000_000_000) == "2023-11-14T22:13:20.000000Z"
    assert encode_scalar(FIELD, "2026-01-01T01:00:00+01:00") == "2026-01-01T00:00:00.000000Z"
