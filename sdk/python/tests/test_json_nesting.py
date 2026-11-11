"""Deeply nested JSON within the byte limit is a ProtocolError that keeps the write identity."""
import httpx
import pytest

from glamdb_sdk import Client, Field, ProtocolError, Schema

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
NESTED = b'{"outcome":' + b"[" * 100_000 + b"]" * 100_000 + b"}"


def test_nested_append_reply_is_protocol_error_with_identity():
    transport = httpx.MockTransport(lambda request: httpx.Response(
        200, content=NESTED, headers={"content-type": "application/json"}))
    with Client("https://example.test", "token", transport=transport) as client:
        with pytest.raises(ProtocolError) as error:
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=Schema([Field("key", "string", False)]))
    assert error.value.operation_id == OP and error.value.collection_id == CID
