"""A definitely_not_committed reply must be a consistent failure envelope.

It needs an HTTP error status (400-599), no recovery_required:true and no recovery object;
anything else is a ProtocolError rather than a terminal outcome.
"""
import httpx
import pytest

from glamdb_sdk import Client, Field, ProtocolError, Schema

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("key", "string", False)])
BASE = {"outcome": "definitely_not_committed", "phase": "catalog_commit",
        "error": {"code": "IO_ERROR", "message": "fsync failed", "retry_hint": "NewOperationId"}}
CONTRADICTORY = [
    (200, {}), (202, {}),
    (503, {"recovery_required": True}),
    (503, {"recovery_required": True, "recovery": {"operation_id": OP, "publication_started": True}}),
    (500, {"recovery": {"operation_id": OP, "publication_started": True}}),
]


def _client(body, status):
    return Client("https://example.test", "token",
                  transport=httpx.MockTransport(lambda request: httpx.Response(status, json=body)))


@pytest.mark.parametrize("status,extra", CONTRADICTORY)
def test_contradictory_failure_envelope_is_rejected_on_append_and_lookup(status, extra):
    body = BASE | {"operation_id": OP} | extra
    with _client(body, status) as client:
        with pytest.raises(ProtocolError):
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
        with pytest.raises(ProtocolError):
            client.find_append_outcome(CID, OP)


@pytest.mark.parametrize("status,extra", CONTRADICTORY)
def test_contradictory_failure_envelope_is_rejected_on_create(status, extra):
    with _client(BASE | extra, status) as client:
        with pytest.raises(ProtocolError):
            client.create_collection("events", SCHEMA, collection_id=CID)


@pytest.mark.parametrize("status,extra", [
    (409, {"recovery_required": False}),       # mock shape (state.py:172-177)
    (503, {"error": {"code": "RECOVERY_REQUIRED", "message": "m", "retry_hint": "SameIdAfterReconcile"}}),  # native pre-binding refusal
])
def test_consistent_definite_failures_remain_terminal(status, extra):
    with _client(BASE | {"operation_id": OP} | extra, status) as client:
        outcome = client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
    assert outcome.outcome == "definitely_not_committed" and outcome.terminal
