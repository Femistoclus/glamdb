"""A different-content retry of a bound operation ID is an error, never a terminal outcome.

The conflicting request changes nothing; the original operation may be committed or still
unknown, so the SDK must not return a terminal outcome that would license a new operation ID.
Both the native (outcome-shaped) and the mock (plain error) replies must raise HTTPError.
"""
import json

import httpx
import pytest

from glamdb_sdk import AppendOutcome, Client, Field, HTTPError, ProtocolError, Schema

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("key", "string", False)])

# Byte-for-byte the body produced by json_adapter::ToJson for the core's conflict outcome
# (Disposition/Phase/Error helpers, ErrorStatus("OPERATION_CONFLICT") == 409).
NATIVE_CONFLICT = (
    '{"outcome":"definitely_not_committed","phase":"validation","operation_id":"%s",'
    '"error":{"code":"OPERATION_CONFLICT","message":"operation ID is permanently bound to different content",'
    '"retry_hint":"OperatorAction"}}' % OP).encode()
# What apps/mock_server/src/glamdb_mock/state.py:211 returns for the same situation.
MOCK_CONFLICT = json.dumps({"error": {"code": "IDEMPOTENCY_CONFLICT",
                                      "message": "operation_id is bound to a different canonical payload"}}).encode()


def _client(body, status=409):
    return Client("https://example.test", "token", transport=httpx.MockTransport(
        lambda request: httpx.Response(status, content=body, headers={"content-type": "application/json"})))


@pytest.mark.parametrize("body", [MOCK_CONFLICT, NATIVE_CONFLICT], ids=["mock-shape", "native-shape"])
def test_operation_conflict_is_not_a_terminal_outcome_of_the_original_operation(body):
    with _client(body) as client:
        try:
            outcome = client.append(CID, [{"key": "regenerated payload"}], operation_id=OP, schema=SCHEMA)
        except (HTTPError, ProtocolError) as error:
            assert error.operation_id == OP
            return
    assert not (isinstance(outcome, AppendOutcome) and outcome.terminal), (
        f"operation {OP} may already be committed or commit_unknown; a conflicting retry was returned as "
        f"terminal {outcome.outcome!r}, which licenses a new operation ID and a duplicate write")
