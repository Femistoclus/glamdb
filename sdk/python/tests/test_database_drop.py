"""An explicit saved identity is the only authority for database cleanup."""

import json
from uuid import UUID

import httpx
import pytest

from glamdb_sdk import (AuthenticationError, Client, HTTPError, ProtocolError,
                        TransportError, ValidationError)

DATABASE = "11111111-2222-4333-8444-555555555555"
OTHER = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
RESULT = {"database_id": DATABASE, "store_id": DATABASE, "dropped": True}


def test_drop_uses_explicit_identity_and_bearer_auth_without_scoped_prefix():
    sent = []

    def handler(request):
        sent.append(request)
        return httpx.Response(200, json=RESULT)

    with Client("https://example.test/proxy/", token_provider=lambda: "drop-secret",
                database_id=OTHER, transport=httpx.MockTransport(handler)) as client:
        assert client.drop_database(UUID(DATABASE), store_id=DATABASE, deadline=2) == RESULT

    assert len(sent) == 1
    assert (sent[0].method, str(sent[0].url)) == (
        "POST", f"https://example.test/proxy/databases/{DATABASE}/drop")
    assert sent[0].headers["authorization"] == "Bearer drop-secret"
    assert sent[0].headers["content-type"] == "application/json"
    assert json.loads(sent[0].content) == {"store_id": DATABASE}


@pytest.mark.parametrize("database,store", [
    ("benchmark-name", DATABASE), (DATABASE, "benchmark-name"),
    (None, DATABASE), (DATABASE, None), (DATABASE, OTHER),
])
def test_invalid_drop_identity_is_rejected_before_network(database, store):
    sent = []
    with Client("https://example.test", "secret", transport=httpx.MockTransport(
            lambda request: sent.append(request))) as client:
        with pytest.raises(ValidationError):
            client.drop_database(database, store_id=store)
    assert sent == []


@pytest.mark.parametrize("result", [
    {}, {"database_id": DATABASE, "store_id": DATABASE},
    RESULT | {"database_id": OTHER}, RESULT | {"store_id": OTHER},
    RESULT | {"database_id": None}, RESULT | {"store_id": 1},
    RESULT | {"dropped": False}, RESULT | {"dropped": 1},
    RESULT | {"dropped": "true"}, [],
])
def test_malformed_or_mismatched_success_is_never_reported_as_dropped(result):
    with Client("https://example.test", "secret", transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=result))) as client:
        with pytest.raises(ProtocolError):
            client.drop_database(DATABASE, store_id=DATABASE)


def test_lost_drop_reply_is_sent_once_and_manual_retry_preserves_original_identity():
    sent = []

    def handler(request):
        sent.append((request.method, str(request.url), bytes(request.content)))
        if len(sent) == 1:
            raise httpx.ReadError("lost reply", request=request)
        return httpx.Response(200, json=RESULT)

    with Client("https://example.test", "secret", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TransportError):
            client.drop_database(DATABASE, store_id=DATABASE)
        assert len(sent) == 1
        assert client.drop_database(DATABASE, store_id=DATABASE) == RESULT

    assert sent[0] == sent[1]


@pytest.mark.parametrize("status,error", [(403, AuthenticationError), (404, HTTPError), (503, HTTPError)])
def test_drop_preserves_http_failure_without_retry(status, error):
    sent = []

    def handler(request):
        sent.append(request)
        return httpx.Response(status, json={"error": {"code": "DROP_FAILED", "message": "not complete"}})

    with Client("https://example.test", "secret", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(error) as caught:
            client.drop_database(DATABASE, store_id=DATABASE)
        assert caught.value.status_code == status
    assert len(sent) == 1
