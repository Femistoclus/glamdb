"""Mock-only development faults and collection pagination, separate from native evidence."""
from uuid import uuid4

import httpx
import pytest

from glamdb_sdk import Client, HTTPError
from .helpers.http_server import MockHTTPServer, ScenarioHTTPTransport, TOKEN
from .test_network_sdk import ROWS, SCHEMA, create


@pytest.fixture
def server(tmp_path):
    with MockHTTPServer(tmp_path) as running:
        yield running


def test_mock_collection_pagination_has_fixed_membership(server):
    with Client(server.url, TOKEN) as client:
        collection_ids = [create(client, name=f"collection-{index}") for index in range(3)]
        first = client.list_collections(page_size=1)
        later_collection = create(client, name="created-after-page")
        pages = [first]
        cursor = first.next_cursor
        while cursor is not None:
            page = client.list_collections(page_size=1, cursor=cursor)
            assert client.list_collections(page_size=1, cursor=cursor).raw == page.raw
            pages.append(page)
            cursor = page.next_cursor
        assert [item["collection_id"] for page in pages for item in page.items] == collection_ids
        assert later_collection not in [item["collection_id"] for page in pages for item in page.items]
        with pytest.raises(HTTPError) as mismatch:
            client.list_collections(page_size=2, cursor=first.next_cursor)
        assert mismatch.value.detail.code == "CURSOR_MISMATCH"



def test_real_http_unknown_blocks_absence_and_new_writes_until_explicit_mock_resolution(server):
    operation = str(uuid4())
    failure_operation = str(uuid4())
    transport = ScenarioHTTPTransport(scenarios={operation: "commit_unknown", failure_operation: "definitely_not_committed"})
    with Client(server.url, TOKEN, transport=transport) as client:
        cid = create(client)
        request = client.prepare_append(cid, ROWS, operation_id=operation, schema=SCHEMA)
        unknown = client.append(request)
        assert unknown.outcome == "commit_unknown" and not unknown.terminal
        assert unknown.error.code == "RECOVERY_REQUIRED" and unknown.raw["mock"] is True
        assert client.find_append_outcome(cid, operation).raw == unknown.raw
        assert client.append(request).raw == unknown.raw
        assert client.stats(cid)["rows"] == 0
        with pytest.raises(HTTPError) as absence:
            client.find_append_outcome(cid, uuid4())
        assert absence.value.status_code == 503 and absence.value.detail.code == "RECOVERY_REQUIRED"
        with pytest.raises(HTTPError) as blocked:
            client.append(cid, [{"key": "blocked"}], operation_id=uuid4(), schema=SCHEMA)
        assert blocked.value.detail.code == "RECOVERY_REQUIRED"
        resolution = httpx.post(f"{server.url}/development/collections/{cid}/operations/{operation}:resolve",
                                headers={"Authorization": f"Bearer {TOKEN}"}, json={"outcome": "committed"}, timeout=5)
        assert resolution.status_code == 200
        original = client.find_append_outcome(cid, operation)
        assert original.outcome == "committed" and client.append(request).raw == original.raw
        assert original.result["payload_hash"] == request.payload_hash and client.stats(cid)["rows"] == 3
        failure = client.append(cid, [{"key": "failure"}], operation_id=failure_operation, schema=SCHEMA)
        assert failure.outcome == "definitely_not_committed" and failure.error.code == "MOCK_FAILURE"
        assert client.find_append_outcome(cid, failure_operation).raw == failure.raw
        assert client.stats(cid)["rows"] == 3


def test_real_http_mock_lost_response_returns_original_receipt_without_second_append(server):
    operation = str(uuid4())
    transport = ScenarioHTTPTransport(scenarios={operation: "lost_response"})
    with Client(server.url, TOKEN, transport=transport) as client:
        cid = create(client)
        request = client.prepare_append(cid, ROWS, operation_id=operation, schema=SCHEMA)
        assert client.append(request).outcome == "commit_unknown"
        original = client.find_append_outcome(cid, operation)
        assert original.outcome == "committed"
        assert client.append(request).raw == original.raw
        assert client.stats(cid)["rows"] == 3
        assert [op for op, body in transport.requests] == [operation, operation]
        assert transport.requests[0][1] == transport.requests[1][1]
