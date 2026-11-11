"""Real disposable mock state consumed by the SDK; not native storage evidence."""

import json
from pathlib import Path

import httpx
import pytest

from glamdb_mock.state import MockStore
from glamdb_sdk import Client, Schema


FIXTURE = json.loads((Path(__file__).resolve().parents[3] / "fixtures/http/outcomes-v0.json").read_text())


@pytest.mark.parametrize("scenario", ["commit_unknown", "lost_response"])
def test_mock_emits_complete_recovery_for_the_shared_request(scenario):
    store = MockStore("token")
    store.create({"collection_id": FIXTURE["collection_id"], "name": FIXTURE["exact_name"], "schema": FIXTURE["schema"]})
    collection = store.collection(FIXTURE["collection_id"])
    request = FIXTURE["appends"]["a"]
    response, status = store.append(collection, {"operation_id": request["operation_id"], "rows": request["rows"]}, scenario)
    with Client("https://example.test", "token", transport=httpx.MockTransport(
            lambda _: httpx.Response(status, json=response))) as client:
        outcome = client.append(FIXTURE["collection_id"], request["rows"], operation_id=request["operation_id"],
                                schema=Schema.from_dict(FIXTURE["schema"]))
    assert not outcome.terminal
    assert outcome.raw["recovery"]["payload_hash"] == request["payload_hash"]
    assert outcome.raw["recovery"]["base"]["current_snapshot_id"] is None
    assert outcome.raw["recovery"]["publication_started"] == (scenario == "lost_response")
    if scenario == "lost_response":
        assert collection.operations[request["operation_id"]].response["outcome"] == "committed"
    else:
        assert store.append(collection, {"operation_id": request["operation_id"], "rows": request["rows"]}) == (response, status)
