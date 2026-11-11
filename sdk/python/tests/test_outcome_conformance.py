"""Literal cross-language expectations; transport doubles do not prove persistence."""

import copy
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from glamdb_sdk import Client, ProtocolError, Schema
from glamdb_sdk.codec import decode_rows, encode_rows


FIXTURE = json.loads((Path(__file__).resolve().parents[3] / "fixtures/http/outcomes-v0.json").read_text())
CASES = {case["name"]: case for case in FIXTURE["cases"]}
SCHEMA = Schema.from_dict(FIXTURE["schema"])


def invoke(client, case):
    cid = FIXTURE["collection_id"]
    if case["kind"] == "create":
        return client.create_collection(FIXTURE["exact_name"], SCHEMA, collection_id=cid)
    request = FIXTURE["appends"][case["request"]]
    if case["kind"] == "lookup":
        return client.find_append_outcome(cid, request["operation_id"])
    return client.append(cid, request["rows"], operation_id=request["operation_id"], schema=SCHEMA)


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["name"])
def test_shared_outcomes_preserve_complete_native_wire_values(case):
    with Client("https://example.test", "token", transport=httpx.MockTransport(
            lambda request: httpx.Response(case["status"], json=case["response"]))) as client:
        outcome = invoke(client, case)
    assert outcome.raw == case["response"]
    assert outcome.outcome == case["response"]["outcome"]
    assert outcome.terminal == (outcome.outcome != "commit_unknown")


@pytest.mark.parametrize("invalid", FIXTURE["invalid_recovery"], ids=lambda case: case["name"])
def test_incomplete_or_inconsistent_recovery_fails_with_original_identity(invalid):
    case = CASES[invalid["case"]]
    response = copy.deepcopy(case["response"])
    parts = invalid.get("remove", invalid.get("path")).strip("/").split("/")
    target = response
    for part in parts[:-1]:
        target = target[part]
    if "remove" in invalid:
        del target[parts[-1]]
    else:
        target[parts[-1]] = invalid["value"]
    with Client("https://example.test", "token", transport=httpx.MockTransport(
            lambda request: httpx.Response(case["status"], json=response))) as client:
        with pytest.raises(ProtocolError) as caught:
            invoke(client, case)
    assert caught.value.collection_id == FIXTURE["collection_id"]
    if case["kind"] != "create":
        assert caught.value.operation_id == FIXTURE["appends"][case["request"]]["operation_id"]
    assert caught.value.raw == response


@pytest.mark.parametrize("name", ["unknown_publication", "blocked_lookup", "lost_create"])
def test_recovery_extension_fields_survive_validation(name):
    case = CASES[name]
    response = copy.deepcopy(case["response"])
    response["extension"] = {"future": True}
    response["recovery"]["extension"] = {"future": "9007199254740993"}
    with Client("https://example.test", "token", transport=httpx.MockTransport(
            lambda request: httpx.Response(case["status"], json=response))) as client:
        assert invoke(client, case).raw == response


@pytest.mark.parametrize("name", ["a", "b"])
def test_shared_canonical_hash_and_exact_scalar_round_trip(name):
    request = FIXTURE["appends"][name]
    assert hashlib.sha256(bytes.fromhex(request["canonical_hex"])).hexdigest() == request["payload_hash"]
    with Client("https://example.test", "token") as client:
        prepared = client.prepare_append(FIXTURE["collection_id"], request["rows"],
                                         operation_id=request["operation_id"], schema=SCHEMA)
    assert prepared.payload_hash == request["payload_hash"]
    assert prepared.to_dict()["rows"] == request["rows"]
    assert encode_rows(SCHEMA, decode_rows(SCHEMA, request["rows"])) == request["rows"]


def test_retry_a_after_b_retains_original_receipt_and_payload():
    replies = iter([CASES[name]["response"] for name in ("committed", "committed_b", "retry_a_after_b")])
    bodies = []

    def handle(request):
        bodies.append(request.content)
        return httpx.Response(200, json=next(replies))

    with Client("https://example.test", "token", transport=httpx.MockTransport(handle)) as client:
        outcomes = [invoke(client, CASES[name]) for name in ("committed", "committed_b", "retry_a_after_b")]
    assert outcomes[0] == outcomes[2]
    assert bodies[0] == bodies[2]
    assert outcomes[1].result["parent_snapshot_id"] == outcomes[0].result["snapshot_id"]
