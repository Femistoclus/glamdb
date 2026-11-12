"""KV preparation, strict receipts and ordered fixed-view lookup."""
import hashlib
import json
from uuid import UUID

import httpx
import pytest

from glamdb_sdk import ANY, Client, Field, ProtocolError, Schema, ValidationError
from glamdb_sdk.request import canonical_payload_hash, schema_fingerprint

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("n", "int64", True), Field("key", "string", False)])


def kv_hash(kind, source, max_files=256, target=8388608):
    return hashlib.sha256(f"glamdb.kv1/{kind}/3/{schema_fingerprint(SCHEMA).hex()}/{source}/{max_files}/{target}/".encode()).hexdigest()


def receipt(request, **changes):
    result = dict(collection_id=CID, operation_id=OP, payload_hash=request.payload_hash,
                  hash_version=1, receipt_version=3, snapshot_id="0", parent_snapshot_id=None,
                  rows_appended="0", data_files_added="0", compressed_data_bytes_added="0",
                  snapshot_created_at_us="0", durability_profile="local-fsync-v1",
                  mutation_kind=request.kind, snapshot_created=False, input_rows="2",
                  rows_affected="0", rows_written="0", rows_removed="0", data_files_removed="0",
                  compressed_data_bytes_removed="0")
    result.update(changes)
    return dict(outcome="committed", phase="complete", operation_id=OP, result=result)


def test_upsert_owns_full_rows_and_domain_separated_identity():
    rows = [{"key": "same", "n": "9007199254740993"}, {"key": "same"}]
    with Client("http://127.0.0.1:1", "unused") as client:
        request = client.prepare_upsert(CID, rows, operation_id=OP, schema=SCHEMA, expected_snapshot=None)
    rows[0]["key"] = "changed"
    normalized = [{"n": "9007199254740993", "key": "same"}, {"n": None, "key": "same"}]
    assert request.to_dict() == {"operation_id": OP, "rows": normalized, "expected_snapshot": None}
    assert request.payload_hash == kv_hash(1, canonical_payload_hash(CID, SCHEMA, normalized))
    assert request.kind == "upsert"


def test_delete_keeps_duplicate_key_identity_and_empty_compact_hash():
    with Client("http://127.0.0.1:1", "unused") as client:
        keys = ["same", "same"]
        request = client.prepare_delete(CID, keys, operation_id=OP, schema=SCHEMA)
        key_schema = Schema([Field("key", "string", False)])
        assert request.payload_hash == kv_hash(2, canonical_payload_hash(CID, key_schema, [{"key": k} for k in keys]))
        keys.clear()
        assert request.to_dict()["keys"] == ["same", "same"]
        empty = client.prepare_delete(CID, [], operation_id=OP, schema=SCHEMA)
        assert empty.payload_hash == kv_hash(2, hashlib.sha256(b"empty").hexdigest())
        compact = client.prepare_compact(CID, operation_id=OP, schema=SCHEMA, max_files=4, target_file_bytes=1024)
        assert compact.payload_hash == kv_hash(3, hashlib.sha256(b"empty").hexdigest(), 4, 1024)


def test_prepared_delete_is_sent_once_and_noop_receipt_survives_lookup():
    calls = []
    with Client("http://127.0.0.1:1", "unused") as prepare:
        request = prepare.prepare_delete(CID, ["missing", "missing"], operation_id=OP, schema=SCHEMA)
    def handle(req):
        calls.append(req)
        return httpx.Response(200, json=receipt(request))
    with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(handle)) as client:
        assert client.delete(request).result["snapshot_created"] is False
        assert client.find_append_outcome(CID, OP).result["snapshot_id"] == "0"
    assert len(calls) == 2
    assert calls[0].url.path.endswith("/delete")
    assert calls[0].content == request.body


@pytest.mark.parametrize("change", [dict(input_rows=2), dict(snapshot_created=0), dict(rows_affected="1"),
                                     dict(mutation_kind="append"), dict(rows_written="-1"),
                                     dict(snapshot_id="4"), dict(rows_appended="1"), dict(data_files_removed="1")])
def test_v3_noop_receipt_rejects_wrong_types_and_impossible_counters(change):
    with Client("http://127.0.0.1:1", "unused") as prepare:
        request = prepare.prepare_delete(CID, ["missing", "missing"], operation_id=OP, schema=SCHEMA)
    with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json=receipt(request, **change)))) as client:
        with pytest.raises(ProtocolError):
            client.delete(request)


def test_lookup_preserves_requested_order_misses_and_exact_integers():
    keys = ["same", "missing", "same"]
    def handle(req):
        assert json.loads(req.content) == {"keys": keys, "snapshot_id": "9"}
        return httpx.Response(200, json=dict(collection_id=CID, snapshot_id="9", schema=SCHEMA.to_dict(),
            rows=[{"key": "same", "n": "9007199254740993"}, None, {"key": "same", "n": "9007199254740993"}]))
    with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(handle)) as client:
        result = client.lookup(CID, keys, snapshot_id="9")
    assert result.snapshot_id == "9"
    assert result.rows == [{"key": "same", "n": 9007199254740993}, None, {"key": "same", "n": 9007199254740993}]


@pytest.mark.parametrize("kind,kwargs", [("delete", {"keys": ["x"] * 10001}), ("lookup", {"keys": ["x"] * 4097}),
                                         ("compact", {"max_files": True}), ("compact", {"target_file_bytes": "1024"})])
def test_bounded_options_fail_before_network(kind, kwargs):
    with Client("http://127.0.0.1:1", "unused") as client:
        with pytest.raises(ValidationError):
            if kind == "lookup":
                client.lookup(CID, **kwargs)
            else:
                getattr(client, "prepare_" + kind)(CID, operation_id=OP, schema=SCHEMA, **kwargs)


from pathlib import Path
from copy import deepcopy
KV_FIXTURE = json.loads((Path(__file__).resolve().parents[3] / "fixtures/http/kv-v3.json").read_text())


@pytest.mark.parametrize("example", KV_FIXTURE["cases"], ids=lambda case: case["name"])
def test_shared_v3_literal_matches_preparation_lookup_and_mutation(example):
    schema = Schema.from_dict(KV_FIXTURE["schema"])
    cid, body, kind = KV_FIXTURE["collection_id"], example["body"], example["kind"]
    with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(
            lambda request: httpx.Response(example["status"], json=example["response"]))) as client:
        prepare = getattr(client, "prepare_" + kind)
        content = {key: value for key, value in body.items() if key != "operation_id"}
        request = prepare(cid, operation_id=body["operation_id"], schema=schema, **content)
        assert request.payload_hash == example["response"]["result"]["payload_hash"]
        assert getattr(client, kind)(request).raw == example["response"]
        assert client.find_append_outcome(cid, body["operation_id"]).raw == example["response"]


@pytest.mark.parametrize("invalid", KV_FIXTURE["invalid_receipts"], ids=lambda case: case["name"])
def test_shared_invalid_v3_literal_is_a_protocol_error(invalid):
    example = next(case for case in KV_FIXTURE["cases"] if case["name"] == invalid["case"])
    value = deepcopy(example["response"])
    if "payload_remove" in invalid:
        del value[invalid["payload_remove"]]
    elif "payload_changes" in invalid:
        value.update(invalid["payload_changes"])
    elif "remove" in invalid:
        del value["result"][invalid["remove"]]
    elif "changes" in invalid:
        value["result"].update(invalid["changes"])
    else:
        value["result"][invalid["field"]] = invalid["value"]
    with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=value))) as client:
        with pytest.raises(ProtocolError):
            client.find_append_outcome(KV_FIXTURE["collection_id"], example["body"]["operation_id"])


@pytest.mark.parametrize("kind", ["upsert", "delete", "compact"])
def test_streaming_bulk_append_rejects_kv_receipts_but_operation_lookup_accepts_them(kind):
    from glamdb_sdk import BulkAppendRequest, LookupResult, MutationRequest
    assert all(item is not None for item in (BulkAppendRequest, LookupResult, MutationRequest))
    with Client("http://127.0.0.1:1", "unused") as prepare:
        mutation = prepare.prepare_delete(CID, ["missing"], operation_id=OP, schema=SCHEMA)
        with prepare.prepare_bulk_append(CID, [[{"key": "a"}]], rows=1, operation_id=OP, schema=SCHEMA) as bulk:
            wrong = receipt(mutation, mutation_kind=kind, input_rows="0" if kind == "compact" else "1",
                            payload_hash=bulk.payload_hash)
            calls = []
            def respond(request):
                calls.append(request)
                return httpx.Response(200, json=wrong)
            with Client("http://127.0.0.1", "token", transport=httpx.MockTransport(respond)) as client:
                with pytest.raises(ProtocolError, match="bulk append returned a KV mutation receipt") as caught:
                    client.bulk_append(bulk)
                assert caught.value.collection_id == CID
                assert caught.value.operation_id == OP
                assert client.find_append_outcome(CID, OP).raw == wrong
            assert calls[0].url.path.endswith("/bulk-append")
            assert calls[0].headers["content-type"] == "application/x-ndjson"
            assert calls[0].headers["content-length"] == str(bulk.content_length)
            assert calls[1].url.path.endswith("/operations/" + OP)
