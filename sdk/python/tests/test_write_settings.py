import json
from pathlib import Path

import httpx
import pytest

from glamdb_sdk import (DICTIONARY_POLICIES, PARQUET_CODECS, PARQUET_ENCODINGS, Client, Field, HTTPError,
                        OutcomeUnknownError, ProtocolError, Schema, TransportError, ValidationError,
                        canonical_write_settings)

CID = "11111111-2222-4333-8444-555555555555"
DB = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("key", "string", False), Field("n", "int64", True), Field("payload", "string", True)])
DEFAULT = {"parquet": {"compression": None, "dictionary": None, "dictionary_page_size_limit": None, "columns": []}}
# Columns deliberately out of schema order, without explicit levels or unset
# options, and with an integer page size limit.
REQUEST = {"parquet": {"compression": {"codec": "zstd", "level": 3}, "dictionary": "auto",
                       "dictionary_page_size_limit": 65536,
                       "columns": [{"field": "payload", "compression": {"codec": "uncompressed"}},
                                   {"field": "n", "encoding": "delta_binary_packed"},
                                   {"field": "key", "compression": {"codec": "gzip", "level": 9},
                                    "dictionary": "disabled"}]}}
CANONICAL = {"parquet": {
    "compression": {"codec": "zstd", "level": 3}, "dictionary": "auto", "dictionary_page_size_limit": "65536",
    "columns": [
        {"field": "key", "compression": {"codec": "gzip", "level": 9}, "dictionary": "disabled", "encoding": None},
        {"field": "n", "compression": None, "dictionary": None, "encoding": "delta_binary_packed"},
        {"field": "payload", "compression": {"codec": "uncompressed", "level": None}, "dictionary": None,
         "encoding": None}]}}


COMPACT = {"parquet": {
    "compression": {"codec": "zstd", "level": 3}, "dictionary": "auto", "dictionary_page_size_limit": "65536",
    "columns": [{"field": "key", "compression": {"codec": "gzip", "level": 9}, "dictionary": "disabled"},
                {"field": "n", "encoding": "delta_binary_packed"},
                {"field": "payload", "compression": {"codec": "uncompressed", "level": None}}]}}


def reply(data, status=200):
    return httpx.Response(status, json=data)


# The server stores non-default creation settings as version "1"; a creation
# without settings stays at "0" until the first update.
def collection(settings=CANONICAL, version="1", **changes):
    return {"collection_id": CID, "name": "exact", "table_uuid": DB, "schema": SCHEMA.to_dict(), "mode": "append",
            "creation_sequence": "1", "current_snapshot_id": None, "write_settings": settings,
            "write_settings_version": version} | changes


def client_with(handler, **kwargs):
    return Client("https://example.test", "secret-value", transport=httpx.MockTransport(handler), **kwargs)


@pytest.mark.parametrize("value", [None, {}, {"parquet": None}, {"parquet": {}},
                                   {"parquet": {"compression": None, "columns": None}},
                                   {"parquet": {"dictionary": None, "dictionary_page_size_limit": None}}, DEFAULT])
def test_default_settings_have_one_canonical_form(value):
    assert canonical_write_settings(value) == DEFAULT
    assert canonical_write_settings(value, SCHEMA) == DEFAULT


def test_canonical_form_orders_columns_by_schema_and_makes_default_levels_explicit():
    assert canonical_write_settings(REQUEST, SCHEMA) == CANONICAL
    # Without a schema the caller order is kept; the server checks field names.
    assert [column["field"] for column in canonical_write_settings(REQUEST)["parquet"]["columns"]] == [
        "payload", "n", "key"]
    assert canonical_write_settings(CANONICAL, SCHEMA) == CANONICAL
    # An explicit default level is a different setting from "codec default".
    assert canonical_write_settings({"parquet": {"compression": {"codec": "zstd", "level": 1}}}) != \
        canonical_write_settings({"parquet": {"compression": {"codec": "zstd"}}})
    assert PARQUET_CODECS == ("uncompressed", "snappy", "gzip", "lz4_raw", "zstd")
    assert DICTIONARY_POLICIES == ("auto", "enabled", "disabled")
    assert PARQUET_ENCODINGS == ("plain", "delta_binary_packed", "delta_length_byte_array", "delta_byte_array",
                                 "byte_stream_split", "rle")


def test_columns_may_set_only_a_dictionary_policy_or_only_an_encoding():
    value = {"parquet": {"dictionary_page_size_limit": "4096", "columns": [
        {"field": "payload", "dictionary": "enabled"}, {"field": "key", "encoding": "delta_length_byte_array"}]}}
    assert canonical_write_settings(value, SCHEMA) == {"parquet": {
        "compression": None, "dictionary": None, "dictionary_page_size_limit": "4096", "columns": [
            {"field": "key", "compression": None, "dictionary": None, "encoding": "delta_length_byte_array"},
            {"field": "payload", "compression": None, "dictionary": "enabled", "encoding": None}]}}
    # An explicit "auto" is a different setting from an unset policy.
    assert canonical_write_settings({"parquet": {"dictionary": "auto"}}) != DEFAULT


@pytest.mark.parametrize("value", [
    [], "zstd", {"parquet": [], "other": None}, {"compression": {"codec": "zstd"}},
    {"parquet": {"codec": "zstd"}}, {"parquet": {"compression": "zstd"}},
    {"parquet": {"compression": {"codec": "brotli"}}}, {"parquet": {"compression": {"level": 3}}},
    {"parquet": {"compression": {"codec": "zstd", "level": True}}},
    {"parquet": {"compression": {"codec": "zstd", "level": 3.0}}},
    {"parquet": {"compression": {"codec": "zstd", "level": "3"}}},
    {"parquet": {"compression": {"codec": "zstd", "level": 2**31}}},
    {"parquet": {"compression": {"codec": "zstd", "dictionary": True}}},
    {"parquet": {"columns": "key"}}, {"parquet": {"columns": {"field": "key"}}},
    {"parquet": {"columns": [{"field": "key"}]}},
    {"parquet": {"columns": [{"field": "key", "compression": None}]}},
    {"parquet": {"columns": [{"field": "", "compression": {"codec": "snappy"}}]}},
    {"parquet": {"columns": [{"field": "key", "compression": {"codec": "snappy"}, "statistics": None}]}},
    {"parquet": {"columns": [{"field": "key", "compression": None, "dictionary": None, "encoding": None}]}},
    {"parquet": {"dictionary": "on"}}, {"parquet": {"dictionary": True}}, {"parquet": {"dictionary": "AUTO"}},
    {"parquet": {"columns": [{"field": "key", "dictionary": "off"}]}},
    {"parquet": {"columns": [{"field": "key", "encoding": "rle_dictionary"}]}},
    {"parquet": {"columns": [{"field": "key", "encoding": ["plain"]}]}},
    {"parquet": {"dictionary_page_size_limit": "4k"}}, {"parquet": {"dictionary_page_size_limit": "065536"}},
    {"parquet": {"dictionary_page_size_limit": 0}}, {"parquet": {"dictionary_page_size_limit": "-4096"}},
    {"parquet": {"dictionary_page_size_limit": True}}, {"parquet": {"dictionary_page_size_limit": 4096.0}},
    {"parquet": {"dictionary_page_size_limit": 2**63}},
    {"parquet": {"columns": [{"field": "key", "compression": {"codec": "snappy"}},
                             {"field": "key", "compression": {"codec": "zstd"}}]}},
    {"parquet": {"columns": [{"field": f"c{index}", "compression": {"codec": "snappy"}} for index in range(129)]}},
])
def test_invalid_shapes_are_rejected_before_any_request(value):
    sent = []
    with client_with(lambda request: sent.append(request) or reply({})) as client:
        with pytest.raises(ValidationError):
            client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=value)
        with pytest.raises(ValidationError):
            client.update_write_settings(CID, "0", value)
    assert sent == []


def test_columns_must_name_schema_fields_when_the_schema_is_known():
    value = {"parquet": {"columns": [{"field": "missing", "compression": {"codec": "snappy"}}]}}
    assert canonical_write_settings(value)["parquet"]["columns"][0]["field"] == "missing"
    with pytest.raises(ValidationError, match="unknown field"):
        canonical_write_settings(value, SCHEMA)


def test_shared_route_fixture_settings_are_accepted_and_canonicalization_is_idempotent():
    root = Path(__file__).resolve().parents[3]
    routes = json.loads((root / "fixtures/http/v0.json").read_text())["routes"]
    values = [route["request"]["write_settings"] for route in routes
              if isinstance(route.get("request"), dict) and "write_settings" in route["request"]]
    for value in values:
        canonical = canonical_write_settings(value)
        assert canonical_write_settings(canonical) == canonical


def test_create_without_settings_keeps_the_previous_request_bytes_and_accepts_older_servers():
    sent = []
    def handler(request):
        sent.append(request.content)
        info = {key: value for key, value in collection().items()
                if key not in ("write_settings", "write_settings_version")}
        return reply({"outcome": "committed", "info": info} if request.method == "POST" else info)
    with client_with(handler) as client:
        assert client.create_collection("exact", SCHEMA, collection_id=CID).outcome == "committed"
        assert client.resolve_create("exact", SCHEMA, collection_id=CID)["collection_id"] == CID
    expected = json.dumps({"collection_id": CID, "name": "exact", "schema": SCHEMA.to_dict()},
                          separators=(",", ":")).encode()
    assert sent[0] == expected


def test_create_sends_canonical_settings_and_verifies_the_created_definition():
    sent = []
    def handler(request):
        sent.append(json.loads(request.content))
        return reply({"outcome": "committed", "phase": "complete", "info": collection()}, 201)
    with client_with(handler) as client:
        outcome = client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
    assert outcome.outcome == "committed" and outcome.info["write_settings_version"] == "1"
    # Unset options are omitted from requests (compression-only settings stay
    # acceptable to servers without dictionary/encoding options).
    assert sent == [{"collection_id": CID, "name": "exact", "schema": SCHEMA.to_dict(), "write_settings": COMPACT}]


@pytest.mark.parametrize("info", [
    collection(DEFAULT), collection(DEFAULT, version="0"), collection(version="0"),
    collection({"parquet": {"compression": {"codec": "zstd", "level": None}, "columns": CANONICAL["parquet"]["columns"]}}),
    collection({"parquet": {"compression": {"codec": "zstd", "level": 3}, "columns": []}}),
    {key: value for key, value in collection().items() if key not in ("write_settings", "write_settings_version")},
    {key: value for key, value in collection().items() if key != "write_settings_version"},
    {key: value for key, value in collection().items() if key != "write_settings"},
    collection(version=0), collection(version="-1"), collection(version="01"),
    collection({"parquet": {"compression": {"codec": "zstd", "level": 3}}}),
    collection({"parquet": {"compression": {"codec": "brotli", "level": None}, "columns": []}}),
], ids=["default", "created-without", "nondefault-at-0", "level-default", "missing-column", "unreported", "no-version", "no-settings", "int-version",
        "negative-version", "noncanonical-version", "partial", "unknown-codec"])
def test_created_collection_with_different_or_invalid_settings_is_a_protocol_error(info):
    with client_with(lambda request: reply({"outcome": "committed", "info": info}, 201)) as client:
        with pytest.raises(ProtocolError) as error:
            client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
    assert error.value.collection_id == CID


def test_lost_create_resolution_trusts_current_settings_only_while_none_were_stored():
    current, posts = {}, []

    def handler(request):
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return current["post"]
        return reply(current["info"])

    mismatch = reply({"outcome": "definitely_not_committed", "phase": "validation",
                      "error": {"code": "WRITE_SETTINGS_MISMATCH", "message": "differs", "retry_hint": "FixInput"}},
                     409)
    with client_with(handler) as client:
        # Version "0": no settings were ever stored, so GET alone decides.
        current["info"] = collection(DEFAULT, version="0")
        assert client.resolve_create("exact", SCHEMA, collection_id=CID)["collection_id"] == CID
        assert client.resolve_create("exact", SCHEMA, collection_id=CID, write_settings={})["collection_id"] == CID
        with pytest.raises(ProtocolError, match="write settings"):
            client.resolve_create("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
        assert posts == []
        # A stored version cannot prove the creation settings: a default
        # creation updated once looks like a creation with settings, and later
        # updates replace them. The original create is repeated and the server
        # compares the settings the collection was created with.
        for version, settings in (("1", CANONICAL), ("2", DEFAULT)):
            current["info"] = collection(settings, version=version)
            current["post"] = reply({"outcome": "committed", "info": current["info"]})
            assert client.resolve_create("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)["name"] == "exact"
            assert posts[-1]["write_settings"] == COMPACT
            current["post"] = mismatch
            with pytest.raises(ProtocolError, match="write settings"):
                client.resolve_create("exact", SCHEMA, collection_id=CID,
                                      write_settings={"parquet": {"compression": {"codec": "gzip"}}})
            with pytest.raises(ProtocolError, match="write settings"):
                client.resolve_create("exact", SCHEMA, collection_id=CID)
            assert "write_settings" not in posts[-1]
        # A refused confirmation leaves the create unresolved.
        current["post"] = reply({"outcome": "commit_unknown", "phase": "reconciling", "recovery_required": True,
                                 "error": {"code": "RECOVERY_REQUIRED", "message": "blocked",
                                           "retry_hint": "SameIdAfterReconcile"},
                                 "recovery": {}}, 503)
        with pytest.raises((OutcomeUnknownError, ProtocolError)):
            client.resolve_create("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
        # Identity and schema mismatches never need the server's comparison.
        count = len(posts)
        with pytest.raises(ProtocolError):
            client.resolve_create("exact", Schema([Field("key", "string", False)]), collection_id=CID,
                                  write_settings={"parquet": {"compression": {"codec": "zstd", "level": 3}}})
        assert len(posts) == count
        current["info"] = collection(DEFAULT, version="x")
        with pytest.raises(ProtocolError):
            client.resolve_create("exact", SCHEMA, collection_id=CID)


def test_create_definite_settings_mismatch_is_a_noncommitted_outcome():
    refused = {"outcome": "definitely_not_committed", "phase": "binding",
               "error": {"code": "WRITE_SETTINGS_MISMATCH", "message": "differs", "retry_hint": "FixInput"}}
    with client_with(lambda request: reply(refused, 409)) as client:
        outcome = client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
    assert outcome.outcome == "definitely_not_committed" and outcome.error.code == "WRITE_SETTINGS_MISMATCH"


def test_update_posts_compare_and_set_body_to_the_scoped_route_and_verifies_the_result():
    seen = []
    def handler(request):
        seen.append((request.method, request.url.path, request.content))
        return reply(collection(version="4"))
    with client_with(handler, database_id=DB) as client:
        assert client.update_write_settings(CID, 3, REQUEST)["write_settings_version"] == "4"
        assert client.update_write_settings(CID, "3", REQUEST)["write_settings"] == CANONICAL
    compact = canonical_write_settings(REQUEST)["parquet"]
    compact = {"parquet": {key: value for key, value in compact.items() if key != "columns"} | {"columns": [
        {key: value for key, value in column.items() if value is not None} for column in compact["columns"]]}}
    body = json.dumps({"expected_version": "3", "write_settings": compact},
                      separators=(",", ":")).encode()
    assert seen == [("POST", f"/databases/{DB}/collections/{CID}/write-settings", body)] * 2


def test_update_to_defaults_resets_overrides():
    with client_with(lambda request: reply(collection(DEFAULT, version="1"))) as client:
        assert client.update_write_settings(CID, "0", {})["write_settings"] == DEFAULT


@pytest.mark.parametrize("version", [-1, "-1", True, 1.0, "01", "x", None, 2**63 - 1, 2**63])
def test_update_rejects_invalid_expected_version_before_post(version):
    sent = []
    with client_with(lambda request: sent.append(request) or reply({})) as client:
        with pytest.raises(ValidationError):
            client.update_write_settings(CID, version, REQUEST)
    assert sent == []


@pytest.mark.parametrize("value", [
    collection(version="3"), collection(version="5"), collection(DEFAULT, version="4"),
    collection(version="4", collection_id=DB), collection(version="4", schema={"fields": []}),
    {key: value for key, value in collection(version="4").items() if key != "write_settings"},
    {key: value for key, value in collection(version="4").items() if key != "write_settings_version"},
    collection({"parquet": {"compression": {"codec": "zstd", "level": 3}, "columns": [{"field": "x"}]}}, version="4"),
], ids=["same-version", "skipped-version", "different-settings", "other-collection", "invalid-schema",
        "no-settings", "no-version", "invalid-settings"])
def test_update_response_must_describe_the_applied_request(value):
    with client_with(lambda request: reply(value)) as client:
        with pytest.raises(ProtocolError) as error:
            client.update_write_settings(CID, "3", REQUEST)
    assert error.value.collection_id == CID and error.value.raw == value


@pytest.mark.parametrize("status,code", [(409, "WRITE_SETTINGS_CONFLICT"), (400, "INVALID_ARGUMENT"),
                                         (404, "COLLECTION_NOT_FOUND")])
def test_update_definite_refusals_are_http_errors(status, code):
    error_body = {"error": {"code": code, "message": "refused", "retry_hint": "FixInput"}}
    with client_with(lambda request: reply(error_body, status)) as client:
        with pytest.raises(HTTPError) as error:
            client.update_write_settings(CID, "3", REQUEST)
    assert not isinstance(error.value, OutcomeUnknownError)
    assert error.value.status_code == status and error.value.detail.code == code
    assert error.value.collection_id == CID


def test_update_uncertain_outcome_requires_repeating_the_same_request():
    bodies = []
    responses = [
        reply({"error": {"code": "RECOVERY_REQUIRED", "message": "reconcile", "retry_hint": "SameIdAfterReconcile"}}, 503),
        None,
        reply(collection(version="4")),
    ]
    def handler(request):
        bodies.append(request.content)
        response = responses.pop(0)
        if response is None:
            raise httpx.ReadError("lost response", request=request)
        return response
    with client_with(handler) as client:
        with pytest.raises(OutcomeUnknownError) as unknown:
            client.update_write_settings(CID, "3", REQUEST)
        assert unknown.value.status_code == 503 and unknown.value.detail.code == "RECOVERY_REQUIRED"
        assert unknown.value.detail.retry_hint == "SameIdAfterReconcile" and unknown.value.collection_id == CID
        with pytest.raises(TransportError) as lost:
            client.update_write_settings(CID, "3", REQUEST)
        assert lost.value.collection_id == CID
        assert client.update_write_settings(CID, "3", REQUEST)["write_settings_version"] == "4"
    assert len(set(bodies)) == 1 and len(bodies) == 3


def test_pre_execution_refusal_is_an_ordinary_http_error():
    busy = {"execution_started": False,
            "error": {"code": "SERVER_BUSY", "message": "busy", "retry_hint": "SameIdAfterReconcile"}}
    with client_with(lambda request: reply(busy, 503)) as client:
        with pytest.raises(HTTPError) as error:
            client.update_write_settings(CID, "3", REQUEST)
    assert not isinstance(error.value, OutcomeUnknownError) and error.value.detail.code == "SERVER_BUSY"


def with_unset_options(settings):
    """Settings as reported by a future server with options this SDK does not know, all unset."""
    parquet = settings["parquet"]
    return {"parquet": {"bloom_filter": None, "columns": [{"statistics": None} | column
                                                          for column in parquet["columns"]]} | parquet}


def test_reported_settings_ignore_unknown_unset_options_and_refuse_set_ones():
    reported = with_unset_options(CANONICAL)
    assert canonical_write_settings(reported, SCHEMA, reported=True) == CANONICAL
    with pytest.raises(ValidationError):
        canonical_write_settings(reported, SCHEMA)
    changed = {"parquet": reported["parquet"] | {"bloom_filter": "enabled"}}
    with pytest.raises(ValidationError, match="does not support"):
        canonical_write_settings(changed, SCHEMA, reported=True)
    column = {"parquet": reported["parquet"] | {"columns": [
        {"field": "n", "compression": None, "dictionary": None, "encoding": None, "statistics": "page"}]}}
    with pytest.raises(ValidationError, match="does not support"):
        canonical_write_settings(column, SCHEMA, reported=True)
    # Known options keep their strict value checks in reported settings too.
    with pytest.raises(ValidationError):
        canonical_write_settings({"parquet": reported["parquet"] | {"dictionary": "sometimes"}}, SCHEMA, reported=True)


def test_newer_server_options_do_not_break_verification_unless_they_are_set():
    with client_with(lambda request: reply({"outcome": "committed", "info": collection(with_unset_options(CANONICAL))},
                                           201)) as client:
        assert client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=REQUEST).outcome == "committed"
    with client_with(lambda request: reply(collection(with_unset_options(CANONICAL), version="4"))) as client:
        assert client.update_write_settings(CID, "3", REQUEST)["write_settings_version"] == "4"
    enabled = with_unset_options(CANONICAL)
    enabled["parquet"]["bloom_filter"] = "enabled"
    with client_with(lambda request: reply(collection(enabled))) as client:
        with pytest.raises(ProtocolError, match="unsupported"):
            client.resolve_create("exact", SCHEMA, collection_id=CID, write_settings=REQUEST)
    with client_with(lambda request: reply(collection(enabled, version="4"))) as client:
        with pytest.raises(ProtocolError):
            client.update_write_settings(CID, "3", REQUEST)


def test_type_and_dictionary_conflicts_are_left_to_the_server():
    # rle needs a bool column and an explicit encoding cannot use an enabled
    # dictionary; the SDK sends the shape and the server's 400 is authoritative.
    conflicting = {"parquet": {"columns": [{"field": "n", "encoding": "rle", "dictionary": "enabled"}]}}
    sent = []
    def handler(request):
        sent.append(json.loads(request.content))
        return reply({"outcome": "definitely_not_committed", "phase": "binding", "error": {
            "code": "INVALID_ARGUMENT", "message": "Parquet encoding rle does not accept column n",
            "retry_hint": "FixInput"}}, 400)
    with client_with(handler) as client:
        outcome = client.create_collection("exact", SCHEMA, collection_id=CID, write_settings=conflicting)
    assert outcome.outcome == "definitely_not_committed" and outcome.error.code == "INVALID_ARGUMENT"
    assert sent[0]["write_settings"]["parquet"]["columns"] == [
        {"field": "n", "dictionary": "enabled", "encoding": "rle"}]


def test_compression_only_requests_keep_the_shape_older_servers_accept():
    sent = []
    def handler(request):
        sent.append(json.loads(request.content))
        return reply(collection({"parquet": {"compression": {"codec": "snappy", "level": None}, "columns": []}},
                                version="2"))
    with client_with(handler) as client:
        client.update_write_settings(CID, "1", {"parquet": {"compression": {"codec": "snappy"}}})
        with pytest.raises(ProtocolError):
            client.update_write_settings(CID, "1", {"parquet": {"dictionary": "disabled"}})
    assert sent[0]["write_settings"] == {"parquet": {"compression": {"codec": "snappy", "level": None}, "columns": []}}
    assert sent[1]["write_settings"] == {"parquet": {"dictionary": "disabled", "columns": []}}
