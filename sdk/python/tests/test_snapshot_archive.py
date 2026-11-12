import json
from pathlib import Path

import httpx
import pytest

from glamdb_sdk import (KEEP_ALL_SNAPSHOTS_IN_ROOT, Client, Field, HTTPError, OutcomeUnknownError, ProtocolError, Schema,
                        ValidationError)

CID = "11111111-2222-4333-8444-555555555555"
DB = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("key", "string", False), Field("n", "int64", True)])
DEFAULT = {"parquet": {"compression": None, "dictionary": None, "dictionary_page_size_limit": None, "columns": []}}
STATUS = {"hot_snapshots": "2", "root_snapshots": "3", "archived_snapshots": "7", "expired_snapshots": "0",
          "archive_pending": "1"}
ROUTES = json.loads((Path(__file__).resolve().parents[3] / "fixtures/http/v0.json").read_text())["routes"]


def reply(data, status=200):
    return httpx.Response(status, json=data)


def collection(hot="0", defaulted=False, **changes):
    return {"collection_id": CID, "name": "exact", "table_uuid": DB, "schema": SCHEMA.to_dict(), "mode": "append",
            "creation_sequence": "1", "current_snapshot_id": None, "write_settings": DEFAULT,
            "write_settings_version": "0", "hot_snapshots": hot, "hot_snapshots_defaulted": defaulted} | changes


def client_with(handler, **kwargs):
    return Client("https://example.test", "secret-value", transport=httpx.MockTransport(handler), **kwargs)


def test_create_sends_hot_snapshots_as_a_decimal_string_and_omits_it_by_default():
    sent = []

    def handler(request):
        body = json.loads(request.content)
        sent.append(body)
        # The server default applies to an omitted value.
        return reply(collection(body.get("hot_snapshots", "100"), "hot_snapshots" not in body))

    with client_with(handler) as client:
        for value, wire in ((None, None), (KEEP_ALL_SNAPSHOTS_IN_ROOT, "0"), (8, "8"), ("0", "0"),
                            ("1099511627776", "1099511627776")):
            outcome = client.create_collection("exact", SCHEMA, collection_id=CID, hot_snapshots=value)
            assert outcome.outcome == "committed"
            assert outcome.info["hot_snapshots"] == (wire or "100")
            assert outcome.info["hot_snapshots_defaulted"] is (wire is None)
            assert sent[-1].get("hot_snapshots", None) == wire and ("hot_snapshots" in sent[-1]) == (wire is not None)
        for invalid in (-1, 2**40 + 1, "08", "-0", "x", True, 1.5):
            with pytest.raises(ValidationError):
                client.create_collection("exact", SCHEMA, collection_id=CID, hot_snapshots=invalid)
    assert len(sent) == 5


def test_create_refuses_a_server_that_does_not_report_a_requested_setting():
    info = {key: value for key, value in collection().items()
            if key not in ("hot_snapshots", "hot_snapshots_defaulted")}
    with client_with(lambda request: reply(info)) as client:
        assert client.create_collection("exact", SCHEMA, collection_id=CID).outcome == "committed"
        with pytest.raises(ProtocolError, match="hot_snapshots"):
            client.create_collection("exact", SCHEMA, collection_id=CID, hot_snapshots=4)
        with pytest.raises(ProtocolError, match="hot_snapshots"):
            client.create_collection("exact", SCHEMA, collection_id=CID, hot_snapshots=0)
    # The effective value and the defaulted flag come together, as a
    # canonical decimal in 0..2^40; null is no longer a reported value.
    for invalid in (collection(None), collection("01"), collection("1099511627777"), collection("100", None),
                    collection("0", True), {key: value for key, value in collection().items()
                                            if key != "hot_snapshots_defaulted"}):
        with client_with(lambda request: reply(invalid)) as client:
            with pytest.raises(ProtocolError, match="hot_snapshots"):
                client.create_collection("exact", SCHEMA, collection_id=CID)


def test_create_mismatch_is_a_noncommitted_outcome_and_resolution_defers_to_the_server():
    refused = {"outcome": "definitely_not_committed", "phase": "validation",
               "error": {"code": "SNAPSHOT_ARCHIVE_MISMATCH", "message": "differs", "retry_hint": "FixInput"}}
    current, posts = {}, []

    def handler(request):
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return current["post"]
        return reply(current["info"])

    with client_with(handler) as client:
        current["post"] = reply(refused, 409)
        outcome = client.create_collection("exact", SCHEMA, collection_id=CID, hot_snapshots=3)
        assert outcome.outcome == "definitely_not_committed" and outcome.error.code == "SNAPSHOT_ARCHIVE_MISMATCH"
        posts.clear()
        # A GET proves the creation value only when it cannot have changed:
        # still the server default for a create that omitted it, a KV
        # collection asked without a value, or a server without the archive.
        without = {key: value for key, value in collection().items()
                   if key not in ("hot_snapshots", "hot_snapshots_defaulted")}
        for info, mode, requested in ((collection("100", True), "append", None),
                                      (collection("7", True), "append", None),
                                      (collection(mode="kv"), "kv", None), (collection(mode="kv"), "kv", 0),
                                      (without, "append", None)):
            current["info"] = info
            assert client.resolve_create("exact", SCHEMA, collection_id=CID, mode=mode,
                                         hot_snapshots=requested)["collection_id"] == CID
            assert posts == []
        # Otherwise the reported value is the current one, which an update can
        # change: the create is repeated and the server compares effective
        # values with the creation.
        for requested, reported, defaulted in ((3, "0", False), (None, "3", False), (3, "3", False),
                                               (3, "5", False), (None, "0", False), (0, "0", False),
                                               (100, "100", True), (0, "100", True)):
            current["info"] = collection(reported, defaulted)
            current["post"] = reply(collection(reported, defaulted))
            assert client.resolve_create("exact", SCHEMA, collection_id=CID,
                                         hot_snapshots=requested)["collection_id"] == CID
            assert posts[-1].get("hot_snapshots") == (None if requested is None else str(requested))
            current["post"] = reply(refused, 409)
            with pytest.raises(ProtocolError):
                client.resolve_create("exact", SCHEMA, collection_id=CID, hot_snapshots=requested)
        # A requested value against a server that reports none fails closed.
        current["info"] = without
        with pytest.raises(ProtocolError, match="hot_snapshots"):
            client.resolve_create("exact", SCHEMA, collection_id=CID, hot_snapshots=0)


def test_status_reads_the_scoped_route_and_validates_counts():
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path))
        return reply(current, 404 if "error" in current else 200)

    current = STATUS
    with client_with(handler, database_id=DB) as client:
        assert client.snapshot_archive(CID) == STATUS
        assert seen == [("GET", f"/databases/{DB}/collections/{CID}/snapshot-archive")]
        for invalid in (dict(STATUS, archive_pending="0"), dict(STATUS, root_snapshots=3),
                        dict(STATUS, hot_snapshots=None), dict(STATUS, archived_snapshots="-1"),
                        {key: value for key, value in STATUS.items() if key != "expired_snapshots"},
                        dict(STATUS, hot_snapshots="0", archive_pending="3"),
                        dict(STATUS, hot_snapshots="1099511627777")):
            current = invalid
            with pytest.raises(ProtocolError):
                client.snapshot_archive(CID)
        current = dict(STATUS, hot_snapshots="0", archive_pending="0")
        assert client.snapshot_archive(CID)["hot_snapshots"] == "0"
        current = {"error": {"code": "COLLECTION_NOT_FOUND", "message": "missing", "retry_hint": "FixInput"}}
        with pytest.raises(HTTPError):
            client.snapshot_archive(CID)


def test_update_posts_the_setting_and_verifies_the_collection():
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append((request.method, request.url.path, body))
        return reply(collection(body["hot_snapshots"]))

    with client_with(handler) as client:
        assert client.update_snapshot_archive(CID, 4)["hot_snapshots"] == "4"
        assert client.update_snapshot_archive(CID, None)["hot_snapshots"] == "0"
        assert client.update_snapshot_archive(CID, KEEP_ALL_SNAPSHOTS_IN_ROOT)["hot_snapshots"] == "0"
        with pytest.raises(ValidationError):
            client.update_snapshot_archive(CID, -1)
    assert seen == [("POST", f"/collections/{CID}/snapshot-archive", {"hot_snapshots": "4"}),
                    ("POST", f"/collections/{CID}/snapshot-archive", {"hot_snapshots": "0"}),
                    ("POST", f"/collections/{CID}/snapshot-archive", {"hot_snapshots": "0"})]
    # An update makes the value explicit: a response still marked defaulted
    # did not apply it.
    for response in (collection("5"), collection("4", collection_id=DB), collection(4), collection("4", True),
                     collection("4", None)):
        with client_with(lambda request: reply(response)) as client:
            with pytest.raises(ProtocolError):
                client.update_snapshot_archive(CID, 4)


@pytest.mark.parametrize("code,status,error", [
    ("COLLECTION_MODE_MISMATCH", 409, HTTPError), ("INVALID_ARGUMENT", 400, HTTPError),
    ("RECOVERY_REQUIRED", 503, OutcomeUnknownError)])
def test_update_and_maintenance_failures_keep_the_server_detail(code, status, error):
    hint = "SameIdAfterReconcile" if status == 503 else "FixInput"
    body = {"error": {"code": code, "message": "refused", "retry_hint": hint}}
    with client_with(lambda request: reply(body, status)) as client:
        with pytest.raises(error) as update:
            client.update_snapshot_archive(CID, 4)
        with pytest.raises(error) as maintenance:
            client.archive_snapshots(CID)
    for raised in (update.value, maintenance.value):
        assert raised.detail.code == code and raised.collection_id == CID
        assert raised.detail.retry_hint == hint


def test_maintenance_posts_an_empty_object_and_reports_moved_snapshots():
    seen = []
    result = dict(STATUS, root_snapshots="2", archive_pending="0", snapshots_archived="1")

    def handler(request):
        seen.append((request.method, request.url.path, request.content))
        return reply(current)

    current = result
    with client_with(handler) as client:
        assert client.archive_snapshots(CID) == result
        current = STATUS
        with pytest.raises(ProtocolError):
            client.archive_snapshots(CID)
    assert seen[0] == ("POST", f"/collections/{CID}/snapshot-archive/maintenance", b"{}")


def test_shared_route_fixtures_match_the_sdk_requests_and_response_checks():
    fixtures = {(route["method"], route["path"].rsplit("/{collection_id}", 1)[-1]): route for route in ROUTES
                if "{collection_id}/snapshot-archive" in route["path"]}
    assert set(fixtures) == {("GET", "/snapshot-archive"), ("POST", "/snapshot-archive"),
                             ("POST", "/snapshot-archive/maintenance")}
    assert fixtures[("POST", "/snapshot-archive")]["request"] == {"hot_snapshots": "1"}
    assert fixtures[("POST", "/snapshot-archive/maintenance")]["request"] == {}
    status = fixtures[("GET", "/snapshot-archive")]["response_fields"]
    assert set(status) == set(STATUS)
    assert set(fixtures[("POST", "/snapshot-archive/maintenance")]["response_fields"]) == set(STATUS) | {
        "snapshots_archived"}
