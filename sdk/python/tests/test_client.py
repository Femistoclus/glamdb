import json
import ssl
from uuid import UUID

import httpx
import pytest

from glamdb_sdk import (ANY, AuthenticationError, Client, DeadlineExceeded, Field,
                        HTTPError, ProtocolError, Schema, TransportError, ValidationError, col)

CID = "11111111-2222-4333-8444-555555555555"
OP = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
SCHEMA = Schema([Field("key", "string", False), Field("n", "int64", True)])
# Independent SHA256 over hand-written schema/row v1 bytes for the fixtures
# below. The append receipt must describe the same frozen request payload.
A_HASH = "c397d1a570d972f2f64d59f1dc6f7715060619fa2d0f8913f08cdf66f3c96a71"
DUPLICATE_HASH = "ca4cafb1652b0d68d583bc3a309c8676215fe4e82ec01f72b59499ead3f51494"
ORIGINAL_HASH = "782925010eaaad639efc062f1dc0f0ea8da93e7ab102282efe5f957754808588"


def reply(data, status=200):
    return httpx.Response(status, json=data)


def committed(snapshot="4", operation=OP, payload_hash=A_HASH):
    return {"outcome": "committed", "phase": "complete", "result": {
        "collection_id": CID, "operation_id": operation, "snapshot_id": snapshot,
        "payload_hash": payload_hash, "hash_version": 1, "receipt_version": 1,
        "rows_appended": "1", "data_files_added": "1", "compressed_data_bytes_added": "12",
        "snapshot_created_at_us": "0", "parent_snapshot_id": None,
        "durability_profile": "MOCK-NOT-DURABLE"}}


def client_with(handler, **kwargs):
    def legacy_handler(request):
        # These codec/receipt tests model the older single-response adapter.
        # Open/page/cancel wire behavior is tested in test_scan_pages.py.
        if request.url.path.endswith('/scan/open'):
            return reply({'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy adapter'}}, 404)
        return handler(request)
    return Client("https://example.test", "secret-value", transport=httpx.MockTransport(legacy_handler), **kwargs)


def test_public_reads_use_bearer_auth_exact_routes_and_preserve_cursor_parameters():
    seen = []
    def handler(request):
        seen.append((request.method, request.url.path, dict(request.url.params)))
        assert request.headers["authorization"] == "Bearer secret-value"
        assert request.headers["accept"] == "application/json"
        if request.url.path.endswith("capabilities"):
            return reply({"mode": "append", "mock": True})
        if request.url.path.endswith("schema"):
            return reply(SCHEMA.to_dict())
        if request.url.path.endswith(("snapshots", "files")) or request.url.path == "/collections":
            return reply({"items": [{"id": "retained"}], "next_cursor": "opaque +/="})
        return reply({"collection_id": CID, "name": "exact name", "schema": SCHEMA.to_dict()})
    with client_with(handler) as client:
        assert client.capabilities()["mock"] is True
        assert client.list_collections(page_size=7, cursor="opaque +/=").next_cursor == "opaque +/="
        assert client.resolve_collection("exact name")["collection_id"] == CID
        assert client.get_collection(CID)["name"] == "exact name"
        assert client.get_schema(CID) == SCHEMA
        client.stats(CID, snapshot_id=None)
        client.snapshots(CID, page_size=7, cursor="opaque +/=", snapshot_id="4")
        client.files(CID, page_size=7, cursor="opaque +/=", snapshot_id="4")
        client.file(CID, "file-1", snapshot_id="4")
    assert seen == [
        ("GET", "/capabilities", {}),
        ("GET", "/collections", {"page_size": "7", "cursor": "opaque +/="}),
        ("GET", "/collections:resolve", {"name": "exact name"}),
        ("GET", f"/collections/{CID}", {}),
        ("GET", f"/collections/{CID}/schema", {}),
        ("GET", f"/collections/{CID}/stats", {"snapshot_id": "initial"}),
        ("GET", f"/collections/{CID}/snapshots", {"page_size": "7", "cursor": "opaque +/=", "snapshot_id": "4"}),
        ("GET", f"/collections/{CID}/files", {"page_size": "7", "cursor": "opaque +/=", "snapshot_id": "4"}),
        ("GET", f"/collections/{CID}/files/file-1", {"snapshot_id": "4"}),
    ]


def test_append_is_owned_and_retry_reuses_identical_operation_and_body_after_lost_response():
    seen = []
    def handler(request):
        seen.append(bytes(request.content))
        if len(seen) == 1:
            raise httpx.ReadError("lost response", request=request)
        return reply(committed(payload_hash=DUPLICATE_HASH))
    with client_with(handler) as client:
        rows = [{"key": "duplicate", "n": 9007199254740993}]
        request = client.prepare_append(CID, rows, operation_id=UUID(OP), schema=SCHEMA)
        rows[0]["n"] = 2
        with pytest.raises(TransportError) as error:
            client.append(request)
        assert error.value.operation_id == OP
        assert len(seen) == 1  # no automatic second POST
        result = client.append(request)
        assert result.outcome == "committed" and result.result["snapshot_id"] == "4"
    assert seen[0] == seen[1]
    assert json.loads(seen[0]) == {"operation_id": OP, "rows": [{"key": "duplicate", "n": "9007199254740993"}]}


def test_append_omitted_precondition_and_explicit_no_snapshot_are_distinct():
    bodies = []
    def handler(request):
        bodies.append(json.loads(request.content))
        return reply(committed())
    with client_with(handler) as client:
        for selector in (ANY, None, 9007199254740993):
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA, expected_snapshot=selector)
    assert "expected_snapshot" not in bodies[0]
    assert bodies[1]["expected_snapshot"] is None
    assert bodies[2]["expected_snapshot"] == "9007199254740993"


def test_unknown_and_original_terminal_failure_are_returned_without_raising_or_retrying():
    replies = [
        reply({"outcome": "commit_unknown", "phase": "reconciling", "operation_id": OP,
               "recovery_required": True, "error": {"code": "RECOVERY_REQUIRED", "message": "fence", "retry_hint": "SameIdAfterReconcile"},
               "recovery": {"store_id": CID, "collection_id": CID, "operation_id": OP,
                            "payload_hash": A_HASH, "hash_version": 1, "attempt_id": OP,
                            "base": {"store_id": CID, "collection_id": CID, "table_uuid": OP,
                                     "metadata_location": f"store://{CID}/warehouse/initial.json",
                                     "current_snapshot_id": None, "schema_id": 0, "spec_id": 0},
                            "candidate_snapshot_id": None, "candidate_metadata_location": None,
                            "first_error": {"code": "RECOVERY_REQUIRED", "message": "fence", "retry_hint": "SameIdAfterReconcile"},
                            "publication_started": True, "lookup_blocked": False}}, 503),
        reply({"outcome": "definitely_not_committed", "phase": "validation", "operation_id": OP,
               "error": {"code": "RESOURCE_EXHAUSTED", "message": "original capacity", "retry_hint": "NewOperationId", "extra": {"keep": 1}}}, 429),
    ]
    def handler(request):
        return replies.pop(0)
    with client_with(handler) as client:
        unknown = client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
        assert unknown.outcome == "commit_unknown" and not unknown.terminal
        assert unknown.raw["recovery"]["attempt_id"] == OP
        original = client.find_append_outcome(CID, OP)
        assert original.outcome == "definitely_not_committed" and original.terminal
        assert original.error.raw["extra"] == {"keep": 1}
        assert original.error.retry_hint == "NewOperationId"


def test_find_distinguishes_in_progress_absence_and_original_committed_receipt():
    replies = [reply({"state": "in_progress", "operation_id": OP}),
               reply({"error": {"code": "OPERATION_NOT_FOUND", "message": "absent"}}, 404), reply(committed("1"))]
    with client_with(lambda request: replies.pop(0)) as client:
        assert client.find_append_outcome(CID, OP).state == "in_progress"
        assert client.find_append_outcome(CID, OP) is None
        assert client.find_append_outcome(CID, OP).result["snapshot_id"] == "1"


def test_create_uses_caller_identity_and_lost_create_resolution_checks_uuid_and_schema():
    sent = []
    def handler(request):
        if request.method == "POST":
            sent.append(json.loads(request.content))
            return reply({"outcome": "committed", "info": {"collection_id": CID, "name": "exact", "schema": SCHEMA.to_dict()}})
        return reply({"collection_id": CID, "name": "exact", "schema": SCHEMA.to_dict()})
    with client_with(handler) as client:
        assert client.create_collection("exact", SCHEMA, collection_id=CID).info["collection_id"] == CID
        assert client.resolve_create("exact", SCHEMA, collection_id=CID)["name"] == "exact"
        with pytest.raises(ProtocolError):
            client.resolve_create("exact", Schema([Field("different", "bool", False)]), collection_id=CID)
        with pytest.raises(ProtocolError):
            client.resolve_create("exact", SCHEMA, collection_id=OP)
    assert sent[0]["collection_id"] == CID


def test_scan_json_decodes_exact_values_and_explain_shares_fixed_request():
    sent = []
    def handler(request):
        sent.append((request.url.path, json.loads(request.content)))
        if request.url.path.endswith("explain"):
            return reply({"identity": {"collection_id": CID, "current_snapshot_id": "5"},
                          "snapshot_id": "5", "synthetic": True})
        return reply({"schema": SCHEMA.to_dict(), "snapshot_id": "5", "rows": [{"key": "a", "n": "9007199254740993"}],
                      "profile": {"synthetic": True}, "identity": {"current_snapshot_id": "5"}})
    with client_with(handler) as client:
        predicate = col("n", "int64").ge(9007199254740993)
        result = client.scan_json(CID, projection=["key", "n"], predicate=predicate, snapshot_id="5", limit=1)
        assert result.rows == [{"key": "a", "n": 9007199254740993}]
        assert result.snapshot_id == "5" and result.profile["synthetic"] is True
        assert client.explain(CID, projection=["key", "n"], predicate=predicate, snapshot_id="5", limit=1)["synthetic"] is True
    assert sent[0][1] == sent[1][1] == {"projection": ["key", "n"], "predicate": {"op": "ge", "field": "n", "value": "9007199254740993"}, "snapshot_id": "5", "limit": 1}


@pytest.mark.parametrize("extra", [0, 1], ids=["exact-limit", "one-byte-over"])
def test_scan_json_applies_canonical_decoded_row_limit(extra):
    schema = Schema([Field("a", "string"), Field("b", "string")] + [
        Field(f"n{index}", "int64") for index in range(62)])
    # 1048576 + 1048008 + 2 * 5 + 62 * 9 = 2097152 decoded bytes.
    row = {"a": "x" * 1048576, "b": "y" * (1048008 + extra)} | {
        f"n{index}": "9223372036854775807" for index in range(62)}
    response = {"schema": schema.to_dict(), "snapshot_id": "5", "rows": [row],
                "profile": {"synthetic": True}, "identity": {"current_snapshot_id": "5"}}
    with client_with(lambda request: reply(response)) as client:
        if extra:
            with pytest.raises(ProtocolError, match="invalid scan JSON response"):
                client.scan_json(CID)
        else:
            expected = row | {f"n{index}": 9223372036854775807 for index in range(62)}
            assert client.scan_json(CID).rows == [expected]


@pytest.mark.parametrize("status,error_type", [(401, AuthenticationError), (403, AuthenticationError), (503, HTTPError)])
def test_http_errors_preserve_full_detail_separate_from_write_outcomes(status, error_type):
    with client_with(lambda request: reply({"error": {"code": "NO", "message": "reason", "retry_hint": "OperatorAction", "trace": {"id": "x"}}}, status)) as client:
        with pytest.raises(error_type) as error:
            client.capabilities()
        assert error.value.status_code == status
        assert error.value.detail.raw["trace"] == {"id": "x"}


@pytest.mark.parametrize("body,status", [
    ({"outcome": "committed"}, 200), ({"outcome": "committed", "result": {}}, 202),
    ({"outcome": "definitely_not_committed"}, 409), ({"outcome": "commit_unknown", "recovery_required": False}, 503),
    ({"outcome": "new_unrecognized_kind"}, 200), (committed(operation=CID), 200),
])
def test_malformed_or_wrong_identity_write_response_is_not_fabricated_failure(body, status):
    with client_with(lambda request: reply(body, status)) as client:
        with pytest.raises(ProtocolError) as error:
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
        assert error.value.operation_id == OP


def test_read_timeout_retains_mutation_identity_without_fabricated_server_outcome():
    def handler(request):
        assert request.extensions["timeout"]["read"] == 2
        raise httpx.ReadTimeout("socket", request=request)
    with client_with(handler, timeout=2) as client:
        with pytest.raises(DeadlineExceeded) as error:
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
        assert error.value.operation_id == OP


def test_zero_deadline_stops_before_post():
    sent = []
    with client_with(lambda request: sent.append(request) or reply(committed())) as client:
        with pytest.raises(DeadlineExceeded):
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA, deadline=0)
    assert sent == []


def test_tls_verification_is_enabled_by_default_and_can_use_custom_context(monkeypatch):
    import glamdb_sdk.client as module
    calls = []
    original = httpx.Client
    def make_client(*args, **kwargs):
        calls.append(kwargs["verify"])
        return original(*args, **kwargs)
    monkeypatch.setattr(module.httpx, "Client", make_client)
    context = ssl.create_default_context()
    with Client("https://example.test", "secret", transport=httpx.MockTransport(lambda r: reply({}))):
        pass
    with Client("https://example.test", "secret", verify=context, transport=httpx.MockTransport(lambda r: reply({}))):
        pass
    assert calls == [True, context]


@pytest.mark.parametrize("kwargs", [{"token": ""}, {"token": "x\n"}, {"base_url": "ftp://example.test"}, {"base_url": "https://user:secret@example.test"}])
def test_invalid_connection_config_is_rejected_before_connecting(kwargs):
    config = {"base_url": "https://example.test", "token": "secret"} | kwargs
    with pytest.raises(ValidationError):
        Client(**config)


@pytest.mark.parametrize("page_size", [0, 1001, True])
def test_page_size_validation_prevents_invalid_requests(page_size):
    with client_with(lambda request: pytest.fail("must not connect")) as client:
        with pytest.raises(ValidationError):
            client.list_collections(page_size=page_size)


def test_json_protocol_failure_and_redirect_do_not_leak_bearer_to_other_host():
    replies = [httpx.Response(200, text="not JSON", headers={"content-type": "application/json"}),
               httpx.Response(307, headers={"location": "https://other.test/"})]
    seen = []
    def handler(request):
        seen.append(str(request.url))
        return replies.pop(0)
    with client_with(handler) as client:
        for _ in range(2):
            with pytest.raises(ProtocolError):
                client.capabilities()
    assert len(seen) == 2 and all(url.startswith("https://example.test/") for url in seen)


def test_closed_client_cannot_start_new_requests():
    client = client_with(lambda request: reply({}))
    client.close()
    with pytest.raises(TransportError):
        client.capabilities()


def test_operation_lookup_recovery_pending_is_not_authoritative_absence():
    with client_with(lambda request: reply({"error": {"code": "RECOVERY_REQUIRED", "message": "not authoritative", "retry_hint": "OperatorAction"}}, 503)) as client:
        with pytest.raises(HTTPError) as error:
            client.find_append_outcome(CID, OP)
        assert error.value.operation_id == OP and error.value.detail.code == "RECOVERY_REQUIRED"


def test_schema_fetch_failure_keeps_caller_operation_id_before_append():
    def handler(request):
        assert request.method == "GET" and request.url.path.endswith("schema")
        raise httpx.ReadTimeout("schema fetch", request=request)
    with client_with(handler) as client:
        with pytest.raises(DeadlineExceeded) as error:
            client.append(CID, [{"key": "a"}], operation_id=OP)
        assert error.value.operation_id == OP and error.value.collection_id == CID


def test_oversized_json_response_is_closed_and_rejected():
    response = httpx.Response(200, json={"extra": "x" * 10000})
    with client_with(lambda request: response, max_json_bytes=1024) as client:
        with pytest.raises(ProtocolError, match="byte limit"):
            client.capabilities()
    assert response.is_closed


@pytest.mark.parametrize("kwargs", [{"limit": -1}, {"limit": True}, {"projection": "key"},
                                    {"projection": ["key", "key"]}, {"snapshot_id": True}])
def test_invalid_scan_input_is_rejected_before_post(kwargs):
    with client_with(lambda request: pytest.fail("must not POST")) as client:
        with pytest.raises(ValidationError):
            client.scan_json(CID, **kwargs)


@pytest.mark.parametrize("rows", [[{"key": "a", "n": True}], [{"key": "a", "n": 9007199254740993}]])
def test_json_wire_int64_requires_decimal_strings(rows):
    with client_with(lambda request: reply({"schema": SCHEMA.to_dict(), "snapshot_id": "5", "rows": rows,
                                           "profile": {}, "identity": {"current_snapshot_id": "5"}})) as client:
        with pytest.raises(ProtocolError):
            client.scan_json(CID)


def test_empty_projection_retains_empty_schema_and_each_matching_row():
    def handler(request):
        assert json.loads(request.content)["projection"] == []
        return reply({"schema": {"fields": []}, "snapshot_id": "5", "rows": [{}, {}],
                      "profile": {"rows_returned": 2}, "identity": {"current_snapshot_id": "5"}})
    with client_with(handler) as client:
        result = client.scan_json(CID, projection=[])
    assert result.schema.fields == () and result.rows == [{}, {}]


def test_prepared_append_view_cannot_mutate_the_request_sent_on_retry():
    bodies = []
    with client_with(lambda request: bodies.append(json.loads(request.content)) or reply(committed(payload_hash=ORIGINAL_HASH))) as client:
        request = client.prepare_append(CID, [{"key": "original"}], operation_id=OP, schema=SCHEMA)
        view = request.to_dict()
        view["operation_id"] = CID
        view["rows"][0]["key"] = "changed"
        client.append(request)
    assert bodies[0]["operation_id"] == OP and bodies[0]["rows"][0]["key"] == "original"


@pytest.mark.parametrize("replacement", ["a" * 64, "", "g" * 64, "a" * 63, 123, None])
def test_append_rejects_missing_malformed_or_different_receipt_payload_hash(replacement):
    body = committed(payload_hash=replacement)
    if replacement is None:
        del body["result"]["payload_hash"]
    with client_with(lambda request: reply(body)) as client:
        request = client.prepare_append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
        with pytest.raises(ProtocolError) as failure:
            client.append(request)
    assert failure.value.operation_id == OP and failure.value.collection_id == CID
    assert failure.value.raw == body


@pytest.mark.parametrize("version", [None, 0, 2, True, "1"])
def test_append_rejects_missing_or_unsupported_receipt_hash_version(version):
    body = committed()
    if version is None:
        del body["result"]["hash_version"]
    else:
        body["result"]["hash_version"] = version
    with client_with(lambda request: reply(body)) as client:
        with pytest.raises(ProtocolError) as failure:
            client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA)
    assert failure.value.operation_id == OP and failure.value.raw == body


@pytest.mark.parametrize("replacement", ["", "g" * 64, "a" * 63, 123, None])
def test_outcome_lookup_requires_structurally_valid_payload_hash(replacement):
    body = committed(payload_hash=replacement)
    if replacement is None:
        del body["result"]["payload_hash"]
    with client_with(lambda request: reply(body)) as client:
        with pytest.raises(ProtocolError):
            client.find_append_outcome(CID, OP)


@pytest.mark.parametrize("version", [None, 0, 2, True, "1"])
def test_outcome_lookup_rejects_missing_or_unsupported_hash_version(version):
    body = committed()
    if version is None:
        del body["result"]["hash_version"]
    else:
        body["result"]["hash_version"] = version
    with client_with(lambda request: reply(body)) as client:
        with pytest.raises(ProtocolError):
            client.find_append_outcome(CID, OP)


def test_lookup_does_not_guess_original_payload_and_append_compares_digest_bytes():
    body = committed(payload_hash="A" * 64)
    with client_with(lambda request: reply(body)) as client:
        assert client.find_append_outcome(CID, OP).result["payload_hash"] == "A" * 64
    with client_with(lambda request: reply(committed(payload_hash=A_HASH.upper()))) as client:
        assert client.append(CID, [{"key": "a"}], operation_id=OP, schema=SCHEMA).outcome == "committed"


@pytest.mark.parametrize("field,value", [
    ("rows_appended", 0), ("rows_appended", -1), ("rows_appended", True), ("rows_appended", "1.5"),
    ("receipt_version", 2), ("receipt_version", True), ("snapshot_created_at_us", "253402300800000000"),
    ("snapshot_created_at_us", "not-microseconds"), ("data_files_added", -1), ("data_files_added", True),
    ("compressed_data_bytes_added", -1), ("compressed_data_bytes_added", None),
    ("parent_snapshot_id", True), ("parent_snapshot_id", 4), ("durability_profile", ""),
])
def test_committed_receipt_rejects_invalid_core_fields_and_optional_metrics(field, value):
    body = committed()
    body["result"][field] = value
    with client_with(lambda request: reply(body)) as client:
        with pytest.raises(ProtocolError):
            client.find_append_outcome(CID, OP)


@pytest.mark.parametrize("field", ["rows_appended", "receipt_version", "snapshot_created_at_us"])
def test_committed_receipt_requires_shared_core_fields(field):
    body = committed()
    del body["result"][field]
    with client_with(lambda request: reply(body)) as client:
        with pytest.raises(ProtocolError):
            client.find_append_outcome(CID, OP)


def test_database_selection_keeps_management_at_server_root():
    seen = []
    def handler(request):
        seen.append((request.method, request.url.path))
        if request.url.path.endswith('/collections:resolve'):
            assert request.url.params['name'] == 'exact'
            return reply({'collection_id': CID})
        if request.url.path == '/databases' and request.method == 'GET':
            return reply({'items': [], 'next_cursor': None})
        if request.url.path.endswith('/schema'):
            return reply(SCHEMA.to_dict())
        return reply({'database_id': OP, 'state': 'open', 'name': 'a'})
    with Client('https://example.test', 'token', database_id=OP, transport=httpx.MockTransport(handler)) as client:
        client.create_database('a', database_id=OP)
        client.list_databases()
        client.get_database(OP)
        client.close_database(OP)
        client.open_database(OP)
        client.reconcile_database(OP)
        client.capabilities()
        client.resolve_collection('exact')
        client.get_schema(CID)
    assert seen == [('POST', '/databases'), ('GET', '/databases'), ('GET', f'/databases/{OP}'),
                    ('POST', f'/databases/{OP}/close'), ('POST', f'/databases/{OP}/open'),
                    ('POST', f'/databases/{OP}/reconcile'), ('GET', f'/databases/{OP}/capabilities'),
                    ('GET', f'/databases/{OP}/collections:resolve'), ('GET', f'/databases/{OP}/collections/{CID}/schema')]


def test_database_mutations_do_not_retry_on_transport_failure():
    seen = []
    def handler(request):
        seen.append(request.content)
        raise httpx.ReadError('lost response', request=request)
    with client_with(handler) as client:
        with pytest.raises(TransportError):
            client.create_database('a', database_id=OP)
    assert len(seen) == 1
    assert json.loads(seen[0]) == {'name': 'a', 'database_id': OP}
    with pytest.raises(ValidationError):
        Client('https://example.test', 'token', database_id='../escape')


def test_database_limits_preserve_partial_and_omitted_configuration():
    seen = []
    def handler(request):
        seen.append((request.url.path, json.loads(request.content)))
        return reply({'database_id': OP, 'state': 'open', 'name': 'a'})
    with client_with(handler) as client:
        client.create_database('a', limits={'max_collections': 2, 'writer_wait_ms': 0})
        client.create_database('a')
        client.open_database(OP, limits={'max_collections': 3})
        client.open_database(OP)
    assert seen == [('/databases', {'name': 'a', 'limits': {'max_collections': 2, 'writer_wait_ms': 0}}),
                    ('/databases', {'name': 'a'}),
                    (f'/databases/{OP}/open', {'limits': {'max_collections': 3}}),
                    (f'/databases/{OP}/open', {})]
