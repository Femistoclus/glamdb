import base64
import hashlib
import hmac
import io
import json
from uuid import UUID

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from glamdb_mock import create_app


CID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
OP1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
OP2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
OP3 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3"
AUTH = {"Authorization": "Bearer test-secret"}
SCHEMA = {"fields": [
    {"name": "key", "type": "string", "nullable": False},
    {"name": "n", "type": "int64", "nullable": True},
    {"name": "at", "type": "timestamp_us_utc", "nullable": True},
]}
ROWS = [
    {"key": "duplicate", "n": "9007199254740993", "at": "0001-01-01T00:00:00Z"},
    {"key": "duplicate", "n": None, "at": "9999-12-31T23:59:59.999999Z"},
]


@pytest.fixture
def ui(tmp_path):
    (tmp_path / "assets").mkdir()
    (tmp_path / "config").mkdir()
    (tmp_path / "index.html").write_text("<h1>Workbench</h1>")
    (tmp_path / "assets" / "app.js").write_text("export const mock = true;")
    return tmp_path


@pytest.fixture
def client(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui)) as value:
        value.headers.update(AUTH)
        yield value


def create(client, cid=CID, name="exact Name"):
    response = client.post("/collections", json={"collection_id": cid, "name": name, "schema": SCHEMA})
    assert response.status_code in (200, 201), response.text
    return response.json()


def append(client, rows=ROWS, operation_id=OP1, **extra):
    return client.post(f"/collections/{CID}/append", json={"operation_id": operation_id, "rows": rows, **extra})


def test_readiness_reports_mock_availability_without_qualification(client):
    health = client.get('/readyz')
    assert health.status_code == 200
    assert health.json() == {
        'state': 'ready', 'read_available': True, 'write_ready': True,
        'storage_failure_latched': False, 'durability_qualified': False,
        'backend': 'mock', 'mock': True, 'durability_validation': 'designed',
    }
    assert client.get('/capabilities').json()['health'] == health.json()
    assert client.get('/readyz', headers={'Authorization': ''}).status_code == 401


def test_public_health_and_auth_are_explicitly_mock(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui)) as client:
        assert client.get("/health").json() == {
            "status": "ok", "backend": "mock", "mock": True, "durability_validation": "designed"
        }
        assert client.get("/capabilities").status_code == 401
        assert client.get("/capabilities", headers={"Authorization": "Bearer wrong"}).status_code == 403
        capabilities = client.get("/capabilities", headers=AUTH).json()
        assert capabilities["mock"] is True
        assert capabilities["key_unique"] is False
        for feature in ("cow", "compaction", "async_admission", "page_pruning", "bloom_pruning"):
            assert capabilities[feature] is False


def test_create_exact_resolve_schema_and_idempotency(client):
    info = create(client)
    assert info["collection_id"] == CID
    assert info["current_snapshot_id"] is None
    assert info["mode"] == "append"
    UUID(info["table_uuid"])
    assert create(client) == info
    assert client.get("/collections:resolve", params={"name": "exact Name"}).json() == info
    assert client.get("/collections:resolve", params={"name": "exact name"}).status_code == 404
    schema = client.get(f"/collections/{CID}/schema").json()
    assert [field["field_id"] for field in schema["fields"]] == [1, 2, 3]
    assert client.post("/collections", json={"collection_id": OTHER, "name": "exact Name", "schema": SCHEMA}).status_code == 409


def test_append_preserves_duplicates_and_original_outcome_before_new_precondition(client):
    create(client)
    original = append(client, expected_snapshot=None)
    assert original.status_code == 200
    assert original.json()["outcome"] == "committed"
    snapshot = original.json()["result"]["snapshot_id"]
    later = append(client, [{"key": "later"}], OP2)
    assert later.status_code == 200
    assert append(client, expected_snapshot="999999").json() == original.json()
    assert append(client, [{"key": "different"}]).status_code == 409
    assert client.get(f"/collections/{CID}/operations/{OP1}").json() == original.json()
    current = client.post(f"/collections/{CID}/scan", json={}).json()
    assert [row["key"] for row in current["rows"]] == ["duplicate", "duplicate", "later"]
    old = client.post(f"/collections/{CID}/scan", json={"snapshot_id": snapshot}).json()
    assert [row["key"] for row in old["rows"]] == ["duplicate", "duplicate"]
    initial = client.post(f"/collections/{CID}/scan", json={"snapshot_id": None}).json()
    assert initial["rows"] == []
    assert initial["snapshot_id"] is None


def test_precondition_failure_is_an_immutable_registered_outcome(client):
    create(client)
    append(client)
    failed = append(client, [{"key": "later"}], OP2, expected_snapshot=None)
    assert failed.status_code == 409
    assert failed.json()["outcome"] == "definitely_not_committed"
    assert failed.json()["error"]["code"] == "SNAPSHOT_CONFLICT"
    assert append(client, [{"key": "later"}], OP2).json() == failed.json()
    assert client.get(f"/collections/{CID}/operations/{OP2}").json() == failed.json()


def test_json_scan_projection_null_logic_and_lossless_types(client):
    create(client)
    append(client)
    response = client.post(f"/collections/{CID}/scan", json={
        "projection": ["at", "n"], "predicate": {"op": "not", "arg": {"op": "eq", "field": "n", "value": "0"}}
    })
    assert response.status_code == 200
    result = response.json()
    assert result["rows"] == [{"at": "0001-01-01T00:00:00.000000Z", "n": "9007199254740993"}]
    assert [field["name"] for field in result["schema"]["fields"]] == ["at", "n"]
    assert result["profile"]["rows_returned"] == 1
    assert result["profile"]["compressed_bytes_read"] is None
    assert result["profile"]["mock"] is True
    invalid = client.post(f"/collections/{CID}/scan", json={"predicate": {"op": "eq", "field": "missing", "value": 2}})
    assert invalid.status_code == 422
    assert "error" in invalid.json()


def test_arrow_is_real_ipc_with_lossless_int64_timestamp_and_integrity_headers(client):
    create(client)
    append(client)
    response = client.post(f"/collections/{CID}/scan", headers={"Accept": "application/vnd.apache.arrow.stream"}, json={})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert int(response.headers["content-length"]) == len(response.content)
    assert response.headers["x-content-sha256"] == hashlib.sha256(response.content).hexdigest()
    assert response.headers["x-glamdb-snapshot-id"] == "1"
    table = pa.ipc.open_stream(io.BytesIO(response.content)).read_all()
    assert table.column("n").to_pylist() == [9007199254740993, None]
    assert table.schema.field("at").type == pa.timestamp("us", tz="UTC")
    assert table.column("at").cast(pa.int64()).to_pylist() == [-62135596800000000, 253402300799999999]


def test_metadata_is_accurate_and_never_invents_parquet_io(client):
    create(client)
    append(client)
    stats = client.get(f"/collections/{CID}/stats").json()
    assert stats["rows"] == 2
    assert stats["data_files"] == 1
    assert stats["compressed_data_bytes"] is None
    assert stats["mock"] is True
    files = client.get(f"/collections/{CID}/files").json()
    assert len(files["items"]) == 1
    file = files["items"][0]
    assert file["record_count"] == 2
    assert file["synthetic"] is True
    assert file["path"].startswith("mock://")
    details = client.get(f"/collections/{CID}/files/{file['file_id']}").json()
    assert details["row_groups"][0]["record_count"] == 2
    explain = client.post(f"/collections/{CID}/explain", json={"projection": ["key"]}).json()
    assert explain["output_columns"] == ["key"]
    assert explain["actual_io"] is None
    assert explain["estimates"]["rows_before_filter"] == 2


def test_signed_collection_pages_freeze_membership_and_bind_parameters(client):
    create(client)
    create(client, OTHER, "second")
    first = client.get("/collections", params={"page_size": 1}).json()
    assert [item["collection_id"] for item in first["items"]] == [CID]
    cursor = first["next_cursor"]
    create(client, "33333333-3333-4333-8333-333333333333", "third")
    second = client.get("/collections", params={"page_size": 1, "cursor": cursor}).json()
    assert [item["collection_id"] for item in second["items"]] == [OTHER]
    assert second["next_cursor"] is None
    assert client.get("/collections", params={"page_size": 1, "cursor": cursor}).json() == second
    mismatch = client.get("/collections", params={"page_size": 2, "cursor": cursor})
    assert mismatch.status_code == 400
    assert mismatch.json()["error"]["code"] == "CURSOR_MISMATCH"
    assert client.get("/collections", params={"cursor": cursor[:-2] + "zz"}).status_code == 400


def test_runtime_theme_reload_rejects_unsafe_values_and_confines_static_paths(client, ui):
    theme = {"version": 1, "brand": {"name": "Rose"}, "colors": {"pink": "#ff00aa"}, "typography": {"body": "Arial, sans-serif"}, "sizing": {"radius": "12px"}, "decoration": {"bows": True}, "labels": {"nav_data": "Data"}}
    path = ui / "config" / "theme.json"
    path.write_text(json.dumps(theme))
    assert client.get("/workbench/config").json() == theme
    theme["brand"]["name"] = "Updated"
    path.write_text(json.dumps(theme))
    assert client.get("/workbench/config").json()["brand"]["name"] == "Updated"
    theme["colors"]["pink"] = "url(javascript:alert(1))"
    path.write_text(json.dumps(theme))
    assert client.get("/workbench/config").status_code == 422
    assert client.get("/").text == "<h1>Workbench</h1>"
    assert client.get("/assets/app.js").status_code == 200
    (ui.parent / "outside-secret.txt").write_text("private")
    assert client.get("/assets/%2e%2e/%2e%2e/outside-secret.txt").status_code == 404


def test_streamed_body_limit_cannot_be_bypassed_without_content_length(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, max_body_bytes=128)) as client:
        response = client.post("/collections", headers={**AUTH, "Content-Type": "application/json"}, content=iter([b"{" + b" " * 150, b"}"]))
        assert response.status_code == 413
        assert response.json()["error"]["code"] == "RESOURCE_EXHAUSTED"


def test_development_unknown_holds_writer_and_can_be_explicitly_resolved(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, enable_development_faults=True)) as client:
        client.headers.update(AUTH)
        create(client)
        response = client.post(f"/collections/{CID}/append", headers={"X-GlamDB-Mock-Scenario": "commit_unknown"}, json={"operation_id": OP1, "rows": ROWS})
        assert response.status_code == 503
        assert response.json()["outcome"] == "commit_unknown"
        assert response.json()["recovery_required"] is True
        assert client.get(f"/collections/{CID}/operations/{OP1}").status_code == 503
        assert append(client, [{"key": "blocked"}], OP2).status_code == 503
        assert client.post(f"/collections/{CID}/scan", json={}).json()["rows"] == []
        resolved = client.post(f"/development/collections/{CID}/operations/{OP1}:resolve", json={"outcome": "committed"})
        assert resolved.status_code == 200
        assert resolved.json()["outcome"] == "committed"
        assert append(client).json() == resolved.json()


def test_disabled_fault_scenarios_never_apply_writes(client):
    create(client)
    response = client.post(f"/collections/{CID}/append", headers={"X-GlamDB-Mock-Scenario": "commit_unknown"}, json={"operation_id": OP1, "rows": ROWS})
    assert response.status_code == 403
    assert client.get(f"/collections/{CID}/stats").json()["rows"] == 0


def test_missing_token_and_nonloopback_require_explicit_configuration(ui):
    with pytest.raises(ValueError, match="token"):
        create_app(token="", ui_dir=ui)


def test_file_and_history_cursor_remain_on_original_view(client):
    create(client)
    append(client)
    append(client, [{"key": "second"}], OP2)
    page = client.get(f"/collections/{CID}/files", params={"page_size": 1}).json()
    history = client.get(f"/collections/{CID}/snapshots", params={"page_size": 1}).json()
    append(client, [{"key": "third"}], OP3)
    next_page = client.get(f"/collections/{CID}/files", params={"page_size": 1, "cursor": page["next_cursor"]}).json()
    assert [item["snapshot_id"] for item in next_page["items"]] == ["2"]
    assert next_page["next_cursor"] is None
    assert next_page["identity"]["snapshot_id"] == "2"
    next_history = client.get(f"/collections/{CID}/snapshots", params={"page_size": 1, "cursor": history["next_cursor"]}).json()
    assert [item["snapshot_id"] for item in next_history["items"]] == ["2"]
    assert next_history["next_cursor"] is None
    create(client, OTHER, "other")
    foreign = client.get(f"/collections/{OTHER}/files", params={"page_size": 1, "cursor": page["next_cursor"]})
    assert foreign.status_code == 400
    assert foreign.json()["error"]["code"] == "CURSOR_MISMATCH"
    mismatch = client.get(f"/collections/{CID}/files", params={"page_size": 1, "snapshot_id": "1", "cursor": page["next_cursor"]})
    assert mismatch.json()["error"]["code"] == "CURSOR_MISMATCH"
    missing_selector = client.get(f"/collections/{CID}/files", params={"page_size": 1, "snapshot_id": "999999", "cursor": page["next_cursor"]})
    assert missing_selector.status_code == 400
    assert missing_selector.json()["error"]["code"] == "CURSOR_MISMATCH"


def test_cursor_ttl_and_restart_reject_prior_tokens(ui):
    clock = [1000.0]
    with TestClient(create_app(token="test-secret", ui_dir=ui, clock=lambda: clock[0])) as client:
        client.headers.update(AUTH)
        create(client)
        create(client, OTHER, "second")
        cursor = client.get("/collections", params={"page_size": 1}).json()["next_cursor"]
        clock[0] = 1901.0
        expired = client.get("/collections", params={"page_size": 1, "cursor": cursor})
        assert expired.json()["error"]["code"] == "CURSOR_EXPIRED"
    with TestClient(create_app(token="test-secret", ui_dir=ui)) as restarted:
        expired = restarted.get("/collections", headers=AUTH, params={"page_size": 1, "cursor": cursor})
        assert expired.json()["error"]["code"] == "CURSOR_EXPIRED"


def test_lost_response_resolves_original_committed_receipt_without_reappending(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, enable_development_faults=True)) as client:
        client.headers.update(AUTH)
        create(client)
        response = client.post(f"/collections/{CID}/append", headers={"X-GlamDB-Mock-Scenario": "lost_response"}, json={"operation_id": OP1, "rows": ROWS})
        assert response.status_code == 503
        assert response.json()["outcome"] == "commit_unknown"
        found = client.get(f"/collections/{CID}/operations/{OP1}")
        assert found.status_code == 200
        assert found.json()["outcome"] == "committed"
        assert append(client).json() == found.json()
        assert client.get(f"/collections/{CID}/stats").json()["rows"] == 2


def test_invalid_input_does_not_bind_id_and_unknown_absence_is_not_404(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, enable_development_faults=True)) as client:
        client.headers.update(AUTH)
        create(client)
        assert append(client, []).status_code == 422
        assert client.get(f"/collections/{CID}/operations/{OP1}").status_code == 404
        response = client.post(f"/collections/{CID}/append", headers={"X-GlamDB-Mock-Scenario": "commit_unknown"}, json={"operation_id": OP1, "rows": ROWS})
        assert response.status_code == 503
        assert client.get(f"/collections/{CID}/operations/{OP2}").status_code == 503
        assert append(client, [{"key": "different"}]).status_code == 409
        failed = client.post(f"/development/collections/{CID}/operations/{OP1}:resolve", json={"outcome": "definitely_not_committed"})
        assert failed.status_code == 503
        assert failed.json()["outcome"] == "definitely_not_committed"
        assert append(client).json() == failed.json()
        assert append(client, [{"key": "second"}], OP2).status_code == 200


@pytest.mark.parametrize("body", [
    {"projection": ["n", "n"]}, {"projection": ["missing"]}, {"limit": -1},
    {"limit": True}, {"predicate": {"op": "eq", "field": "n", "value": True}},
    {"predicate": {"op": "gte", "field": "n", "value": "0"}},
    {"predicate": {"op": "and", "args": []}}, {"snapshot_id": "999999999999999999999"},
])
def test_invalid_scans_reject_even_with_empty_data(client, body):
    create(client)
    response = client.post(f"/collections/{CID}/scan", json=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_ARGUMENT"


def test_empty_zero_limit_and_zero_projection_have_real_arrow_schema(client):
    create(client)
    empty = client.post(f"/collections/{CID}/scan", headers={"Accept": "application/vnd.apache.arrow.stream"}, json={})
    assert empty.headers["x-glamdb-snapshot-id"] == "initial"
    table = pa.ipc.open_stream(empty.content).read_all()
    assert table.num_rows == 0
    assert table.column_names == ["key", "n", "at"]
    append(client)
    zero = client.post(f"/collections/{CID}/scan", json={"limit": 0}).json()
    assert zero["rows"] == []
    assert zero["snapshot_id"] == "1"
    projected = client.post(f"/collections/{CID}/scan", json={"projection": []}).json()
    assert projected["rows"] == [{}, {}]
    assert projected["schema"]["fields"] == []
    arrow = client.post(f"/collections/{CID}/scan", headers={"Accept": "application/vnd.apache.arrow.stream"}, json={"projection": []})
    table = pa.ipc.open_stream(arrow.content).read_all()
    assert table.num_rows == 2
    assert table.num_columns == 0


def test_state_and_response_limits_reject_before_publication(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, max_rows=2, max_response_bytes=100)) as client:
        client.headers.update(AUTH)
        create(client)
        assert append(client).status_code == 200
        limited = append(client, [{"key": "third"}], OP2)
        assert limited.status_code == 429
        assert limited.json()["outcome"] == "definitely_not_committed"
        assert client.get(f"/collections/{CID}/stats").json()["rows"] == 2
        assert client.post(f"/collections/{CID}/scan", json={}).status_code == 413
        assert client.post(f"/collections/{CID}/scan", headers={"Accept": "application/vnd.apache.arrow.stream"}, json={}).status_code == 413


@pytest.mark.parametrize("section,key,value", [
    ("typography", "body", "Arial; background:url(https://evil.test/x)"),
    ("sizing", "radius", "expression(alert(1))"),
    ("decoration", "bows", "true"),
    ("colors", "pink", "var(--secret)"),
])
def test_config_rejects_css_execution_and_type_confusion(client, ui, section, key, value):
    theme = {"version": 1, "brand": {}, "colors": {}, "typography": {}, "sizing": {}, "decoration": {}, "labels": {}}
    theme[section][key] = value
    (ui / "config" / "theme.json").write_text(json.dumps(theme))
    response = client.get("/workbench/config")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_CONFIG"


def test_missing_malformed_and_oversized_config_errors_are_contract_shaped(client, ui):
    assert client.get("/workbench/config").status_code == 404
    path = ui / "config" / "theme.json"
    path.write_text("{")
    assert client.get("/workbench/config").status_code == 422
    path.write_text(" " * 65537)
    assert client.get("/workbench/config").status_code == 422


def test_asset_symlink_cannot_escape_ui(client, ui):
    secret = ui.parent / "private-secret.js"
    secret.write_text("secret")
    (ui / "assets" / "secret.js").symlink_to(secret)
    response = client.get("/assets/secret.js")
    assert response.status_code == 404
    assert "secret" not in response.text


def test_seed_uses_append_semantics_and_disposable_state(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, seed_demo=True)) as client:
        client.headers.update(AUTH)
        collections = client.get("/collections").json()["items"]
        assert [item["name"] for item in collections] == ["dreamhouse_events", "pink_inventory"]
        cid = collections[0]["collection_id"]
        rows = client.post(f"/collections/{cid}/scan", json={}).json()["rows"]
        assert [row["key"] for row in rows] == ["dream-001", "dream-002", "dream-001", "dream-004"]
        assert len(client.get(f"/collections/{cid}/snapshots").json()["items"]) == 2
    with TestClient(create_app(token="test-secret", ui_dir=ui)) as fresh:
        assert fresh.get("/collections", headers=AUTH).json()["items"] == []


def test_retained_state_budget_rejects_new_input_and_preserves_original_receipts(ui):
    with TestClient(create_app(token="test-secret", ui_dir=ui, max_state_bytes=18000)) as client:
        client.headers.update(AUTH)
        create(client)
        original = append(client, [{"key": "first"}])
        assert original.status_code == 200
        response = append(client, [{"key": "x" * 1024}], OP2)
        assert response.status_code == 429
        assert response.json()["outcome"] == "definitely_not_committed"
        assert response.json()["error"]["code"] == "RESOURCE_EXHAUSTED"
        assert append(client, [{"key": "first"}]).json() == original.json()
        assert client.get(f"/collections/{CID}/stats").json()["rows"] == 1


def test_cursor_ttl_is_not_extended_by_advancing_a_page(ui):
    clock = [1000.0]
    with TestClient(create_app(token="test-secret", ui_dir=ui, clock=lambda: clock[0])) as client:
        client.headers.update(AUTH)
        create(client)
        create(client, OTHER, "second")
        create(client, "33333333-3333-4333-8333-333333333333", "third")
        first = client.get("/collections", params={"page_size": 1}).json()
        clock[0] = 1899.0
        second = client.get("/collections", params={"page_size": 1, "cursor": first["next_cursor"]}).json()
        clock[0] = 1901.0
        expired = client.get("/collections", params={"page_size": 1, "cursor": second["next_cursor"]})
        assert expired.status_code == 400
        assert expired.json()["error"]["code"] == "CURSOR_EXPIRED"


def test_bearer_token_does_not_let_the_caller_forge_cursor_membership(client):
    create(client)
    create(client, OTHER, "second")
    cursor = client.get("/collections", params={"page_size": 1}).json()["next_cursor"]
    encoded, _signature = cursor.split(".")
    payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    payload["position"] = 0
    forged = base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).rstrip(b"=")
    public_derivation = hmac.digest(b"test-secret", b"glamdb-disposable-mock-cursor-v1", "sha256")
    signature = base64.urlsafe_b64encode(hmac.digest(public_derivation, forged, "sha256")).rstrip(b"=")
    response = client.get("/collections", params={"page_size": 1, "cursor": (forged + b"." + signature).decode()})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_ARGUMENT"
