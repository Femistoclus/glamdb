"""Real localhost TCP/TLS against the disposable MOCK, never C++ durability evidence."""

from datetime import datetime, timezone
import subprocess
import sys
from uuid import uuid4

import httpx
import pytest

from glamdb_sdk import (AuthenticationError, Client, Field, HTTPError, ProtocolError,
                        Schema, TransportError, col)

from .helpers.http_server import (MockHTTPServer, REPOSITORY, ScenarioHTTPTransport, TOKEN,
                                 make_tls_material, package_environment)

SCHEMA = Schema([Field("key", "string", False), Field("n", "int64", True),
                 Field("at", "timestamp_us_utc", True), Field("flag", "bool", True),
                 Field("ratio", "float64", True)])
ROWS = [
    {"key": "duplicate", "n": 9007199254740993, "at": "0001-01-01T00:00:00Z", "flag": True, "ratio": -0.0},
    {"key": "duplicate", "n": 9223372036854775807, "at": "9999-12-31T23:59:59.999999Z", "flag": False, "ratio": 1.5},
    {"key": "duplicate", "n": None, "at": None, "flag": None, "ratio": None},
]


@pytest.fixture(autouse=True)
def loopback_bypasses_environment_proxies(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")


@pytest.fixture
def server(tmp_path):
    with MockHTTPServer(tmp_path) as running:
        yield running


@pytest.fixture(scope="module")
def tls_material(tmp_path_factory):
    return make_tls_material(tmp_path_factory.mktemp("sdk-test-ca"))


def create(client, *, name="integration", collection_id=None):
    cid = str(uuid4()) if collection_id is None else str(collection_id)
    outcome = client.create_collection(name, SCHEMA, collection_id=cid)
    assert outcome.outcome == "committed" and outcome.info["mock"] is True
    return cid


def test_actual_http_authentication_and_mock_capabilities(server):
    with Client(server.url, TOKEN) as client:
        capabilities = client.capabilities()
    assert capabilities["backend"] == "mock" and capabilities["mock"] is True
    assert capabilities["mode"] == "append" and capabilities["key_unique"] is False
    assert capabilities["durability_validation"] == "designed"
    for feature in ("cow", "compaction", "async_admission", "page_pruning", "bloom_pruning"):
        assert capabilities[feature] is False
    with Client(server.url, "wrong-test-token") as client:
        with pytest.raises(AuthenticationError) as error:
            client.capabilities()
    assert error.value.status_code == 403 and error.value.detail.code == "PERMISSION_DENIED"


def test_real_json_arrow_types_duplicates_stats_history_original_retry_and_precondition(server):
    with Client(server.url, TOKEN) as client:
        cid = create(client, name="Exact name")
        assert client.resolve_create("Exact name", SCHEMA, collection_id=cid)["collection_id"] == cid
        assert client.get_collection(cid)["mode"] == "append"
        assert client.get_schema(cid) == SCHEMA
        with pytest.raises(ProtocolError):
            client.resolve_create("Exact name", SCHEMA, collection_id=uuid4())
        with pytest.raises(HTTPError) as case_sensitive:
            client.resolve_collection("exact name")
        assert case_sensitive.value.status_code == 404
        assert client.stats(cid)["rows"] == 0
        initial = client.scan_json(cid, snapshot_id=None, limit=0)
        assert initial.snapshot_id is None and initial.rows == [] and initial.schema == SCHEMA

        operation_a = str(uuid4())
        request_a = client.prepare_append(cid, ROWS, operation_id=operation_a, schema=SCHEMA, expected_snapshot=None)
        first = client.append(request_a)
        snapshot_a = first.result["snapshot_id"]
        assert first.outcome == "committed" and first.result["rows_appended"] == 3
        assert first.result["payload_hash"] == request_a.payload_hash and first.result["durable"] is False
        second = client.append(cid, [{"key": "later", "n": 7}], operation_id=uuid4(), schema=SCHEMA,
                               expected_snapshot=snapshot_a)
        snapshot_b = second.result["snapshot_id"]
        assert snapshot_b != snapshot_a
        # Original receipt is immutable even after the old NoSnapshot condition
        # no longer matches the current state.
        assert client.append(request_a).raw == first.raw
        assert client.find_append_outcome(cid, operation_a).raw == first.raw
        assert client.stats(cid)["rows"] == 4
        assert client.stats(cid, snapshot_id=snapshot_a)["rows"] == 3
        assert client.stats(cid, snapshot_id=None)["rows"] == 0
        assert client.scan_json(cid, snapshot_id=None).rows == []

        historical = client.scan_json(cid, snapshot_id=snapshot_a)
        assert len(historical.rows) == 3 and [row["key"] for row in historical.rows] == ["duplicate"] * 3
        assert historical.rows[0]["n"] == 9007199254740993
        assert historical.rows[0]["at"] == datetime(1, 1, 1, tzinfo=timezone.utc)
        assert historical.rows[1]["at"] == datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        assert historical.rows[2]["n"] is None and historical.rows[2]["flag"] is None
        assert historical.rows[0]["flag"] is True and historical.rows[0]["ratio"] == 0.0
        only_null = client.scan_json(cid, projection=["key", "n"], predicate=col("n", "int64").is_null())
        assert only_null.rows == [{"key": "duplicate", "n": None}]
        assert len(client.scan_json(cid, projection=[]).rows) == 4

        with client.scan_batches(cid, snapshot_id=snapshot_a) as stream:
            arrow_rows = [row for batch in stream for row in batch.to_pylist()]
            assert stream.snapshot_id == snapshot_a
        assert arrow_rows == historical.rows and stream.integrity == "verified" and stream.checksum_verified
        with client.scan_batches(cid, projection=[]) as empty_projection:
            projected = list(empty_projection)
        assert sum(batch.num_rows for batch in projected) == 4 and all(batch.num_columns == 0 for batch in projected)
        with client.scan_batches(cid) as early:
            retained_iterator = iter(early)
            assert next(retained_iterator).num_rows == 4
        assert early.closed and early.integrity == "not_complete" and not early.checksum_verified
        assert list(retained_iterator) == []

        failure_op = str(uuid4())
        failed = client.append(cid, [{"key": "not-published"}], operation_id=failure_op,
                               schema=SCHEMA, expected_snapshot=None)
        assert failed.outcome == "definitely_not_committed" and failed.error.code == "SNAPSHOT_CONFLICT"
        old_failure = client.append(cid, [{"key": "not-published"}], operation_id=failure_op,
                                    schema=SCHEMA, expected_snapshot=snapshot_b)
        assert old_failure.raw == failed.raw and client.find_append_outcome(cid, failure_op).raw == failed.raw
        assert client.stats(cid)["rows"] == 4
        assert client.find_append_outcome(cid, uuid4()) is None


def test_real_metadata_and_cursor_membership_stay_fixed_while_new_data_arrives(server):
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

        cid = collection_ids[0]
        for index in range(3):
            client.append(cid, [{"key": f"k-{index}"}], operation_id=uuid4(), schema=SCHEMA)
        first_history = client.snapshots(cid, page_size=1)
        first_files = client.files(cid, page_size=1)
        original_view = first_files.raw["identity"]["snapshot_id"]
        client.append(cid, [{"key": "after-pages"}], operation_id=uuid4(), schema=SCHEMA)
        history = list(first_history.items)
        cursor = first_history.next_cursor
        while cursor:
            page = client.snapshots(cid, page_size=1, cursor=cursor)
            history.extend(page.items)
            cursor = page.next_cursor
        files = list(first_files.items)
        cursor = first_files.next_cursor
        while cursor:
            page = client.files(cid, page_size=1, cursor=cursor)
            assert page.raw["identity"]["snapshot_id"] == original_view
            files.extend(page.items)
            cursor = page.next_cursor
        assert len(history) == 3 and len(files) == 3
        assert client.stats(cid)["data_files"] == 4
        details = client.file(cid, files[0]["file_id"], snapshot_id=original_view)
        assert details["synthetic"] is True and details["row_groups"]
        assert client.stats(cid)["compressed_data_bytes"] is None
        with pytest.raises(HTTPError) as foreign:
            client.files(collection_ids[1], page_size=1, cursor=first_files.next_cursor)
        assert foreign.value.detail.code == "CURSOR_MISMATCH"
        plan = client.explain(cid, projection=["n"], predicate=col("key", "string").eq("k-0"), snapshot_id=original_view)
        assert plan["mock"] is True and plan["actual_io"] is None
        assert plan["identity"]["snapshot_id"] == original_view


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
        resolution = httpx.post(f"{server.url}/v0/development/collections/{cid}/operations/{operation}:resolve",
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


def test_lost_real_http_response_retains_identity_and_explicit_retry_is_idempotent(server):
    operation = str(uuid4())
    transport = ScenarioHTTPTransport(drop_response_for={operation})
    with Client(server.url, TOKEN, transport=transport) as client:
        cid = create(client)
        request = client.prepare_append(cid, ROWS, operation_id=operation, schema=SCHEMA)
        with pytest.raises(TransportError) as lost:
            client.append(request)
        assert lost.value.operation_id == operation and lost.value.collection_id == cid
        assert len(transport.requests) == 1  # SDK did not repeat the unsafe POST itself
        original = client.find_append_outcome(cid, operation)
        assert original.outcome == "committed" and original.result["payload_hash"] == request.payload_hash
        assert client.append(request).raw == original.raw
        assert transport.requests[0][1] == transport.requests[1][1]
        assert client.stats(cid)["rows"] == 3
        with pytest.raises(HTTPError) as conflict:
            client.append(cid, [{"key": "different"}], operation_id=operation, schema=SCHEMA)
        assert conflict.value.detail.code == "IDEMPOTENCY_CONFLICT" and client.stats(cid)["rows"] == 3


def test_runnable_quickstart_uses_actual_http_and_reports_verified_mock_stream(server):
    env = package_environment()
    env["GLAMDB_URL"] = server.url
    collection_id, operation_id = str(uuid4()), str(uuid4())
    result = subprocess.run([sys.executable, str(REPOSITORY / "sdk/python/examples/quickstart.py"),
                             collection_id, operation_id], env=env, capture_output=True, text=True,
                            check=False, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "integrity: verified" in result.stdout and "9007199254740993" in result.stdout
    assert TOKEN not in result.stdout and TOKEN not in result.stderr
    with Client(server.url, TOKEN) as client:
        assert client.stats(collection_id)["rows"] == 2


def test_actual_https_accepts_explicit_trusted_test_ca(tmp_path, tls_material):
    with MockHTTPServer(tmp_path, tls=tls_material) as server:
        with Client(server.url, TOKEN, verify=tls_material.trusted_context()) as client:
            assert client.capabilities()["backend"] == "mock"
            cid = create(client)
            assert client.append(cid, ROWS, operation_id=uuid4(), schema=SCHEMA).outcome == "committed"
            with client.scan_batches(cid) as stream:
                assert sum(batch.num_rows for batch in stream) == 3
            assert stream.integrity == "verified"


@pytest.mark.parametrize("trust", ["system", "wrong_ca"])
def test_actual_https_rejects_untrusted_or_wrong_ca_without_fabricating_write_failure(tmp_path, tls_material, trust):
    operation, cid = str(uuid4()), str(uuid4())
    with MockHTTPServer(tmp_path, tls=tls_material) as server:
        verify = True if trust == "system" else tls_material.wrong_context()
        with Client(server.url, TOKEN, verify=verify, timeout=3) as client:
            with pytest.raises(TransportError) as failure:
                client.append(cid, ROWS, operation_id=operation, schema=SCHEMA)
        assert failure.value.operation_id == operation and failure.value.collection_id == cid
        assert TOKEN not in str(failure.value)
        assert isinstance(failure.value.__cause__, httpx.ConnectError)
        assert "CERTIFICATE_VERIFY_FAILED" in str(failure.value.__cause__)
