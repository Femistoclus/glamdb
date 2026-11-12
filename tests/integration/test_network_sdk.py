"""The same real TCP/TLS client contract against explicit mock or native backends."""

from datetime import datetime, timezone
import subprocess
import sys
from uuid import uuid4

import httpx
import pytest

from glamdb_sdk import (AuthenticationError, Client, Field, HTTPError, ProtocolError,
                        IntegrityError, Schema, TransportError, col)

from .helpers.http_server import (REPOSITORY, ScenarioHTTPTransport, TOKEN,
                                 make_tls_material, package_environment)

from .helpers.backends import assert_storage, client_server
from .helpers.arrow_transport import ArrowHTTPTransport

SCHEMA = Schema([Field("key", "string", False), Field("n", "int64", True),
                 Field("at", "timestamp_us_utc", True), Field("flag", "bool", True),
                 Field("ratio", "float64", True)])
ROWS = [
    {"key": "duplicate", "n": 9007199254740993, "at": "0001-01-01T00:00:00Z", "flag": True, "ratio": -0.0},
    {"key": "duplicate", "n": 9223372036854775807, "at": "9999-12-31T23:59:59.999999Z", "flag": False, "ratio": 1.5},
    {"key": "duplicate", "n": None, "at": None, "flag": None, "ratio": None},
]


@pytest.fixture
def server(tmp_path):
    with client_server(tmp_path) as running:
        yield running


@pytest.fixture(scope="module")
def tls_material(tmp_path_factory):
    return make_tls_material(tmp_path_factory.mktemp("sdk-test-ca"))


def create(client, *, name="integration", collection_id=None):
    cid = str(uuid4()) if collection_id is None else str(collection_id)
    outcome = client.create_collection(name, SCHEMA, collection_id=cid)
    assert outcome.outcome == "committed" and outcome.info["collection_id"] == cid
    assert outcome.info["name"] == name
    return cid


def test_actual_http_authentication_and_backend_capabilities(server):
    with Client(server.url, TOKEN) as client:
        capabilities = client.capabilities()
    native = not server.mock
    assert capabilities["backend"] == ("local" if native else "mock")
    assert capabilities["mock"] is (not native)
    if server.mock:
        assert capabilities["mode"] == "append" and capabilities["key_unique"] is False
        for feature in ("cow", "compaction", "async_admission"):
            assert capabilities[feature] is False
    else:
        assert capabilities["collection_modes"] == ["append"]
        assert capabilities["features"]["pagination"] is True
    assert capabilities["durability_validation"] == "designed"
    for feature in ("page_pruning", "bloom_pruning"):
        assert capabilities[feature] is False
    with Client(server.url, "wrong-test-token") as client:
        with pytest.raises(AuthenticationError) as error:
            client.capabilities()
    assert (error.value.status_code, error.value.detail.code) == (
        (403, "PERMISSION_DENIED") if server.mock else (401, "UNAUTHORIZED"))


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
        assert int(client.stats(cid)["rows"]) == 0
        initial = client.scan_json(cid, snapshot_id=None, limit=0)
        assert initial.snapshot_id is None and initial.rows == [] and initial.schema == SCHEMA

        operation_a = str(uuid4())
        request_a = client.prepare_append(cid, ROWS, operation_id=operation_a, schema=SCHEMA, expected_snapshot=None)
        first = client.append(request_a)
        snapshot_a = first.result["snapshot_id"]
        assert first.outcome == "committed" and int(first.result["rows_appended"]) == 3
        assert first.result["payload_hash"] == request_a.payload_hash
        if server.mock:
            assert first.result["durable"] is False
        second = client.append(cid, [{"key": "later", "n": 7}], operation_id=uuid4(), schema=SCHEMA,
                               expected_snapshot=snapshot_a)
        snapshot_b = second.result["snapshot_id"]
        assert snapshot_b != snapshot_a
        # Original receipt is immutable even after the old NoSnapshot condition
        # no longer matches the current state.
        assert client.append(request_a).raw == first.raw
        assert client.find_append_outcome(cid, operation_a).raw == first.raw
        assert int(client.stats(cid)["rows"]) == 4
        assert int(client.stats(cid, snapshot_id=snapshot_a)["rows"]) == 3
        assert int(client.stats(cid, snapshot_id=None)["rows"]) == 0
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
        assert failed.outcome == "definitely_not_committed" and failed.error.code == ("SNAPSHOT_CONFLICT" if server.mock else "PRECONDITION_FAILED")
        old_failure = client.append(cid, [{"key": "not-published"}], operation_id=failure_op,
                                    schema=SCHEMA, expected_snapshot=snapshot_b)
        assert old_failure.raw == failed.raw and client.find_append_outcome(cid, failure_op).raw == failed.raw
        assert int(client.stats(cid)["rows"]) == 4
        assert client.find_append_outcome(cid, uuid4()) is None
    assert_storage(server, cid, 4)


def test_real_metadata_and_cursor_membership_stay_fixed_while_new_data_arrives(server):
    with Client(server.url, TOKEN) as client:
        collection_ids = [create(client, name=f"collection-{index}") for index in range(3)]
        cid = collection_ids[0]
        for index in range(3):
            client.append(cid, [{"key": f"k-{index}"}], operation_id=uuid4(), schema=SCHEMA)
        first_history = client.snapshots(cid, page_size=1)
        first_files = client.files(cid, page_size=1)
        original_view = (first_files.raw["identity"]["snapshot_id"] if server.mock
                         else first_files.raw["snapshot_id"])
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
            assert (page.raw["identity"]["snapshot_id"] if server.mock else page.raw["snapshot_id"]) == original_view
            files.extend(page.items)
            cursor = page.next_cursor
        assert len(history) == 3 and len(files) == 3
        assert int(client.stats(cid)["data_files"]) == 4
        details = client.file(cid, files[0]["file_id"], snapshot_id=original_view)
        assert details["synthetic"] is server.mock and details["row_groups"]
        assert (client.stats(cid)["compressed_data_bytes"] is None) is server.mock
        with pytest.raises(HTTPError) as foreign:
            client.files(collection_ids[1], page_size=1, cursor=first_files.next_cursor)
        assert foreign.value.detail.code == ("CURSOR_MISMATCH" if server.mock else "NOT_FOUND")
        plan = client.explain(cid, projection=["n"], predicate=col("key", "string").eq("k-0"), snapshot_id=original_view)
        assert plan["mock"] is server.mock and plan["actual_io"] is None
        assert (plan["identity"]["snapshot_id"] if server.mock else plan["snapshot_id"]) == original_view
    assert_storage(server, cid, 4)


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
        assert int(client.stats(cid)["rows"]) == 3
        with pytest.raises(HTTPError) as conflict:
            client.append(cid, [{"key": "different"}], operation_id=operation, schema=SCHEMA)
        assert conflict.value.detail.code == ("IDEMPOTENCY_CONFLICT" if server.mock else "OPERATION_CONFLICT") and int(client.stats(cid)["rows"]) == 3
    assert_storage(server, cid, 3)


def test_runnable_quickstart_uses_actual_http_and_reports_verified_stream(server):
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
        assert int(client.stats(collection_id)["rows"]) == 2
    assert_storage(server, collection_id, 2)


def test_parquet_write_settings_are_applied_or_refused_explicitly(server):
    # The native backend applies and reports Parquet write settings; the mock
    # does not implement them and must not look as if it applied them.
    settings = {"parquet": {"compression": {"codec": "zstd", "level": 3}}}
    with Client(server.url, TOKEN) as client:
        capabilities = client.capabilities()
        cid = str(uuid4())
        if not server.mock:
            assert capabilities["features"]["write_settings"] is True
            assert {item["codec"] for item in capabilities["parquet"]["codecs"]} >= {"uncompressed", "zstd"}
            created = client.create_collection("parquet_settings", SCHEMA, collection_id=cid,
                                               write_settings=settings)
            assert created.outcome == "committed" and created.info["write_settings_version"] == "1"
            assert created.info["write_settings"]["parquet"]["compression"] == {"codec": "zstd", "level": 3}
            assert client.resolve_create("parquet_settings", SCHEMA, collection_id=cid,
                                         write_settings=settings)["collection_id"] == cid
            updated = client.update_write_settings(cid, "1", {"parquet": {"compression": {"codec": "snappy"}}})
            assert updated["write_settings_version"] == "2"
            # A later update never confirms a lost create with other settings.
            with pytest.raises(ProtocolError):
                client.resolve_create("parquet_settings", SCHEMA, collection_id=cid,
                                      write_settings={"parquet": {"compression": {"codec": "gzip"}}})
            return
        assert capabilities.get("parquet") is None
        assert not capabilities.get("features", {}).get("write_settings")
        with pytest.raises(HTTPError) as refused:
            client.create_collection("parquet_settings", SCHEMA, collection_id=cid, write_settings=settings)
        assert refused.value.status_code == 422 and refused.value.collection_id == cid
        with pytest.raises(HTTPError) as absent:
            client.get_collection(cid)
        assert absent.value.status_code == 404
        create(client, name="parquet_settings", collection_id=cid)
        assert client.resolve_create("parquet_settings", SCHEMA, collection_id=cid)["collection_id"] == cid
        with pytest.raises(ProtocolError):
            client.resolve_create("parquet_settings", SCHEMA, collection_id=cid, write_settings=settings)
        with pytest.raises(HTTPError) as missing:
            client.update_write_settings(cid, "0", settings)
        assert missing.value.status_code == 404


def test_real_arrow_empty_schema_eos_and_fixed_view_survive_later_append(server):
    import pyarrow as pa
    transport = ArrowHTTPTransport()
    with Client(server.url, TOKEN, transport=transport) as client:
        cid = create(client)
        with client.scan_batches(cid, snapshot_id=None) as empty:
            assert empty.schema.names == ['key', 'n', 'at', 'flag', 'ratio']
            assert [field.type for field in empty.schema] == [
                pa.string(), pa.int64(), pa.timestamp('us', 'UTC'), pa.bool_(), pa.float64()]
            assert not empty.schema.field('key').nullable
            assert list(empty) == []
        assert empty.snapshot_id is None and empty.integrity == 'verified'
        assert empty.checksum_verified and empty.bytes_read > 8
        first = client.append(cid, ROWS, operation_id=uuid4(), schema=SCHEMA)
        snapshot = first.result['snapshot_id']
        stream = client.scan_batches(cid)
        try:
            client.append(cid, [{'key': 'after-open'}], operation_id=uuid4(), schema=SCHEMA)
            with stream:
                batches = list(stream)
        finally:
            stream.close()
        assert stream.snapshot_id == snapshot and stream.integrity == 'verified'
        assert stream.checksum_verified
        assert all(body.closed for body in transport.bodies)
    # Closing all readers, response bodies and the HTTP pool must not invalidate buffers.
    rows = [row for batch in batches for row in batch.to_pylist()]
    assert len(rows) == 3 and [row['key'] for row in rows] == ['duplicate'] * 3
    assert rows[0]['n'] == 9007199254740993
    assert rows[1]['at'] == datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert_storage(server, cid, 4)


@pytest.mark.parametrize('fault', [
    'checksum', 'length', 'missing_eos', 'truncated_body', 'trailing_data',
    'snapshot', 'missing_snapshot', 'schema',
])
def test_real_arrow_fault_never_reports_integrity_success_and_closes_owners(server, fault):
    transport = ArrowHTTPTransport()
    with Client(server.url, TOKEN, transport=transport) as client:
        cid = create(client)
        receipt = client.append(cid, ROWS, operation_id=uuid4(), schema=SCHEMA)
        transport.fault = fault
        stream = None
        retained = []
        expected = ProtocolError if fault in ('snapshot', 'missing_snapshot') else IntegrityError
        with pytest.raises(expected):
            stream = client.scan_batches(cid, snapshot_id=receipt.result['snapshot_id'])
            with stream:
                for batch in stream:
                    retained.append(batch)
        assert transport.bodies and all(body.closed for body in transport.bodies)
        if stream is not None:
            assert stream.closed and stream.integrity == 'not_complete' and not stream.checksum_verified
            with pytest.raises(expected):
                next(stream)  # decode failures stay failures, never successful EOS
        transport.fault = None
        with client.scan_batches(cid) as healthy:
            assert sum(batch.num_rows for batch in healthy) == 3
        assert healthy.integrity == 'verified'
    for batch in retained:
        assert all(row['key'] == 'duplicate' for row in batch.to_pylist())


@pytest.mark.parametrize('finish', ['early', 'consumer', 'client', 'close_error', 'client_error', 'view_error'])
def test_real_arrow_paged_ownership_closes_promptly_and_keeps_returned_batch(server, finish):
    if finish == 'view_error' and server.mock:
        # The legacy mock has no cursor pages; this path requires a real native session.
        pytest.skip('legacy mock has no cursor pages')
    transport = ArrowHTTPTransport()
    client = Client(server.url, TOKEN, transport=transport)
    try:
        cid = create(client)
        # Native pages hold at most 10,000 rows; this guarantees a continuation.
        client.append(cid, [{'key': 'same', 'n': 9007199254740993}] * 10000,
                      operation_id=uuid4(), schema=SCHEMA)
        client.append(cid, [{'key': 'last'}], operation_id=uuid4(), schema=SCHEMA)
        if finish in ('close_error', 'client_error'):
            transport.fault = 'close'
        stream = client.scan_batches(cid)
        retained = next(stream)
        sent = transport.bodies[-1].bytes_yielded
        if finish == 'view_error':
            transport.fault = 'snapshot_close'
            with pytest.raises(ProtocolError, match='view changed'):
                list(stream)
        elif finish == 'client_error':
            with pytest.raises(ValueError, match='consumer failed'):
                with client:
                    raise ValueError('consumer failed')
        elif finish in ('consumer', 'close_error'):
            with pytest.raises(ValueError, match='consumer failed'):
                with stream:
                    raise ValueError('consumer failed')
        elif finish == 'early':
            stream.close()
        else:
            client.close()
        assert stream.closed and stream.integrity == 'not_complete' and not stream.checksum_verified
        assert all(body.closed for body in transport.bodies)
        if finish == 'view_error':
            with pytest.raises(ProtocolError, match='view changed'):
                next(stream)
        else:
            assert transport.bodies[-1].bytes_yielded == sent
            assert list(stream) == []
        if not server.mock:
            assert len(transport.cancellations) == 1 and transport.cancellations[0][1] == 200
        else:
            assert transport.cancellations == []
    finally:
        client.close()
    assert retained.num_rows > 0
    retained_rows = retained.to_pylist()
    # Native file enumeration is not an insertion-order guarantee.
    assert all(row['key'] in ('same', 'last') for row in retained_rows)
    assert any(row['key'] == 'same' and row['n'] == 9007199254740993 for row in retained_rows)


def test_actual_https_accepts_explicit_trusted_test_ca(tmp_path, tls_material):
    with client_server(tmp_path, tls=tls_material) as server:
        with Client(server.url, TOKEN, verify=tls_material.trusted_context()) as client:
            assert client.capabilities()["backend"] == ("mock" if server.mock else "local")
            cid = create(client)
            assert client.append(cid, ROWS, operation_id=uuid4(), schema=SCHEMA).outcome == "committed"
            with client.scan_batches(cid) as stream:
                assert sum(batch.num_rows for batch in stream) == 3
            assert stream.integrity == "verified"


@pytest.mark.parametrize("trust", ["system", "wrong_ca"])
def test_actual_https_rejects_untrusted_or_wrong_ca_without_fabricating_write_failure(tmp_path, tls_material, trust):
    operation, cid = str(uuid4()), str(uuid4())
    with client_server(tmp_path, tls=tls_material) as server:
        verify = True if trust == "system" else tls_material.wrong_context()
        with Client(server.url, TOKEN, verify=verify, timeout=3) as client:
            with pytest.raises(TransportError) as failure:
                client.append(cid, ROWS, operation_id=operation, schema=SCHEMA)
        assert failure.value.operation_id == operation and failure.value.collection_id == cid
        assert TOKEN not in str(failure.value)
        assert isinstance(failure.value.__cause__, httpx.ConnectError)
        assert "CERTIFICATE_VERIFY_FAILED" in str(failure.value.__cause__)
