from datetime import datetime, timezone
import hashlib

import httpx
import pyarrow as pa
import pytest

from glamdb_sdk import Client, DeadlineExceeded, IntegrityError, ProtocolError, TransportError

CID = "11111111-2222-4333-8444-555555555555"
MIME = "application/vnd.apache.arrow.stream"


def ipc_bytes(batches=None, schema=None):
    if batches is None:
        schema = pa.schema([pa.field("n", pa.int64(), nullable=False)])
        batches = [pa.record_batch([pa.array([9007199254740993, 9007199254740993])], schema=schema),
                   pa.record_batch([pa.array([3])], schema=schema)]
    elif schema is None:
        schema = batches[0].schema
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class TrackedBody(httpx.SyncByteStream):
    def __init__(self, body, chunk_size=97, fail_after=None):
        self.body = body
        self.chunk_size = chunk_size
        self.closed = False
        self.sent = 0
        self.fail_after = fail_after

    def __iter__(self):
        for index in range(0, len(self.body), self.chunk_size):
            if self.fail_after is not None and self.sent >= self.fail_after:
                raise httpx.ReadTimeout("read stalled")
            chunk = self.body[index:index+self.chunk_size]
            self.sent += len(chunk)
            yield chunk

    def close(self):
        self.closed = True


def connection(body=None, *, headers=None, tracked=None, **kwargs):
    body = ipc_bytes() if body is None else body
    tracked = TrackedBody(body) if tracked is None else tracked
    response_headers = {"content-type": MIME, "content-length": str(len(body)),
                        "X-GlamDB-Snapshot-Id": "9007199254740993", "X-Content-SHA256": hashlib.sha256(body).hexdigest()}
    if headers:
        for key, value in headers.items():
            if value is None:
                response_headers.pop(key, None)
            else:
                response_headers[key] = value
    def handler(request):
        if request.url.path.endswith('/scan/open'):
            return httpx.Response(404, json={'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy adapter'}})
        assert request.headers["accept"] == MIME
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(200, headers=response_headers, stream=tracked)
    return Client("https://example.test", "secret", transport=httpx.MockTransport(handler), **kwargs), tracked


def test_full_real_arrow_consumption_verifies_eos_length_sha256_and_closes():
    client, body = connection()
    with client:
        with client.scan_batches(CID) as stream:
            assert stream.snapshot_id == "9007199254740993"
            assert stream.integrity == "not_complete" and not stream.checksum_verified
            batches = list(stream)
            assert [batch.column(0).to_pylist() for batch in batches] == [[9007199254740993, 9007199254740993], [3]]
            assert stream.integrity == "verified" and stream.checksum_verified
            assert stream.bytes_read == len(body.body)
            assert stream.closed
    assert body.closed
    # Returned batches own their buffers even after reader/response/client close.
    assert batches[0].column(0)[0].as_py() == 9007199254740993


def test_early_break_closes_on_with_exit_without_claiming_complete_integrity():
    batch = pa.record_batch([pa.array(list(range(20000)))], names=["n"])
    raw = ipc_bytes([batch] * 5)
    client, body = connection(raw, tracked=TrackedBody(raw, chunk_size=4096))
    with client:
        with client.scan_batches(CID) as stream:
            iterator = iter(stream)
            for result in iterator:
                assert result.num_rows == 20000
                break
            assert not stream.closed
            assert body.sent < len(raw)  # the SDK has not materialized the complete response
        assert stream.closed and body.closed
        assert stream.integrity == "not_complete" and not stream.checksum_verified


def test_explicit_close_and_client_close_release_retained_iterators():
    client, body = connection()
    stream = client.scan_batches(CID)
    iterator = iter(stream)
    next(iterator)
    client.close()
    assert stream.closed and body.closed and stream.integrity == "not_complete"
    stream.close()
    assert list(iterator) == []


def test_consumer_exception_closes_reader_and_http():
    client, body = connection()
    with client:
        with pytest.raises(RuntimeError, match="consumer"):
            with client.scan_batches(CID) as stream:
                next(stream)
                raise RuntimeError("consumer")
    assert body.closed and stream.closed and stream.integrity == "not_complete"


@pytest.mark.parametrize("headers,integrity,checksum", [
    ({"X-Content-SHA256": None}, "complete", False),
    ({"content-length": None}, "complete", True),
    ({"X-Content-SHA256": None, "content-length": None}, "complete", False),
])
def test_missing_integrity_metadata_never_claims_all_checks_verified(headers, integrity, checksum):
    client, body = connection(headers=headers)
    with client, client.scan_batches(CID) as stream:
        list(stream)
        assert stream.integrity == integrity and stream.checksum_verified is checksum
    assert body.closed


@pytest.mark.parametrize("mutation", ["sha", "length", "missing_eos", "truncated_body", "trailing_data", "fake_eos_tail"])
def test_complete_corrupt_truncated_or_extra_stream_is_rejected_and_closed(mutation):
    raw = ipc_bytes()
    headers = {}
    if mutation == "sha":
        headers["X-Content-SHA256"] = "f" * 64
    elif mutation == "length":
        headers["content-length"] = str(len(raw) + 1)
    elif mutation == "missing_eos":
        raw = raw[:-8]
    elif mutation == "truncated_body":
        raw = raw[:-13]
    elif mutation == "trailing_data":
        raw += b"trailing"
    elif mutation == "fake_eos_tail":
        # An int64 payload can equal the modern EOS bytes. Those bytes are not
        # an EOS marker when the Arrow decoder consumed them as a batch body.
        raw = ipc_bytes([pa.record_batch([pa.array([4294967295])], names=["n"])])[:-8]
    client, body = connection(raw, headers=headers)
    with client:
        with pytest.raises(IntegrityError):
            with client.scan_batches(CID) as stream:
                list(stream)
    assert body.closed and stream.closed and stream.integrity == "not_complete"


@pytest.mark.parametrize("headers", [
    {"X-GlamDB-Snapshot-Id": None}, {"X-GlamDB-Snapshot-Id": "1.5"},
    {"content-length": "-1"}, {"X-Content-SHA256": "invalid"},
    {"content-type": "application/json"}, {"content-encoding": "gzip"},
])
def test_invalid_stream_headers_are_protocol_errors_and_close_http(headers):
    client, body = connection(headers=headers)
    with client:
        with pytest.raises(ProtocolError):
            client.scan_batches(CID)
    assert body.closed


def test_bad_arrow_schema_closes_transport_even_before_context_entry():
    client, body = connection(b"not valid Arrow")
    with client:
        with pytest.raises(IntegrityError):
            client.scan_batches(CID)
    assert body.closed


@pytest.mark.parametrize("snapshot", [None, "1"])
def test_requested_snapshot_must_match_arrow_header_before_returning_batches(snapshot):
    client, body = connection()
    with client:
        with pytest.raises(ProtocolError, match="snapshot"):
            client.scan_batches(CID, snapshot_id=snapshot)
        assert body.closed
        assert not client._streams


def test_initial_empty_stream_retains_schema_and_has_no_fabricated_batch():
    schema = pa.schema([("n", pa.int64())])
    client, body = connection(ipc_bytes([], schema), headers={"X-GlamDB-Snapshot-Id": "initial"})
    with client, client.scan_batches(CID, snapshot_id=None, limit=0) as stream:
        assert stream.snapshot_id is None and stream.schema == schema
        assert list(stream) == []
        assert stream.integrity == "verified"
    assert body.closed


def test_stream_validates_timestamp_years_without_float_or_datetime_overflow():
    schema = pa.schema([("t", pa.timestamp("us", "UTC")), ("n", pa.int64())])
    batch = pa.record_batch([pa.array([-62135596800000000, 253402300799999999, None], type=pa.timestamp("us", "UTC")),
                             pa.array([9007199254740993, None, 0])], schema=schema)
    client, body = connection(ipc_bytes([batch]))
    with client, client.scan_batches(CID) as stream:
        rows = next(stream).to_pylist()
        assert rows[0] == {"t": datetime(1, 1, 1, tzinfo=timezone.utc), "n": 9007199254740993}
        assert rows[1]["t"] == datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
        list(stream)
    assert body.closed


@pytest.mark.parametrize("arrow_type,values", [
    (pa.timestamp("us", "UTC"), [253402300800000000]),
    (pa.timestamp("us", "UTC"), [-62135596800000001]),
    (pa.float64(), [float("nan")]), (pa.float64(), [float("inf")]),
    (pa.timestamp("ns", "UTC"), [0]), (pa.timestamp("us"), [0]), (pa.int32(), [1]),
])
def test_arrow_type_and_scalar_contract_violations_close_stream(arrow_type, values):
    batch = pa.record_batch([pa.array(values, type=arrow_type)], names=["value"])
    client, body = connection(ipc_bytes([batch]))
    with client:
        with pytest.raises(ProtocolError):
            with client.scan_batches(CID) as stream:
                list(stream)
    assert body.closed


def test_network_timeout_during_arrow_read_is_separate_from_decode_failure():
    raw = ipc_bytes([pa.record_batch([pa.array(list(range(30000)))], names=["n"])])
    body = TrackedBody(raw, chunk_size=4096, fail_after=65536)
    client, body = connection(raw, tracked=body)
    with client:
        with pytest.raises(DeadlineExceeded):
            with client.scan_batches(CID) as stream:
                list(stream)
    assert body.closed and stream.closed and stream.integrity == "not_complete"


def test_total_caller_deadline_is_checked_between_stream_chunks(monkeypatch):
    import glamdb_sdk.transport as module
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    raw = ipc_bytes()
    class SlowBody(TrackedBody):
        def __iter__(self):
            yield raw[:128]
            clock[0] = 2.0
            yield raw[128:]
    client, body = connection(raw, tracked=SlowBody(raw))
    with client:
        with pytest.raises(DeadlineExceeded):
            with client.scan_batches(CID, deadline=1) as stream:
                list(stream)
    assert body.closed


def test_arrow_read_allocation_bound_rejects_large_batch_before_allocating_it():
    raw = ipc_bytes([pa.record_batch([pa.array(list(range(30000)))], names=["n"])])
    client, body = connection(raw, max_arrow_read_bytes=1024)
    with client:
        with pytest.raises(IntegrityError, match="bound"):
            with client.scan_batches(CID) as stream:
                list(stream)
    assert body.closed


def test_client_close_attempts_every_stream_even_if_one_close_fails():
    raw = ipc_bytes()
    bodies = []
    class CloseFailure(TrackedBody):
        def close(self):
            self.closed = True
            raise RuntimeError("close failed")
    def handler(request):
        if request.url.path.endswith('/scan/open'):
            return httpx.Response(404, json={'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy adapter'}})
        body = CloseFailure(raw)
        bodies.append(body)
        return httpx.Response(200, headers={"content-type": MIME, "X-GlamDB-Snapshot-Id": "initial"}, stream=body)
    client = Client("https://example.test", "secret", transport=httpx.MockTransport(handler))
    streams = [client.scan_batches(CID), client.scan_batches(CID)]
    with pytest.raises(RuntimeError, match="close failed"):
        client.close()
    assert all(body.closed for body in bodies)
    assert all(stream.closed for stream in streams)


def test_token_provider_preserves_prefix_and_real_arrow_stream_ownership():
    raw = ipc_bytes()
    seen, provider_calls = [], []
    body = TrackedBody(raw)
    def provider():
        provider_calls.append(1)
        return "fresh-token"
    def handler(request):
        if request.url.path.endswith("/scan/open"):
            assert request.url.path == f"/databases/{CID}/collections/{CID}/scan/open"
            return httpx.Response(404, json={"error": {"code": "ROUTE_NOT_FOUND", "message": "legacy adapter"}})
        seen.append(request)
        assert request.headers["authorization"] == "Bearer fresh-token"
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(200, headers={"content-type": MIME, "content-length": str(len(raw)),
                                           "X-GlamDB-Snapshot-Id": "7",
                                           "X-Content-SHA256": hashlib.sha256(raw).hexdigest()}, stream=body)
    with Client("https://example.test/databases/" + CID, token_provider=provider,
                transport=httpx.MockTransport(handler)) as client:
        with client.scan_batches(CID) as stream:
            batches = list(stream)
            assert stream.integrity == "verified"
    assert len(seen) == 1 and len(provider_calls) == 2
    assert seen[0].url.path == f"/databases/{CID}/collections/{CID}/scan"
    assert seen[0].method == "POST" and body.closed
    assert batches[0].column(0)[0].as_py() == 9007199254740993
