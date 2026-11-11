"""JSON replies: the SDK advertises only codings it inflates with a bound, decodes them as httpx did,
and reports a corrupt coding as an SDK transport error."""
import zlib

import httpx
import pytest

from glamdb_sdk import Client, Field, Schema, TransportError

COLLECTION = "00112233-4455-6677-8899-aabbccddeeff"


class Streamed(httpx.SyncByteStream):
    """A network-like body: httpx eagerly reads (and decodes) a bytes body, which would bypass read_json."""

    def __init__(self, body):
        self.body = body

    def __iter__(self):
        for start in range(0, len(self.body), 7):
            yield self.body[start:start + 7]


def _client(body, coding, seen=None):
    def handler(request):
        if seen is not None:
            seen.append(request.headers["accept-encoding"])
        return httpx.Response(200, stream=Streamed(body), headers={
            "content-type": "application/json", "content-encoding": coding})

    return Client("https://example.test", "token", transport=httpx.MockTransport(handler))


def _compress(data, wbits):
    compressor = zlib.compressobj(6, zlib.DEFLATED, wbits)
    return compressor.compress(data) + compressor.flush()


def test_json_requests_advertise_only_bounded_codings(monkeypatch):
    # httpx adds br/zstd to its default Accept-Encoding when brotli or zstandard is installed.
    monkeypatch.setattr(httpx._client, "ACCEPT_ENCODING", "gzip, deflate, br, zstd")
    seen = []
    with _client(b"{}", "identity", seen) as client:
        assert client.capabilities() == {}
    assert seen == ["gzip, deflate"]


@pytest.mark.parametrize("coding,wbits", [("gzip", 31), ("deflate", 15), ("deflate", -15)])
def test_each_advertised_coding_decodes_like_httpx(coding, wbits):
    body = _compress(b'{"api_version":"v0"}', wbits)
    assert httpx.Response(200, content=body, headers={"content-encoding": coding}).json() == {"api_version": "v0"}
    with _client(body, coding) as client:
        assert client.capabilities() == {"api_version": "v0"}


@pytest.mark.parametrize("coding", ["gzip", "deflate"])
def test_corrupt_json_coding_is_a_transport_error_with_request_identity(coding):
    # A complete header followed by a reserved deflate block type: zlib rejects it outright.
    corrupt = (b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03" if coding == "gzip" else b"\x78\x9c") + b"\xff" * 8
    with _client(corrupt, coding) as client:
        with pytest.raises(TransportError):
            client.capabilities()
        with pytest.raises(TransportError) as raised:
            client.create_collection("events", Schema([Field("key", "string", False)]), collection_id=COLLECTION)
    assert raised.value.collection_id == COLLECTION
