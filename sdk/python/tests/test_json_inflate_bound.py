"""A compressed JSON reply is inflated with an output bound near max_json_bytes, not per whole network read."""
import tracemalloc
import zlib

import httpx
import pytest

from glamdb_sdk import Client, ProtocolError

LIMIT = 1024 * 1024


def _gzip_bomb(decoded_mib):
    compressor = zlib.compressobj(9, zlib.DEFLATED, 31)
    parts = [compressor.compress(b'{"x":"')]
    zeros = b"0" * (1 << 20)
    parts += [compressor.compress(zeros) for _ in range(decoded_mib)]
    parts.append(compressor.flush())
    return b"".join(parts)


def test_gzip_json_reply_is_bounded_close_to_max_json_bytes():
    body = _gzip_bomb(256)   # ~256 KiB on the wire, 256 MiB decoded

    class SocketLikeStream(httpx.SyncByteStream):   # httpcore delivers at most 64 KiB per read
        def __iter__(self):
            for start in range(0, len(body), 16384):
                yield body[start:start + 16384]

    def handler(request):
        return httpx.Response(200, stream=SocketLikeStream(), headers={
            "content-type": "application/json", "content-encoding": "gzip", "content-length": str(len(body))})

    with Client("https://example.test", "token", transport=httpx.MockTransport(handler), max_json_bytes=LIMIT) as client:
        tracemalloc.start()
        try:
            with pytest.raises(ProtocolError):
                client.capabilities()
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
    assert peak <= 4 * LIMIT, f"Python-heap peak {peak / 2**20:.1f} MiB for max_json_bytes={LIMIT / 2**20:.0f} MiB"
