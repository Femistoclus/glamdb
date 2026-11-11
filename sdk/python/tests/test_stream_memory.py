"""Decoding one Arrow IPC message keeps a single Python copy of its body (read_buffer is zero-copy)."""
import hashlib
import tracemalloc

import httpx
import pyarrow as pa

from glamdb_sdk import Client

CID = "11111111-2222-4333-8444-555555555555"


def test_single_large_batch_peak_python_heap_is_about_one_body():
    values = pa.array([b"x" * 1024] * (16 * 1024), type=pa.string())   # ~16 MiB body
    schema = pa.schema([pa.field("s", pa.string(), nullable=False)])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(pa.record_batch([values], schema=schema))
    data = sink.getvalue().to_pybytes()
    del values, sink

    class Chunked(httpx.SyncByteStream):
        def __iter__(self):
            for start in range(0, len(data), 65536):
                yield data[start:start + 65536]

    def handler(request):
        return httpx.Response(200, stream=Chunked(), headers={
            "content-type": "application/vnd.apache.arrow.stream", "x-glamdb-snapshot-id": "1",
            "content-length": str(len(data)), "x-content-sha256": hashlib.sha256(data).hexdigest()})

    with Client("https://example.test", "token", transport=httpx.MockTransport(handler)) as client:
        tracemalloc.start()
        try:
            base = tracemalloc.get_traced_memory()[0]
            with client.scan_batches(CID) as stream:
                batches = list(stream)
            peak = tracemalloc.get_traced_memory()[1] - base
        finally:
            tracemalloc.stop()
    assert stream.integrity == "verified" and sum(batch.num_rows for batch in batches) == 16 * 1024
    assert peak <= 1.25 * len(data), f"peak {peak / 2**20:.1f} MiB for a {len(data) / 2**20:.1f} MiB body"
