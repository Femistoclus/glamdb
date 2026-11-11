import json

import httpx
import pytest

from glamdb_sdk import Client, ProtocolError

CID = '11111111-2222-4333-8444-555555555555'
SCHEMA = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False, 'field_id': 1}]}


def page(key, cursor, snapshot='9'):
    return {'schema': SCHEMA, 'snapshot_id': snapshot, 'identity': {'fixed': True},
            'rows': [{'key': key}], 'next_cursor': cursor, 'profile': {'rows_returned': '1'}}


def test_scan_json_drains_pages_without_changing_snapshot():
    seen = []
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=page('duplicate', 'next' if len(seen) == 1 else None))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        result = client.scan_json(CID)
        assert result.rows == [{'key': 'duplicate'}] * 2
        assert result.profile['rows_returned'] == '2'
    assert seen[1] == {'cursor': 'next'}


def test_scan_pages_retries_lost_first_page_and_cancels_early():
    seen = []
    lost = True
    def handle(request):
        nonlocal lost
        seen.append((request.url.path, json.loads(request.content)))
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': SCHEMA, 'snapshot_id': '9', 'identity': {'fixed': True}})
        if request.url.path.endswith('/cancel'):
            return httpx.Response(200, json={'cancelled': True})
        if lost:
            lost = False
            raise httpx.ReadError('response lost', request=request)
        return httpx.Response(200, json=page('duplicate', 'second'))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        iterator = client.scan_pages(CID)
        assert next(iterator).rows == [{'key': 'duplicate'}]
        iterator.close()
    assert seen[1][1] == seen[2][1] == {'cursor': 'first'}
    assert seen[-1][0].endswith('/cancel')
    assert seen[-1][1] == {'cursor': 'second'}


def test_scan_json_rejects_view_change_between_pages():
    calls = 0
    def handle(request):
        nonlocal calls
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        calls += 1
        return httpx.Response(200, json=page('a', 'next' if calls == 1 else None, str(calls)))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProtocolError, match='view'):
            client.scan_json(CID)


def test_arrow_scan_follows_pages_and_validates_every_page():
    import hashlib
    import pyarrow as pa
    bodies = []
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        bodies.append(json.loads(request.content))
        sink = pa.BufferOutputStream()
        batch = pa.record_batch([pa.array(['same'])], schema=pa.schema([pa.field('key', pa.string(), nullable=False)]))
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        body = sink.getvalue().to_pybytes()
        headers = {'content-type': 'application/vnd.apache.arrow.stream',
                   'x-glamdb-snapshot-id': '9', 'x-content-sha256': hashlib.sha256(body).hexdigest()}
        if len(bodies) == 1:
            headers['x-glamdb-next-cursor'] = 'second'
        headers['content-length'] = str(len(body))
        return httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with client.scan_batches(CID) as scan:
            assert sum(batch.num_rows for batch in scan) == 2
            assert scan.integrity == 'verified'
    assert bodies[1] == {'cursor': 'second'}


def test_client_close_cancels_an_unfinished_json_page_iterator():
    seen = []
    def handle(request):
        seen.append(request.url.path)
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': SCHEMA, 'snapshot_id': '9', 'identity': {'fixed': True}})
        if request.url.path.endswith('/cancel'):
            return httpx.Response(200, json={'cancelled': True})
        return httpx.Response(200, json=page('a', 'next'))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        pages = client.scan_pages(CID)
        next(pages)
    assert seen[-1].endswith('/scan/cancel')


def test_abandoned_page_iterator_is_not_retained_by_client():
    import gc
    import weakref
    cancelled = []
    def handle(request):
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': SCHEMA, 'snapshot_id': '9', 'identity': {'fixed': True}})
        return httpx.Response(200, json=page('a', 'next'))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        iterator = client.scan_pages(CID)
        assert next(iterator).rows
        reference = weakref.ref(iterator)
        del iterator
        gc.collect()
        assert reference() is None
        assert cancelled == []
        client.capabilities()
        assert cancelled == ['next']
        assert not client._streams


def test_failed_page_iterator_never_turns_failure_into_successful_eos():
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': SCHEMA, 'snapshot_id': '9', 'identity': {'fixed': True}})
        if request.url.path.endswith('/cancel'):
            return httpx.Response(200, json={'cancelled': True})
        return httpx.Response(200, json=page('a', None, snapshot='wrong'))
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        iterator = client.scan_pages(CID)
        for _ in range(2):
            with pytest.raises(ProtocolError):
                next(iterator)
        assert iterator.integrity == 'not_complete'

@pytest.mark.parametrize('entrypoint', ['scan_json', 'scan_batches'])
def test_scan_entrypoints_open_before_reading_and_retry_same_first_cursor(entrypoint):
    seen = []
    lost = True
    def handle(request):
        nonlocal lost
        body = json.loads(request.content)
        seen.append((request.url.path, body))
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': SCHEMA, 'snapshot_id': '9', 'identity': {'fixed': True}})
        if request.url.path.endswith('/cancel'):
            return httpx.Response(200, json={'cancelled': True})
        if lost:
            lost = False
            raise httpx.ReadError('first response lost', request=request)
        if entrypoint == 'scan_json': return httpx.Response(200, json=page('a', None))
        import pyarrow as pa
        sink = pa.BufferOutputStream()
        schema = pa.schema([pa.field('key', pa.string(), nullable=False)])
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(pa.record_batch([pa.array(['a'])], schema=schema))
        return httpx.Response(200, stream=httpx.ByteStream(sink.getvalue().to_pybytes()), headers={
            'content-type': 'application/vnd.apache.arrow.stream', 'x-glamdb-snapshot-id': '9'})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        if entrypoint == 'scan_json': assert client.scan_json(CID).rows == [{'key': 'a'}]
        else:
            with client.scan_batches(CID) as batches: assert sum(b.num_rows for b in batches) == 1
    assert seen[0][0].endswith('/scan/open')
    assert seen[1][1] == seen[2][1] == {'cursor': 'first'}


def test_abandoned_arrow_iterator_releases_live_cursor():
    import gc
    import weakref
    import pyarrow as pa
    cancelled = []
    sink = pa.BufferOutputStream()
    schema = pa.schema([pa.field('key', pa.string(), nullable=False)])
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(pa.record_batch([pa.array(['a'])], schema=schema))
    def handle(request):
        if request.url.path.endswith('/capabilities'): return httpx.Response(200, json={})
        if request.url.path.endswith('/open'): return httpx.Response(200, json={'cursor': 'first'})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        return httpx.Response(200, stream=httpx.ByteStream(sink.getvalue().to_pybytes()), headers={
            'content-type': 'application/vnd.apache.arrow.stream', 'x-glamdb-snapshot-id': '9', 'x-glamdb-next-cursor': 'next'})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        batches = client.scan_batches(CID)
        assert next(batches).num_rows == 1
        reference = weakref.ref(batches)
        del batches
        gc.collect()
        assert reference() is None
        assert cancelled == []
        client.capabilities()
        assert cancelled == ['next']
