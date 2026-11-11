"""Scan ownership must survive failure before the public iterator is returned."""
import json

import httpx
import pyarrow as pa
import pytest

from glamdb_sdk import Client, IntegrityError, ProtocolError

CID = '11111111-2222-4333-8444-555555555555'


def arrow_response(cursor, valid=True):
    sink = pa.BufferOutputStream()
    schema = pa.schema([pa.field('key', pa.string(), nullable=False)])
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(pa.record_batch([pa.array(['a'])], schema=schema))
    data = sink.getvalue().to_pybytes() if valid else b'not arrow'
    headers = {'content-type': 'application/vnd.apache.arrow.stream',
               'x-glamdb-snapshot-id': '1', 'content-length': str(len(data))}
    if cursor:
        headers['x-glamdb-next-cursor'] = cursor
    return httpx.Response(200, stream=httpx.ByteStream(data), headers=headers)


@pytest.mark.parametrize('cancel_status', [200, 500])
def test_initial_arrow_constructor_failure_releases_advertised_cursor(cancel_status):
    cancelled = []
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(cancel_status, json={'error': {'code': 'INTERNAL', 'message': 'cleanup failed'}})
        return arrow_response('next')
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle), max_arrow_read_bytes=1) as client:
        with pytest.raises(IntegrityError, match='allocation bound'):
            client.scan_batches(CID)
    assert cancelled == ['next']


def test_initial_json_decode_failure_releases_advertised_cursor():
    cancelled = []
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        return httpx.Response(200, json={'next_cursor': 'next', 'schema': {'fields': []},
            'snapshot_id': '1', 'rows': 'malformed', 'profile': {}, 'identity': {}})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProtocolError, match='invalid scan JSON'):
            client.scan_json(CID)
    assert cancelled == ['next']


def test_following_arrow_constructor_failure_cancels_consumed_replay_token():
    cancelled = []
    count = 0
    def handle(request):
        nonlocal count
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first'})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        count += 1
        return arrow_response('second' if count == 1 else 'third', valid=count == 1)
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with client.scan_batches(CID) as scan:
            assert next(scan).num_rows == 1
            with pytest.raises(IntegrityError):
                next(scan)
    # The just-consumed token remains valid as a replay token even though the
    # response advertises a new token. It can still cancel the entire session.
    assert cancelled == ['second']


@pytest.mark.parametrize('entrypoint', ['scan_json', 'scan_pages'])
def test_later_json_failure_cancels_replay_token(entrypoint):
    cancelled = []
    count = 0
    schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}
    def handle(request):
        nonlocal count
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'snapshot_id': '1', 'schema': schema, 'identity': {}})
        count += 1
        return httpx.Response(200, json={'next_cursor': 'second' if count == 1 else 'third', 'schema': schema,
            'snapshot_id': '1', 'rows': [{'key': 'a'}] if count == 1 else 'malformed', 'profile': {}, 'identity': {}})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProtocolError, match='invalid scan JSON'):
            if entrypoint == 'scan_json':
                client.scan_json(CID)
            else:
                with client.scan_pages(CID) as pages:
                    assert next(pages).rows == [{'key': 'a'}]
                    next(pages)
    assert cancelled == ['second']


def test_invalid_open_metadata_releases_cursor_before_iteration():
    cancelled = []
    def handle(request):
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        return httpx.Response(200, json={'cursor': 'first', 'schema': 'malformed'})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ProtocolError):
            next(client.scan_pages(CID))
    assert cancelled == ['first']
