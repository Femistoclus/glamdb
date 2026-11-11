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


@pytest.mark.parametrize('kind', ['scan_pages', 'scan_batches', 'legacy_arrow'])
@pytest.mark.parametrize('drain', ['call', 'close'])
def test_gc_under_transport_lock_defers_all_transport_cleanup(kind, drain):
    import gc
    import threading
    import weakref
    lock = threading.Lock()
    events = []
    class LockedStream(httpx.ByteStream):
        def close(self):
            if not lock.acquire(blocking=False):
                events.append('response-close-under-lock')
                raise AssertionError('response close attempted under transport lock')
            try:
                events.append('response-close')
            finally:
                lock.release()
    schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}
    def handle(request):
        if not lock.acquire(blocking=False):
            events.append('request-under-lock')
            raise AssertionError('request attempted under transport lock')
        lock.release()
        if request.url.path.endswith('/capabilities'):
            return httpx.Response(200, json={})
        if request.url.path.endswith('/cancel'):
            events.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={'cancelled': True})
        if request.url.path.endswith('/open'):
            if kind == 'legacy_arrow':
                return httpx.Response(404, json={'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy'}})
            return httpx.Response(200, json={'cursor': 'first', 'schema': schema, 'snapshot_id': '1', 'identity': {}})
        if kind == 'scan_pages':
            return httpx.Response(200, json={'rows': [{'key': 'a'}], 'schema': schema, 'snapshot_id': '1',
                                           'identity': {}, 'profile': {}, 'next_cursor': 'second'})
        response = arrow_response('second' if kind == 'scan_batches' else None)
        response.stream = LockedStream(b''.join(response.stream))
        return response
    client = Client('http://example.test', 'secret', transport=httpx.MockTransport(handle))
    scan = client.scan_pages(CID) if kind == 'scan_pages' else client.scan_batches(CID)
    next(scan)
    ref = weakref.ref(scan)
    lock.acquire()
    try:
        del scan
        gc.collect()
        assert ref() is None
        assert events == []
    finally:
        lock.release()
    if drain == 'call':
        client.capabilities()
    client.close()
    assert events == (['second'] if kind == 'scan_pages' else
                      ['response-close', 'second'] if kind == 'scan_batches' else ['response-close'])

@pytest.mark.parametrize('paged', [False, True])
def test_gc_returns_while_real_httpcore_pool_lock_is_held(paged):
    import gc
    import threading
    import httpcore
    from httpcore._sync.connection_pool import PoolByteStream
    from httpx._transports.default import ResponseStream

    pool = httpcore.ConnectionPool()
    pool_request = object()
    pool._requests.append(pool_request)
    response = arrow_response('second' if paged else None)
    response.stream = ResponseStream(PoolByteStream(iter(response.stream), pool_request, pool))
    cancelled = []
    def handle(request):
        if request.url.path.endswith('/open'):
            return (httpx.Response(200, json={'cursor': 'first'}) if paged else
                    httpx.Response(404, json={'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy'}}))
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={})
        if request.url.path.endswith('/capabilities'):
            return httpx.Response(200, json={})
        return response
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        owners = [client.scan_batches(CID)]
        next(owners[0])
        done = threading.Event()
        def collect():
            owners.clear()
            gc.collect()
            done.set()
        worker = threading.Thread(target=collect)
        try:
            with pool._optional_thread_lock:
                worker.start()
                assert done.wait(1), 'GC blocked trying to reacquire the HTTPcore pool lock'
                assert not response.is_closed
                assert cancelled == []
        finally:
            worker.join(3)
        client.capabilities()
        assert response.is_closed
        assert pool._requests == []
    assert cancelled == (['second'] if paged else [])


@pytest.mark.parametrize('kind', ['scan_pages', 'scan_batches', 'legacy_arrow'])
def test_close_drains_gc_cleanup_queued_after_its_initial_drain(kind):
    import gc
    import inspect
    import sys
    import threading
    import weakref
    cancelled, responses = [], []
    schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}
    def handle(request):
        if request.url.path.endswith('/open'):
            if kind == 'legacy_arrow':
                return httpx.Response(404, json={'error': {'code': 'ROUTE_NOT_FOUND', 'message': 'legacy'}})
            return httpx.Response(200, json={'cursor': 'first', 'schema': schema, 'snapshot_id': '1', 'identity': {}})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={})
        if kind != 'scan_pages':
            response = arrow_response('second' if kind == 'scan_batches' else None)
            responses.append(response)
            return response
        return httpx.Response(200, json={'rows': [{'key': 'a'}], 'schema': schema, 'snapshot_id': '1',
                                       'identity': {}, 'profile': {}, 'next_cursor': 'second'})
    client = Client('http://example.test', 'secret', transport=httpx.MockTransport(handle))
    owners = [client.scan_pages(CID) if kind == 'scan_pages' else client.scan_batches(CID)]
    next(owners[0])
    reference = weakref.ref(owners[0])
    ready, dropped = threading.Event(), threading.Event()
    def collect():
        assert ready.wait(3)
        owners.clear()
        gc.collect()
        dropped.set()
    worker = threading.Thread(target=collect)
    lines, start = inspect.getsourcelines(Client.close)
    barrier = start + next(i for i, line in enumerate(lines) if 'first_error = None' in line)
    def trace(frame, event, arg):
        if frame.f_code is Client.close.__code__ and event == 'line' and frame.f_lineno == barrier:
            ready.set()
            assert dropped.wait(3)
        return trace
    worker.start()
    previous = sys.gettrace()
    try:
        sys.settrace(trace)
        client.close()
    finally:
        sys.settrace(previous)
        ready.set()
        worker.join(3)
    assert reference() is None
    expected = [] if kind == 'legacy_arrow' else ['second']
    assert cancelled == expected
    assert all(response.is_closed for response in responses)
    assert not client._cleanup._pending
    assert not client._cleanup._registered
    client.close()
    assert cancelled == expected


def test_concurrent_cleanup_drains_serialize_and_allow_recursive_drain():
    import threading
    from glamdb_sdk.cleanup import CleanupQueue
    queue = CleanupQueue()
    entered, release, second_done = threading.Event(), threading.Event(), threading.Event()
    events, errors = [], []
    def first():
        events.append('first')
        queue.drain()
        entered.set()
        assert release.wait(3)
    class Owner:
        pass
    first_owner, second_owner = Owner(), Owner()
    queue.register(first_owner, first)
    queue.register(second_owner, lambda: events.append('second'))
    del first_owner, second_owner
    def drain(done=None):
        try:
            queue.drain()
        except BaseException as exc:
            errors.append(exc)
        finally:
            if done is not None:
                done.set()
    one = threading.Thread(target=drain)
    two = threading.Thread(target=drain, args=(second_done,))
    one.start()
    try:
        assert entered.wait(3)
        two.start()
        assert not second_done.wait(0.1), 'a concurrent drain returned while cleanup was still active'
    finally:
        release.set()
        one.join(3)
        if two.ident is not None:
            two.join(3)
    assert not one.is_alive() and not two.is_alive()
    assert errors == []
    assert events == ['first', 'second']

@pytest.mark.parametrize('distance', [1, 2, 3])
def test_close_releases_cyclic_scan_during_automatic_gc(distance):
    import gc
    import weakref
    cancelled = []
    schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}
    def handle(request):
        if request.url.path.endswith('/open'):
            return httpx.Response(200, json={'cursor': 'first', 'schema': schema, 'snapshot_id': '1', 'identity': {}})
        if request.url.path.endswith('/cancel'):
            cancelled.append(json.loads(request.content)['cursor'])
            return httpx.Response(200, json={})
        return httpx.Response(200, json={'rows': [{'key': 'a'}], 'schema': schema, 'snapshot_id': '1',
                                       'identity': {}, 'profile': {}, 'next_cursor': 'second'})
    previous_threshold, was_enabled = gc.get_threshold(), gc.isenabled()
    gc.collect()
    gc.disable()
    client = Client('http://example.test', 'secret', transport=httpx.MockTransport(handle))
    try:
        scan = client.scan_pages(CID)
        next(scan)
        scan.cycle = scan
        reference = weakref.ref(scan)
        del scan
        gc.set_threshold(gc.get_count()[0] + distance, 100000, 100000)
        gc.enable()
        client.close()
        gc.collect()
        assert reference() is None
        assert cancelled == ['second']
        assert not client._cleanup._registered
        assert all(ticket.cleanup is None for ticket in client._cleanup._pending)
    finally:
        client.close()
        gc.set_threshold(*previous_threshold)
        if was_enabled:
            gc.enable()
        else:
            gc.disable()
