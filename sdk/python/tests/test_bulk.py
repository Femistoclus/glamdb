import json
import pytest
from glamdb_sdk import Client, Field, Schema, ValidationError

CID = '00112233-4455-6677-8899-aabbccddeeff'
OP = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
SCHEMA = Schema([Field('n', 'int64', True)])


def test_prepared_bulk_preserves_canonical_identity_across_chunk_boundaries():
    from glamdb_sdk import BulkAppendRequest
    rows = [{'n': '9007199254740993'}, {'n': None}, {'n': '9007199254740993'}]
    with BulkAppendRequest.prepare(CID, OP, SCHEMA, [rows[:1], rows[1:]], rows=3) as prepared:
        assert prepared.payload_hash == 'e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7'
        with prepared.open_body() as body:
            first = b''.join(body)
        with prepared.open_body() as body:
            assert b''.join(body) == first
        lines = [json.loads(line) for line in first.splitlines()]
        assert lines[0] == {'operation_id': OP, 'rows': '3'}
        assert [row for line in lines[1:] for row in line['rows']] == rows
        assert prepared.content_length == len(first)
    with pytest.raises(ValidationError, match='closed'):
        with prepared.open_body():
            pass


@pytest.mark.parametrize('batches,total', [([], 0), ([[{'n': 1}]], 2), ([[]], 1),
                                         ([[{'n': 1}] * 10001], 10001)])
def test_bulk_rejects_invalid_batches_before_sending(batches, total):
    with Client('http://127.0.0.1:1', 'unused') as client:
        with pytest.raises(ValidationError):
            client.prepare_bulk_append(CID, batches, rows=total, operation_id=OP, schema=SCHEMA)


def test_bulk_generator_is_spilled_with_bounded_python_memory():
    import tracemalloc
    from glamdb_sdk import BulkAppendRequest
    schema = Schema([Field('s', 'string', False)])
    tracemalloc.start()
    try:
        with BulkAppendRequest.prepare(CID, OP, schema,
                ([{'s': 'x' * 1024}] * 100 for _ in range(200)), rows=20000) as prepared:
            _, peak = tracemalloc.get_traced_memory()
            assert prepared.content_length > 16 * 1024**2
            assert peak < 8 * 1024**2
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize('point', ['seek', 'read', 'eof'])
def test_bulk_source_io_failure_retains_original_identity_and_retry_body(point):
    import httpx
    from glamdb_sdk import BulkAppendRequest, TransportError

    class FaultFile:
        def __init__(self, source):
            self.source, self.fail = source, True

        @property
        def closed(self):
            return self.source.closed

        def seek(self, offset):
            if self.fail and point == 'seek':
                self.fail = False
                raise OSError('injected private file path')
            return self.source.seek(offset)

        def read(self, count):
            data = self.source.read(count)
            if self.fail and (point == 'read' or (point == 'eof' and not data)):
                self.fail = False
                raise OSError('injected private file path')
            return data

        def close(self):
            self.source.close()

    with BulkAppendRequest.prepare(CID, OP, SCHEMA, [[{'n': 1}]], rows=1) as prepared:
        with prepared.open_body() as body:
            original = b''.join(body)
        object.__setattr__(prepared, '_file', FaultFile(prepared._file))
        with Client('https://example.test', 'secret',
                    transport=httpx.MockTransport(lambda request: httpx.Response(500))) as client:
            with pytest.raises(TransportError) as caught:
                client.bulk_append(prepared)
        assert caught.value.collection_id == CID
        assert caught.value.operation_id == OP
        assert 'private file path' not in str(caught.value)
        with prepared.open_body() as body:
            assert b''.join(body) == original


def test_bulk_schema_lookup_failure_retains_original_operation_identity():
    import httpx
    from glamdb_sdk import TransportError
    def fail(request):
        raise httpx.ConnectError('injected', request=request)
    with Client('https://example.test', 'unused', transport=httpx.MockTransport(fail)) as client:
        with pytest.raises(TransportError) as caught:
            client.prepare_bulk_append(CID, [[{'n': 1}]], rows=1, operation_id=OP)
    assert caught.value.collection_id == CID
    assert caught.value.operation_id == OP
