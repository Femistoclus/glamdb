import httpx
import pytest
from glamdb_sdk import Client, Field, Schema, HTTPError

CID = '11111111-2222-4333-8444-555555555555'
OP = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
SCHEMA = Schema([Field('key', 'string', False)])

@pytest.mark.parametrize('method', ['append', 'create_collection'])
@pytest.mark.parametrize('status', [500, 502, 503, 504])
def test_bare_server_write_failure_has_distinct_unknown_outcome_error(method, status):
    from glamdb_sdk import OutcomeUnknownError
    seen = []
    def handle(request):
        seen.append(request)
        return httpx.Response(status, json={'error': {'code': 'INTERNAL', 'message': 'response unavailable'}})
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(OutcomeUnknownError) as caught:
            if method == 'append':
                client.append(CID, [{'key': 'a'}], operation_id=OP, schema=SCHEMA)
            else:
                client.create_collection('events', SCHEMA, collection_id=CID)
        assert caught.value.collection_id == CID
        assert caught.value.status_code == status
        assert caught.value.detail.code == 'INTERNAL'
        assert caught.value.detail.retry_hint == 'SameIdAfterReconcile'
        if method == 'append': assert caught.value.operation_id == OP
    assert len(seen) == 1

@pytest.mark.parametrize('code', ['DATABASE_NOT_FOUND', 'ROUTE_NOT_FOUND', 'COLLECTION_NOT_FOUND', 'NOT_FOUND'])
def test_only_operation_not_found_is_authoritative_absence(code):
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(
        lambda request: httpx.Response(404, json={'error': {'code': code, 'message': 'missing'}}))) as client:
        with pytest.raises(HTTPError): client.find_append_outcome(CID, OP)

def test_non_json_gateway_failure_preserves_write_identity():
    from glamdb_sdk import OutcomeUnknownError
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(
        lambda request: httpx.Response(502, text='<html>gateway failure</html>'))) as client:
        with pytest.raises(OutcomeUnknownError) as caught:
            client.append(CID, [{'key': 'a'}], operation_id=OP, schema=SCHEMA)
        assert caught.value.collection_id == CID
        assert caught.value.operation_id == OP
