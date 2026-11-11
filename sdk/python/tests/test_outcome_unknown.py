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


@pytest.mark.parametrize('method', ['append', 'create_collection'])
@pytest.mark.parametrize('status,code', [(503, 'SERVER_BUSY'), (408, 'REQUEST_TIMEOUT')])
def test_explicit_pre_execution_refusal_is_http_error_without_retry(method, status, code):
    seen = []
    body = {'error': {'code': code, 'message': 'not executed', 'retry_hint': 'SameIdAfterReconcile'},
            'execution_started': False}
    def handle(request):
        seen.append(request)
        return httpx.Response(status, json=body)
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(HTTPError) as caught:
            if method == 'append':
                client.append(CID, [{'key': 'a'}], operation_id=OP, schema=SCHEMA)
            else:
                client.create_collection('events', SCHEMA, collection_id=CID)
        assert type(caught.value) is HTTPError
        assert caught.value.raw == body
    assert len(seen) == 1

@pytest.mark.parametrize('status,code', [(503, 'SERVER_BUSY'), (408, 'REQUEST_TIMEOUT')])
@pytest.mark.parametrize('extra', [
    {}, {'execution_started': 0}, {'execution_started': None}, {'execution_started': True},
    {'execution_started': False, 'recovery_required': True},
    {'execution_started': False, 'result': {}},
    {'execution_started': False, 'phase': 'catalog_commit'},
    {'execution_started': False, 'error': {'code': 'SERVER_BUSY', 'message': 'busy', 'retry_hint': 1}},
    {'execution_started': False, 'error': {'code': 'SERVER_BUSY', 'message': 'busy'}},
    {'execution_started': False, 'error': {'code': 'INTERNAL', 'message': 'busy', 'retry_hint': 'SameIdAfterReconcile'}},
])
def test_malformed_pre_execution_refusal_stays_unknown(extra, status, code):
    from glamdb_sdk import OutcomeUnknownError
    body = {'error': {'code': code, 'message': 'busy', 'retry_hint': 'SameIdAfterReconcile'}} | extra
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(
            lambda request: httpx.Response(status, json=body))) as client:
        with pytest.raises(OutcomeUnknownError):
            client.append(CID, [{'key': 'a'}], operation_id=OP, schema=SCHEMA)

@pytest.mark.parametrize('method', ['append', 'create_collection'])
@pytest.mark.parametrize('status', [408, 503])
@pytest.mark.parametrize('outcome', [None, 'garbage', '', False, 0, [], {}])
def test_junk_outcome_never_makes_a_write_failure_definite(method, status, outcome):
    from glamdb_sdk import OutcomeUnknownError
    body = {'outcome': outcome, 'error': {'code': 'SERVER_BUSY', 'message': 'failure', 'retry_hint': 'FixInput'}}
    seen = []
    def handle(request):
        seen.append(request)
        return httpx.Response(status, json=body)
    with Client('http://example.test', 'secret', transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(OutcomeUnknownError) as caught:
            if method == 'append':
                client.append(CID, [{'key': 'a'}], operation_id=OP, schema=SCHEMA)
            else:
                client.create_collection('events', SCHEMA, collection_id=CID)
        assert caught.value.raw == body
        assert caught.value.detail.retry_hint == 'SameIdAfterReconcile'
        assert caught.value.collection_id == CID
        if method == 'append':
            assert caught.value.operation_id == OP
    assert len(seen) == 1
