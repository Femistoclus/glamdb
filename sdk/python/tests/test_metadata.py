"""Metadata adapters keep selected views distinct from the current root head."""
import json
from pathlib import Path
import httpx
import pytest

from glamdb_sdk import Client, ProtocolError

CID = '00112233-4455-6677-8899-aabbccddeeff'
HEAD = '9223372036854775807'
SELECTED = '9007199254740993'


def response(snapshot=SELECTED):
    return {'identity': {'collection_id': CID, 'current_snapshot_id': HEAD},
            'snapshot_id': snapshot, 'mock': False, 'rows': SELECTED,
            'logical_bytes': None, 'extension': {'preserve': True}}


@pytest.mark.parametrize('method', ['stats', 'snapshots', 'files', 'file', 'explain'])
def test_metadata_preserves_decimal_values_unknowns_and_selected_identity(method):
    value = response()
    if method in ('snapshots', 'files'):
        value.update(items=[{'record_count': SELECTED}], next_cursor='opaque-token')
    with Client('https://example.test', 'token', transport=httpx.MockTransport(lambda _: httpx.Response(200, json=value))) as client:
        args = [CID, '0' * 64] if method == 'file' else [CID]
        result = getattr(client, method)(*args, snapshot_id=int(SELECTED))
        raw = result.raw if method in ('snapshots', 'files') else result
        assert raw == value
        assert raw['identity']['current_snapshot_id'] != raw['snapshot_id']


@pytest.mark.parametrize('method', ['stats', 'snapshots', 'files', 'file', 'explain'])
@pytest.mark.parametrize('change', ['collection', 'selected', 'numeric_snapshot', 'missing_selected'])
def test_native_metadata_rejects_wrong_collection_or_selected_view(method, change):
    value = response()
    if method in ('snapshots', 'files'):
        value.update(items=[], next_cursor=None)
    if change == 'collection':
        value['identity']['collection_id'] = '11111111-2222-4333-8444-555555555555'
    elif change == 'selected':
        value['snapshot_id'] = HEAD
    elif change == 'numeric_snapshot':
        value['snapshot_id'] = int(SELECTED)
    else:
        del value['snapshot_id']
    with Client('https://example.test', 'token', transport=httpx.MockTransport(lambda _: httpx.Response(200, json=value))) as client:
        args = [CID, '0' * 64] if method == 'file' else [CID]
        with pytest.raises(ProtocolError):
            getattr(client, method)(*args, snapshot_id=int(SELECTED))


def test_legacy_mock_identity_selector_and_initial_empty_are_still_supported():
    mock = {'mock': True, 'identity': {'collection_id': CID, 'snapshot_id': None}, 'rows': 0}
    with Client('https://example.test', 'token', transport=httpx.MockTransport(lambda _: httpx.Response(200, json=mock))) as client:
        assert client.stats(CID, snapshot_id=None) == mock


def test_mock_file_creation_snapshot_does_not_replace_its_selected_view():
    mock = {'mock': True, 'identity': {'collection_id': CID, 'snapshot_id': '2'}, 'snapshot_id': '1', 'file_id': 'opaque'}
    with Client('https://example.test', 'token', transport=httpx.MockTransport(lambda _: httpx.Response(200, json=mock))) as client:
        assert client.file(CID, 'opaque', snapshot_id='2') == mock


def test_shared_native_metadata_vectors_preserve_every_literal_field():
    root = Path(__file__).resolve().parents[3]
    fixture = json.loads((root / 'fixtures/http/metadata-v0.json').read_text())
    for case in fixture['cases']:
        value = case['response']
        with Client('https://example.test', 'token', transport=httpx.MockTransport(lambda _: httpx.Response(200, json=value))) as client:
            args = [fixture['collection_id']]
            if case['method'] == 'file':
                args.append(case['file_id'])
            result = getattr(client, case['method'])(*args, snapshot_id=fixture['selector'])
            assert (result.raw if case['method'] == 'files' else result) == value
