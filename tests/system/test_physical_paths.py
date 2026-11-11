from pathlib import Path

import pytest

from tools.acceptance.physical import resolve_store_uri


STORE = '00000000-0000-4000-8000-000000000001'


@pytest.fixture
def store(tmp_path):
    root = (tmp_path / 'store').resolve()
    (root / 'warehouse' / 'data').mkdir(parents=True)
    (root / 'warehouse' / 'data' / 'one.parquet').write_bytes(b'fixture')
    return root


def test_only_exact_owned_logical_locations_resolve(store):
    path = resolve_store_uri(store, STORE, f'store://{STORE}/warehouse/data/one.parquet')
    assert path == store / 'warehouse' / 'data' / 'one.parquet'


@pytest.mark.parametrize('uri', [
    '/etc/passwd', 'file:///etc/passwd',
    f'store://{STORE}/warehouse/../store.identity',
    f'store://{STORE}/warehouse/%2e%2e/store.identity',
    f'store://{STORE}/warehouse//data/one.parquet',
    f'store://{STORE}/warehouse/data/one.parquet?x=1',
    f'store://{STORE}/warehouse/data/one.parquet#fragment',
    'store://00000000-0000-4000-8000-000000000002/warehouse/data/one.parquet',
])
def test_physical_inspection_rejects_path_and_identity_substitution(store, uri):
    with pytest.raises(ValueError):
        resolve_store_uri(store, STORE, uri)


def test_physical_inspection_never_follows_a_symlink(store, tmp_path):
    outside = tmp_path / 'outside'
    outside.write_bytes(b'private')
    (store / 'warehouse' / 'linked').symlink_to(outside)
    with pytest.raises(ValueError):
        resolve_store_uri(store, STORE, f'store://{STORE}/warehouse/linked')


def test_physical_inspection_requires_a_regular_file(store):
    with pytest.raises(ValueError):
        resolve_store_uri(store, STORE, f'store://{STORE}/warehouse/data')
