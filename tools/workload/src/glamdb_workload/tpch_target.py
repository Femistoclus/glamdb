"""Physical targets for one immutable logical TPC-H dataset."""

from glamdb_sdk import AppendRequest, Field, MutationRequest, Schema
from glamdb_sdk.codec import int64

from .tpch_schema import PRIMARY_KEYS

KEY_ENCODING = 'int64-offset-decimal-v1'


def collection_mode(saved):
    mode = saved.get('collection_mode', 'append')
    if mode not in ('append', 'kv'):
        raise ValueError('TPC-H collection mode must be append or kv')
    return mode


def target_schema(source, mode):
    if collection_mode({'collection_mode': mode}) == 'append':
        return source
    return Schema([*source.fields, Field('key', 'string', field_id=max(f.field_id for f in source.fields) + 1)])


def target_rows(table, rows, mode):
    if collection_mode({'collection_mode': mode}) == 'append':
        return rows
    # Fixed-width, offset int64 components keep numeric tuple order in UTF-8
    # key order, including sparse order keys and the two composite primary keys.
    # Source rows/files stay untouched; the 61 logical fields remain unchanged.
    return [dict(row, key=':'.join(f'{int64(row[name]) + 2**63:020d}' for name in PRIMARY_KEYS[table]))
            for row in rows]


def prepare_request(saved, table, source_schema, operation, rows, expected_snapshot):
    mode = collection_mode(saved)
    cid = saved['tables'][table]['collection_id']
    schema = target_schema(source_schema, mode)
    if mode == 'kv':
        return MutationRequest.prepare('upsert', cid, operation, schema,
                                       rows=target_rows(table, rows, mode), expected_snapshot=expected_snapshot)
    return AppendRequest.prepare(cid, operation, schema, rows, expected_snapshot)
