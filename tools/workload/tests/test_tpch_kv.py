"""KV targets preserve logical TPC-H data and immutable operation identity."""
import hashlib
import json
import sqlite3
from types import SimpleNamespace
from uuid import uuid4

import pytest
from glamdb_sdk import AppendOutcome, AppendRequest, Field, MutationRequest, Schema

from glamdb_workload.ledger import Ledger
from glamdb_workload.tpch_schema import COLUMNS, PRIMARY_KEYS, table_schema


def test_kv_keys_have_independent_ordered_vectors_and_no_composite_collisions():
    from glamdb_workload.tpch_target import target_rows
    values = [-(2**63), -1, 0, 9, 10, 2**63 - 1]
    rows = [{'o_orderkey': value, 'untouched': 'value'} for value in values]
    result = target_rows('orders', rows, 'kv')
    assert [row['key'] for row in result] == [
        '00000000000000000000', '09223372036854775807', '09223372036854775808',
        '09223372036854775817', '09223372036854775818', '18446744073709551615']
    assert [row['key'] for row in result] == sorted(row['key'] for row in result)
    assert all('key' not in row for row in rows)
    assert all(row['untouched'] == 'value' for row in result)
    composite = target_rows('lineitem', [
        {'l_orderkey': 1, 'l_linenumber': 23}, {'l_orderkey': 12, 'l_linenumber': 3},
        {'l_orderkey': '1', 'l_linenumber': '23'}], 'kv')
    assert composite[0]['key'] == '09223372036854775809:09223372036854775831'
    assert composite[0]['key'] != composite[1]['key']
    assert composite[0]['key'] == composite[2]['key']


@pytest.mark.parametrize('profile', ['native', 'legacy-scaled'])
def test_every_kv_table_keeps_original_columns_ids_and_types(profile):
    from glamdb_workload.tpch_target import target_schema, target_rows
    total = 0
    for table in COLUMNS:
        source = table_schema(table, profile)
        schema = target_schema(source, 'kv')
        assert schema.fields[:-1] == source.fields
        assert schema.fields[-1] == Field('key', 'string', field_id=len(source.fields) + 1)
        assert target_schema(source, 'append') == source
        rows = [dict.fromkeys(PRIMARY_KEYS[table], 1)]
        assert target_rows(table, rows, 'append') == rows
        assert target_rows(table, rows, 'kv')[0]['key']
        total += len(schema.fields) - 1
    assert total == 61


@pytest.mark.parametrize('value', [None, 'invalid', 1])
def test_unknown_persisted_modes_are_rejected(value):
    from glamdb_workload.tpch_target import collection_mode
    assert collection_mode({}) == 'append'
    with pytest.raises(ValueError, match='mode'):
        collection_mode({'collection_mode': value})


def mutation():
    return MutationRequest.prepare('upsert', str(uuid4()), str(uuid4()),
                                   Schema([Field('key', 'string')]),
                                   rows=[{'key': 'a'}, {'key': 'b'}], expected_snapshot=None)


def test_upsert_journal_survives_restart_with_exact_kind_hash_and_bytes(tmp_path):
    request = mutation()
    path = tmp_path / 'load.sqlite'
    with Ledger(path) as log:
        log.prepare(request)
        log.attempted(request.operation_id)
        log.mark(request.operation_id, 'transport_error')
    original = path.read_bytes()
    for readonly in (True, False):
        with Ledger(path, readonly=readonly) as log:
            recovered = log.request(request.operation_id)
            assert recovered == request
            assert recovered.kind == 'upsert'
            assert recovered.body == request.body
            assert log.summary()['pending'] == 1
    assert path.read_bytes() == original


def test_legacy_append_journal_migration_preserves_original_request(tmp_path):
    path = tmp_path / 'old.sqlite'
    request = AppendRequest.prepare(str(uuid4()), str(uuid4()), Schema([Field('key', 'string')]), [{'key': 'a'}])
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE operations (operation_id TEXT PRIMARY KEY,collection_id TEXT NOT NULL,'
                   'payload_hash TEXT NOT NULL,body BLOB NOT NULL,body_hash TEXT NOT NULL,rows INTEGER NOT NULL,'
                   'bytes INTEGER NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,error_code TEXT,outcome TEXT)')
        db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?,?, ?,0,NULL,NULL)',
                   (request.operation_id, request.collection_id, request.payload_hash, request.body,
                    hashlib.sha256(request.body).hexdigest(), 1, len(request.body), 'prepared'))
    original = path.read_bytes()
    with Ledger(path, readonly=True) as log:
        assert log.request(request.operation_id) == request
    assert path.read_bytes() == original
    with Ledger(path) as log:
        assert log.request(request.operation_id) == request
        upsert = mutation()
        log.prepare(upsert)
    with Ledger(path) as log:
        assert log.request(upsert.operation_id) == upsert
        assert log.request(request.operation_id) == request


def test_journal_rejects_reinterpreting_upsert_as_append(tmp_path):
    request = mutation()
    with Ledger(tmp_path / 'load.sqlite') as log:
        log.prepare(request)
        # Body/hash alone must not authorize a different mutation kind.
        different = AppendRequest(request.collection_id, request.operation_id, request.payload_hash, request.body)
        with pytest.raises(ValueError, match='identity'):
            log.prepare(different)


@pytest.mark.parametrize('change', [None, {'rows_written': '5'}, {'mutation_kind': 'delete'},
                                    {'input_rows': '1'}, {'snapshot_created': False},
                                    {'rows_affected': '1'}, {'receipt_version': 2}])
def test_upsert_receipt_counts_only_new_rows_and_preserves_cow_statistics(tmp_path, change):
    from glamdb_workload.tpch_lifecycle import record_outcome, load_byte_metrics
    request = mutation()
    # Two inserted rows plus four rows rewritten by COW: only two confirmed rows.
    receipt = {'receipt_version': 3, 'payload_hash': request.payload_hash, 'rows_appended': '0',
               'mutation_kind': 'upsert', 'snapshot_created': True, 'snapshot_id': '42',
               'input_rows': '2', 'rows_affected': '2', 'rows_written': '6', 'rows_removed': '4',
               'data_files_added': '2', 'data_files_removed': '1',
               'compressed_data_bytes_added': '600', 'compressed_data_bytes_removed': '400'}
    receipt.update(change or {})
    outcome = AppendOutcome('committed', 'publication', request.operation_id, receipt, None, {})
    with Ledger(tmp_path / 'receipt.sqlite') as log:
        log.prepare(request)
        state = record_outcome(log, request, outcome)
        assert state == ('protocol_error' if change else 'committed')
        assert log.summary()['confirmed_rows'] == (0 if change else 2)
        if not change:
            log.attempted(request.operation_id)
            record_outcome(log, request, outcome)
            metrics = load_byte_metrics(log, SimpleNamespace(manifest={'source_bytes': 17, 'bytes': 23}))
            assert metrics['committed_parquet_bytes'] == 600
            assert metrics['kv_mutations'] == {
                'input_rows': 2, 'rows_affected': 2, 'rows_written': 6, 'rows_removed': 4,
                'data_files_added': 2, 'data_files_removed': 1, 'compressed_data_bytes_removed': 400,
                'row_write_amplification': 3.0}


def test_kv_plan_binds_mode_key_bytes_and_sdk_hash(tmp_path):
    from glamdb_workload.tpch_lifecycle import freeze_plan
    source = table_schema('region', 'native')
    rows = [{'r_regionkey': i, 'r_name': 'region', 'r_comment': 'comment'} for i in range(5)]
    data = SimpleNamespace(tables={'region': SimpleNamespace(schema=source, iter_parts=lambda: iter([rows]))}, validate=lambda: None)
    saved = {'load_id': str(uuid4()), 'dataset_sha256': 'a' * 64, 'collection_mode': 'kv',
             'tables': {'region': {'collection_id': str(uuid4())}}}
    with Ledger(tmp_path / 'plan.sqlite') as log:
        plan = freeze_plan(log, data, saved, batch_rows=3, batch_bytes=500, workers=2)
        chunks = list(log.db.execute('SELECT * FROM chunks ORDER BY ordinal'))
        assert plan['rows'] == 5
        for chunk in chunks:
            schema = Schema([*source.fields, Field('key', 'string', field_id=4)])
            physical = [dict(row, key=f"{row['r_regionkey'] + 2**63:020d}") for row in rows[chunk['start_row']:chunk['end_row']]]
            request = MutationRequest.prepare('upsert', saved['tables']['region']['collection_id'],
                                               chunk['operation_id'], schema, rows=physical,
                                               expected_snapshot=-(2**63))
            assert request.payload_hash == chunk['payload_hash']
            assert len(request.body) == chunk['wire_bytes'] <= 500
        assert freeze_plan(log, data, saved, batch_rows=3, batch_bytes=500, workers=2) == plan
        with pytest.raises(ValueError, match='plan'):
            freeze_plan(log, data, dict(saved, collection_mode='append'), batch_rows=3, batch_bytes=500, workers=2)


def test_old_append_plan_resumes_without_rewriting_its_configuration(tmp_path):
    from glamdb_workload.tpch_lifecycle import freeze_plan
    source = table_schema('region', 'native')
    rows = [{'r_regionkey': 0, 'r_name': 'region', 'r_comment': 'comment'}]
    data = SimpleNamespace(tables={'region': SimpleNamespace(schema=source, iter_parts=lambda: iter([rows]))}, validate=lambda: None)
    saved = {'load_id': str(uuid4()), 'dataset_sha256': 'a' * 64,
             'tables': {'region': {'collection_id': str(uuid4())}}}
    with Ledger(tmp_path / 'plan.sqlite') as log:
        plan = freeze_plan(log, data, saved, batch_rows=3, batch_bytes=500, workers=2)
        original = json.loads(log.db.execute('SELECT value FROM benchmark_meta').fetchone()[0])
        original['config'].pop('collection_mode', None)
        with log.db:
            log.db.execute('UPDATE benchmark_meta SET value=?', (json.dumps(original),))
        assert freeze_plan(log, data, dict(saved, collection_mode='append'), batch_rows=3, batch_bytes=500, workers=2) == plan
        assert json.loads(log.db.execute('SELECT value FROM benchmark_meta').fetchone()[0]) == original
