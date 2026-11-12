"""Immutable append plans, capacity refusal and exclusive run ownership."""

import hashlib
import json
from pathlib import Path
import subprocess
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest

from glamdb_sdk import Field, Schema
from glamdb_sdk.codec import encode_rows
from glamdb_sdk.request import json_bytes
from glamdb_workload.dataset import Dataset
from glamdb_workload.ledger import Ledger
from glamdb_workload.tpch_lifecycle import freeze_plan, capacity_preflight
from glamdb_workload.tpch_state import read_state, run_lock, write_state

DATABASE = '11111111-2222-4333-8444-555555555555'
COLLECTION = '22222222-3333-4444-8555-666666666666'
LOAD = '33333333-4444-4555-8666-777777777777'


def dataset(tmp_path, *, rows=7, part_rows=3):
    directory = tmp_path / 'dataset'
    directory.mkdir()
    schema = Schema([Field('id', 'int64'), Field('text', 'string')])
    parts = []
    for offset in range(0, rows, part_rows):
        values = [{'id': i, 'text': 'source value | with spaces  '}
                  for i in range(offset, min(rows, offset + part_rows))]
        body = b''.join(json_bytes(row) + b'\n' for row in encode_rows(schema, values))
        name = f'part-{len(parts):08d}.jsonl'
        (directory / name).write_bytes(body)
        parts.append({'path': name, 'rows': len(values), 'bytes': len(body),
                      'sha256': hashlib.sha256(body).hexdigest()})
    (directory / 'manifest.json').write_text(json.dumps({
        'version': 1, 'format': 'glamdb-jsonl', 'schema': schema.to_dict(),
        'rows': rows, 'parts': parts,
    }))
    return SimpleNamespace(tables={'lineitem': Dataset(directory)},
                           validate=lambda: [None for _ in Dataset(directory).iter_parts()])


def saved():
    return {'version': 1, 'url': 'http://localhost:8080', 'database_id': DATABASE,
            'store_id': DATABASE, 'load_id': LOAD, 'dataset_sha256': 'a' * 64,
            'tables': {'lineitem': {'collection_id': COLLECTION}}, 'load_complete': False}


def test_plan_chunks_have_stable_ids_independent_of_worker_and_keep_every_row(tmp_path):
    data = dataset(tmp_path)
    state = saved()
    with Ledger(tmp_path / 'plan.sqlite') as log:
        result = freeze_plan(log, data, state, batch_rows=2, batch_bytes=1024, workers=3)
        assert result['operations'] == 5
        assert result['rows'] == 7
        chunks = list(log.db.execute('SELECT * FROM chunks ORDER BY ordinal'))
        assert [r['rows'] for r in chunks] == [2, 1, 2, 1, 1]
        assert [r['worker'] for r in chunks] == [0, 1, 2, 0, 1]
        assert len({r['operation_id'] for r in chunks}) == 5
        assert all(UUID(r['operation_id']).version == 5 for r in chunks)
        assert log.summary()['attempts'] == 0
        original = [tuple(row) for row in chunks]
        assert freeze_plan(log, data, state, batch_rows=2, batch_bytes=1024, workers=3) == result
        assert [tuple(row) for row in log.db.execute('SELECT * FROM chunks ORDER BY ordinal')] == original
        with pytest.raises(ValueError, match='plan'):
            freeze_plan(log, data, state, batch_rows=3, batch_bytes=1024, workers=3)


def test_plan_respects_serialized_byte_limit_and_refuses_one_oversized_row(tmp_path):
    data = dataset(tmp_path, rows=3)
    with Ledger(tmp_path / 'small.sqlite') as log:
        result = freeze_plan(log, data, saved(), batch_rows=3, batch_bytes=200, workers=1)
        assert result['operations'] == 3
        assert max(r[0] for r in log.db.execute('SELECT wire_bytes FROM chunks')) <= 200
    with Ledger(tmp_path / 'too-small.sqlite') as log:
        with pytest.raises(ValueError, match='row|bytes'):
            freeze_plan(log, data, saved(), batch_rows=3, batch_bytes=100, workers=1)
        assert log.db.execute('SELECT count(*) FROM chunks').fetchone()[0] == 0


def test_corrupt_later_source_part_cannot_publish_partial_append_plan(tmp_path):
    data = dataset(tmp_path)
    last = data.tables['lineitem'].path / data.tables['lineitem'].manifest['parts'][-1]['path']
    last.write_bytes(b'changed\n')
    with Ledger(tmp_path / 'corrupt.sqlite') as log:
        with pytest.raises(ValueError, match='hash|size'):
            freeze_plan(log, data, saved(), batch_rows=2, batch_bytes=1024, workers=1)
        assert log.db.execute('SELECT count(*) FROM chunks').fetchone()[0] == 0


def test_resume_rejects_damaged_chunk_range_even_when_source_is_unchanged(tmp_path):
    data = dataset(tmp_path)
    with Ledger(tmp_path / 'damaged-plan.sqlite') as log:
        freeze_plan(log, data, saved(), batch_rows=2, batch_bytes=1024, workers=1)
        with log.db:
            log.db.execute('UPDATE chunks SET start_row=1 WHERE ordinal=0')
        with pytest.raises(ValueError, match='plan'):
            freeze_plan(log, data, saved(), batch_rows=2, batch_bytes=1024, workers=1)


def test_verification_and_resolution_cross_check_saved_plan_and_complete_receipt_inventory(tmp_path):
    from glamdb_workload.tpch_lifecycle import validate_saved_plan
    data = dataset(tmp_path)
    original = saved()
    with Ledger(tmp_path / 'verify-plan.sqlite') as log:
        summary = freeze_plan(log, data, original, batch_rows=2, batch_bytes=1024, workers=1)
        original['append_plan'] = dict(summary, batch_rows=2, batch_bytes=1024, workers=1)
        assert validate_saved_plan(log, data, original) == summary
        original['append_plan']['sha256'] = 'b' * 64
        with pytest.raises(ValueError, match='state|checksum'):
            validate_saved_plan(log, data, original)
        original['append_plan']['sha256'] = summary['sha256']
        with log.db:
            log.db.execute('DELETE FROM chunks WHERE ordinal=0')
        with pytest.raises(ValueError, match='plan'):
            validate_saved_plan(log, data, original)


def test_cached_operation_row_counts_cannot_change_frozen_chunk_accounting(tmp_path):
    from glamdb_sdk import AppendRequest
    from glamdb_workload.tpch_lifecycle import load_chunk, validate_saved_plan
    data = dataset(tmp_path)
    original = saved()
    with Ledger(tmp_path / 'counters.sqlite') as log:
        summary = freeze_plan(log, data, original, batch_rows=2, batch_bytes=1024, workers=1)
        original['append_plan'] = dict(summary, batch_rows=2, batch_bytes=1024, workers=1)
        chunk = log.db.execute('SELECT * FROM chunks WHERE ordinal=0').fetchone()
        request = AppendRequest.prepare(COLLECTION, chunk['operation_id'], data.tables['lineitem'].schema,
                                        load_chunk(data, chunk), None)
        log.prepare(request)
        with log.db:
            log.db.execute('UPDATE operations SET rows=99')
        with pytest.raises(ValueError, match='journal|chunk|account'):
            validate_saved_plan(log, data, original)


def test_capacity_counts_failed_bindings_existing_history_and_eight_tables():
    limits = {'max_collections': 8, 'max_operations': 10, 'max_catalog_bytes': 1 << 28,
              'history': {'max_snapshots': 10, 'max_data_files': 20, 'max_manifests': 20,
                          'max_recovery_workspace_bytes': 1 << 28}}
    plan = {'operations': 8, 'rows': 30, 'wire_bytes': 3000, 'tables': {'lineitem': 8}}
    assert capacity_preflight(limits, plan, operations_used=2, snapshots_used=1)['operations_required'] == 10
    with pytest.raises(ValueError, match='max_operations'):
        capacity_preflight(limits, plan, operations_used=3, snapshots_used=1)
    with pytest.raises(ValueError, match='max_snapshots'):
        capacity_preflight(limits, plan, operations_used=0, snapshots_used=3)


def test_preflight_counts_retained_manifests_and_recovery_workspace_before_first_send():
    limits = {'max_collections': 8, 'max_operations': 20, 'max_catalog_bytes': 1 << 28,
              'history': {'max_snapshots': 20, 'max_data_files': 20, 'max_manifests': 8,
                          'max_recovery_workspace_bytes': 1 << 28}}
    plan = {'operations': 15, 'rows': 86805, 'wire_bytes': 30 << 20, 'tables': {'lineitem': 7}}
    with pytest.raises(ValueError, match='max_manifests'):
        capacity_preflight(limits, plan)
    limits['history']['max_manifests'] = 20
    limits['history']['max_recovery_workspace_bytes'] = 8192 * 45
    with pytest.raises(ValueError, match='max_recovery_workspace_bytes'):
        capacity_preflight(limits, plan)


def test_state_replace_is_validated_and_lock_rejects_duplicate_loader(tmp_path):
    path = tmp_path / 'run.json'
    state = saved()
    write_state(path, state)
    assert read_state(path) == state
    with run_lock(path):
        with pytest.raises(ValueError, match='active|locked'):
            with run_lock(path):
                pytest.fail('second loader acquired the run')
    with run_lock(path):
        pass
    path.write_text('{"version":1,"version":1}')
    with pytest.raises(ValueError, match='duplicate'):
        read_state(path)


@pytest.mark.parametrize('change', [{'token': 'secret'}, {'url': 'https://user:secret@example.org'},
                                     {'store_id': COLLECTION}, {'version': True}])
def test_bad_state_is_rejected_without_serializing_secrets(tmp_path, change):
    path = tmp_path / 'run.json'
    path.write_text(json.dumps(saved() | change))
    with pytest.raises(ValueError) as failure:
        read_state(path)
    assert 'secret' not in str(failure.value)


def test_receipt_validation_keeps_malformed_commit_uncertain(tmp_path):
    from glamdb_sdk import AppendOutcome, AppendRequest
    from glamdb_workload.tpch_lifecycle import record_outcome
    schema = Schema([Field('id', 'int64')])
    request = AppendRequest.prepare(COLLECTION, LOAD, schema, [{'id': 17}], None)
    receipt = {'payload_hash': request.payload_hash, 'rows_appended': '2', 'snapshot_id': '42'}
    outcome = AppendOutcome('committed', 'publication', LOAD, receipt, None, {})
    with Ledger(tmp_path / 'receipt.sqlite') as log:
        log.prepare(request)
        assert record_outcome(log, request, outcome) == 'protocol_error'
        assert log.summary()['pending'] == 1
        assert log.summary()['confirmed_rows'] == 0


def test_receipt_records_original_snapshot_and_content_once(tmp_path):
    from glamdb_sdk import AppendOutcome, AppendRequest
    from glamdb_workload.tpch_lifecycle import record_outcome
    schema = Schema([Field('id', 'int64')])
    request = AppendRequest.prepare(COLLECTION, LOAD, schema, [{'id': 17}], None)
    receipt = {'payload_hash': request.payload_hash, 'rows_appended': '1', 'snapshot_id': '42'}
    outcome = AppendOutcome('committed', 'publication', LOAD, receipt, None, {})
    with Ledger(tmp_path / 'receipt.sqlite') as log:
        log.prepare(request)
        assert record_outcome(log, request, outcome) == 'committed'
        original = log.db.execute('SELECT outcome FROM operations').fetchone()[0]
        assert json.loads(original)['result'] == receipt
        assert log.summary()['confirmed_rows'] == 1


def test_loader_locust_import_preserves_existing_https_context():
    code = '''
import os, ssl
from glamdb_workload.tpch_lifecycle import _locust_runtime
context = ssl.create_default_context()
original = ssl.SSLContext
before = os.environ.get('LOCUST_SKIP_MONKEY_PATCH')
_locust_runtime()
assert ssl.SSLContext is original
context.verify_mode = ssl.CERT_REQUIRED
context.minimum_version = ssl.TLSVersion.TLSv1_2
assert os.environ.get('LOCUST_SKIP_MONKEY_PATCH') == before
'''
    child = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=20)
    assert child.returncode == 0, child.stderr


def test_status_journal_reader_does_not_create_or_mutate_a_journal(tmp_path):
    from glamdb_sdk import AppendRequest
    missing = tmp_path / 'missing.sqlite'
    with pytest.raises(sqlite3.OperationalError):
        Ledger(missing, readonly=True)
    assert not missing.exists()
    path = tmp_path / 'journal.sqlite'
    request = AppendRequest.prepare(COLLECTION, LOAD, Schema([Field('id', 'int64')]), [{'id': 1}])
    with Ledger(path) as writer:
        writer.prepare(request)
        original = path.read_bytes()
        with Ledger(path, readonly=True) as reader:
            assert reader.summary()['pending'] == 1
        assert path.read_bytes() == original


def test_prebinding_definite_refusal_remains_resolvable_and_keeps_original_failure(tmp_path):
    from glamdb_sdk import AppendOutcome, AppendRequest
    from glamdb_sdk.errors import ErrorDetail
    from glamdb_workload.tpch_lifecycle import record_outcome
    request = AppendRequest.prepare(COLLECTION, LOAD, Schema([Field('id', 'int64')]), [{'id': 1}], None)
    error = ErrorDetail.from_dict({'code': 'RESOURCE_EXHAUSTED', 'message': 'admission refused',
                                   'retry_hint': 'OperatorAction'})
    refused = AppendOutcome('definitely_not_committed', 'validation', LOAD, None, error, {})
    receipt = {'payload_hash': request.payload_hash, 'rows_appended': '1', 'snapshot_id': '42'}
    committed = AppendOutcome('committed', 'publication', LOAD, receipt, None, {})
    with Ledger(tmp_path / 'admission.sqlite') as log:
        log.prepare(request)
        log.attempted(LOAD)
        assert record_outcome(log, request, refused) == 'failure_lookup_required'
        assert [row['operation_id'] for row in log.operations(pending=True)] == [LOAD]
        assert log.summary()['definitely_not_committed'] == 0
        log.mark(LOAD, 'not_found')  # Only an authoritative lookup permits explicit retry.
        log.attempted(LOAD)
        assert record_outcome(log, request, committed) == 'committed'
        attempts = list(log.db.execute('SELECT server_outcome,error_code,retry_hint FROM attempt_outcomes ORDER BY attempt'))
        assert [tuple(r) for r in attempts] == [('definitely_not_committed', 'RESOURCE_EXHAUSTED', 'OperatorAction'),
                                               ('committed', None, None)]
        assert log.summary()['confirmed_rows'] == 1


def test_authoritative_bound_failure_is_permanent_and_never_replaced(tmp_path):
    from glamdb_sdk import AppendOutcome, AppendRequest
    from glamdb_sdk.errors import ErrorDetail
    from glamdb_workload.tpch_lifecycle import record_outcome
    request = AppendRequest.prepare(COLLECTION, LOAD, Schema([Field('id', 'int64')]), [{'id': 1}], None)
    error = ErrorDetail.from_dict({'code': 'PRECONDITION_FAILED', 'message': 'snapshot changed', 'retry_hint': 'NewOperationId'})
    failed = AppendOutcome('definitely_not_committed', 'binding', LOAD, None, error, {})
    with Ledger(tmp_path / 'bound.sqlite') as log:
        log.prepare(request)
        record_outcome(log, request, failed, authoritative=True)
        assert log.summary()['definitely_not_committed'] == 1
        assert list(log.operations(pending=True)) == []
        with pytest.raises(ValueError, match='terminal'):
            log.mark(LOAD, 'not_found')


def test_load_byte_metrics_count_unique_committed_receipts_not_retry_attempts(tmp_path):
    from glamdb_sdk import AppendOutcome, AppendRequest
    from glamdb_workload.tpch_lifecycle import load_byte_metrics, record_outcome
    request = AppendRequest.prepare(COLLECTION, LOAD, Schema([Field('id', 'int64')]), [{'id': 1}], None)
    result = {'payload_hash': request.payload_hash, 'rows_appended': '1', 'snapshot_id': '42',
              'compressed_data_bytes_added': '321'}
    committed = AppendOutcome('committed', 'publication', LOAD, result, None, {})
    with Ledger(tmp_path / 'metrics.sqlite') as log:
        log.prepare(request)
        log.attempted(LOAD)
        record_outcome(log, request, committed)
        log.attempted(LOAD)
        record_outcome(log, request, committed)
        metrics = load_byte_metrics(log, SimpleNamespace(manifest={'source_bytes': 17, 'bytes': 23}))
    assert metrics == {'source_tbl_bytes': 17, 'dataset_jsonl_bytes': 23,
                       'confirmed_wire_bytes': len(request.body), 'committed_parquet_bytes': 321}
