"""Real native KV HTTP/SDK/CLI parity, fixed views and fresh-process exact retries."""
from datetime import date, datetime, timezone
from decimal import Decimal
import http.client
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
import uuid

from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
from glamdb_sdk import Client

SERVER, CLI, UI = sys.argv[1:4]
del sys.argv[1:4]
TOKEN = 'kv-mode-test-secret'
WIRE = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.schema.json').read_text())


class NativeKV(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(
            dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = str(Path(self.tmp.name) / 'server')
        self.env = dict(os.environ, GLAMDB_TOKEN=TOKEN)
        self.start()
        self.addCleanup(self.stop)
        self.database = self.api('/databases', {'name': 'key_values'})['database_id']
        self.prefix = '/databases/' + self.database

    def start(self):
        self.proc = subprocess.Popen(
            [SERVER, '--data-dir', self.data_dir, '--port', '0', '--ui-root', UI,
             '--http-readers', '1', '--http-readers-per-database', '1'],
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        ready, _, _ = select.select([self.proc.stdout], [], [], 20)
        self.assertTrue(ready, 'native server did not report its listening address')
        line = self.proc.stdout.readline()
        if not line:
            self.fail(self.proc.stderr.read())
        self.port = json.loads(line)['port']

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
        stdout, stderr = self.proc.communicate(timeout=30)
        self.assertEqual(self.proc.returncode, 0, stdout + stderr)
        self.assertNotIn(TOKEN, stdout + stderr)

    def request(self, path, body=None, accept='application/json'):
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=65)
        headers = {'Authorization': 'Bearer ' + TOKEN, 'Accept': accept}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        conn.request('GET' if body is None else 'POST', path,
                     None if body is None else json.dumps(body), headers)
        response = conn.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        conn.close()
        return result

    def api(self, path, body=None, status=200):
        actual, _, raw = self.request(path, body)
        self.assertEqual(actual, status, raw)
        return json.loads(raw)

    def cli(self, args):
        result = subprocess.run(
            [CLI, *args, '--url', f'http://127.0.0.1:{self.port}'], env=self.env,
            text=True, capture_output=True, timeout=65)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def check_wire(self, name, value):
        Draft202012Validator({'$ref': '#/$defs/' + name, '$defs': WIRE['$defs']}).validate(value)

    def test_kv_native_sdk_cli_history_and_restart(self):
        cid = str(uuid.uuid4())
        schema = {'fields': [
            {'name': 'n', 'type': 'int64', 'nullable': True},
            {'name': 'key', 'type': 'string', 'nullable': False},
            {'name': 'amount', 'type': 'decimal', 'precision': 20, 'scale': 2, 'nullable': True},
            {'name': 'day', 'type': 'date', 'nullable': True},
            {'name': 'flag', 'type': 'bool', 'nullable': True},
            {'name': 'score', 'type': 'float64', 'nullable': True},
            {'name': 'at', 'type': 'timestamp_us_utc', 'nullable': True}]}
        create = {'collection_id': cid, 'name': 'key_values', 'schema': schema, 'mode': 'kv'}
        info = self.api(self.prefix + '/collections', create)
        self.assertEqual(info['mode'], 'kv')
        self.check_wire('collection', info)
        caps = self.api(self.prefix + '/capabilities')
        self.assertEqual(caps['collection_modes'], ['append', 'kv'])
        self.check_wire('capabilities', caps)
        for feature in ('key_value', 'upsert', 'delete', 'lookup', 'compact'):
            self.assertTrue(caps['features'][feature])
        self.assertTrue(caps['features']['bulk_append'])
        self.assertFalse(caps['features']['bulk_upsert'])
        self.assertFalse(caps['features']['migration'])
        path = self.prefix + '/collections/' + cid
        conflict = self.api(self.prefix + '/collections', dict(create, mode='append'), status=409)
        self.assertEqual(conflict['error']['code'], 'COLLECTION_MODE_MISMATCH')
        append = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'x'}]}
        self.assertEqual(self.api(path + '/append', append, status=409)['error']['code'], 'COLLECTION_MODE_MISMATCH')
        original = {'operation_id': str(uuid.uuid4()), 'rows': [
            {'key': 'a', 'n': '1', 'amount': '1.00', 'day': '2000-02-29',
             'flag': True, 'score': 1.25, 'at': '1969-12-31T23:59:59.999999Z'},
            {'key': 'a', 'n': '9007199254740993', 'amount': '123456789012345678.90', 'day': '0001-01-01',
             'flag': False, 'score': -2.5, 'at': '9999-12-31T23:59:59.999999Z'},
            {'key': 'b', 'flag': True, 'score': 1.25, 'at': '1969-12-31T23:59:59.999999Z'}]}
        first = self.api(path + '/upsert', original)
        self.check_wire('appendOutcome', first)
        self.assertEqual(first['result']['receipt_version'], 3)
        self.assertEqual(first['result']['rows_appended'], '0')
        self.assertEqual(first['result']['input_rows'], '3')
        self.assertEqual(first['result']['rows_affected'], '2')
        snapshot = first['result']['snapshot_id']
        lookup = self.api(path + '/lookup', {'keys': ['a', 'missing', 'a', 'b']})
        self.check_wire('lookupResult', lookup)
        self.assertEqual(lookup['snapshot_id'], snapshot)
        self.assertEqual([None if x is None else x['key'] for x in lookup['rows']], ['a', None, 'a', 'b'])
        self.assertEqual(lookup['rows'][0]['n'], '9007199254740993')
        self.assertEqual(lookup['rows'][0]['amount'], '123456789012345678.90')
        self.assertEqual(lookup['rows'][0]['day'], '0001-01-01')
        self.assertIs(lookup['rows'][0]['flag'], False)
        self.assertIsInstance(lookup['rows'][0]['score'], float)
        self.assertEqual(lookup['rows'][0]['score'], -2.5)
        self.assertEqual(lookup['rows'][0]['at'], '9999-12-31T23:59:59.999999Z')
        self.assertEqual(lookup['rows'][3]['n'], None)
        self.assertIs(lookup['rows'][3]['flag'], True)
        self.assertEqual(lookup['rows'][3]['score'], 1.25)
        self.assertEqual(lookup['rows'][3]['at'], '1969-12-31T23:59:59.999999Z')
        cross_kind = self.api(path + '/delete', {'operation_id': original['operation_id'], 'keys': []}, status=409)
        self.assertEqual(cross_kind['error']['code'], 'OPERATION_CONFLICT')
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.database) as client:
            empty = client.upsert(client.prepare_upsert(cid, [], operation_id=str(uuid.uuid4())))
            self.assertFalse(empty.result['snapshot_created'])
            self.assertEqual(empty.result['input_rows'], '0')
            rejected = client.upsert(cid, [{'key': 'stale'}], operation_id=str(uuid.uuid4()), expected_snapshot=None)
            self.assertEqual(rejected.outcome, 'definitely_not_committed')
            self.assertEqual(client.find_append_outcome(cid, rejected.operation_id).raw, rejected.raw)
            before_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
            last_microsecond = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
            typed_rows = [
                dict(original['rows'][0], n=1, amount=Decimal('1.00'), day=date(2000, 2, 29), at=before_epoch),
                dict(original['rows'][1], n=9007199254740993, amount=Decimal('123456789012345678.90'),
                     day=date(1, 1, 1), at=last_microsecond),
                dict(original['rows'][2], at=before_epoch)]
            prepared = client.prepare_upsert(cid, typed_rows, operation_id=original['operation_id'])
            self.assertEqual(prepared.payload_hash, first['result']['payload_hash'])
            self.assertEqual(client.upsert(prepared).raw, first)
            append_id = str(uuid.uuid4())
            client.create_collection('bulk_append_with_kv', schema, collection_id=append_id)
            bulk_prepared = client.prepare_bulk_append(
                append_id, [typed_rows[:1], typed_rows[1:]], rows=3, operation_id=str(uuid.uuid4()))
            self.addCleanup(bulk_prepared.close)
            bulk_receipt = client.bulk_append(bulk_prepared)
            self.assertEqual(bulk_receipt.result['receipt_version'], 2)
            self.assertEqual(bulk_receipt.result['rows_appended'], '3')
            self.assertEqual(bulk_receipt.result['payload_hash'], bulk_prepared.payload_hash)
            with client.prepare_bulk_append(append_id, [typed_rows], rows=3,
                                            operation_id=bulk_prepared.operation_id) as rechunked:
                self.assertEqual(rechunked.payload_hash, bulk_prepared.payload_hash)
                self.assertEqual(client.bulk_append(rechunked).raw, bulk_receipt.raw)
            self.assertEqual(len(client.scan_json(append_id).rows), 3)
            refused_id = str(uuid.uuid4())
            client.create_collection('kv_refuses_append_bulk', schema, collection_id=refused_id, mode='kv')
            reuse_operation = str(uuid.uuid4())
            with client.prepare_bulk_append(refused_id, [[typed_rows[0]]], rows=1,
                                            operation_id=reuse_operation) as invalid_bulk:
                refused = client.bulk_append(invalid_bulk)
            self.assertEqual(refused.outcome, 'definitely_not_committed')
            self.assertEqual(refused.error.code, 'COLLECTION_MODE_MISMATCH')
            self.api(self.prefix + '/collections/' + refused_id + '/operations/' + reuse_operation, status=404)
            reused = client.upsert(refused_id, [typed_rows[0]], operation_id=reuse_operation)
            self.assertEqual(reused.outcome, 'committed')
            self.assertEqual(reused.result['receipt_version'], 3)
            self.assertTrue(reused.result['snapshot_created'])
            self.assertEqual(client.lookup(refused_id, ['a']).rows[0], typed_rows[0])
            append_mutation = client.upsert(append_id, [typed_rows[0]], operation_id=str(uuid.uuid4()))
            self.assertEqual(append_mutation.outcome, 'definitely_not_committed')
            self.assertEqual(append_mutation.error.code, 'COLLECTION_MODE_MISMATCH')
            hit = client.lookup(cid, ['a', 'missing', 'a', 'b'], snapshot_id=snapshot)
            self.assertEqual(hit.snapshot_id, snapshot)
            self.assertEqual(hit.rows[0], typed_rows[1])
            self.assertIsInstance(hit.rows[0]['n'], int)
            self.assertIsInstance(hit.rows[0]['amount'], Decimal)
            self.assertIsInstance(hit.rows[0]['day'], date)
            self.assertIs(hit.rows[0]['flag'], False)
            self.assertIsInstance(hit.rows[0]['score'], float)
            self.assertIsInstance(hit.rows[0]['at'], datetime)
            self.assertEqual(hit.rows[0]['at'], last_microsecond)
            self.assertIsNone(hit.rows[1])
            self.assertEqual(hit.rows[2], hit.rows[0])
            self.assertIs(hit.rows[3]['flag'], True)
            self.assertEqual(hit.rows[3]['score'], 1.25)
            self.assertEqual(hit.rows[3]['at'], before_epoch)
            deleted = client.delete(cid, ['missing', 'b', 'b'], operation_id=str(uuid.uuid4()))
            self.assertEqual(deleted.result['input_rows'], '3')
            self.assertEqual(deleted.result['rows_affected'], '1')
            self.assertEqual(client.lookup(cid, ['b']).rows, [None])
            self.assertEqual(client.lookup(cid, ['b'], snapshot_id=snapshot).rows[0]['key'], 'b')
            noop_request = client.prepare_delete(cid, ['missing', 'missing'], operation_id=str(uuid.uuid4()))
            noop = client.delete(noop_request)
            self.assertFalse(noop.result['snapshot_created'])
            self.assertEqual(noop.result['snapshot_created_at_us'], '0')
            compact = client.compact(cid, operation_id=str(uuid.uuid4()))
            self.assertFalse(compact.result['snapshot_created'])
            replacement = client.upsert(cid, [{'key': 'a'}], operation_id=str(uuid.uuid4()))
            self.assertEqual(client.lookup(cid, ['a']).rows[0],
                             dict(n=None, key='a', amount=None, day=None, flag=None, score=None, at=None))
            fixed = client.lookup(cid, ['a'], snapshot_id=snapshot)
            self.assertEqual(fixed.rows[0], typed_rows[1])
            self.assertEqual(client.lookup(cid, ['a'], snapshot_id=None).rows, [None])
        snapshots = self.api(path + '/snapshots')['items']
        self.assertEqual(len(snapshots), 3)
        self.assertEqual(snapshots[1]['receipt']['mutation_kind'], 'delete')
        self.assertEqual(snapshots[1]['rows_removed'], '2')
        self.assertEqual(self.api(path + '/operations/' + noop_request.operation_id), noop.raw)
        with tempfile.TemporaryDirectory() as tmp:
            schema_file = Path(tmp) / 'schema.json'
            schema_file.write_text(json.dumps(schema))
            cli_id = str(uuid.uuid4())
            made = self.cli(['create-collection', '--database-id', self.database, '--collection-id', cli_id,
                             '--name', 'cli_kv', '--mode', 'kv', '--schema', str(schema_file)])
            self.assertEqual(made['mode'], 'kv')
            for action, body in [('upsert', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'cli'}]}),
                                 ('lookup', {'keys': ['cli', 'missing']}),
                                 ('delete', {'operation_id': str(uuid.uuid4()), 'keys': ['cli']}),
                                 ('compact', {'operation_id': str(uuid.uuid4())})]:
                body_file = Path(tmp) / 'body.json'
                body_file.write_text(json.dumps(body))
                value = self.cli([action, '--database-id', self.database, '--collection-id', cli_id, '--body', str(body_file)])
                if action == 'lookup':
                    self.assertEqual([None if row is None else row['key'] for row in value['rows']], ['cli', None])
                else:
                    self.assertEqual(value['result']['mutation_kind'], action)
            bulk_file = Path(tmp) / 'bulk.ndjson'
            bulk_file.write_text(json.dumps({'operation_id': str(uuid.uuid4()), 'rows': '1'}) + '\n' +
                                 json.dumps({'rows': [{'key': 'cli_bulk'}]}) + '\n')
            bulk_args = ['bulk-append', '--database-id', self.database, '--collection-id', append_id,
                         '--body', str(bulk_file)]
            cli_bulk = self.cli(bulk_args)
            self.assertEqual(cli_bulk['result']['receipt_version'], 2)
            self.assertEqual(cli_bulk['result']['rows_appended'], '1')
            self.assertEqual(self.cli(bulk_args), cli_bulk)
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/upsert', original), first)
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.database) as client:
            self.assertEqual(client.upsert(prepared).raw, first)
            self.assertEqual(client.bulk_append(bulk_prepared).raw, bulk_receipt.raw)
            self.assertEqual(client.find_append_outcome(append_id, bulk_prepared.operation_id).raw, bulk_receipt.raw)
            self.assertEqual(prepared.payload_hash, first['result']['payload_hash'])
            self.assertEqual(client.lookup(cid, ['a'], snapshot_id=snapshot).rows[0], typed_rows[1])
            self.assertEqual(client.lookup(cid, ['a']).rows[0],
                             dict(n=None, key='a', amount=None, day=None, flag=None, score=None, at=None))
            self.assertEqual(client.delete(noop_request).raw, noop.raw)
            self.assertEqual(client.find_append_outcome(cid, noop_request.operation_id).raw, noop.raw)
            self.assertEqual(client.lookup(cid, ['b'], snapshot_id=snapshot).rows[0]['key'], 'b')


if __name__ == '__main__':
    unittest.main()
