"""Exact native decimal/date JSON, Arrow, CLI and fresh-process retry checks."""
from datetime import date
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

import pyarrow as pa
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
from glamdb_sdk import Client, col

SERVER, CLI, UI = sys.argv[1:4]
del sys.argv[1:4]
TOKEN = 'scalar-types-test-secret'
WIRE = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.schema.json').read_text())


class NativeScalarTypes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(
            dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = str(Path(self.tmp.name) / 'server')
        self.env = dict(os.environ, GLAMDB_TOKEN=TOKEN)
        self.start()
        self.addCleanup(self.stop)
        self.database = self.api('/databases', {'name': 'scalars'})['database_id']
        self.prefix = '/databases/' + self.database

    def start(self):
        self.proc = subprocess.Popen(
            [SERVER, '--data-dir', self.data_dir, '--port', '0', '--ui-root', UI],
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

    def test_native_scalars_round_trip_and_retry(self):
        cid = str(uuid.uuid4())
        schema = {'fields': [
            {'name': 'key', 'type': 'string', 'nullable': False},
            {'name': 'amount', 'type': 'decimal', 'precision': 38, 'scale': 4, 'nullable': True},
            {'name': 'day', 'type': 'date', 'nullable': True}]}
        create = {'collection_id': cid, 'name': 'money_days', 'schema': schema}
        info = self.api(self.prefix + '/collections', create)
        self.check_wire('create', create)
        self.check_wire('collection', info)
        path = self.prefix + '/collections/' + cid
        self.assertEqual(info['schema']['fields'][1]['precision'], 38)
        self.assertEqual(info['schema']['fields'][1]['scale'], 4)
        capabilities = self.api(self.prefix + '/capabilities')
        self.check_wire('capabilities', capabilities)
        self.assertIn('decimal', capabilities['schema_types'])
        self.assertIn('date', capabilities['schema_types'])
        rows = [
            {'key': 'large', 'amount': '9999999999999999999999999999999999.9999', 'day': '0001-01-01'},
            {'key': 'negative', 'amount': '-000123.450000', 'day': '2000-02-29'},
            {'key': 'zero', 'amount': '-000.00000', 'day': '9999-12-31'},
            {'key': 'missing', 'amount': None, 'day': None}]
        expected = [dict(row) for row in rows]
        expected[1]['amount'] = '-123.4500'
        expected[2]['amount'] = '0.0000'
        append = {'operation_id': str(uuid.uuid4()), 'rows': rows}
        receipt = self.api(path + '/append', append)
        self.check_wire('appendOutcome', receipt)
        self.assertEqual(receipt['outcome'], 'committed')
        self.assertEqual(receipt['result']['hash_version'], 1)
        snapshot = receipt['result']['snapshot_id']
        scanned = self.api(path + '/scan', {'batch_rows': 2})
        self.check_wire('scanResult', scanned)
        received = scanned['rows']
        cursor = scanned['next_cursor']
        while cursor is not None:
            page = self.api(path + '/scan', {'cursor': cursor})
            received += page['rows']
            cursor = page['next_cursor']
        self.assertEqual(received, expected)
        status, _, raw = self.request(path + '/scan', {'snapshot_id': snapshot},
                                      'application/vnd.apache.arrow.stream')
        self.assertEqual(status, 200, raw)
        table = pa.ipc.open_stream(raw).read_all()
        self.assertEqual(table.schema.field('amount').type, pa.decimal128(38, 4))
        self.assertEqual(table.schema.field('day').type, pa.date32())
        arrow_rows = [{**row, 'amount': None if row['amount'] is None else Decimal(row['amount']),
                       'day': None if row['day'] is None else date.fromisoformat(row['day'])}
                      for row in expected]
        self.assertEqual(table.to_pylist(), arrow_rows)
        decimal = lambda value: {'type': 'decimal', 'precision': 38, 'scale': 4, 'value': value}
        date_literal = lambda value: {'type': 'date', 'value': value}
        comparisons = [
            ('amount', 'lt', decimal('0.0000'), ['negative']),
            ('day', 'eq', date_literal('2000-02-29'), ['negative']),
            ('amount', 'eq', decimal(None), []),
            ('day', 'eq', date_literal(None), [])]
        for field, op, value, keys in comparisons:
            result = self.api(path + '/scan', {'predicate': {'op': op, 'field': field, 'value': value}})
            self.assertEqual([row['key'] for row in result['rows']], keys)
        result = self.api(path + '/scan', {'predicate': {'op': 'in', 'field': 'amount',
                                                       'values': [decimal('-123.4500'), decimal(None)]}})
        self.assertEqual([row['key'] for row in result['rows']], ['negative'])
        explanation = self.api(path + '/explain', {'predicate': {
            'op': 'eq', 'field': 'amount', 'value': decimal('-00123.450000')}})
        self.check_wire('explanation', explanation)
        self.assertEqual(explanation['predicate']['value'], decimal('-123.4500'))
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.database) as client:
            sdk_schema = client.get_schema(cid)
            self.assertEqual(sdk_schema.fields[1].precision, 38)
            self.assertEqual(sdk_schema.fields[1].scale, 4)
            self.assertEqual(client.scan_json(cid).rows, arrow_rows)
            prepared = client.prepare_append(cid, arrow_rows, operation_id=append['operation_id'])
            self.assertEqual(prepared.payload_hash, receipt['result']['payload_hash'])
            self.assertEqual(client.append(prepared).raw, receipt)
            selected = client.scan_json(cid, predicate=col('amount', 'decimal', precision=38, scale=4).lt(Decimal('0')))
            self.assertEqual([row['key'] for row in selected.rows], ['negative'])
            selected = client.scan_json(cid, predicate=col('day', 'date').eq(date(2000, 2, 29)))
            self.assertEqual([row['key'] for row in selected.rows], ['negative'])
            with client.scan_batches(cid) as stream:
                sdk_table = pa.Table.from_batches(list(stream), schema=stream.schema)
            self.assertEqual(sdk_table.to_pylist(), arrow_rows)
        inventory = self.api(path + '/files')
        self.check_wire('filesPage', inventory)
        files = inventory['items']
        self.assertEqual(len(files), 1)
        details = self.api(path + '/files/' + files[0]['file_id'])
        self.check_wire('fileDetails', details)
        for metrics in (files[0]['field_metrics'], details['row_groups'][0]['field_metrics']):
            by_name = {field['name']: field for field in metrics}
            self.assertEqual(by_name['amount']['precision'], 38)
            self.assertEqual(by_name['amount']['scale'], 4)
            self.assertEqual(by_name['amount']['lower_bound'], '-123.4500')
            self.assertEqual(by_name['amount']['upper_bound'], expected[0]['amount'])
            self.assertEqual(by_name['day']['lower_bound'], '0001-01-01')
            self.assertEqual(by_name['day']['upper_bound'], '9999-12-31')
        for value in ['+1', '1e2', ' 1', '1.', '.1', '0.00001', '1' * 39, '0' * 129, 1.25]:
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [
                {'key': 'bad', 'amount': value, 'day': '2026-11-12'}]}, status=400)
        for value in ['0000-01-01', '1900-02-29', '2026-04-31', '2026-11-12T00:00:00Z',
                      '2026-1-01', '10000-01-01', 0]:
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [
                {'key': 'bad', 'amount': '1', 'day': value}]}, status=400)
        self.api(path + '/scan', {'predicate': {'op': 'eq', 'field': 'amount', 'value': {
            'type': 'decimal', 'precision': 37, 'scale': 4, 'value': '1.0000'}}}, status=400)
        for parameters in [{'precision': 37, 'scale': 4}, {'precision': 38, 'scale': 3}]:
            self.api(path + '/scan', {'predicate': {'op': 'eq', 'field': 'amount',
                'value': {'type': 'decimal', **parameters, 'value': None}}}, status=400)
        schema_file = Path(self.tmp.name) / 'schema.json'
        schema_file.write_text(json.dumps(schema))
        cli_id = str(uuid.uuid4())
        cli_info = self.cli(['create-collection', '--database-id', self.database,
                             '--collection-id', cli_id, '--name', 'cli_scalars', '--schema', str(schema_file)])
        self.assertEqual(cli_info['schema']['fields'], info['schema']['fields'])
        append_file = Path(self.tmp.name) / 'append.json'
        append_file.write_text(json.dumps(append))
        self.cli(['append', '--database-id', self.database, '--collection-id', cli_id,
                  '--body', str(append_file)])
        cli_scan = self.cli(['scan', '--database-id', self.database, '--collection-id', cli_id])
        self.assertEqual(cli_scan['rows'], expected)
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/scan', {})['rows'], expected)
        self.assertEqual(self.api(path + '/append', append), receipt)
        recreated = self.api(self.prefix + '/collections', create)
        self.assertEqual(recreated['collection_id'], info['collection_id'])
        self.assertEqual(recreated['schema'], info['schema'])
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.database) as client:
            self.assertEqual(client.scan_json(cid).rows, arrow_rows)
            self.assertEqual(client.append(prepared).raw, receipt)


    def test_warm_scans_keep_decimal_parameters_and_native_in_matches(self):
        collections = []
        for precision, scale in [(8, 2), (38, 4)]:
            cid = str(uuid.uuid4())
            self.api(self.prefix + '/collections', {
                'collection_id': cid, 'name': f'cached_{precision}_{scale}',
                'schema': {'fields': [
                    {'name': 'key', 'type': 'string', 'nullable': False},
                    {'name': 'amount', 'type': 'decimal', 'precision': precision,
                     'scale': scale, 'nullable': True},
                    {'name': 'day', 'type': 'date', 'nullable': True}]}})
            path = self.prefix + '/collections/' + cid
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [
                {'key': 'match', 'amount': '12.34', 'day': '2000-02-29'},
                {'key': 'other', 'amount': '9.5', 'day': '1970-01-01'},
                {'key': 'null', 'amount': None, 'day': None}]})
            collections.append((path, precision, scale))
        for _ in range(2):
            for path, precision, scale in reversed(collections):
                decimal_in = {'op': 'in', 'field': 'amount', 'values': [
                    {'type': 'decimal', 'precision': precision, 'scale': scale,
                     'value': '12.34'}, None]}
                date_in = {'op': 'in', 'field': 'day', 'values': [
                    {'type': 'date', 'value': '2000-02-29'}]}
                for predicate, keys in [
                    (decimal_in, ['match']), (date_in, ['match']),
                    ({'op': 'not', 'arg': decimal_in}, []),
                    ({'op': 'not', 'arg': date_in}, ['other'])]:
                    result = self.api(path + '/scan', {'predicate': predicate})
                    self.assertEqual([row['key'] for row in result['rows']], keys)
                    for row in result['rows']:
                        expected = '12.' + '34'.ljust(scale, '0') if row['key'] == 'match' \
                            else '9.' + '5'.ljust(scale, '0')
                        self.assertEqual(row['amount'], expected)

    def test_decimal_metadata_distinguishes_parquet_and_iceberg_encodings(self):
        for precision, maximum in [(8, '12.34'), (18, '1234567890123456.78'),
                                   (38, '999999999999999999999999999999999999.99')]:
            with self.subTest(precision=precision):
                cid = str(uuid.uuid4())
                self.api(self.prefix + '/collections', {
                    'collection_id': cid, 'name': f'precision_{precision}',
                    'schema': {'fields': [{'name': 'amount', 'type': 'decimal',
                                           'precision': precision, 'scale': 2, 'nullable': True}]}})
                path = self.prefix + '/collections/' + cid
                self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                    'rows': [{'amount': '-' + maximum}, {'amount': '0.00'},
                             {'amount': maximum}, {'amount': None}]})
                files = self.api(path + '/files')['items']
                self.assertEqual(len(files), 1)
                manifest = files[0]['field_metrics'][0]
                details = self.api(path + '/files/' + files[0]['file_id'])
                self.assertEqual(len(details['row_groups']), 1)
                footer = details['row_groups'][0]['field_metrics'][0]
                self.assertEqual(manifest['bound_encoding'], 'iceberg-single-value')
                self.assertEqual(footer['bound_encoding'], 'parquet-plain')
                for metrics in (manifest, footer):
                    self.assertEqual(metrics['lower_bound'], '-' + maximum)
                    self.assertEqual(metrics['upper_bound'], maximum)
                    self.assertEqual(metrics['precision'], precision)
                    self.assertEqual(metrics['scale'], 2)


if __name__ == '__main__':
    unittest.main()
