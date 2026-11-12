"""Initialized managed database cleanup against real native storage."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
import uuid

import native_http_test as native


class DatabaseDrop(unittest.TestCase):
    setUp = native.NativeHTTP.setUp
    start = native.NativeHTTP.start
    stop = native.NativeHTTP.stop
    request = native.NativeHTTP.request
    api = native.NativeHTTP.api

    def test_eight_tables_drop_reload_and_exact_retry(self):
        base = '/databases/' + self.store_id
        tables = ('region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem')
        schema = {'fields': [{'name': 'key', 'type': 'string'}, {'name': 'n', 'type': 'int64'}]}
        for table in tables:
            collection = str(uuid.uuid4())
            self.api(base + '/collections', {'collection_id': collection, 'name': table, 'schema': schema})
            path = base + '/collections/' + collection
            rows = [{'key': table, 'n': '153'}, {'key': table, 'n': '153'}]
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
            self.assertEqual(self.api(path + '/scan', {})['rows'], rows)
        root = Path(self.root)
        self.assertEqual(len(list(root.rglob('*.parquet'))), 8)
        self.assertTrue(self.api('/capabilities')['features']['database_drop'])
        self.assertTrue(self.api(base + '/capabilities')['features']['database_drop'])
        self.api(base + '/discard', {}, status=412)
        other = self.api('/databases', {'name': 'unrelated'})['database_id']
        state = Path(self.tmp.name) / 'tpch-run.json'
        state.write_text(json.dumps({'version': 1, 'url': f'http://127.0.0.1:{self.port}',
                                     'database_id': self.store_id, 'store_id': self.store_id}))
        env = dict(self.env, GLAMDB_PYTHON=sys.executable)
        clean = subprocess.run([native.CLI, 'bench', 'tpch', 'clean', '--state', str(state)],
                               env=env, capture_output=True, text=True, timeout=65)
        self.assertEqual(clean.returncode, 0, clean.stderr + clean.stdout)
        self.assertEqual(json.loads(clean.stdout), {'database_id': self.store_id,
                                                  'store_id': self.store_id, 'dropped': True})
        self.assertFalse(root.exists())
        self.assertEqual(self.api('/databases/' + other + '/collections')['items'], [])
        with sqlite3.connect(Path(self.data_dir) / 'registry.sqlite') as registry:
            self.assertEqual(registry.execute('SELECT count(*) FROM databases WHERE id=?', (self.store_id,)).fetchone()[0], 0)
        replacement = self.api('/databases', {'name': 'test_database'})['database_id']
        self.assertNotEqual(replacement, self.store_id)
        for table in tables:
            collection = str(uuid.uuid4())
            path = '/databases/' + replacement + '/collections/' + collection
            self.api('/databases/' + replacement + '/collections',
                     {'collection_id': collection, 'name': table, 'schema': schema})
            rows = [{'key': 'fresh-' + table, 'n': '154'}]
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
            self.assertEqual(self.api(path + '/scan', {})['rows'], rows)
        original = {'store_id': self.store_id}
        self.assertTrue(self.api(base + '/drop', original)['dropped'])
        self.stop()
        self.start()
        self.assertTrue(self.api(base + '/drop', original)['dropped'])
        self.assertEqual(len(self.api('/databases/' + replacement + '/collections')['items']), 8)
        self.assertFalse(root.exists())

    def test_drop_requires_authentication_and_original_identity(self):
        path = '/databases/' + self.store_id + '/drop'
        self.assertEqual(self.request(path, {'store_id': self.store_id}, token=None)[0], 401)
        for body in ({}, {'store_id': None}, {'store_id': 'bad'},
                     {'store_id': self.store_id, 'path': '/tmp/unrelated'}):
            self.api(path, body, status=400)
        self.api(path, {'store_id': str(uuid.uuid4())}, status=412)
        self.assertTrue(Path(self.root).exists())
        self.assertEqual(self.api('/databases/' + self.store_id)['state'], 'open')

    def test_drop_cancels_active_scan_and_releases_its_store_lease(self):
        base = '/databases/' + self.store_id
        collection = str(uuid.uuid4())
        self.api(base + '/collections', {'collection_id': collection, 'name': 'scan-pinned',
                 'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
        path = base + '/collections/' + collection
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'old'}]})
        cursor = self.api(path + '/scan/open', {})['cursor']
        self.assertTrue(cursor)
        self.assertTrue(self.api(base + '/drop', {'store_id': self.store_id})['dropped'])
        self.assertFalse(Path(self.root).exists())
        self.api(path + '/scan', {'cursor': cursor}, status=404)
        replacement = self.api('/databases', {'name': 'test_database'})['database_id']
        self.api('/databases/' + replacement + '/collections/' + collection + '/scan',
                 {'cursor': cursor}, status=404)


if __name__ == '__main__':
    unittest.main()
