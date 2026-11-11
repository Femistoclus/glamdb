"""Native HTTP coverage for persistent, validated engine resource limits."""
import http.client
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid

SERVER = sys.argv[1]
del sys.argv[1]
DEFAULTS = {'max_operations': 4096, 'max_collections': 8, 'max_catalog_bytes': 268435456,
            'max_pending_calls': 16, 'max_owned_input_bytes': 134217728,
            'writer_wait_ms': 5000, 'append_deadline_ms': 60000}


class DatabaseLimits(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.directory = str(Path(self.tmp.name).resolve() / 'service')
        self.proc = None
        self.addCleanup(self.stop)
        self.start()

    def start(self):
        self.proc = subprocess.Popen([SERVER, '--data-dir', self.directory, '--port', '0'],
            env=dict(os.environ, GLAMDB_TOKEN='limits-test'), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.assertTrue(select.select([self.proc.stdout], [], [], 15)[0])
        line = self.proc.stdout.readline()
        self.assertTrue(line, self.proc.stderr.read() if self.proc.poll() is not None else 'no server output')
        self.port = json.loads(line)['port']

    def stop(self):
        if self.proc is not None:
            if self.proc.poll() is None:
                self.proc.terminate()
            out, err = self.proc.communicate(timeout=30)
            self.assertEqual(self.proc.returncode, 0, out + err)
            self.proc = None

    def api(self, path, body=None, status=200):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        headers = {'Authorization': 'Bearer limits-test', 'Content-Type': 'application/json'}
        connection.request('GET' if body is None else 'POST', path,
                           None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(response.status, status, data)
        return json.loads(data)

    def collection(self, base, name, status=200):
        return self.api(base + '/collections', {'collection_id': str(uuid.uuid4()), 'name': name,
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}, status)

    def test_limits_apply_and_survive_closed_restart_and_explicit_reopen(self):
        limits = dict(DEFAULTS, max_collections=1, max_operations=1, writer_wait_ms=0)
        database = self.api('/databases', {'name': 'limited', 'limits': {
            'max_collections': 1, 'max_operations': 1, 'writer_wait_ms': 0}})
        self.assertEqual(database['limits'], limits)
        base = '/databases/' + database['database_id']
        first = self.collection(base, 'one')
        # Exhausted catalog capacity needs operator action: 507, not a 429 retry signal.
        rejected = self.collection(base, 'two', 507)
        self.assertEqual(rejected['error']['code'], 'RESOURCE_EXHAUSTED')
        self.assertEqual(rejected['error']['retry_hint'], 'OperatorAction')
        append = base + '/collections/' + first['collection_id'] + '/append'
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'one'}]})
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'two'}]}, 507)
        self.assertEqual(self.api('/databases', {'name': 'limited'})['limits'], limits)
        self.api('/databases', {'name': 'limited', 'limits': {'max_collections': 2}}, 409)
        self.api(base + '/open', {'limits': {'max_collections': 2}}, 412)
        self.assertEqual(self.api(base + '/close', {})['limits'], limits)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['state'], 'closed')
        self.assertEqual(self.api(base)['limits'], limits)
        changed = self.api(base + '/open', {'limits': {'max_collections': 2, 'max_operations': 2}})
        limits.update(max_collections=2, max_operations=2)
        self.assertEqual(changed['limits'], limits)
        self.collection(base, 'two')
        self.api(append, {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'two'}]})
        self.assertEqual(self.api(base + '/open', {'limits': limits})['limits'], limits)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['limits'], limits)
        self.assertEqual(self.api(base)['state'], 'open')

    def test_invalid_limits_do_not_reserve_names_or_change_open_database(self):
        bad = [None, [], {'unknown': 1}, {'max_operations': True}, {'max_operations': 1.0},
               {'max_operations': '1'}, {'max_operations': 0}, {'max_operations': 4097},
               {'max_collections': 9}, {'max_catalog_bytes': 65535},
               {'max_pending_calls': 17}, {'max_owned_input_bytes': 0},
               {'writer_wait_ms': -1}, {'writer_wait_ms': 5001},
               {'append_deadline_ms': 0}, {'append_deadline_ms': 60001},
               {'max_operations': 18446744073709551615}]
        for limits in bad:
            with self.subTest(limits=limits):
                self.api('/databases', {'name': 'invalid', 'limits': limits}, 400)
                self.assertEqual(self.api('/databases')['items'], [])
        database = self.api('/databases', {'name': 'invalid'})
        self.assertEqual(database['limits'], DEFAULTS)
        base = '/databases/' + database['database_id']
        self.api(base + '/open', {'limits': {'writer_wait_ms': -1}}, 400)
        self.assertEqual(self.api(base)['state'], 'open')
        self.assertEqual(self.api(base)['limits'], DEFAULTS)
        self.collection(base, 'still-usable')

    def test_failed_open_preserves_requested_limits_for_recovery(self):
        database = self.api('/databases', {'name': 'reconfigure'})
        base = '/databases/' + database['database_id']
        self.collection(base, 'one')
        self.collection(base, 'two')
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'max_collections': 1}}, 400)
        failed = self.api(base)
        self.assertEqual(failed['state'], 'error')
        self.assertEqual(failed['limits']['max_collections'], 1)
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['state'], 'error')
        self.assertEqual(self.api(base)['limits']['max_collections'], 1)
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'max_collections': 2}})
        self.assertEqual(len(self.api(base + '/collections')['items']), 2)

    def test_new_registry_has_complete_schema_without_migration(self):
        database = self.api('/databases', {'name': 'complete'})
        base = '/databases/' + database['database_id']
        self.api(base + '/close', {})
        self.stop()
        with sqlite3.connect(Path(self.directory) / 'registry.sqlite') as registry:
            self.assertEqual(registry.execute('PRAGMA user_version').fetchone()[0], 2)
            limits = registry.execute('SELECT limits FROM databases').fetchone()[0]
            self.assertEqual(json.loads(limits), DEFAULTS)
        self.start()
        self.assertEqual(self.api(base)['state'], 'closed')
        self.assertEqual(self.api(base)['limits'], DEFAULTS)


if __name__ == '__main__':
    unittest.main()
