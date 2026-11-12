"""Exercise structured events from the actual server and offline administrator."""
import http.client
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid

SERVER, ADMIN = sys.argv[1:3]
del sys.argv[1:3]
TOKEN = 'logging-secret-token'


class ServerLogging(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.log = tempfile.TemporaryFile(mode='w+')
        self.addCleanup(self.log.close)

    def start(self, *options, sink=None):
        self.process = subprocess.Popen(
            [SERVER, '--data-dir', str(Path(self.tmp.name) / 'server'), '--port', '0',
             '--request-timeout-ms', '100', *options],
            env=dict(os.environ, GLAMDB_TOKEN=TOKEN), stdout=subprocess.PIPE,
            stderr=self.log if sink is None else sink, text=True)
        self.addCleanup(self.cleanup_process)
        self.assertTrue(select.select([self.process.stdout], [], [], 20)[0])
        line = self.process.stdout.readline()
        if not line:
            self.log.seek(0)
            self.fail('server must start: ' + self.log.read())
        self.port = json.loads(line)['port']

    def cleanup_process(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)

    def stop(self):
        self.process.terminate()
        out, _ = self.process.communicate(timeout=20)
        self.assertEqual(self.process.returncode, 0)
        self.assertEqual(out, '')
        self.log.seek(0)
        text = self.log.read()
        self.assertNotIn(TOKEN, text)
        self.assertNotIn('private-marker', text)
        return [json.loads(line) for line in text.splitlines()]

    def request(self, target, body=None, token=TOKEN):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=10)
        connection.request('GET' if body is None else 'POST', target,
                           None if body is None else json.dumps(body),
                           {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
                            'X-Request-ID': 'private-marker', 'Cookie': 'private-marker'})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, json.loads(data)

    def test_lifecycle_http_errors_redaction_and_success_sampling(self):
        self.start()
        for _ in range(3):
            self.assertEqual(self.request('/healthz')[0], 200)
        self.assertEqual(self.request('/databases', token='private-marker')[0], 401)
        self.assertEqual(self.request('/private-marker?secret=private-marker')[0], 404)
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall(b'GET / HTTP/1.1\r\nHost: private-marker\r\nBadHeader\r\n\r\n')
            self.assertIn(b' 400 ', stream.recv(4096))
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall(b'GET /healthz HTTP/1.1\r\n')
            self.assertIn(b' 408 ', stream.recv(4096))
        records = self.stop()
        self.assertEqual(sum(e['event'] == 'runtime_started' for e in records), 1)
        self.assertEqual(sum(e['event'] == 'runtime_stopped' for e in records), 1)
        requests = [e for e in records if e['event'] == 'request_completed']
        self.assertCountEqual([int(e['status_code']) for e in requests], [200, 401, 404, 400, 408])
        self.assertEqual(len({e['request_id'] for e in requests}), len(requests))
        self.assertEqual(next(e for e in requests if int(e['status_code']) == 200)['operation'], 'health')
        self.assertEqual(next(e for e in requests if int(e['status_code']) == 401)['outcome'], 'denied')
        for event in records:
            self.assertRegex(event['request_id'], r'^[a-f0-9]{32}$')
            self.assertEqual(str(event['schema']), '1')

    def test_real_append_scan_and_corruption_emit_correlated_core_events(self):
        self.start('--log-success-sample-every', '1', '--log-max-success-per-second', '1000000')
        status, database = self.request('/databases', {'name': 'private-marker'})
        self.assertEqual(status, 200)
        base = '/databases/' + database['database_id']
        cid = str(uuid.uuid4())
        self.assertEqual(self.request(base + '/collections', {'collection_id': cid, 'name': 'private-marker',
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})[0], 200)
        collection = base + '/collections/' + cid
        self.assertEqual(self.request(collection + '/append', {'operation_id': str(uuid.uuid4()),
            'rows': [{'key': 'private-marker'}]})[0], 200)
        self.assertEqual(self.request(collection + '/scan', {})[0], 200)
        files = list(Path(self.tmp.name).rglob('*.parquet'))
        self.assertTrue(files)
        files[0].write_bytes(b'corrupt')
        self.assertGreaterEqual(self.request(collection + '/scan', {})[0], 400)
        records = self.stop()
        requests = [e for e in records if e['event'] == 'request_completed']
        self.assertTrue(any(e['operation'] == 'append' and e['outcome'] == 'ok' for e in requests))
        self.assertTrue(any(e['operation'] == 'scan' and e['outcome'] == 'ok' for e in requests))
        # Shutdown may independently retry recovery and report an operation=other failure.
        failures = [e for e in records if e['event'] == 'storage_failed' and e['operation'] == 'scan']
        self.assertTrue(failures, records)
        self.assertTrue(any(e['event'] == 'recovery_required' for e in records))
        for event in failures:
            self.assertTrue(any(r['request_id'] == event['request_id'] and r['operation'] == 'scan'
                                and r['outcome'] == 'error' for r in requests), event)

    def test_disconnected_session_is_logged_once_without_inventing_http_success(self):
        self.start()
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall(b'GET /private-marker HTTP/1.1\r\n')
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        self.assertEqual(self.request('/healthz')[0], 200)
        records = self.stop()
        cancelled = [e for e in records if e['event'] == 'request_completed' and e['outcome'] == 'cancelled']
        self.assertEqual(len(cancelled), 1, records)
        self.assertEqual(int(cancelled[0]['status_code']), 0)
        self.assertEqual(sum(e['request_id'] == cancelled[0]['request_id'] for e in records), 1)

    def test_reconcile_and_reopen_failures_emit_correlated_core_events(self):
        self.start('--log-success-sample-every', '1', '--log-max-success-per-second', '1000000')
        status, database = self.request('/databases', {'name': 'recovery'})
        self.assertEqual(status, 200)
        base = '/databases/' + database['database_id']
        cid = str(uuid.uuid4())
        self.assertEqual(self.request(base + '/collections', {'collection_id': cid, 'name': 'events',
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})[0], 200)
        self.assertEqual(self.request(base + '/collections/' + cid + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'private-marker'}]})[0], 200)
        files = list(Path(self.tmp.name).rglob('*.parquet'))
        self.assertTrue(files)
        files[0].write_bytes(b'corrupt')
        status, response = self.request(base + '/reconcile', {})
        self.assertEqual(status, 500)
        self.assertEqual(response['error']['code'], 'CORRUPTION')
        records = self.stop()
        completion = next(e for e in records if e['event'] == 'request_completed'
                          and e['operation'] == 'database' and int(e['status_code']) == 500)
        correlated = [e['event'] for e in records if e['request_id'] == completion['request_id']]
        self.assertIn('storage_failed', correlated)
        self.assertIn('recovery_required', correlated)
        # Managed startup reopens the same corrupt database through EngineFactory.
        self.log.seek(0)
        self.log.truncate()
        self.start()
        self.assertEqual(self.request(base + '/readyz')[0], 503)
        reopened = self.stop()
        self.assertTrue(any(e['event'] == 'storage_failed' for e in reopened), reopened)
        self.assertTrue(any(e['event'] == 'recovery_required' for e in reopened), reopened)

    def test_failed_startup_records_error_lifecycle(self):
        env = dict(os.environ)
        env.pop('GLAMDB_TOKEN', None)
        result = subprocess.run([SERVER, '--data-dir', str(Path(self.tmp.name) / 'server'), '--port', '0'],
                                env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, '')
        records = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{')]
        self.assertEqual([e['event'] for e in records], ['runtime_started', 'runtime_stopped'])
        self.assertEqual(records[-1]['outcome'], 'error')

    @unittest.skipUnless(Path('/dev/full').exists(), 'Linux ENOSPC sink')
    def test_logging_sink_failure_preserves_server_requests_and_exit(self):
        with open('/dev/full', 'w') as sink:
            self.start(sink=sink)
            self.assertEqual(self.request('/healthz')[0], 200)
            self.stop()

    @unittest.skipIf(ADMIN == "-", "offline admin is not built")
    def test_admin_quiesce_and_missing_store_preserve_stdout_and_classification(self):
        root = str(Path(self.tmp.name) / 'store')
        store = str(uuid.uuid4())
        for command in ('create-store', 'inspect-store', 'quiesce-store'):
            result = subprocess.run([ADMIN, command, '--root', root, '--store-id', store],
                                    input='', text=True, capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['store_id'], store)
            records = [json.loads(line) for line in result.stderr.splitlines()]
            event = next(e for e in records if e['event'] == 'request_completed')
            self.assertEqual(event['operation'], command.replace('-', '_'))
            self.assertEqual(event['outcome'], 'ok')
        result = subprocess.run([ADMIN, 'read', '--root', root + '-missing', '--store-id', store,
                                 '--collection-id', str(uuid.uuid4())], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, '')
        records = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{')]
        self.assertTrue(any(e['event'] == 'request_completed' and e['operation'] == 'scan'
                            and e['outcome'] == 'error' for e in records))


if __name__ == '__main__':
    unittest.main()
