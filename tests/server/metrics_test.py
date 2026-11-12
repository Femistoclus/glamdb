"""Private native metrics must observe the actual engines and durable operations."""
import http.client
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

SERVER = sys.argv[1]
del sys.argv[1]


class ServerMetrics(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp'))
        self.addCleanup(self.tmp.cleanup)
        self.directory = str(Path(self.tmp.name).resolve() / 'service')
        self.proc = None
        self.addCleanup(self.stop)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            self.metrics_port = listener.getsockname()[1]

    def start(self, metrics=True, *extra):
        options = ['--metrics-listen-address', f'127.0.0.1:{self.metrics_port}'] if metrics else []
        self.proc = subprocess.Popen([SERVER, '--data-dir', self.directory, '--port', '0', *options, *extra],
            env=dict(os.environ, GLAMDB_TOKEN='metrics-test'), stdout=subprocess.PIPE,
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
            code = self.proc.returncode
            self.proc = None
            self.assertEqual(code, 0, out + err)

    def api(self, path, body=None, status=200, token='metrics-test'):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        connection.request('GET' if body is None else 'POST', path,
            None if body is None else json.dumps(body),
            {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(response.status, status, data)
        return json.loads(data)

    def scrape(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.metrics_port, timeout=5)
        connection.request('GET', '/metrics')
        response = connection.getresponse()
        data = response.read().decode()
        connection.close()
        self.assertEqual(response.status, 200, data)
        return data

    def value(self, name, **labels):
        matches = [line.rsplit(' ', 1)[1] for line in self.scrape().splitlines()
            if line.startswith(name + '{') and all(f'{key}="{value}"' in line for key, value in labels.items())]
        self.assertEqual(len(matches), 1, matches)
        return float(matches[0])

    def ready(self):
        return self.value('glamdb_server_ready', service_instance='core')

    def collection(self, base, name):
        return self.api(base + '/collections', {'collection_id': str(uuid.uuid4()), 'name': name,
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})

    def wait_value(self, name, expected, **labels):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if self.value(name, **labels) == expected:
                return
            time.sleep(0.01)
        self.assertEqual(self.value(name, **labels), expected)

    def test_http_metrics_register_idle_families_and_observe_real_auth_status(self):
        self.start()
        self.assertEqual(self.value('glamdb_http_request_duration_seconds_count', route='append'), 0)
        self.assertEqual(self.value('glamdb_http_requests_in_flight', route='append'), 0)
        self.api('/databases', status=401, token='private-token-marker')
        self.api('/databases')
        self.wait_value('glamdb_http_requests_total', 1, route='databases', method='GET', status_class='4xx')
        self.wait_value('glamdb_http_requests_total', 1, route='databases', method='GET', status_class='2xx')
        self.wait_value('glamdb_http_responses_total', 1, status_code='401')
        self.wait_value('glamdb_http_responses_total', 1, status_code='200')
        self.assertGreater(self.value('glamdb_http_request_duration_seconds_sum', route='databases'), 0)
        self.assertNotIn('private-token-marker', self.scrape())
        self.assertEqual(self.value('glamdb_http_requests_in_flight', route='databases'), 0)

    def test_http_scope_covers_incomplete_body_and_cancellation(self):
        self.start()
        with socket.create_connection(('127.0.0.1', self.port)) as client:
            client.sendall(b'POST /databases HTTP/1.1\r\nHost: localhost\r\n'
                b'Authorization: Bearer metrics-test\r\nContent-Type: application/json\r\n'
                b'Content-Length: 100\r\n\r\n{')
            self.wait_value('glamdb_http_requests_in_flight', 1, route='databases')
            time.sleep(0.05)
        self.wait_value('glamdb_http_requests_in_flight', 0, route='databases')
        self.assertEqual(self.value('glamdb_http_requests_total', route='databases', method='POST',
            status_class='cancelled'), 1)
        self.assertGreaterEqual(self.value('glamdb_http_request_duration_seconds_sum', route='databases'), 0.05)

    def test_http_scope_retains_inflight_until_response_body_finishes(self):
        ui = Path(self.tmp.name).resolve() / 'ui'
        (ui / 'assets').mkdir(parents=True)
        (ui / 'assets/large.js').write_bytes(b' ' * (8 * 1024 * 1024))
        self.start(True, '--ui-root', str(ui))
        with socket.socket() as client:
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            client.connect(('127.0.0.1', self.port))
            client.sendall(b'GET /assets/large.js?private-url-marker=1 HTTP/1.1\r\nHost: localhost\r\n\r\n')
            self.wait_value('glamdb_http_requests_in_flight', 1, route='static')
            time.sleep(0.05)
            self.assertEqual(self.value('glamdb_http_requests_in_flight', route='static'), 1)
        self.wait_value('glamdb_http_requests_in_flight', 0, route='static')
        self.assertEqual(self.value('glamdb_http_requests_total', route='static', method='GET',
            status_class='cancelled'), 1)
        self.assertNotIn('private-url-marker', self.scrape())

    def test_opt_in_logs_use_bounded_schema_for_real_requests_and_lifecycle(self):
        logs = Path(self.tmp.name).resolve() / 'logs'
        self.start(True, '--log-directory', str(logs), '--log-success-sample-every', '1')
        self.api('/databases', status=401, token='private-token-marker')
        database = self.api('/databases', {'name': 'private-database-marker'})
        base = '/databases/' + database['database_id']
        collection = self.collection(base, 'private-collection-marker')
        self.api(base + '/collections/' + collection['collection_id'] + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'private-payload-marker'}]})
        self.assertGreater(self.value('glamdb_log_accepted_total', service_instance='core'), 0)
        self.assertEqual(self.value('glamdb_log_dropped_unregistered_total', service_instance='core'), 0)
        self.stop()
        files = list(logs.glob('glamdb*.jsonl'))
        self.assertTrue(files)
        text = ''.join(path.read_text() for path in files)
        records = [json.loads(line) for line in text.splitlines()]
        self.assertTrue(all(str(record['schema']) == '1' for record in records))
        self.assertTrue(all(len(record['request_id']) == 32 for record in records))
        self.assertTrue(all(len(json.dumps(record).encode()) <= 1024 for record in records))
        events = {record['event'] for record in records}
        self.assertTrue({'runtime_started', 'runtime_stopped', 'request_completed'} <= events)
        self.assertTrue(any(record['operation'] == 'auth' and record['outcome'] == 'denied' for record in records))
        self.assertTrue(any(record['operation'] == 'append' and int(record['status_code']) == 200 for record in records))
        for private in ('private-token-marker', 'private-database-marker', 'private-collection-marker',
                        'private-payload-marker', database['database_id'], collection['collection_id'], self.directory):
            self.assertNotIn(private, text)

    def test_quill_rotation_keeps_file_count_and_size_bounded(self):
        logs = Path(self.tmp.name).resolve() / 'logs'
        logs.mkdir()
        limit = 8 * 1024 * 1024
        for name in ('glamdb.jsonl', 'glamdb.1.jsonl', 'glamdb.2.jsonl', 'glamdb.3.jsonl', 'glamdb.4.jsonl'):
            (logs / name).write_bytes(b'{}\n' * (limit // 3))
        self.start(True, '--log-directory', str(logs), '--log-success-sample-every', '1')
        self.api('/databases', status=401, token='denied')
        self.stop()
        files = list(logs.glob('glamdb*.jsonl'))
        self.assertLessEqual(len(files), 4)
        self.assertTrue(any(path.name != 'glamdb.jsonl' for path in files))
        self.assertTrue(all(path.stat().st_size <= limit + 1024 for path in files))
        self.assertIn('runtime_started', (logs / 'glamdb.jsonl').read_text())

    def test_default_does_not_publish_metrics_on_http(self):
        self.start(metrics=False)
        self.api('/metrics', status=404)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', self.metrics_port))

    def test_private_listener_tracks_real_operations_health_restart_and_shutdown(self):
        self.start()
        self.api('/metrics', status=404)
        self.assertEqual(self.ready(), 0)
        database = self.api('/databases', {'name': 'metrics-private-name'})
        base = '/databases/' + database['database_id']
        self.assertEqual(self.ready(), 1)
        collection = self.collection(base, 'metrics-private-collection')
        append = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'first'}, {'key': 'second'}]}
        self.api(base + '/collections/' + collection['collection_id'] + '/append', append)
        self.api(base + '/collections/' + collection['collection_id'] + '/schema')
        self.api(base + '/collections/' + collection['collection_id'] + '/scan/open', {})
        self.wait_value('glamdb_http_requests_total', 1, route='collection', method='GET', status_class='2xx')
        self.wait_value('glamdb_http_requests_total', 1, route='scan', method='POST', status_class='2xx')
        self.assertEqual(self.value('glamdb_operations_total', service_instance='core',
            operation='create_collection', outcome='committed'), 1)
        self.assertEqual(self.value('glamdb_operations_total', service_instance='core',
            operation='append', outcome='committed'), 1)
        for private in ('metrics-private-name', 'metrics-private-collection', database['database_id'],
                        collection['collection_id'], append['operation_id'], self.directory):
            self.assertNotIn(private, self.scrape())
        self.api(base + '/close', {})
        self.assertEqual(self.ready(), 0)
        self.api(base + '/open', {})
        self.assertEqual(self.ready(), 1)
        self.stop()
        with self.assertRaises(OSError):
            self.scrape()
        self.start()
        self.assertEqual(self.ready(), 1)
        self.assertEqual(len(self.api(base + '/collections')['items']), 1)

    def test_one_failed_database_closes_service_readiness(self):
        self.start()
        self.api('/databases', {'name': 'healthy'})
        database = self.api('/databases', {'name': 'will-fail-open'})
        base = '/databases/' + database['database_id']
        self.collection(base, 'one')
        self.collection(base, 'two')
        self.assertEqual(self.ready(), 1)
        self.api(base + '/close', {})
        self.assertEqual(self.ready(), 1)
        self.api(base + '/open', {'limits': {'max_collections': 1}}, status=400)
        self.assertEqual(self.api(base)['state'], 'error')
        self.assertEqual(self.ready(), 0)
        self.api(base + '/close', {})
        self.api(base + '/open', {'limits': {'max_collections': 2}})
        self.assertEqual(self.ready(), 1)


if __name__ == '__main__':
    unittest.main()
