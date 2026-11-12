"""Actual native request/engine logs and unsampled Prometheus measurements."""
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
import time
import unittest
import uuid

SERVER = sys.argv[1]
del sys.argv[1]
PROBE = None
if '--probe' in sys.argv:
    index = sys.argv.index('--probe')
    PROBE = sys.argv[index + 1]
    del sys.argv[index:index + 2]
TOKEN = 'observability-secret-token-canary'
PRIVATE = 'observability-private-payload-canary'


class ServerObservability(unittest.TestCase):
    @unittest.skipUnless(PROBE, 'pass --probe for core outcome classification')
    def test_core_unknown_failure_and_nested_request_context(self):
        result = subprocess.run([PROBE], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('private-diagnostic-canary', result.stderr)
        records = [json.loads(line) for line in result.stderr.splitlines()]
        unknown = [event for event in records if event['event'] == 'recovery_required']
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]['outcome'], 'unknown')
        self.assertEqual(unknown[0]['phase'], 'catalog_commit')
        self.assertEqual(unknown[0]['error_code'], 'IO_ERROR')
        self.assertTrue(unknown[0]['request_id'].endswith('0000000000000016'))
        read = [event for event in records if event['operation'] == 'scan_next']
        self.assertEqual(len(read), 1)
        self.assertEqual(read[0]['event'], 'storage_failed')
        self.assertTrue(read[0]['request_id'].endswith('000000000000000b'))
        self.assertTrue(any(event['error_code'] == 'OTHER' for event in records))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp'))
        self.addCleanup(self.tmp.cleanup)
        self.directory = str(Path(self.tmp.name).resolve() / 'service')
        self.proc = None
        self.records = []
        self.stderr = ''
        self.last_body_bytes = 0
        self.addCleanup(self.stop)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            self.metrics_port = listener.getsockname()[1]

    def start(self, *, full_logging=False, timeout=1000):
        options = ['--log-success-sample-every', '1', '--log-max-success-per-second', '1000'] if full_logging else []
        self.proc = subprocess.Popen([SERVER, '--data-dir', self.directory, '--port', '0',
            '--request-timeout-ms', str(timeout), '--metrics-listen-address', f'127.0.0.1:{self.metrics_port}',
            *options], env=dict(os.environ, GLAMDB_TOKEN=TOKEN), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.assertTrue(select.select([self.proc.stdout], [], [], 20)[0], 'server did not start')
        line = self.proc.stdout.readline()
        self.assertTrue(line, self.proc.stderr.read() if self.proc.poll() is not None else 'empty stdout')
        self.port = json.loads(line)['port']

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        out, self.stderr = self.proc.communicate(timeout=30)
        code, self.proc = self.proc.returncode, None
        self.assertEqual(code, 0, out + self.stderr)
        self.records = []
        for line in self.stderr.splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get('event'):
                self.records.append(record)

    def api(self, path, body=None, *, status=200, token=TOKEN):
        client = http.client.HTTPConnection('127.0.0.1', self.port, timeout=15)
        client.request('GET' if body is None else 'POST', path,
            None if body is None else json.dumps(body),
            {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'X-Request-ID': PRIVATE})
        response = client.getresponse()
        data = response.read()
        self.last_body_bytes = len(data)
        client.close()
        self.assertEqual(response.status, status, data)
        return json.loads(data)

    def scrape(self):
        client = http.client.HTTPConnection('127.0.0.1', self.metrics_port, timeout=10)
        client.request('GET', '/metrics')
        response = client.getresponse()
        data = response.read().decode()
        client.close()
        self.assertEqual(response.status, 200, data)
        return data

    def metric(self, name, **labels):
        matches = [float(line.rsplit(' ', 1)[1]) for line in self.scrape().splitlines()
            if line.startswith(name + '{') and all(f'{key}="{value}"' in line for key, value in labels.items())]
        self.assertEqual(len(matches), 1, (name, labels, matches))
        return matches[0]

    def wait_metric(self, name, expected, **labels):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.metric(name, **labels) == expected:
                return
            time.sleep(0.01)
        self.assertEqual(self.metric(name, **labels), expected)

    def test_early_denial_unknown_route_timeout_and_payload_privacy(self):
        self.start(timeout=100)
        self.api('/databases', status=401, token=PRIVATE)
        self.api('/' + PRIVATE + '?token=' + TOKEN, status=404)
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as connection:
            connection.sendall(('POST /databases HTTP/1.1\r\nHost: localhost\r\n'
                'Authorization: Bearer ' + TOKEN + '\r\nContent-Length: 2\r\n\r\n').encode())
            self.assertIn(b'408', connection.recv(4096))
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as connection:
            connection.sendall(b'GET /capabilities HTTP/1.1\r\n')
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as connection:
            connection.sendall(b'GET /capabilities HTTP/1.1\r\nHost: localhost\r\nHost: duplicate\r\n\r\n')
            response = http.client.HTTPResponse(connection)
            response.begin()
            response.read()
            self.assertEqual(response.status, 400)
        self.api('/healthz')
        self.wait_metric('glamdb_http_requests_total', 1, route='databases', method='GET', status_class='4xx')
        self.wait_metric('glamdb_http_requests_total', 1, route='other', method='GET', status_class='4xx')
        self.wait_metric('glamdb_http_requests_total', 1, route='databases', method='POST', status_class='4xx')
        self.wait_metric('glamdb_http_requests_total', 1, route='other', method='OTHER', status_class='4xx')
        self.wait_metric('glamdb_http_requests_total', 1, route='other', method='OTHER', status_class='cancelled')
        for code in ('400', '401', '404', '408'):
            self.assertEqual(self.metric('glamdb_http_responses_total', status_code=code), 1)
        self.assertNotIn('status_code="499"', self.scrape())
        self.stop()
        self.assertNotIn(TOKEN, self.stderr)
        self.assertNotIn(PRIVATE, self.stderr)
        self.assertNotIn(self.directory, self.stderr)
        requests = [event for event in self.records if event['event'] == 'request_completed']
        self.assertEqual({int(event['status_code']) for event in requests if event['outcome'] != 'ok'},
            {0, 400, 401, 404, 408})
        cancelled = [event for event in requests if event['outcome'] == 'cancelled']
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(int(cancelled[0]['byte_count']), 0)
        for event in self.records:
            self.assertRegex(event['request_id'], r'^[a-f0-9]{32}$')
        self.assertTrue(any(event['outcome'] == 'denied' for event in requests))
        self.assertTrue(any(event['event'] == 'runtime_started' for event in self.records))
        self.assertTrue(any(event['event'] == 'runtime_stopped' for event in self.records))

    def test_queued_body_cancellation_is_observed_before_running_a_handler(self):
        self.start(timeout=30000)
        held = []
        try:
            for _ in range(16):
                connection = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                held.append(connection)
                connection.sendall(('POST /databases HTTP/1.1\r\nHost: localhost\r\n'
                    'Authorization: Bearer ' + TOKEN + '\r\nContent-Length: 2\r\n'
                    'Expect: 100-continue\r\n\r\n').encode())
                self.assertIn(b'100 Continue', connection.recv(4096))
            queued = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            held.append(queued)
            queued.sendall(('POST /databases HTTP/1.1\r\nHost: localhost\r\n'
                'Authorization: Bearer ' + TOKEN + '\r\nContent-Length: 2\r\n'
                'Expect: 100-continue\r\n\r\n').encode())
            self.assertFalse(select.select([queued], [], [], .1)[0], 'queued body was admitted')
            queued.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        finally:
            for connection in held:
                connection.close()
        self.wait_metric('glamdb_http_requests_total', 17, route='databases',
            method='POST', status_class='cancelled')
        self.assertEqual(self.api('/databases'), {'items': [], 'next_cursor': None})
        self.stop()
        cancelled = [event for event in self.records if event['event'] == 'request_completed'
                     and event['outcome'] == 'cancelled']
        self.assertEqual(len(cancelled), 17)
        self.assertTrue(all(int(event['status_code']) == 0 for event in cancelled))

    def test_prometheus_counts_every_request_despite_log_sampling(self):
        self.start()
        encoded_bytes = 0
        for _ in range(150):
            self.api('/capabilities')
            encoded_bytes += self.last_body_bytes
        self.wait_metric('glamdb_http_requests_total', 150, route='capabilities', method='GET', status_class='2xx')
        self.assertEqual(self.metric('glamdb_http_responses_total', status_code='200'), 150)
        self.assertEqual(self.metric('glamdb_http_request_duration_seconds_count', route='capabilities'), 150)
        self.assertEqual(self.metric('glamdb_http_requests_in_flight', route='capabilities'), 0)
        self.assertEqual(self.metric('glamdb_http_response_bytes_total', route='capabilities'), encoded_bytes)
        self.assertNotIn(PRIVATE, self.scrape())
        self.assertNotIn(TOKEN, self.scrape())
        self.stop()
        completed = [event for event in self.records if event['event'] == 'request_completed']
        self.assertEqual(len({event['request_id'] for event in completed}), len(completed))
        self.assertGreaterEqual(len(completed), 1)
        self.assertLess(len(completed), 150)

    def test_real_append_retry_scan_recovery_and_request_correlation(self):
        self.start(full_logging=True)
        database = self.api('/databases', {'name': PRIVATE})
        base = '/databases/' + database['database_id']
        collection = self.api(base + '/collections', {'collection_id': str(uuid.uuid4()), 'name': PRIVATE,
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
        prefix = base + '/collections/' + collection['collection_id']
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': PRIVATE}, {'key': PRIVATE}]}
        first = self.api(prefix + '/append', body)
        self.assertEqual(self.api(prefix + '/append', body), first)
        cursor = self.api(prefix + '/scan/open', {})['cursor']
        page = self.api(prefix + '/scan', {'cursor': cursor})
        self.assertEqual(len(page['rows']), 2)
        self.api(prefix + '/scan/cancel', {'cursor': cursor})
        self.api(base + '/reconcile', {})
        self.assertEqual(self.metric('glamdb_rows_published_total', service_instance='core'), 2)
        self.assertGreater(self.metric('glamdb_data_bytes_published_total', service_instance='core'), 0)
        self.assertGreaterEqual(self.metric('glamdb_read_operations_total', operation='scan_next', outcome='ok'), 1)
        self.assertEqual(self.metric('glamdb_read_rows_total', service_instance='core'), 2)
        self.assertEqual(self.metric('glamdb_log_dropped_unregistered_total', service_instance='core'), 0)
        for secret in (PRIVATE, TOKEN, database['database_id'], collection['collection_id'], body['operation_id']):
            self.assertNotIn(secret, self.scrape())
        self.stop()
        self.assertNotIn(PRIVATE, self.stderr)
        self.assertNotIn(TOKEN, self.stderr)
        for secret in (self.directory, database['database_id'], collection['collection_id'], body['operation_id']):
            self.assertNotIn(secret, self.stderr)
        requests = [event for event in self.records if event['event'] == 'request_completed']
        self.assertEqual(len(requests), 8)
        self.assertEqual(len({event['request_id'] for event in requests}), 8)
        self.assertEqual({event['operation'] for event in requests},
            {'create_store', 'create_collection', 'append', 'scan_open', 'scan', 'scan_cancel', 'database'})
        core = [event for event in self.records if event['event'] == 'operation_completed' and event['operation'] == 'append']
        http = [event for event in self.records if event['event'] == 'request_completed' and event['operation'] == 'append']
        self.assertEqual(len(core), 2)
        self.assertEqual(len(http), 2)
        self.assertEqual({event['request_id'] for event in core}, {event['request_id'] for event in http})
        self.assertTrue(all(event['phase'] == 'complete' for event in core))
        self.assertTrue(all(int(event['byte_count']) > 0 for event in http))
        self.assertTrue(any(event['operation'] == 'reconcile' for event in self.records))


if __name__ == '__main__':
    unittest.main()
