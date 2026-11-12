"""Remote bulk uses real native storage, sockets, CLI and fresh-process reopen."""
import http.client
import json
import socket
import sys
import time
import unittest
import uuid
from pathlib import Path
from native_http_test import NativeHTTP, TOKEN, CLI

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
from glamdb_sdk import Client


class BulkHTTP(NativeHTTP):
    def wire(self, operation, batches, rows, **extra):
        return (json.dumps({'operation_id': operation, 'rows': str(rows), **extra}) + '\n' +
                ''.join(json.dumps({'rows': batch}) + '\n' for batch in batches)).encode()

    def upload(self, path, body, status=200, content_type='application/x-ndjson', *, retry_busy=False):
        expires = time.monotonic() + 5
        while True:
            conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=120)
            conn.request('POST', '/databases/' + self.store_id + path + '/bulk-append', body,
                         {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': content_type})
            response = conn.getresponse()
            raw = response.read()
            conn.close()
            value = json.loads(raw)
            if retry_busy and response.status == 503 and value.get('error', {}).get('code') == 'SERVER_BUSY':
                self.assertIs(value.get('execution_started'), False)
                if time.monotonic() < expires:
                    time.sleep(0.01)
                    continue
            self.assertEqual(response.status, status, raw)
            return value

    @unittest.skipUnless(Path('/proc/mounts').exists() and ' /dev/shm tmpfs ' in Path('/proc/mounts').read_text(),
                         'requires a known Linux tmpfs')
    def test_bulk_refuses_memory_backed_temporary_storage(self):
        path = self.collection()
        operation = str(uuid.uuid4())
        self.stop()
        self.env['TMPDIR'] = '/dev/shm'
        self.start()
        self.upload(path, self.wire(operation, [[{'key': 'memory'}]], 1), 507)
        self.api(path + '/operations/' + operation, status=404)
        self.assertEqual(self.api('/healthz')['status'], 'live')

    def test_bulk_media_type_is_case_insensitive(self):
        path = self.collection()
        body = self.wire(str(uuid.uuid4()), [[{'key': 'case'}]], 1)
        self.upload(path, body, content_type='Application/X-NDJSON')

    def test_bulk_upload_atomic_retry_and_reopen(self):
        path = self.collection()
        operation = str(uuid.uuid4())
        rows = [{'key': 'dup', 'n': str(index % 5), 'ts': None} for index in range(12001)]
        body = self.wire(operation, [rows[:6000], rows[6000:]], len(rows))
        receipt = self.upload(path, body)
        self.assertEqual(receipt['outcome'], 'committed')
        self.assertEqual(receipt['result']['rows_appended'], '12001')
        self.assertEqual(receipt['result']['receipt_version'], 2)
        # Chunk boundaries and a now-stale precondition do not alter identity.
        self.assertEqual(self.upload(path, self.wire(operation, [rows[:1], rows[1:6000], rows[6000:]],
                                                    len(rows), expected_snapshot=None)), receipt)
        self.assertEqual(self.api(path + '/scan', {'snapshot_id': 'initial'})['rows'], [])
        self.assertEqual(self.api(path + '/stats')['rows'], '12001')
        received = []
        page = self.api(path + '/scan', {'batch_rows': 1000})
        while True:
            received.extend(page['rows'])
            if page['next_cursor'] is None:
                break
            page = self.api(path + '/scan', {'cursor': page['next_cursor']})
        from collections import Counter
        self.assertEqual(Counter(tuple(sorted(row.items())) for row in received),
                         Counter(tuple(sorted(row.items())) for row in rows))
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + operation), receipt)
        self.assertEqual(self.upload(path, body), receipt)
        rows[0]['n'] = '42'
        conflict = self.upload(path, self.wire(operation, [rows[:6000], rows[6000:]], len(rows)), 409)
        self.assertEqual(conflict['error']['code'], 'OPERATION_CONFLICT')

    def test_bulk_lost_reply_returns_original_receipt(self):
        path = self.collection()
        operation = str(uuid.uuid4())
        body = self.wire(operation, [[{'key': 'lost'}] * 6000] * 2, 12000)
        with socket.create_connection(('127.0.0.1', self.port), timeout=10) as stream:
            stream.sendall((f'POST /databases/{self.store_id}{path}/bulk-append HTTP/1.1\r\nHost: local\r\n'
                            f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/x-ndjson\r\n'
                            f'Content-Length: {len(body)}\r\n\r\n').encode() + body)
            stream.shutdown(socket.SHUT_WR)
            # Deliberately never consume the POST response. Observe the authoritative receipt.
            for _ in range(200):
                status, _, raw = self.request(path + '/operations/' + operation)
                if status == 200:
                    original = json.loads(raw)
                    break
                self.assertIn(status, (404, 429, 503), raw)
                time.sleep(0.01)
            else:
                self.fail('bulk operation did not finish')
        self.assertEqual(self.upload(path, body), original)
        self.assertEqual(self.api(path + '/stats')['rows'], '12000')

    def test_bulk_upload_limits_preserve_control_admission(self):
        path = self.collection()
        second = self.api('/databases', {'name': 'second'})['database_id']
        third = self.api('/databases', {'name': 'third'})['database_id']
        held = []
        try:
            for database in (self.store_id, second):
                stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                held.append(stream)
                stream.sendall((f'POST /databases/{database}{path}/bulk-append HTTP/1.1\r\nHost: local\r\n'
                                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/x-ndjson\r\n'
                                'Content-Length: 1048576\r\nExpect: 100-continue\r\n\r\n').encode())
                interim = b''
                while not interim.endswith(b'\r\n\r\n'):
                    interim += stream.recv(1)
                self.assertIn(b'100 Continue', interim)
            body = self.wire(str(uuid.uuid4()), [[{'key': 'refused'}]], 1)
            self.upload(path, body, 503)
            conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
            conn.request('POST', f'/databases/{third}{path}/bulk-append', body,
                         {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/x-ndjson'})
            response = conn.getresponse()
            self.assertEqual(response.status, 503, response.read())
            conn.close()
            self.assertEqual(self.api('/healthz')['status'], 'live')
            self.assertTrue(self.api('/databases/' + self.store_id + '/readyz')['write_ready'])
            self.api('/databases/' + third + '/close', {})
        finally:
            for stream in held:
                stream.close()

    def test_bulk_large_wire_keeps_upload_memory_bounded(self):
        operation = str(uuid.uuid4())
        # Add a 1 KiB nullable string field through a separate collection.
        cid = str(uuid.uuid4())
        self.api('/collections', {'collection_id': cid, 'name': 'large_wire', 'schema': {'fields': [
            {'name': 'key', 'type': 'string', 'nullable': False},
            {'name': 'text', 'type': 'string', 'nullable': True}]}})
        path = '/collections/' + cid
        line = (json.dumps({'rows': [{'key': 'duplicate', 'text': 'x' * 1024}] * 1000}) + '\n').encode()
        header = (json.dumps({'operation_id': operation, 'rows': '40001'}) + '\n').encode()
        tail = b'{"rows":[{"key":"tail","text":null}]}\n'
        length = len(header) + 40 * len(line) + len(tail)
        self.assertGreater(length, 16 * 1024**2)
        def rss():
            return int(next(value.split()[1] for value in Path(f'/proc/{self.proc.pid}/status').read_text().splitlines()
                            if value.startswith('VmRSS:'))) * 1024
        baseline = rss()
        with socket.create_connection(('127.0.0.1', self.port), timeout=120) as stream:
            stream.sendall((f'POST /databases/{self.store_id}{path}/bulk-append HTTP/1.1\r\nHost: local\r\n'
                            f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/x-ndjson\r\n'
                            f'Content-Length: {length}\r\n\r\n').encode() + header)
            for _ in range(40):
                stream.sendall(line)
            # A control request acts as a barrier while the bulk body remains incomplete.
            self.api(path + '/operations/' + operation, status=404)
            self.assertEqual(self.api(path + '/stats')['rows'], '0')
            growth = rss() - baseline
            self.assertLess(growth, 32 * 1024**2)
            stream.sendall(tail)
            response = http.client.HTTPResponse(stream)
            response.begin()
            raw = response.read()
            self.assertEqual(response.status, 200, raw)
            self.assertEqual(json.loads(raw)['result']['rows_appended'], '40001')
        print(f'bulk upload wire={length} bytes; staging RSS growth={growth} bytes', flush=True)
        self.assertEqual(self.api(path + '/stats')['rows'], '40001')

    def test_bulk_cli_preserves_early_refusals_for_large_uploads(self):
        import subprocess
        from tls_proxy import tls_proxy
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        operation = str(uuid.uuid4())
        source = Path(self.tmp.name) / 'refused-upload.ndjson'
        line = (json.dumps({'rows': [{'key': 'x' * 1024}] * 1000}) + '\n').encode()
        with source.open('wb') as output:
            output.write((json.dumps({'operation_id': operation, 'rows': '40000'}) + '\n').encode())
            for _ in range(40):
                output.write(line)
        self.assertGreater(source.stat().st_size, 32 * 1024**2)
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as held, \
                tls_proxy(self.port, self.tmp.name) as (tls_port, _):
            held.sendall((f'POST /databases/{self.store_id}{path}/bulk-append HTTP/1.1\r\nHost: local\r\n'
                          f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/x-ndjson\r\n'
                          'Content-Length: 1048576\r\nExpect: 100-continue\r\n\r\n').encode())
            interim = b''
            while not interim.endswith(b'\r\n\r\n'):
                chunk = held.recv(1)
                self.assertTrue(chunk, interim)
                interim += chunk
            self.assertIn(b'100 Continue', interim)
            for url, tls_args in ((f'http://127.0.0.1:{self.port}', []),
                                  (f'https://localhost:{tls_port}',
                                   ['--ca-file', str(Path(self.tmp.name) / 'certificate.pem')])):
                for token, code in ((TOKEN, 'SERVER_BUSY'), ('wrong', 'UNAUTHORIZED')):
                    with self.subTest(url=url, code=code):
                        result = subprocess.run([CLI, 'bulk-append', '--url', url, *tls_args,
                            '--database-id', self.store_id, '--collection-id', cid, '--body', str(source),
                            '--timeout', '30'], env={**self.env, 'GLAMDB_TOKEN': token},
                            capture_output=True, text=True, timeout=40)
                        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                        refusal = json.loads(result.stdout)
                        self.assertEqual(refusal['error']['code'], code)
                        if code == 'SERVER_BUSY':
                            self.assertIs(refusal['execution_started'], False)
        self.api(path + '/operations/' + operation, status=404)
        self.assertEqual(self.api(path + '/stats')['rows'], '0')
        self.assertEqual(self.api('/healthz')['status'], 'live')

    def test_bulk_invalid_tail_and_interrupted_upload_do_not_bind(self):
        path = self.collection()
        for tail in (b'{invalid}\n', b'{"rows":[]}\n', b'{"rows":[{"key":"x"}]}',
                     b'\n', b'{"rows":[{"unknown":true}]}\n'):
            operation = str(uuid.uuid4())
            body = self.wire(operation, [[{'key': 'first'}]], 2) + tail
            # A reply can precede worker cleanup and release of the previous upload's permit.
            self.upload(path, body, 400, retry_busy=True)
            self.api(path + '/operations/' + operation, status=404)
        operation = str(uuid.uuid4())
        body = self.wire(operation, [[{'key': 'first'}]], 20000)
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall((f'POST /databases/{self.store_id}{path}/bulk-append HTTP/1.1\r\nHost: local\r\n'
                            f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/x-ndjson\r\n'
                            f'Content-Length: {len(body) + 1000}\r\n\r\n').encode() + body)
        self.api(path + '/operations/' + operation, status=404)
        self.assertEqual(self.api(path + '/stats')['rows'], '0')
        self.stop()
        self.start()
        self.api(path + '/operations/' + operation, status=404)

    def test_bulk_sdk_and_cli_stream_over_verified_tls(self):
        import subprocess
        from tls_proxy import tls_proxy
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        with tls_proxy(self.port, self.tmp.name) as (port, context):
            url = f'https://localhost:{port}'
            with Client(url, TOKEN, verify=context, database_id=self.store_id, timeout=120) as client:
                with client.prepare_bulk_append(cid, [[{'key': 'tls', 'n': 9007199254740993}] * 5000,
                                                     [{'key': 'tls', 'n': None}] * 5001], rows=10001,
                                                operation_id=str(uuid.uuid4())) as prepared:
                    result = client.bulk_append(prepared)
                    self.assertEqual(result.raw['result']['payload_hash'], prepared.payload_hash)
            source = Path(self.tmp.name) / 'tls-upload.ndjson'
            source.write_bytes(self.wire(str(uuid.uuid4()), [[{'key': 'cli'}] * 5000,
                                                           [{'key': 'cli'}] * 5001], 10001))
            result = subprocess.run([CLI, 'bulk-append', '--url', url, '--ca-file',
                str(Path(self.tmp.name) / 'certificate.pem'), '--database-id', self.store_id,
                '--collection-id', cid, '--body', str(source), '--timeout', '120'],
                env=self.env, capture_output=True, text=True, timeout=150)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout)['result']['rows_appended'], '10001')
        self.assertEqual(self.api(path + '/stats')['rows'], '20002')

    def test_bulk_sdk_and_cli_publish_more_than_ordinary_append(self):
        import subprocess
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.store_id, timeout=120) as client:
            with client.prepare_bulk_append(cid, ([{'key': 'sdk'}] * 4000 for _ in range(3)), rows=12000,
                                            operation_id=str(uuid.uuid4())) as prepared:
                result = client.bulk_append(prepared)
                self.assertEqual(result.raw, client.bulk_append(prepared).raw)
                self.assertEqual(result.raw['result']['payload_hash'], prepared.payload_hash)
        operation = str(uuid.uuid4())
        source = Path(self.tmp.name) / 'upload.ndjson'
        source.write_bytes(self.wire(operation, [[{'key': 'cli'}] * 6000] * 2, 12000))
        args = [CLI, 'bulk-append', '--url', f'http://127.0.0.1:{self.port}', '--database-id', self.store_id,
                '--collection-id', cid, '--body', str(source), '--timeout', '120']
        result = subprocess.run(args, env=self.env, capture_output=True, text=True, timeout=150)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(json.loads(result.stdout)['result']['rows_appended'], '12000')
        self.assertEqual(self.api(path + '/stats')['rows'], '24000')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(BulkHTTP(name) for name in loader.getTestCaseNames(BulkHTTP)
                              if name.startswith('test_bulk_'))


if __name__ == '__main__':
    unittest.main()
