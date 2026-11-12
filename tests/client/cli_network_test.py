"""Standalone CLI wire tests and optional real native engine integration.

Usage: python cli_network_test.py CLI [SERVER]
"""
import contextlib
import hashlib
import http.server
import json
import os
from pathlib import Path
import resource
import select
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

CLI = str(Path(sys.argv[1]).resolve())
SERVER = str(Path(sys.argv[2]).resolve()) if len(sys.argv) > 2 else None
del sys.argv[1:]
TOKEN = 'cli-wire-test-secret'
ENV = dict(os.environ, GLAMDB_TOKEN=TOKEN)


@contextlib.contextmanager
def raw_server(reply, delay=0, requests=None, before_reply=None):
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        listener.settimeout(0.1)
        stopped = threading.Event()
        errors = []
        def serve():
            try:
                while not stopped.is_set():
                    try:
                        conn, _ = listener.accept()
                        break
                    except socket.timeout:
                        continue
                else:
                    return
                with conn:
                    conn.settimeout(5)
                    data = b''
                    while b'\r\n\r\n' not in data:
                        chunk = conn.recv(4096)
                        if not chunk:
                            return
                        data += chunk
                    if requests is not None:
                        requests.append(data)
                    if before_reply is not None:
                        before_reply()
                    time.sleep(delay)
                    conn.sendall(reply)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=serve)
        thread.start()
        try:
            yield listener.getsockname()[1]
        finally:
            stopped.set()
            thread.join(7)
            if thread.is_alive():
                raise AssertionError('raw response server hung')
            if errors:
                raise errors[0]


@contextlib.contextmanager
def json_server(respond):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
            value = respond(self.path, json.loads(body) if body else None)
            if value is None:
                self.close_connection = True
                return
            data = json.dumps(value).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST

        def log_message(self, *_):
            pass

    with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f'http://127.0.0.1:{server.server_port}'
        finally:
            server.shutdown()
            thread.join(5)


class Client(unittest.TestCase):
    def run_cli(self, *args, **kwargs):
        return subprocess.run([CLI, *args], env=ENV, capture_output=True, timeout=12, **kwargs)

    def test_jwt_sized_bearer_tokens_are_sent_unchanged(self):
        for size in (1481, 8192):
            token = 'header.' + 'p' * (size - 11) + '.sig'
            requests = []
            with self.subTest(size=size), raw_server(
                    b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}', requests=requests) as port:
                result = subprocess.run([CLI, 'request', '--url', f'http://localhost:{port}/databases'],
                    env=dict(ENV, GLAMDB_TOKEN=token), capture_output=True, timeout=12)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(requests), 1)
            self.assertIn(('\r\nAuthorization: Bearer ' + token + '\r\n').encode(), requests[0])

    def test_oversized_or_injected_bearer_is_rejected_before_connecting(self):
        for token in ('p' * 8193, 'prefix\r\nX-Injected: value', 'prefix\nvalue'):
            requests = []
            with self.subTest(size=len(token)), raw_server(
                    b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n', requests=requests) as port:
                result = subprocess.run([CLI, 'request', '--url', f'http://localhost:{port}/databases'],
                    env=dict(ENV, GLAMDB_TOKEN=token), capture_output=True, timeout=12)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(requests, [])
            self.assertNotIn(token.encode(), result.stderr)

    def test_create_store_sends_opt_in_collection_limit(self):
        bodies = []
        def respond(path, body):
            self.assertEqual(path, '/databases')
            bodies.append(body)
            return {'name': body['name'], 'limits': body.get('limits', {})}
        with json_server(respond) as url:
            result = self.run_cli('create-store', '--url', url, '--name', 'profiles', '--max-collections', '64')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(bodies, [{'name': 'profiles', 'limits': {'max_collections': 64}}])

    def test_named_collections_completes_fixed_inventory(self):
        for changed in (False, True):
            paths = []
            def respond(path, body):
                paths.append(path)
                return {'items': [{'collection_id': str(len(paths))}],
                        'creation_high_water_mark': '3' if changed and len(paths) == 2 else '2',
                        'next_cursor': 'collections-token' if len(paths) == 1 else None}
            with self.subTest(changed=changed), json_server(respond) as url:
                result = self.run_cli('collections', '--url', url)
            if changed:
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b'')
            else:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)['items'], [{'collection_id': '1'}, {'collection_id': '2'}])
            self.assertEqual(paths, ['/collections?page_size=1000',
                                     '/collections?page_size=1000&cursor=collections-token'])

    def test_named_append_outcomes_keep_failure_and_unresolved_exits_distinct(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
        expected = {'committed': 0, 'pre_binding_rejection': 1, 'registered_failure': 1,
                    'unknown_publication': 3, 'blocked_lookup': 3}
        for case in fixture['cases']:
            if case['name'] not in expected:
                continue
            with self.subTest(case=case['name']):
                body = json.dumps(case['response']).encode()
                reply = (f"HTTP/1.1 {case['status']} Result\r\nContent-Type: application/json\r\n"
                         f"Content-Length: {len(body)}\r\n\r\n").encode() + body
                with raw_server(reply) as port:
                    result = self.run_cli('append', '--url', f'http://localhost:{port}',
                                          '--collection-id', fixture['collection_id'], '--body', '-', input=b'{}')
                self.assertEqual(result.returncode, expected[case['name']], result.stderr)
                self.assertEqual(json.loads(result.stdout), case['response'])

    def test_named_output_failure_preserves_unresolved_exit(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
        expected = {'committed': 4, 'registered_failure': 4, 'unknown_publication': 3, 'blocked_lookup': 3}
        cases = [(case['name'], case['status'], case['response'], expected[case['name']])
                 for case in fixture['cases'] if case['name'] in expected]
        for code in ('OPERATION_IN_PROGRESS', 'RECOVERY_REQUIRED'):
            cases.append((code, 503, {'error': {'code': code, 'message': 'lookup is blocked',
                                              'retry_hint': 'SameIdAfterReconcile'}}, 3))
        for name, status, response, exit_code in cases:
            body = json.dumps(response).encode()
            reply = (f'HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\n'
                     f'Content-Length: {len(body)}\r\n\r\n').encode() + body
            for command in ('append', 'operation'):
                for destination in ('stdout', '--output', '--headers'):
                    with self.subTest(case=name, command=command, destination=destination), \
                            tempfile.TemporaryDirectory(dir='/tmp') as tmp, open('/dev/full', 'wb') as sink:
                        output = Path(tmp) / 'response.json'
                        # Replace a valid destination only after output preflight and the HTTP request.
                        before_reply = output.mkdir if destination != 'stdout' else None
                        with raw_server(reply, before_reply=before_reply) as port:
                            args = [command, '--url', f'http://localhost:{port}',
                                    '--collection-id', fixture['collection_id']]
                            if command == 'append':
                                args += ['--body', '-']
                            else:
                                args += ['--operation-id', fixture['appends']['a']['operation_id']]
                            if destination != 'stdout':
                                args += [destination, str(output)]
                            result = subprocess.run([CLI, *args], env=ENV,
                                input=b'{}' if command == 'append' else None,
                                stdout=sink if destination == 'stdout' else subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=12)
                        self.assertEqual(result.returncode, exit_code, result.stderr)
                        self.assertIn(b'output failed after complete HTTP', result.stderr)
                        if destination != 'stdout':
                            self.assertTrue(output.is_dir())
                            self.assertEqual(result.stdout, b'')
                            self.assertEqual(list(Path(tmp).glob('*.tmp.*')), [])

    def test_lost_named_scan_reply_is_a_read_failure(self):
        with raw_server(b'') as port:
            result = self.run_cli('scan', '--url', f'http://localhost:{port}',
                                  '--collection-id', '00000000-0000-0000-0000-000000000052')
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(result.stdout, b'')
        self.assertNotIn(b'reconcile using operation ID', result.stderr)

    def test_named_scan_cancels_after_failed_or_invalid_continuation(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
        identity = next(case['response']['recovery']['base'] for case in fixture['cases']
                        if case['name'] == 'unknown_publication')
        for failure in ('disconnect', 'changed_view', 'missing_rows', 'repeated_cursor'):
            requests = []
            def respond(path, body):
                requests.append((path, body))
                if path.endswith('/cancel'):
                    return {'cancelled': True}
                page = {'identity': identity, 'schema': {'fields': []}, 'snapshot_id': None,
                        'profile': {'rows_returned': '1'}, 'rows': [{}], 'next_cursor': 'token-1'}
                if body.get('cursor'):
                    if failure == 'disconnect':
                        return None
                    if failure == 'changed_view':
                        page.update(snapshot_id='1', next_cursor='token-2')
                    if failure == 'missing_rows':
                        del page['rows']
                return page
            with self.subTest(failure=failure), json_server(respond) as url:
                result = self.run_cli('scan', '--url', url,
                                      '--collection-id', '00000000-0000-0000-0000-000000000052')
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(result.stdout, b'')
            self.assertTrue(requests[-1][0].endswith('/cancel'), requests)
            self.assertEqual(requests[-1][1]['cursor'], 'token-2' if failure == 'changed_view' else 'token-1')

    def test_named_metadata_follows_pages_with_original_parameters(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
        unknown = next(case['response'] for case in fixture['cases'] if case['name'] == 'unknown_publication')
        identity = unknown['recovery']['base']
        receipts = [next(case['response']['result'] for case in fixture['cases'] if case['name'] == name)
                    for name in ('committed', 'committed_b')]
        selected = receipts[-1]['snapshot_id']
        identity['current_snapshot_id'] = selected
        for command in ('snapshots', 'files'):
            if command == 'snapshots':
                items = [{'snapshot_id': receipt['snapshot_id'], 'parent_id': receipt['parent_snapshot_id'],
                          'parent_snapshot_id': receipt['parent_snapshot_id'],
                          'created_at_us': receipt['snapshot_created_at_us'], 'rows_added': receipt['rows_appended'],
                          'operation_id': receipt['operation_id'], 'receipt': receipt, 'backend': 'local', 'mock': False}
                         for receipt in receipts]
            else:
                items = [{'file_id': character * 64, 'path': None, 'content': 'data', 'format': 'parquet',
                          'record_count': '2', 'compressed_bytes': '100', 'field_metrics': [], 'synthetic': False,
                          'physical_file_exists': None, 'backend': 'local', 'mock': False}
                         for character in ('a', 'b')]
            paths = []
            def respond(path, body):
                paths.append(path)
                self.assertIsNone(body)
                return {'identity': identity, 'snapshot_id': selected, 'backend': 'local', 'mock': False,
                        'synthetic': False, 'durability_validation': 'designed', 'items': [items[len(paths) - 1]],
                        'next_cursor': 'metadata-token' if len(paths) == 1 else None}
            with self.subTest(command=command), json_server(respond) as url:
                result = self.run_cli(command, '--url', url, '--snapshot', selected,
                                      '--collection-id', fixture['collection_id'])
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIsNone(json.loads(result.stdout)['next_cursor'])
            self.assertEqual(json.loads(result.stdout)['items'], items)
            expected = f"/collections/{fixture['collection_id']}/{command}?snapshot_id={selected}&page_size=1000"
            self.assertEqual(paths, [expected, expected + '&cursor=metadata-token'])

    def test_proxy_headers_preserve_repeated_values(self):
        reply = (b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n'
                 b'Cache-Control: no-store\r\nCache-Control: private\r\n'
                 b'X-Content-Type-Options: nosniff\r\nX-Content-Type-Options: nosniff\r\n'
                 b'Referrer-Policy: no-referrer\r\nReferrer-Policy: same-origin\r\n'
                 b'Set-Cookie: a=1\r\nSet-Cookie: b=2\r\n'
                 b"X_!#$%&'*+-.^`|~: one\ttwo\xff\r\n\r\n{}")
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp, raw_server(reply) as port:
            headers = Path(tmp) / 'headers.json'
            result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--method', 'POST', '--headers', str(headers))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, b'{}')
            metadata = json.loads(headers.read_text())['headers']
            self.assertEqual(metadata['cache-control'], ['no-store', 'private'])
            self.assertEqual(metadata['set-cookie'], ['a=1', 'b=2'])
            self.assertEqual(metadata["x_!#$%&'*+-.^`|~"], 'one\ttwo\u00ff')

    def test_duplicate_integrity_headers_fail_with_unknown_write_exit(self):
        for name in ('Content-Length', 'Transfer-Encoding', 'X-Content-SHA256', 'X-GlamDB-Next-Cursor'):
            fields = b'' if name == 'Content-Length' else b'Content-Length: 0\r\n'
            reply = b'HTTP/1.1 200 OK\r\n' + fields + (name + ': 0\r\n' + name + ': 0\r\n\r\n').encode()
            with self.subTest(name=name), raw_server(reply) as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--method', 'POST')
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertIn(b'write outcome may be unknown', result.stderr)

    def test_invalid_url_port_and_base_query_are_local_errors(self):
        for url in ('http://localhost:abc/', 'http://localhost:0/', 'http://localhost:65536/'):
            with self.subTest(url=url):
                result = self.run_cli('request', '--url', url)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b'invalid URL port', result.stderr)
                self.assertNotIn(b'outcome may be unknown', result.stderr)
        result = self.run_cli('request', '--url', 'http://localhost:1/?query=yes', '--path', '/databases')
        self.assertIn(b'base URL cannot contain a query or fragment', result.stderr)
        result = self.run_cli('request', '--url', 'http://localhost:1/', '--method', 'DELETE')
        self.assertIn(b'unsupported method', result.stderr)
        self.assertNotIn(b'outcome may be unknown', result.stderr)

    def test_query_only_url_preserves_request_target(self):
        seen = []
        with raw_server(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n', requests=seen) as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}?value=one')
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(seen[0].startswith(b'GET /?value=one HTTP/1.1\r\n'))

    def test_failed_atomic_output_preserves_existing_file(self):
        body = b'x' * 8192
        reply = f'HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n'.encode() + body
        def limit_output():
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (1024, 1024))
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            output = Path(tmp) / 'result.arrow'
            output.write_bytes(b'previous complete output')
            with raw_server(reply) as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--method', 'POST', '--output', str(output), preexec_fn=limit_output)
                self.assertEqual(result.returncode, 4, result.stderr)
                self.assertIn(b'HTTP 200', result.stderr)
                self.assertEqual(output.read_bytes(), b'previous complete output')
                self.assertEqual(list(Path(tmp).iterdir()), [output])
            with raw_server(reply) as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--output', str(output))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(output.read_bytes(), body)

    def test_output_does_not_follow_or_replace_symlink(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            target = Path(tmp) / 'original'
            target.write_bytes(b'original')
            link = Path(tmp) / 'output'
            link.symlink_to(target)
            with raw_server(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}') as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--output', str(link))
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertTrue(link.is_symlink())
                self.assertEqual(target.read_bytes(), b'original')

    def test_output_preflight_rejects_invalid_targets_before_mutation(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            root = Path(tmp)
            (root / 'directory').mkdir()
            (root / 'link').symlink_to(root / 'missing')
            os.mkfifo(root / 'fifo')
            targets = ['/dev/null', '/dev/full', root / 'directory', root / 'link', root / 'fifo', root / 'missing' / 'out']
            for command in ('request', 'create-store'):
                for option in ('--output', '--headers'):
                    for target in targets:
                        seen = []
                        with self.subTest(command=command, option=option, target=target), raw_server(
                                b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}', requests=seen) as port:
                            args = [command, '--url', f'http://localhost:{port}/', option, str(target)]
                            args += ['--method', 'POST'] if command == 'request' else ['--name', 'must-not-create']
                            result = self.run_cli(*args)
                            self.assertEqual(result.returncode, 1, result.stderr)
                        self.assertEqual(seen, [], result.stderr)

    def test_atomic_output_preserves_existing_permissions(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            output = Path(tmp) / 'result.json'
            output.write_bytes(b'old')
            output.chmod(0o640)
            with raw_server(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}') as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--output', str(output))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_bytes(), b'{}')
            self.assertEqual(output.stat().st_mode & 0o777, 0o640)

    def test_binary_headers_dns_and_stdout_failure(self):
        body = b'\x00\xff\n\r\x00arrow\xff\xff\xff\xff\x00\x00\x00\x00'
        digest = hashlib.sha256(body).hexdigest()
        reply = (f'HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\nContent-Type: application/vnd.apache.arrow.stream\r\nX-Content-SHA256: {digest}\r\nX-GlamDB-Snapshot-Id: initial\r\n\r\n'.encode() + body)
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp, raw_server(reply) as port:
            headers = Path(tmp) / 'headers.json'
            result = self.run_cli('request', '--url', f'http://localhost:{port}/scan', '--accept', 'application/vnd.apache.arrow.stream', '--headers', str(headers))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, body)
            metadata = json.loads(headers.read_text())
            self.assertEqual(metadata['headers']['x-content-sha256'], digest)
            self.assertEqual(metadata['headers']['x-glamdb-snapshot-id'], 'initial')
        with raw_server(reply) as port, open('/dev/full', 'wb') as sink:
            result = subprocess.run([CLI, 'request', '--url', f'http://localhost:{port}/', '--method', 'POST'], env=ENV, stdout=sink, stderr=subprocess.PIPE, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'may already have committed', result.stderr)
        with raw_server(reply) as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--output', '/dev/full')
            self.assertNotEqual(result.returncode, 0)
        with raw_server(reply) as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--headers', '/dev/full')
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, b'')
            self.assertIn(b'regular file', result.stderr)

    def test_invalid_and_interrupted_responses(self):
        for reply in (
            b'HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort',
            b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nContent-Length: 0junk\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nContent-Length: 67108865\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: gzip\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nInvalid header\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nX-Long: ' + b'x' * 17000 + b'\r\nContent-Length: 0\r\n\r\n',
            b'HTTP/1.1 200 OK\r\n\r\n',
            b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Content-SHA256: wrong\r\n\r\n',
        ):
            with self.subTest(reply=reply[:90]), raw_server(reply) as port:
                result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--method', 'POST')
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b'')
                self.assertIn(b'write outcome may be unknown', result.stderr)

    def test_timeout_and_no_redirect(self):
        with raw_server(b'', delay=2) as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}/', '--method', 'POST', '--timeout', '1')
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'timed out', result.stderr)
            self.assertIn(b'write outcome may be unknown', result.stderr)
        with raw_server(b'HTTP/1.1 302 Found\r\nLocation: https://untrusted.invalid/\r\nContent-Length: 0\r\n\r\n') as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}/')
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, b'')

    def test_remote_http_rejected(self):
        result = self.run_cli('request', '--url', 'http://192.0.2.1:80/', '--method', 'POST')
        self.assertEqual(result.returncode, 1)
        self.assertIn(b'require HTTPS', result.stderr)
        self.assertNotIn(b'outcome may be unknown', result.stderr)

    def test_interrupted_get_does_not_report_unknown_write(self):
        with raw_server(b'HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort') as port:
            result = self.run_cli('request', '--url', f'http://localhost:{port}/')
            self.assertEqual(result.returncode, 1)
            self.assertNotIn(b'outcome may be unknown', result.stderr)

    def test_tls_identity_and_ca(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            tmp = Path(tmp)
            cert, key = tmp / 'cert.pem', tmp / 'key.pem'
            wrong = tmp / 'wrong.pem'
            for certificate, private_key in ((cert, key), (wrong, tmp / 'wrong-key.pem')):
                subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(private_key), '-out', str(certificate), '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], check=True, capture_output=True)
            seen = []
            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    seen.append(self.headers['Authorization'])
                    self.protocol_version = 'HTTP/1.1'
                    self.send_response(200)
                    self.send_header('Content-Length', '2')
                    self.end_headers()
                    self.wfile.write(b'{}')
                def log_message(self, *_args):
                    pass
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            server_names = []
            context.set_servername_callback(lambda _sock, name, _context: server_names.append(name))
            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                port = server.server_address[1]
                for host, ca, expected in [('localhost', cert, 0), ('localhost', wrong, 1), ('127.0.0.1', cert, 1)]:
                    with self.subTest(host=host, ca=ca):
                        result = self.run_cli('request', '--url', f'https://{host}:{port}/', '--ca-file', str(ca), '--method', 'GET' if expected == 0 else 'POST')
                        self.assertEqual(result.returncode, expected, result.stderr)
                        self.assertNotIn(b'outcome may be unknown', result.stderr)
                self.assertEqual(seen, ['Bearer ' + TOKEN])
                self.assertEqual(server_names, ['localhost', 'localhost', None])
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


@unittest.skipUnless(SERVER, 'native server argument not supplied; wire-only invocation')
class NativeClient(unittest.TestCase):
    def test_complete_workflow(self):
        import pyarrow as pa
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            proc = subprocess.Popen([SERVER, '--data-dir', str(Path(tmp) / 'data'), '--port', '0'], env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertTrue(select.select([proc.stdout], [], [], 20)[0], 'server startup timeout')
                port = json.loads(proc.stdout.readline())['port']
                base = f'http://localhost:{port}'
                def request(path, body=None, **options):
                    args = ['request', '--url', base + path]
                    if body is not None:
                        args += ['--body', '-']
                    for name, value in options.items():
                        args += ['--' + name.replace('_', '-'), str(value)]
                    result = subprocess.run([CLI, *args], input=None if body is None else json.dumps(body).encode(), env=ENV, capture_output=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr + result.stdout[:1000])
                    return result.stdout
                result = subprocess.run([CLI, 'create-store', '--url', base, '--name', 'cli_database'], env=ENV, capture_output=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stderr)
                db = json.loads(result.stdout)['database_id']
                scope = '/databases/' + db
                collection_id = str(uuid.uuid4())
                schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}, {'name': 'n', 'type': 'int64', 'nullable': True}]}
                request(scope + '/collections', {'collection_id': collection_id, 'name': 'events', 'schema': schema})
                collection = scope + '/collections/' + collection_id
                operation = str(uuid.uuid4())
                rows = [{'key': 'duplicate', 'n': '9223372036854775807'}, {'key': 'duplicate', 'n': None}]
                receipt = json.loads(request(collection + '/append', {'operation_id': operation, 'rows': rows}))
                self.assertEqual(receipt['outcome'], 'committed')
                self.assertEqual(json.loads(request(collection + '/operations/' + operation)), receipt)
                self.assertEqual(json.loads(request(collection + '/scan', {}))['rows'], rows)
                headers = Path(tmp) / 'arrow-headers.json'
                arrow = request(collection + '/scan', {}, accept='application/vnd.apache.arrow.stream', headers=headers)
                metadata = json.loads(headers.read_text())['headers']
                self.assertEqual(metadata['x-content-sha256'], hashlib.sha256(arrow).hexdigest())
                table = pa.ipc.open_stream(arrow).read_all()
                self.assertEqual(table.column('key').to_pylist(), ['duplicate', 'duplicate'])
                self.assertEqual(table.column('n').to_pylist(), [9223372036854775807, None])
                opened = json.loads(request(collection + '/scan/open', {}))
                cursor_body = {'cursor': opened['cursor']}
                page = json.loads(request(collection + '/scan', cursor_body))
                self.assertEqual(page['rows'], rows)
                self.assertEqual(json.loads(request(collection + '/scan', cursor_body)), page)
                request(collection + '/scan/cancel', cursor_body)
                request(scope + '/close', {})
                request(scope + '/open', {})
                request(scope + '/reconcile', {})
                self.assertEqual(json.loads(request(collection + '/scan', {}))['rows'], rows)
                self.assertEqual(len(json.loads(request('/databases'))['items']), 1)
            finally:
                proc.terminate()
                out, err = proc.communicate(timeout=20)
                self.assertEqual(proc.returncode, 0, out + err)


if __name__ == '__main__':
    unittest.main()
