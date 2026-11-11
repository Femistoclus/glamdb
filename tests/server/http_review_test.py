"""Protocol and admission regressions against the native listener."""
import http.client
import json
import socket
import ssl
import time
import unittest
import uuid
from native_http_test import NativeHTTP, TOKEN
from tls_proxy import tls_proxy


class HTTPReview(NativeHTTP):
    def test_complete_request_after_write_side_half_close_receives_response(self):
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall((f'GET /databases HTTP/1.1\r\nHost: local\r\n'
                f'Authorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n').encode())
            stream.shutdown(socket.SHUT_WR)
            response = http.client.HTTPResponse(stream)
            response.begin()
            body = response.read()
            self.assertEqual(response.status, 200, body)
            self.assertEqual(json.loads(body)['items'][0]['database_id'], self.store_id)

    def test_capabilities_report_the_production_scan_policy(self):
        for path in ('/capabilities', '/databases/' + self.store_id + '/capabilities'):
            limits = self.api(path)['limits']
            self.assertEqual(limits['scan_sessions'], 64)
            self.assertEqual(limits['scan_sessions_per_database'], 8)
            self.assertEqual(limits['scan_lifetime_seconds'], 0)
            self.assertEqual(limits['scan_replay_bytes'], 1024 * 1024 * 1024)
            self.assertEqual(limits['scan_idle_seconds'], 60)

    def test_unauthenticated_peer_headers_leave_room_for_complete_requests(self):
        streams = []
        try:
            for _ in range(32):
                stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                streams.append(stream)
                stream.sendall(b'GET /healthz HTTP/1.1\r\nHost: ')
            time.sleep(0.1)
            self.assertEqual(self.api('/healthz')['status'], 'live')
            self.assertEqual(self.api('/databases/' + self.store_id)['state'], 'open')
        finally:
            for stream in streams:
                stream.close()

    def test_media_parameters_do_not_reject_supported_json(self):
        collection = self.collection()
        for accept in ('application/json; charset=utf-8',
                       'application/json; profile="a,b"; q=0.8',
                       'application/json; profile="a;b"; q=0.8; extension=test'):
            with self.subTest(accept=accept):
                self.assertEqual(self.request(collection + '/scan/open', {}, accept=accept)[0], 200)
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            conn.request('POST', '/databases/' + self.store_id + '/open', '{}',
                {'Authorization': 'Bearer ' + TOKEN,
                 'Content-Type': 'application/json; profile="a;b"; charset=UTF-8'})
            response = conn.getresponse()
            self.assertEqual(response.status, 200, response.read())
        finally:
            conn.close()

    def test_health_post_is_a_method_error_without_content_type(self):
        code, body = self.raw(b'POST /healthz HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n')
        self.assertEqual(code, 400, body)

    def test_eager_oversized_body_receives_error_without_reset(self):
        body = b'x' * (17 * 1024 * 1024)
        with socket.create_connection(('127.0.0.1', self.port), timeout=10) as stream:
            stream.sendall((f'POST /databases HTTP/1.1\r\nHost: local\r\n'
                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                f'Content-Length: {len(body)}\r\n\r\n').encode() + body)
            response = http.client.HTTPResponse(stream)
            response.begin()
            self.assertEqual(response.status, 413)
            self.assertEqual(json.loads(response.read())['error']['code'], 'PAYLOAD_TOO_LARGE')

    def raw(self, payload):
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall(payload)
            response = http.client.HTTPResponse(stream)
            response.begin()
            return response.status, response.read()

    def test_header_errors_have_http_responses(self):
        for payload, status in (
            (b'GET /healthz HTTP/1.1\nHost: local\n\n', 400),
            (b'GET /healthz HTTP/1.1\r\nHost: local\r\nX-Large: ' + b'x' * 17000, 431),
            (b'GET /healthz HTTP/1.1\r\nHost: local\r\nExpect: something\r\n\r\n', 417),
            (b'GET /healthz HTTP/1.1\r\nHost: ', 408),
        ):
            with self.subTest(status=status):
                self.assertEqual(self.raw(payload)[0], status)
        self.assertEqual(self.api('/healthz')['status'], 'live')

    def test_idle_headers_do_not_consume_data_slots(self):
        streams = [socket.create_connection(('127.0.0.1', self.port), timeout=5) for _ in range(16)]
        try:
            for stream in streams:
                stream.sendall(b'GET /healthz HTTP/1.1\r\nHost: ')
            self.assertEqual(self.api('/healthz')['status'], 'live')
            self.assertEqual(self.api('/databases/' + self.store_id)['state'], 'open')
        finally:
            for stream in streams:
                stream.close()

    def test_expect_continue_authenticates_before_body(self):
        path = '/databases/' + self.store_id + '/open'
        headers = f'POST {path} HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: 2\r\nExpect: 100-continue\r\n'
        self.assertEqual(self.raw((headers + '\r\n').encode())[0], 401)
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as stream:
            stream.sendall((headers + f'Authorization: Bearer {TOKEN}\r\n\r\n').encode())
            interim = b''
            while not interim.endswith(b'\r\n\r\n'):
                interim += stream.recv(1)
            self.assertEqual(interim, b'HTTP/1.1 100 Continue\r\n\r\n')
            stream.sendall(b'{}')
            response = http.client.HTTPResponse(stream)
            response.begin()
            self.assertEqual(response.status, 200, response.read())

    def test_body_timeouts_release_ordinary_admission(self):
        from contextlib import ExitStack
        with ExitStack() as stack:
            def upload():
                stream = stack.enter_context(socket.create_connection(('127.0.0.1', self.port), timeout=5))
                stream.sendall((f'POST /assets/unused HTTP/1.1\r\nHost: local\r\n'
                    'Content-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
                head = b''
                while not head.endswith(b'\r\n\r\n'):
                    chunk = stream.recv(1)
                    self.assertTrue(chunk, 'listener closed before an admission response')
                    head += chunk
                return stream, head

            streams = []
            for _ in range(12):
                stream, head = upload()
                self.assertIn(b' 100 ', head)
                streams.append(stream)
            self.assertIn(b' 100 ', upload()[1])
            for stream in streams:
                response = http.client.HTTPResponse(stream)
                response.begin()
                self.assertEqual(response.status, 408, response.read())
            self.assertIn(b' 100 ', upload()[1])

    def test_https_proxy_preserves_native_access_and_input_limits(self):
        collection = self.collection()
        base = '/databases/' + self.store_id
        with tls_proxy(self.port, self.tmp.name) as (port, context):
            def request(path, body=None, token=TOKEN, headers=None):
                connection = http.client.HTTPSConnection('localhost', port, context=context, timeout=15)
                try:
                    fields = {'Content-Type': 'application/json'}
                    if token is not None:
                        fields['Authorization'] = 'Bearer ' + token
                    fields.update(headers or {})
                    connection.request('GET' if body is None and headers is None else 'POST', path,
                        None if body is None else json.dumps(body), fields)
                    response = connection.getresponse()
                    data = response.read()
                    self.assertNotIn(TOKEN.encode(), data)
                    self.assertNotIn(b'wrong-secret', data)
                    self.assertEqual(response.getheader('Cache-Control'), 'no-store')
                    return response.status, json.loads(data)
                finally:
                    connection.close()

            with self.assertRaises(ssl.SSLCertVerificationError):
                untrusted = http.client.HTTPSConnection('localhost', port, timeout=5)
                try:
                    untrusted.request('GET', '/healthz')
                finally:
                    untrusted.close()
            for path in ('/capabilities', '/databases', base, base + '/readyz',
                         base + collection + '/schema', '/%64atabases'):
                for token in (None, 'wrong-secret'):
                    with self.subTest(path=path, token=token):
                        self.assertEqual(request(path, token=token)[0], 401)
            self.assertEqual(request('/capabilities?access_token=' + TOKEN, token=None)[0], 401)
            self.assertEqual(request('/databases', token=None,
                headers={'X-Forwarded-User': 'admin', 'Content-Length': '0'})[0], 401)
            for headers, status in (
                ({'Content-Length': str(16 * 1024 * 1024 + 1)}, 413),
                ({'Content-Length': '2', 'Content-Encoding': 'gzip'}, 400),
                ({'Transfer-Encoding': 'chunked'}, 400),
                ({'Content-Length': '2', 'Transfer-Encoding': 'chunked'}, 400),
            ):
                with self.subTest(headers=headers):
                    # No body is sent: native rejection must precede Continue/decode.
                    self.assertEqual(request(base + '/open', headers=dict(headers,
                        Expect='100-continue'))[0], status)
            operation = str(uuid.uuid4())
            append = base + collection + '/append'
            body = {'operation_id': operation, 'rows': [{'key': 'limit'}] * 10001}
            self.assertEqual(request(append, body)[0], 400)
            body['rows'] = [{'key': 'accepted'}]
            status, receipt = request(append, body)
            self.assertEqual(status, 200, receipt)
            self.assertEqual(receipt['outcome'], 'committed')
            self.assertEqual(request(base + collection + '/operations/' + operation)[1], receipt)
            self.assertEqual(request(base + collection + '/scan', {})[1]['rows'][0]['key'], 'accepted')
            # Native session quotas still apply behind the proxy.
            cursors = []
            for _ in range(8):
                status, opened = request(base + collection + '/scan/open', {})
                self.assertEqual(status, 200, opened)
                cursors.append(opened['cursor'])
            self.assertEqual(request(base + collection + '/scan/open', {})[0], 429)
            for cursor in cursors:
                self.assertEqual(request(base + collection + '/scan/cancel', {'cursor': cursor})[0], 200)
            self.assertEqual(request(base + collection + '/scan', {})[0], 200)
        self.proc.terminate()
        out, err = self.proc.communicate(timeout=20)
        self.assertEqual(self.proc.returncode, 0, out + err)
        self.assertNotIn(TOKEN, out + err)
        self.assertNotIn('wrong-secret', out + err)

    def test_media_negotiation_and_canonical_dispatch(self):
        path = '/databases/' + self.store_id
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        conn.request('POST', path + '/open', '{}', {'Authorization': 'Bearer ' + TOKEN,
                     'Content-Type': 'Application/JSON ; Charset = "UTF-8"'})
        response = conn.getresponse()
        self.assertEqual(response.status, 200, response.read())
        conn.close()
        collection = self.collection()
        code, headers, _ = self.request(collection + '/scan/open', {}, accept='text/html, Application/JSON;q=0.8, */*;q=0.1')
        self.assertEqual(code, 200)
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(self.request(collection + '/scan/open', {}, accept='application/json;q=0, */*;q=0')[0], 406)
        self.assertEqual(self.request(path + '%2Fcollections/healthz')[0], 400)
        missing = '00000000-0000-0000-0000-000000000001'
        error = self.api('/databases/' + missing, status=404)['error']
        self.assertEqual(error['code'], 'DATABASE_NOT_FOUND')
        error = self.api(collection + '/operations/' + missing, status=404)['error']
        self.assertEqual(error['code'], 'OPERATION_NOT_FOUND')


for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(HTTPReview, name, None)
del NativeHTTP
if __name__ == '__main__':
    unittest.main()
