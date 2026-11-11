"""Protocol and admission regressions against the native listener."""
import http.client
import json
import socket
import time
import unittest
from native_http_test import NativeHTTP, TOKEN


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
