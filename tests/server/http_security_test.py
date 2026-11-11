"""Credential diagnostics and upload admission over native HTTP and verified TLS."""
from contextlib import contextmanager, ExitStack
import http.client
import json
import socket
import unittest

from native_http_test import NativeHTTP, TOKEN
from tls_proxy import tls_proxy


class HTTPSecurity(NativeHTTP):
    @contextmanager
    def endpoint(self, secure):
        with ExitStack() as stack:
            port, context = stack.enter_context(tls_proxy(self.port, self.tmp.name)) if secure else (self.port, None)

            def connect():
                stream = socket.create_connection(('127.0.0.1', port), timeout=10)
                if context:
                    try:
                        return context.wrap_socket(stream, server_hostname='localhost')
                    except BaseException:
                        stream.close()
                        raise
                return stream

            yield connect

    def post(self, connect, path, body):
        wire = json.dumps(body).encode()
        with connect() as stream:
            stream.sendall((f'POST {path} HTTP/1.1\r\nHost: localhost\r\n'
                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                f'Content-Length: {len(wire)}\r\n\r\n').encode() + wire)
            response = http.client.HTTPResponse(stream)
            response.begin()
            data = response.read()
            self.assertNotIn(TOKEN.encode(), data)
            self.assertNotIn(TOKEN, str(response.getheaders()))
            return response.status, json.loads(data)

    def first_status(self, connect, headers):
        with connect() as stream:
            stream.sendall(('POST /databases HTTP/1.1\r\nHost: localhost\r\n' + headers + '\r\n').encode())
            # HTTPResponse.begin() skips Continue, hiding a premature body grant.
            head = b''
            while not head.endswith(b'\r\n\r\n'):
                byte = stream.recv(1)
                self.assertTrue(byte, 'connection closed before response headers')
                head += byte
                self.assertLessEqual(len(head), 16384)
            return int(head.split(b' ', 2)[1])

    def test_errors_do_not_echo_request_secrets(self):
        collection = '/databases/' + self.store_id + self.collection()
        for secure in (False, True):
            with self.subTest(tls=secure), self.endpoint(secure) as connect:
                for path, request, expected, code in (
                    ('/databases', {TOKEN: 'private-value'}, 400, 'INVALID_ARGUMENT'),
                    (collection + '/scan', {'predicate': {'op': TOKEN}}, 501, 'UNSUPPORTED_OPERATION'),
                ):
                    with self.subTest(path=path):
                        status, body = self.post(connect, path, request)
                        self.assertEqual(status, expected)
                        self.assertEqual(body['error']['code'], code)
                        self.assertNotIn('private-value', json.dumps(body))

    def test_rejections_precede_continue(self):
        authorization = f'Authorization: Bearer {TOKEN}\r\n'
        for secure in (False, True):
            with self.subTest(tls=secure), self.endpoint(secure) as connect:
                for headers, expected in (
                    ('Content-Length: 2\r\n', 401),
                    ('Authorization: Bearer wrong\r\nContent-Length: 2\r\n', 401),
                    (authorization + 'Content-Length: 16777217\r\n', 413),
                    (authorization + 'Transfer-Encoding: chunked\r\n', 400),
                    (authorization + 'Content-Length: 2\r\nTransfer-Encoding: chunked\r\n', 400),
                    (authorization + 'Content-Length: 2\r\nContent-Encoding: gzip\r\n', 400),
                    (authorization + 'Content-Length: 2\r\nContent-Encoding: br\r\n', 400),
                ):
                    with self.subTest(headers=headers):
                        self.assertEqual(self.first_status(connect, headers + 'Expect: 100-continue\r\n'), expected)


for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(HTTPSecurity, name, None)
del NativeHTTP
if __name__ == '__main__':
    unittest.main()
