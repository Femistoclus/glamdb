"""Disposable TLS termination forwarding exact bytes to the native listener."""
from contextlib import contextmanager
from pathlib import Path
import select
import socket
import socketserver
import ssl
import subprocess
import threading


@contextmanager
def tls_proxy(upstream_port, directory):
    directory = Path(directory)
    certificate, key = directory / 'certificate.pem', directory / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-keyout', str(key), '-out', str(certificate), '-days', '1', '-subj', '/CN=localhost',
        '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
        check=True, capture_output=True, timeout=20)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(5)
            try:
                with context.wrap_socket(self.request, server_side=True) as client, \
                        socket.create_connection(('127.0.0.1', upstream_port), timeout=5) as upstream:
                    while True:
                        readable = [client] if client.pending() else select.select([client, upstream], [], [], 5)[0]
                        if not readable:
                            return
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                return
                            (upstream if source is client else client).sendall(data)
            except OSError:
                # Certificate refusal and peers closing rejected requests are expected.
                return

    with socketserver.ThreadingTCPServer(('127.0.0.1', 0), Forward) as proxy:
        thread = threading.Thread(target=proxy.serve_forever)
        thread.start()
        try:
            yield proxy.server_address[1], ssl.create_default_context(cafile=str(certificate))
        finally:
            proxy.shutdown()
            thread.join(timeout=5)
