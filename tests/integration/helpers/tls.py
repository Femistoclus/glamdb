"""Terminate disposable test TLS while forwarding real native HTTP bytes."""

from contextlib import contextmanager
import select
import socket
import socketserver
import ssl
import threading


@contextmanager
def tls_forwarder(upstream_port, material):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(material.certificate, material.key)

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(5)
            try:
                with context.wrap_socket(self.request, server_side=True) as client, \
                        socket.create_connection(("127.0.0.1", upstream_port), timeout=5) as upstream:
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
                # Certificate refusals and early client closes are expected.
                return

    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), Forward) as proxy:
        thread = threading.Thread(target=proxy.serve_forever)
        thread.start()
        try:
            yield proxy.server_address[1]
        finally:
            proxy.shutdown()
            thread.join(timeout=5)
