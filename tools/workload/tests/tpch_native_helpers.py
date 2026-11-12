"""Real TLS forwarding and immutable-file evidence for native TPC-H acceptance."""

from contextlib import contextmanager
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import ssl
import subprocess
import threading


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_inventory(directory):
    root = Path(directory)
    return {
        str(path.relative_to(root)): file_digest(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@contextmanager
def tls_proxy(upstream_port, directory):
    """Forward real native replies, optionally losing one append request/reply.

    All fault injection happens at the transport boundary. Outcome lookup and
    scans always reach the native server. Only hashes of append bodies are kept.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    certificate, key = directory / "test-ca.pem", directory / "test-key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(certificate), "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
        ],
        check=True, capture_output=True, timeout=30,
    )

    class Forward(BaseHTTPRequestHandler):
        def forward(self):
            size = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(size) if size else None
            is_append = self.command == "POST" and self.path.endswith(("/append", "/upsert"))
            lifecycle_kind = None
            if self.command == "POST":
                if self.path == "/databases":
                    lifecycle_kind = "database"
                elif self.path.startswith("/databases/") and self.path.endswith("/collections"):
                    lifecycle_kind = "collection"
            lifecycle = None
            if lifecycle_kind is not None:
                value = json.loads(body)
                lifecycle = {
                    "kind": lifecycle_kind, "path": self.path,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                    "database_id": value.get("database_id"),
                    "collection_id": value.get("collection_id"), "reply_dropped": False,
                }
                with self.server.lock:
                    self.server.lifecycle_calls.append(lifecycle)
            record = None
            if is_append:
                with self.server.lock:
                    record = {
                        "path": self.path,
                        "operation_id": json.loads(body)["operation_id"],
                        "body_sha256": hashlib.sha256(body).hexdigest(),
                        "forwarded": False,
                        "reply_dropped": False,
                    }
                    self.server.appends.append(record)
                    skip = self.server.drop_request_once
                    self.server.drop_request_once = False
                if skip:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
            connection = http.client.HTTPConnection("127.0.0.1", upstream_port, timeout=305)
            try:
                headers = {
                    name: value for name, value in self.headers.items()
                    if name.lower() not in {"host", "connection"}
                }
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                content = response.read()
                if lifecycle is not None:
                    with self.server.lock:
                        lifecycle["status"] = response.status
                        drop = self.server.drop_lifecycle_reply_once == lifecycle_kind
                        if drop:
                            self.server.drop_lifecycle_reply_once = None
                    if drop:
                        value = json.loads(content)
                        if response.status not in (200, 201) or (
                            lifecycle_kind == "database" and value.get("state") != "open"
                        ) or (lifecycle_kind == "collection" and value.get("outcome", "committed") != "committed"):
                            self.server.fault_errors.append("lost lifecycle reply was not a committed native create")
                        lifecycle["reply_dropped"] = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                if is_append:
                    value = json.loads(content)
                    error = value.get("error") or {}
                    with self.server.lock:
                        record.update(forwarded=True, status=response.status,
                                      outcome=value.get("outcome"), phase=value.get("phase"),
                                      error_code=error.get("code"), retry_hint=error.get("retry_hint"))
                        drop = self.server.drop_reply_once
                        self.server.drop_reply_once = False
                    if drop:
                        if response.status != 200 or record["outcome"] != "committed":
                            self.server.fault_errors.append("lost reply was not a committed native append")
                        record["reply_dropped"] = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in {
                        "connection", "transfer-encoding", "content-length", "server", "date",
                    }:
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                connection.close()

        do_GET = do_POST = do_DELETE = forward

        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Forward)
    proxy.daemon_threads = True
    proxy.lock = threading.Lock()
    proxy.drop_reply_once = proxy.drop_request_once = False
    proxy.appends, proxy.fault_errors = [], []
    proxy.lifecycle_calls, proxy.drop_lifecycle_reply_once = [], None
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    proxy.socket = context.wrap_socket(proxy.socket, server_side=True)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy, f"https://localhost:{proxy.server_port}", certificate
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)
