"""Verified TLS + real simultaneous SDK socket waits inside isolated Locust."""

import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def test_locust_sdk_tls_requests_overlap(tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    lock = threading.Lock()
    active = peak = 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, peak
            assert self.headers["Authorization"] == "Bearer tls-test-only"
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.25)
            body = b'{"api_version":"v0"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            with lock:
                active -= 1

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    script = """
from gevent import monkey
monkey.patch_all()
import gevent, ssl, sys
from locust import User, task
from locust.env import Environment
from locust.exception import StopUser
from glamdb_sdk import Client
completed=[]
class Probe(User):
    @task
    def request(self):
        with Client(sys.argv[1], 'tls-test-only', verify=ssl.create_default_context(cafile=sys.argv[2])) as client:
            assert client.capabilities()['api_version']=='v0'
        completed.append(1)
        raise StopUser()
env=Environment(user_classes=[Probe])
runner=env.create_local_runner()
runner.start(3, spawn_rate=100)
gevent.sleep(2)
runner.quit()
assert len(completed)==3, completed
"""
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                f"https://localhost:{server.server_port}",
                str(cert),
            ],
            text=True,
            capture_output=True,
            timeout=15,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert peak == 3, "synchronous SDK socket waits serialized Locust users"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
