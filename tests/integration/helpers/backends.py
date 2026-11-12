"""Explicit client-test backends. A native setup error never selects a mock."""

from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import select
import subprocess
import time
from uuid import uuid4

import httpx

from .http_server import MockHTTPServer, REPOSITORY, TOKEN, package_environment

READINESS_TIMEOUT = 15


def selected_backend():
    backend = os.environ.get("GLAMDB_TEST_BACKEND", "native" if os.environ.get("GLAMDB_NATIVE_SERVER") else "mock")
    if backend not in ("mock", "native"):
        raise RuntimeError("GLAMDB_TEST_BACKEND must be mock or native")
    return backend


def assert_backend_identity(capabilities, backend):
    assert capabilities["backend"] == ("local" if backend == "native" else "mock"), capabilities
    assert capabilities["mock"] is (backend == "mock"), capabilities
    assert capabilities["durability_validation"] == "designed", capabilities


class NativeHTTPServer:
    """Own a disposable managed database and its real native server process."""

    def __init__(self, directory, *, tls=None):
        self.directory = Path(directory)
        self.tls = tls
        self.process = None
        self.resources = ExitStack()
        self.store_id = str(uuid4())
        self.data_dir = self.directory / "server"
        self.root = self.data_dir / "databases" / self.store_id

    def __enter__(self):
        executable = os.environ.get("GLAMDB_NATIVE_SERVER")
        if not executable or not Path(executable).is_file() or not os.access(executable, os.X_OK):
            raise RuntimeError("native server requires an executable GLAMDB_NATIVE_SERVER")
        try:
            log = self.resources.enter_context((self.directory / "native.log").open("w+b"))
            self.process = subprocess.Popen(
                [str(Path(executable).resolve()), "--data-dir", str(self.data_dir), "--port", "0",
                 "--ui-root", str(REPOSITORY / "ui")], env=package_environment(),
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True)
            deadline = time.monotonic() + READINESS_TIMEOUT
            line = bytearray()
            while b"\n" not in line:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([self.process.stdout], [], [], remaining)[0]:
                    raise RuntimeError("native server readiness deadline exceeded")
                chunk = os.read(self.process.stdout.fileno(), 4096)
                if not chunk:
                    log.seek(0)
                    raise RuntimeError("native server exited before readiness: " + log.read().decode(errors="replace"))
                line.extend(chunk)
                if len(line) > 4096:
                    raise RuntimeError("native server readiness message exceeds 4096 bytes")
            port = json.loads(line.split(b"\n", 1)[0])["port"]
            scheme = "http"
            verify = True
            if self.tls is not None:
                from .tls import tls_forwarder
                port = self.resources.enter_context(tls_forwarder(port, self.tls))
                scheme, verify = "https", self.tls.trusted_context()
            self.base_url = f"{scheme}://127.0.0.1:{port}"
            self.url = f"{self.base_url}/databases/{self.store_id}"
            with httpx.Client(headers={"Authorization": f"Bearer {TOKEN}"}, verify=verify,
                              timeout=30, trust_env=False) as client:
                capabilities = client.get(self.base_url + "/capabilities")
                capabilities.raise_for_status()
                assert_backend_identity(capabilities.json(), "native")
                created = client.post(self.base_url + "/databases", json={
                    "database_id": self.store_id, "name": "client_conformance"})
                created.raise_for_status()
                assert created.json()["database_id"] == self.store_id
            assert (self.root / "catalog.sqlite").is_file(), "native database did not create a catalog"
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        result = None
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
                result = self.process.returncode
                self.process.stdout.close()
                self.process = None
        finally:
            self.resources.close()
        return result

    def __exit__(self, *args):
        result = self.close()
        if args[0] is None:
            assert result in (None, 0), f"native server exited with status {result}"


@contextmanager
def client_server(directory, *, tls=None):
    backend = selected_backend()
    server_type = NativeHTTPServer if backend == "native" else MockHTTPServer
    with server_type(directory, tls=tls) as server:
        server.backend = backend
        server.mock = backend == "mock"
        verify = True if tls is None else tls.trusted_context()
        with httpx.Client(verify=verify, trust_env=False, timeout=10) as client:
            response = client.get(server.url + "/capabilities", headers={"Authorization": f"Bearer {TOKEN}"})
            response.raise_for_status()
            assert_backend_identity(response.json(), backend)
        yield server


def assert_storage(server, collection_id, rows):
    if server.mock:
        assert not list(server.directory.rglob("*.parquet"))
        assert not list(server.directory.rglob("catalog.sqlite"))
        return
    result = server.close()
    assert result in (None, 0), f"native server exited with status {result}"
    # Use the independent inspector: catalog -> Iceberg JSON -> Avro -> Parquet.
    from tools.acceptance.physical import inspect_store
    evidence = inspect_store(server.root, server.store_id, collection_id)
    assert evidence["views"]["latest"]["rows"] == str(rows)
    assert evidence["views"]["latest"]["files"]
    assert evidence["views"]["latest"]["manifests"]
    return evidence
