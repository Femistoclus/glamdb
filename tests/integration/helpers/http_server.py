"""Real subprocess/network helpers. No synthetic HTTP responses are returned."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import time

import httpx

REPOSITORY = Path(__file__).resolve().parents[3]
TOKEN = "integration-only-disposable-token"


def package_environment():
    env = os.environ.copy()
    sources = ("sdk/python/src", "apps/mock_server/src", "tools/verification/src")
    env["PYTHONPATH"] = os.pathsep.join(str(REPOSITORY / path) for path in sources)
    env["GLAMDB_TOKEN"] = TOKEN
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    return env


@dataclass(frozen=True)
class TLSMaterial:
    ca: Path
    certificate: Path
    key: Path
    wrong_ca: Path

    def trusted_context(self):
        return ssl.create_default_context(cafile=str(self.ca))

    def wrong_context(self):
        return ssl.create_default_context(cafile=str(self.wrong_ca))


def make_tls_material(directory: Path) -> TLSMaterial:
    """Create disposable test CA/server keys; never inspect user trust stores."""
    executable = shutil.which("openssl")
    if executable is None:
        raise RuntimeError("OpenSSL CLI is required for actual HTTPS integration")
    directory.mkdir(parents=True, exist_ok=True)
    def run(*args):
        subprocess.run([executable, *map(str, args)], check=True, capture_output=True, timeout=20)
    for prefix, name in (("ca", "GlamDB disposable integration CA"), ("wrong", "Unrelated disposable CA")):
        run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-sha256",
            "-keyout", directory / f"{prefix}.key", "-out", directory / f"{prefix}.pem",
            "-subj", f"/CN={name}", "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    run("req", "-newkey", "rsa:2048", "-nodes", "-sha256", "-keyout", directory / "server.key",
        "-out", directory / "server.csr", "-subj", "/CN=localhost")
    extensions = directory / "server.ext"
    extensions.write_text("subjectAltName=IP:127.0.0.1,DNS:localhost\n"
                          "basicConstraints=critical,CA:FALSE\n"
                          "keyUsage=critical,digitalSignature,keyEncipherment\n"
                          "extendedKeyUsage=serverAuth\n", encoding="utf-8")
    run("x509", "-req", "-in", directory / "server.csr", "-CA", directory / "ca.pem",
        "-CAkey", directory / "ca.key", "-CAcreateserial", "-out", directory / "server.pem",
        "-days", "1", "-sha256", "-extfile", extensions)
    return TLSMaterial(directory / "ca.pem", directory / "server.pem", directory / "server.key",
                       directory / "wrong.pem")


class MockHTTPServer:
    """Own a fresh loopback subprocess and always reap it, including on failure."""

    def __init__(self, directory: Path, *, tls: TLSMaterial | None = None):
        self.directory = directory
        self.tls = tls
        self.process = None
        self._log = None
        self.url = None

    def __enter__(self):
        ui = self.directory / "ui"
        (ui / "assets").mkdir(parents=True)
        (ui / "config").mkdir()
        (ui / "index.html").write_text("<!doctype html><title>Disposable integration mock</title>", encoding="utf-8")
        (ui / "config" / "theme.json").write_text(json.dumps({
            "version": 1, "brand": {}, "colors": {}, "typography": {},
            "sizing": {}, "decoration": {}, "labels": {}}), encoding="utf-8")
        self._log = (self.directory / "server.log").open("w+b")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen(128)
                port = listener.getsockname()[1]
                command = [sys.executable, str(Path(__file__).with_name("process_server.py")),
                           "--fd", str(listener.fileno()), "--ui-dir", str(ui)]
                if self.tls is not None:
                    command += ["--certfile", str(self.tls.certificate), "--keyfile", str(self.tls.key)]
                self.process = subprocess.Popen(command, env=package_environment(), cwd=REPOSITORY,
                                                pass_fds=(listener.fileno(),), stdin=subprocess.DEVNULL,
                                                stdout=self._log, stderr=subprocess.STDOUT)
            self.url = f"{'https' if self.tls else 'http'}://127.0.0.1:{port}"
            end = time.monotonic() + 15
            verify = True if self.tls is None else self.tls.trusted_context()
            with httpx.Client(verify=verify, timeout=0.25, trust_env=False) as readiness:
                while time.monotonic() < end:
                    if self.process.poll() is not None:
                        raise RuntimeError("mock subprocess exited before readiness: " + self.logs())
                    try:
                        response = readiness.get(self.url + "/health")
                        if response.status_code == 200 and response.json().get("backend") == "mock":
                            return self
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.025)  # bounded readiness poll, never a startup timing guess
            raise RuntimeError("mock subprocess readiness deadline exceeded: " + self.logs())
        except BaseException:
            self.close()
            raise

    def logs(self):
        if self._log is None:
            return ""
        self._log.flush()
        self._log.seek(0)
        return self._log.read()[-8000:].decode("utf-8", errors="replace")

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
        if self._log is not None:
            self._log.close()
            self._log = None

    def __exit__(self, *args):
        self.close()


class ScenarioHTTPTransport(httpx.BaseTransport):
    """Inject explicit test-only scenarios while delegating every request to TCP.

    ``drop_response_for`` closes a real successful HTTP response before the SDK
    can see it. This exercises the lost-response branch without mocking the SDK,
    its codec, HTTP serialization, server execution, or operation lookup.
    """

    def __init__(self, *, scenarios=None, drop_response_for=()):
        self.network = httpx.HTTPTransport(retries=0)
        self.scenarios = {} if scenarios is None else dict(scenarios)
        self.drop_response_for = set(drop_response_for)
        self.used = set()
        self.requests = []

    def handle_request(self, request):
        operation = None
        if request.method == "POST" and request.url.path.endswith("/append"):
            operation = json.loads(request.content)["operation_id"]
            self.requests.append((operation, bytes(request.content)))
            if operation not in self.used and operation in self.scenarios:
                request.headers["X-GlamDB-Mock-Scenario"] = self.scenarios[operation]
        response = self.network.handle_request(request)
        first = operation is not None and operation not in self.used
        if operation is not None:
            self.used.add(operation)
        if first and operation in self.drop_response_for:
            response.close()
            raise httpx.ReadError("test dropped a real response after headers", request=request)
        return response

    def close(self):
        self.network.close()
