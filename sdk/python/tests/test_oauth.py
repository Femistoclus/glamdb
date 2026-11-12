"""OAuth uses real Authlib and explicit loopback protocol peers, not a real IdP."""

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from urllib.parse import parse_qs

import httpx
import pytest

from glamdb_sdk import Client, ValidationError


@contextmanager
def oauth_peer():
    class Peer:
        calls = []
        replies = []
        delay = 0

    peer = Peer()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            data = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
            peer.calls.append((self.path, data, self.headers.get("Authorization"), time.monotonic()))
            status, body = peer.replies.pop(0)
            time.sleep(peer.delay)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            if status == 302:
                self.send_header("Location", "/redirected")
            self.end_headers()
            try:
                self.wfile.write(json.dumps(body).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    peer.url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield peer
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def token(value="ACCESS_SECRET_MARKER", expires=3600, **extra):
    return {"access_token": value, "token_type": "Bearer", "expires_in": expires, **extra}


def test_basic_credentials_have_an_explicit_migration_error_before_connecting():
    with pytest.raises(ValidationError, match="Basic.*OAuth|username.*removed"):
        Client("https://example.test", "PASSWORD_SECRET_MARKER", username="organization")


def test_provider_refreshes_header_before_each_call_without_changing_database_prefix():
    values = iter(["first-token", "second-token"])
    seen = []
    with Client("https://example.test/databases/00112233-4455-6677-8899-aabbccddeeff",
                token_provider=lambda: next(values),
                transport=httpx.MockTransport(lambda request: seen.append(request) or httpx.Response(200, json={}))) as client:
        client.capabilities()
        client.capabilities()
        assert "first-token" not in repr(client)
    assert [r.headers["Authorization"] for r in seen] == ["Bearer first-token", "Bearer second-token"]
    assert all(r.url.path == "/databases/00112233-4455-6677-8899-aabbccddeeff/capabilities" for r in seen)


def test_provider_error_or_invalid_result_never_sends_or_renders_secret():
    def failing():
        raise RuntimeError("ACCESS_SECRET_MARKER")
    for provider in (failing, lambda: "bad\nACCESS_SECRET_MARKER", lambda: None):
        with Client("https://example.test", token_provider=provider,
                    transport=httpx.MockTransport(lambda _: pytest.fail("must not send"))) as client:
            with pytest.raises(Exception) as caught:
                client.capabilities()
            assert "ACCESS_SECRET_MARKER" not in str(caught.value)
            assert "ACCESS_SECRET_MARKER" not in repr(caught.value)


def test_provider_and_static_token_are_mutually_exclusive_and_remote_http_is_rejected():
    with pytest.raises(ValidationError):
        Client("https://example.test", "static", token_provider=lambda: "dynamic")
    with pytest.raises(ValidationError, match="HTTPS"):
        Client("http://example.test", token_provider=lambda: "dynamic")


def test_client_credentials_authlib_reacquisition_and_concurrency(caplog):
    from glamdb_sdk.oauth import ClientCredentialsTokenProvider
    with oauth_peer() as peer:
        peer.replies = [(200, token("FIRST_ACCESS_SECRET", 1)), (200, token("SECOND_ACCESS_SECRET"))]
        with ClientCredentialsTokenProvider(peer.url + "/token", "service-client", "CLIENT_SECRET_MARKER",
                                            scope="database", allow_insecure_loopback=True) as provider:
            assert provider() == "FIRST_ACCESS_SECRET"
            with ThreadPoolExecutor(max_workers=8) as workers:
                assert list(workers.map(lambda _: provider(), range(8))) == ["SECOND_ACCESS_SECRET"] * 8
            assert "SECRET" not in repr(provider)
        assert len(peer.calls) == 2
        for _, data, authorization, _ in peer.calls:
            assert data["grant_type"] == ["client_credentials"]
            assert data["scope"] == ["database"]
            assert base64.b64decode(authorization.split(" ")[1]) == b"service-client:CLIENT_SECRET_MARKER"
        assert "CLIENT_SECRET_MARKER" not in caplog.text
        assert "SECOND_ACCESS_SECRET" not in caplog.text


def test_device_pending_slow_down_then_authlib_refresh(caplog):
    from glamdb_sdk.oauth import DeviceAuthorizationClient, OAuthError
    with oauth_peer() as peer:
        peer.replies = [
            (200, {"device_code": "DEVICE_SECRET_MARKER", "user_code": "ABCD-EFGH",
                   "verification_uri": "https://identity.example/device", "expires_in": 60, "interval": 1}),
            (400, {"error": "authorization_pending"}),
            (400, {"error": "slow_down"}),
            (200, token("DEVICE_ACCESS_SECRET", 1, refresh_token="REFRESH_SECRET_MARKER")),
            (200, token("REFRESHED_ACCESS_SECRET")),
        ]
        with DeviceAuthorizationClient(peer.url + "/token", peer.url + "/device", "human-client",
                                       scope="openid database", allow_insecure_loopback=True) as provider:
            challenge = provider.begin()
            assert challenge.user_code == "ABCD-EFGH"
            assert challenge.verification_uri == "https://identity.example/device"
            assert "DEVICE_SECRET_MARKER" not in repr(challenge)
            with pytest.raises(OAuthError) as timed_out:
                provider.poll(challenge, timeout=2.5)
            assert timed_out.value.code == "device_timeout"
            assert provider.poll(challenge, timeout=12) is None
            assert provider() == "REFRESHED_ACCESS_SECRET"
            assert "SECRET" not in repr(provider)
        calls = peer.calls
        assert calls[0][1] == {"client_id": ["human-client"], "scope": ["openid database"]}
        assert [c[1]["grant_type"][0] for c in calls[1:]] == [
            "urn:ietf:params:oauth:grant-type:device_code"] * 3 + ["refresh_token"]
        assert calls[2][3] - calls[1][3] >= 1
        assert calls[3][3] - calls[2][3] >= 6
        assert calls[-1][1]["refresh_token"] == ["REFRESH_SECRET_MARKER"]
        assert all(c[2] is None for c in calls)
        assert "SECRET_MARKER" not in caplog.text
        assert "DEVICE_ACCESS_SECRET" not in caplog.text


def test_device_timeout_stops_polling_and_terminal_errors_are_sanitized():
    from glamdb_sdk.oauth import DeviceAuthorizationClient, OAuthError
    with oauth_peer() as peer:
        peer.replies = [(200, {"device_code": "DEVICE_SECRET_MARKER", "user_code": "ABCD",
                              "verification_uri": "https://identity.example/device", "expires_in": 30, "interval": 1})]
        with DeviceAuthorizationClient(peer.url + "/token", peer.url + "/device", "human",
                                       allow_insecure_loopback=True) as provider:
            challenge = provider.begin()
            with pytest.raises(OAuthError) as caught:
                provider.poll(challenge, timeout=0.05)
            assert caught.value.code == "device_timeout"
            assert len(peer.calls) == 1
            peer.replies = [(400, {"error": "access_denied", "error_description": "DEVICE_SECRET_MARKER"})]
            with pytest.raises(OAuthError) as caught:
                provider.poll(challenge, timeout=3)
            assert caught.value.code == "access_denied"
            assert "DEVICE_SECRET_MARKER" not in str(caught.value)


@pytest.mark.parametrize("url", ["http://identity.example/token", "https://u:SECRET@example.test/token",
                                "https://identity.example/token?secret=SECRET", "https://identity.example/token#SECRET"])
def test_oauth_endpoint_requires_verified_tls_and_no_url_credentials(url):
    from glamdb_sdk.oauth import ClientCredentialsTokenProvider
    with pytest.raises(ValidationError):
        ClientCredentialsTokenProvider(url, "client", "secret")


def test_oauth_loopback_escape_is_explicit_redirects_and_timeout_never_expose_secrets():
    from glamdb_sdk.oauth import ClientCredentialsTokenProvider, OAuthError
    with oauth_peer() as peer:
        with pytest.raises(ValidationError):
            ClientCredentialsTokenProvider(peer.url + "/token", "client", "CLIENT_SECRET_MARKER")
        peer.replies = [(302, token())]
        with ClientCredentialsTokenProvider(peer.url + "/token", "client", "CLIENT_SECRET_MARKER",
                                            allow_insecure_loopback=True) as provider:
            with pytest.raises(OAuthError) as caught:
                provider()
            assert caught.value.code == "redirect_disallowed"
            assert len(peer.calls) == 1
        peer.replies = [(200, token())]
        peer.delay = 0.2
        with ClientCredentialsTokenProvider(peer.url + "/token", "client", "CLIENT_SECRET_MARKER",
                                            timeout=0.05, allow_insecure_loopback=True) as provider:
            with pytest.raises(OAuthError) as caught:
                provider()
            assert caught.value.code == "transport_error"
            assert "CLIENT_SECRET_MARKER" not in str(caught.value)


def test_provider_does_not_replay_a_mutating_post_after_401():
    from glamdb_sdk import AuthenticationError, Field, Schema
    calls, acquired = [], []
    def credentials():
        acquired.append(1)
        return "access-token"
    def denied(request):
        calls.append(request)
        return httpx.Response(401, json={"error": {"code": "UNAUTHENTICATED", "message": "denied"}})
    collection = "00112233-4455-6677-8899-aabbccddeeff"
    with Client("https://example.test/databases/" + collection, token_provider=credentials,
                transport=httpx.MockTransport(denied)) as client:
        with pytest.raises(AuthenticationError):
            client.create_collection("same", Schema([Field("n", "int64", False)]), collection_id=collection)
    assert len(calls) == len(acquired) == 1
    assert calls[0].method == "POST"
    assert calls[0].url.path == "/databases/" + collection + "/collections"


def test_provider_delay_consumes_caller_deadline_before_post():
    from glamdb_sdk import DeadlineExceeded, Field, Schema
    def credentials():
        time.sleep(0.02)
        return "access-token"
    collection = "00112233-4455-6677-8899-aabbccddeeff"
    with Client("https://example.test", token_provider=credentials,
                transport=httpx.MockTransport(lambda _: pytest.fail("must not POST after deadline"))) as client:
        with pytest.raises(DeadlineExceeded) as caught:
            client.create_collection("same", Schema([Field("n", "int64", False)]), collection_id=collection, deadline=0.01)
    assert caught.value.collection_id == collection


@pytest.mark.parametrize("verify,escape", [(False, False), (False, True), (True, "yes")])
def test_oauth_remote_tls_cannot_be_disabled_by_the_loopback_escape(verify, escape):
    from glamdb_sdk.oauth import ClientCredentialsTokenProvider
    with pytest.raises(ValidationError):
        ClientCredentialsTokenProvider("https://identity.example/token", "client", "secret",
                                       verify=verify, allow_insecure_loopback=escape)


def test_id_token_alone_cannot_be_used_as_an_access_token():
    from glamdb_sdk.oauth import ClientCredentialsTokenProvider, OAuthError
    with oauth_peer() as peer:
        peer.replies = [(200, {"id_token": "ID_TOKEN_SECRET", "token_type": "Bearer", "expires_in": 3600})]
        with ClientCredentialsTokenProvider(peer.url + "/token", "client", "CLIENT_SECRET_MARKER",
                                            allow_insecure_loopback=True) as provider:
            with pytest.raises(OAuthError) as caught:
                provider()
            assert caught.value.code == "invalid_response"
            assert "ID_TOKEN_SECRET" not in str(caught.value)
