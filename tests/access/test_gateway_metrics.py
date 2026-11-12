"""Metric contracts use explicit dependency doubles, not a production backend."""

import time
import json
import sqlite3
from uuid import uuid4

import anyio
import httpx
import pytest
from joserfc import jwk, jwt
from starlette.testclient import TestClient
from keycloak.exceptions import KeycloakError
from requests.exceptions import RequestException

from apps.workbench_access import authorization
from apps.workbench_access.authorization import Authorizer
from apps.workbench_access.identity import AuthError, Principal, TokenVerifier
from apps.workbench_access.settings import SsoSettings

ApplicationMetrics = pytest.importorskip(
    "tools.monitoring.application_metrics"
).ApplicationMetrics
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
from .test_gateway import setup


@pytest.fixture
def anyio_backend():
    return "asyncio"


def sample(registry, name, **labels):
    for family in text_string_to_metric_families(generate_latest(registry).decode()):
        for item in family.samples:
            if item.name == name and item.labels == {
                key: str(value) for key, value in labels.items()
            }:
                return item.value
    raise AssertionError((name, labels))


def count_series(registry):
    return sum(
        len(family.samples)
        for family in text_string_to_metric_families(generate_latest(registry).decode())
    )


def test_http_failures_csrf_readiness_and_series_bound(setup):
    app, path, first, second, own, policy, seen = setup
    with TestClient(app, base_url="https://testserver") as client:
        gateway = app.app.state.gateway
        metrics = gateway.metrics
        baseline = count_series(metrics.registry)
        assert sample(metrics.registry, "glamdb_gateway_ready") == 0
        assert client.get("/workbench/session").status_code == 401
        assert (
            sample(
                metrics.registry, "glamdb_gateway_http_responses_total", status_code=401
            )
            == 1
        )
        assert sample(metrics.registry, "glamdb_gateway_ready") == 0
        assert (
            client.get(
                "/workbench/session", headers={"Authorization": "Bearer valid"}
            ).status_code
            == 200
        )
        assert sample(metrics.registry, "glamdb_gateway_ready") == 1
        policy.unavailable = True
        assert (
            client.get(
                "/workbench/session", headers={"Authorization": "Bearer valid"}
            ).status_code
            == 503
        )
        assert sample(metrics.registry, "glamdb_gateway_ready") == 0
        policy.unavailable = False
        client.cookies.set("__Host-glamdb-session_0", "opaque")
        route = f"/databases/{own}/collections/{uuid4()}/scan"
        assert (
            client.post(
                route, headers={"X-Forwarded-Access-Token": "valid"}, json={}
            ).status_code
            == 403
        )
        assert (
            sample(
                metrics.registry,
                "glamdb_gateway_http_requests_total",
                route="scan",
                method="POST",
                status_class="4xx",
            )
            == 1
        )
        assert (
            sample(metrics.registry, "glamdb_gateway_http_in_flight", route="scan") == 0
        )
        assert client.get("/metrics/").status_code == 200
        assert count_series(metrics.registry) == baseline <= 1860
        text = generate_latest(metrics.registry).decode()
        assert own not in text and "Bearer valid" not in text and "opaque" not in text


@pytest.mark.anyio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_stream_completion_and_admission_backpressure(setup, cancelled):
    app, path, first, second, own, policy, seen = setup
    entered = anyio.Event()
    resume = anyio.Event()
    completed = anyio.Event()

    class Stream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            yield b"first"
            await resume.wait()
            yield b"last"

        async def aclose(self):
            self.closed = True

    stream = Stream()

    async def backend(request):
        return httpx.Response(200, stream=stream)

    async with app.app.router.lifespan_context(app.app):
        gateway = app.app.state.gateway
        gateway.backend_transport = httpx.MockTransport(backend)
        gateway.gate.maximum = 1
        registry = gateway.metrics.registry
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "method": "POST",
            "scheme": "https",
            "server": ("testserver", 443),
            "client": ("127.0.0.1", 1),
            "path": f"/databases/{own}/collections/{uuid4()}/scan",
            "query_string": b"",
            "headers": [(b"authorization", b"Bearer valid")],
            "http_version": "1.1",
        }
        scope["raw_path"] = scope["path"].encode()
        received = False

        async def receive():
            nonlocal received
            if not received:
                received = True
                return {"type": "http.request", "body": b"{}", "more_body": False}
            await anyio.sleep_forever()

        async def send(message):
            if (
                message["type"] == "http.response.body"
                and message.get("body") == b"first"
            ):
                entered.set()

        cancel_scope = anyio.CancelScope()

        async def request():
            with cancel_scope:
                await app(scope, receive, send)
            completed.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(request)
            await entered.wait()
            assert gateway.gate.total == 1
            assert sample(registry, "glamdb_gateway_http_in_flight", route="scan") == 1
            assert (
                sample(
                    registry,
                    "glamdb_gateway_http_request_duration_seconds_count",
                    route="scan",
                )
                == 0
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://testserver"
            ) as client:
                rejected = await client.get(
                    f"/databases/{own}/capabilities",
                    headers={"Authorization": "Bearer valid"},
                )
            assert rejected.status_code == 429
            assert gateway.gate.total == 1
            assert (
                sample(registry, "glamdb_gateway_http_responses_total", status_code=429)
                == 1
            )
            policy.unavailable = True
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="https://testserver"
            ) as client:
                failed = await client.get(
                    "/workbench/session", headers={"Authorization": "Bearer valid"}
                )
            assert failed.status_code == 503
            policy.unavailable = False
            if cancelled:
                cancel_scope.cancel()
            else:
                resume.set()
            await completed.wait()
        assert stream.closed and gateway.gate.total == 0
        assert sample(registry, "glamdb_gateway_http_in_flight", route="scan") == 0
        assert (
            sample(
                registry,
                "glamdb_gateway_http_request_duration_seconds_count",
                route="scan",
            )
            == 1
        )
        assert (
            sample(
                registry, "glamdb_gateway_stage_duration_seconds_count", stage="proxy"
            )
            == 1
        )
        assert (
            sample(
                registry, "glamdb_gateway_stage_duration_seconds_count", stage="catalog"
            )
            > 0
        )
        assert (
            sample(
                registry,
                "glamdb_gateway_http_responses_total",
                status_code=499 if cancelled else 200,
            )
            == 1
        )
        # A successful stream admitted before the dependency failure cannot
        # reopen readiness after that failure.
        assert sample(registry, "glamdb_gateway_ready") == 0
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="https://testserver"
        ) as client:
            assert (
                await client.get(
                    "/workbench/session", headers={"Authorization": "Bearer valid"}
                )
            ).status_code == 200
        assert sample(registry, "glamdb_gateway_ready") == 1


@pytest.mark.anyio
async def test_real_client_stage_observers_cannot_change_auth_results(
    tmp_path, monkeypatch
):
    metrics = ApplicationMetrics()

    def observer(stage, seconds):
        metrics.observe_stage(stage, seconds)
        raise RuntimeError("observer failure")

    key = jwk.RSAKey.generate_key(2048)
    key.ensure_kid()
    now = int(time.time())
    claims = {
        "iss": "https://identity.example/realms/glamdb",
        "aud": "glamdb-api",
        "sub": str(uuid4()),
        "iat": now,
        "exp": now + 60,
        "typ": "Bearer",
        "realm_access": {"roles": ["glamdb-operator"]},
    }
    secret = tmp_path / "secret"
    secret.write_text("test-service-secret")
    secret.chmod(0o600)

    async def peer(request):
        if request.url.path.endswith("/certs"):
            return httpx.Response(200, json={"keys": [key.as_dict(private=False)]})
        return httpx.Response(200, json={**claims, "active": True})

    settings = SsoSettings(
        issuer=claims["iss"],
        client_id="gateway",
        client_secret_file=secret,
        test_mode=True,
    )
    async with TokenVerifier(
        settings, transport=httpx.MockTransport(peer), observer=observer
    ) as verifier:
        principal = await verifier.verify(
            jwt.encode({"alg": "RS256", "kid": key.kid}, claims, key)
        )
    assert principal.subject == claims["sub"]
    assert (
        sample(
            metrics.registry,
            "glamdb_gateway_stage_duration_seconds_count",
            stage="jwt_validate",
        )
        == 1
    )
    assert (
        sample(
            metrics.registry,
            "glamdb_gateway_stage_duration_seconds_count",
            stage="introspect",
        )
        == 1
    )

    class Client:
        failed = False

        async def is_allowed(self, *args):
            if self.failed:
                raise RuntimeError("PDP unavailable")
            return True

        async def close(self):
            pass

    client = Client()
    monkeypatch.setattr(
        authorization, "AsyncCerbosClient", lambda *args, **kwargs: client
    )

    class Settings:
        cerbos_plaintext = True
        cerbos_target = "cerbos:3593"

    policy = Authorizer(Settings(), observer=observer)
    assert await policy.allowed(principal, "manage") is True
    client.failed = True
    with pytest.raises(AuthError) as unavailable:
        await policy.allowed(principal, "manage")
    assert unavailable.value.status == 503
    assert (
        sample(
            metrics.registry, "glamdb_gateway_stage_duration_seconds_count", stage="pdp"
        )
        == 2
    )
    await policy.close()


def test_idp_outage_and_observer_faults_preserve_results(setup, monkeypatch):
    app, path, first, second, own, policy, seen = setup
    with TestClient(app, base_url="https://testserver") as client:
        gateway = app.app.state.gateway
        headers = {"Authorization": "Bearer valid"}
        assert client.get("/workbench/session", headers=headers).status_code == 200
        verify = gateway.verifier.verify

        async def unavailable(token):
            raise AuthError(503, "UNAVAILABLE")

        gateway.verifier.verify = unavailable
        assert client.get("/workbench/session", headers=headers).status_code == 503
        assert sample(gateway.metrics.registry, "glamdb_gateway_ready") == 0
        gateway.verifier.verify = verify

        def broken(*args, **kwargs):
            raise RuntimeError("metric callback failure")

        gateway.observer = broken
        for method in ("observe_http", "observe_stage", "in_flight", "set_ready"):
            monkeypatch.setattr(gateway.metrics, method, broken)
        assert client.get("/workbench/session", headers=headers).status_code == 200
        assert (
            client.get(
                "/workbench/session", headers={"Authorization": "Bearer invalid"}
            ).status_code
            == 401
        )


@pytest.mark.parametrize(
    "failure",
    [
        sqlite3.OperationalError("PRIVATE-SOURCE"),
        KeycloakError("PRIVATE-SOURCE", response_code=503),
        RequestException("PRIVATE-SOURCE"),
    ],
)
def test_dependency_errors_are_sanitized_json_503(setup, failure):
    app, path, first, second, own, policy, seen = setup
    with TestClient(app, base_url="https://testserver") as client:
        gateway = app.app.state.gateway

        def unavailable(subject):
            raise failure

        gateway.directory.logout = unavailable
        response = client.post(
            "/workbench/logout", headers={"Authorization": "Bearer valid"}
        )
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "UNAVAILABLE"
        assert "PRIVATE-SOURCE" not in response.text
        assert sample(gateway.metrics.registry, "glamdb_gateway_ready") == 0


@pytest.mark.parametrize("body", [None, []])
def test_operator_requires_json_object(setup, body):
    app, path, first, second, own, policy, seen = setup
    gateway = app.app.state.gateway

    async def allowed(*args, **kwargs):
        return True

    policy.allowed = allowed
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            "/operator",
            headers={
                "Authorization": "Bearer valid",
                "Content-Type": "application/json",
            },
            content=json.dumps(body),
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "INVALID_ARGUMENT"


def test_actual_sdk_route_paths_and_arrow_negotiation():
    cid = str(uuid4())
    assert (
        authorization.classify("GET", "/collections:resolve").metric == "collection"
    )
    assert (
        authorization.classify("GET", f"/collections/{cid}/stats").metric
        == "statistics"
    )
    assert (
        authorization.classify("POST", f"/collections/{cid}/scan").action == "read"
    )
    for unsupported in (
        f"/collections/{cid}/statistics",
        f"/collections/{cid}/scan/arrow",
    ):
        with pytest.raises(ValueError):
            authorization.classify(
                "GET" if unsupported.endswith("statistics") else "POST", unsupported
            )
