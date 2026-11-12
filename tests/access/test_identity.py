import time
from uuid import uuid4

import httpx
import pytest
from joserfc import jwk, jwt

from apps.workbench_access.identity import AuthError, TokenVerifier, credentials
from apps.workbench_access.settings import SsoSettings


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_cookie_and_bearer_context_are_unambiguous():
    assert credentials({"authorization": "Bearer abc"}) == ("abc", False)
    assert credentials(
        {"cookie": "__Host-glamdb-session_0=x", "x-forwarded-access-token": "abc"}
    ) == ("abc", True)
    for headers in [
        {"authorization": "Basic abc"},
        {"authorization": "Bearer abc", "cookie": "__Host-glamdb-session_1=x"},
        {"authorization": "Bearer abc", "x-forwarded-access-token": "different"},
        {"x-forwarded-access-token": "abc"},
    ]:
        with pytest.raises(AuthError):
            credentials(headers)


@pytest.mark.anyio
async def test_real_jose_validation_and_live_introspection(tmp_path):
    key = jwk.RSAKey.generate_key(2048)
    key.ensure_kid()
    now = int(time.time())
    subject = str(uuid4())
    org = str(uuid4())
    claims = {
        "iss": "https://login.example/realms/glamdb",
        "aud": "glamdb-api",
        "sub": subject,
        "iat": now,
        "exp": now + 60,
        "typ": "Bearer",
        "organization": {"pink": {"id": org}},
        "realm_access": {"roles": ["glamdb-member"]},
    }
    active = True
    calls = []
    current_changes = {}

    async def peer(request):
        nonlocal active
        calls.append(request.url.path)
        if request.url.path.endswith("/certs"):
            return httpx.Response(200, json={"keys": [key.as_dict(private=False)]})
        return httpx.Response(200, json={**claims, "active": active, **current_changes})

    secret = tmp_path / "secret"
    secret.write_text("test-service-secret")
    secret.chmod(0o600)
    settings = SsoSettings(
        issuer=claims["iss"], client_id="glamdb-gateway", client_secret_file=secret
    )
    async with TokenVerifier(settings, transport=httpx.MockTransport(peer)) as verifier:
        raw = jwt.encode({"alg": "RS256", "kid": key.kid}, claims, key)
        principal = await verifier.verify(raw)
        assert principal.subject == subject and principal.identity_organization == org
        for changed in [
            {"realm_access": {"roles": []}},
            {"organization": {"new": {"id": str(uuid4())}}},
        ]:
            current_changes.clear()
            current_changes.update(changed)
            with pytest.raises(AuthError):
                await verifier.verify(raw)
        current_changes.clear()
        active = False
        with pytest.raises(AuthError) as failure:
            await verifier.verify(raw)
        assert failure.value.status == 401
        assert (
            calls.count("/realms/glamdb/protocol/openid-connect/token/introspect") == 4
        )
        for changed in [
            {"aud": "other"},
            {"typ": "ID"},
            {"exp": now - 120},
            {"exp": now + 3600},
            {"organization": {}},
            {"organization": {"a": {"id": org}, "b": {"id": str(uuid4())}}},
        ]:
            with pytest.raises(AuthError):
                await verifier.verify(
                    jwt.encode(
                        {"alg": "RS256", "kid": key.kid}, {**claims, **changed}, key
                    )
                )


@pytest.mark.anyio
async def test_idp_failure_is_unavailable_not_grant(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("long-service-secret")
    secret.chmod(0o600)

    async def failed(request):
        raise httpx.ConnectError("unavailable", request=request)

    async with TokenVerifier(
        SsoSettings(
            issuer="https://login.example/realms/glamdb",
            client_id="api",
            client_secret_file=secret,
        ),
        transport=httpx.MockTransport(failed),
    ) as verifier:
        with pytest.raises(AuthError) as failure:
            await verifier.verify("a.b.c")
        assert failure.value.status == 503


@pytest.mark.anyio
@pytest.mark.parametrize(
    "status,code,expected",
    [
        (400, "invalid_request", 401),
        (400, "invalid_token", 401),
        (400, "invalid_client", 503),
        (401, "invalid_client", 503),
        (503, "server_error", 503),
    ],
)
async def test_native_token_rejection_is_not_dependency_outage(
    tmp_path, status, code, expected
):
    key = jwk.RSAKey.generate_key(2048)
    key.ensure_kid()
    now = int(time.time())
    claims = {
        "iss": "https://login.example/realms/glamdb",
        "aud": "glamdb-api",
        "sub": str(uuid4()),
        "iat": now,
        "exp": now + 60,
        "typ": "Bearer",
        "realm_access": {"roles": ["glamdb-operator"]},
    }

    async def peer(request):
        if request.url.path.endswith("/certs"):
            return httpx.Response(200, json={"keys": [key.as_dict(private=False)]})
        return httpx.Response(status, json={"error": code})

    secret = tmp_path / "secret"
    secret.write_text("long-service-client-secret")
    secret.chmod(0o600)
    settings = SsoSettings(
        issuer=claims["iss"], client_id="gateway", client_secret_file=secret
    )
    async with TokenVerifier(settings, transport=httpx.MockTransport(peer)) as verifier:
        token = jwt.encode({"alg": "RS256", "kid": key.kid}, claims, key)
        with pytest.raises(AuthError) as failure:
            await verifier.verify(token)
        assert failure.value.status == expected
