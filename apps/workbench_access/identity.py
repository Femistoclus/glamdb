"""OIDC access-token verification and live revocation checks using upstream clients."""

from dataclasses import dataclass, field
import re
import ssl
import time
from uuid import UUID

from authlib.integrations.httpx_client import AsyncOAuth2Client
import httpx
from joserfc import jwk, jwt

from .settings import secret_file


def finish_stage(observer, stage, started):
    if observer is not None:
        try:
            observer(stage, max(0.0, time.perf_counter() - started))
        except Exception:
            pass


class AuthError(Exception):
    def __init__(self, status=401, code="UNAUTHENTICATED"):
        self.status = status
        self.code = code
        super().__init__(
            "Identity service unavailable"
            if status == 503
            else "Authentication required"
        )


def credentials(headers):
    authorization = headers.get("authorization", "")
    forwarded = headers.get("x-forwarded-access-token", "")
    cookie = any(
        part.strip().split("=", 1)[0] == "__Host-glamdb-session"
        or re.fullmatch(r"__Host-glamdb-session_[0-9]+", part.strip().split("=", 1)[0])
        for part in headers.get("cookie", "").split(";")
    )
    if authorization:
        if cookie or not authorization.startswith("Bearer "):
            raise AuthError()
        token = authorization[7:]
        if forwarded and forwarded != token:
            raise AuthError()
    else:
        if not cookie or not forwarded:
            raise AuthError()
        token = forwarded
    if (
        not token
        or len(token) > 16384
        or any(ord(c) < 33 or ord(c) > 126 for c in token)
    ):
        raise AuthError()
    return token, cookie


def policy_claims(claims):
    try:
        role_values = claims.get("realm_access", {}).get("roles", [])
        if (
            not isinstance(role_values, list)
            or len(role_values) > 128
            or any(not isinstance(role, str) for role in role_values)
        ):
            raise ValueError()
        roles = tuple(
            role for role in ("glamdb-member", "glamdb-operator") if role in role_values
        )
        if not roles:
            raise ValueError()
        organizations = claims.get("organization", {})
        if not isinstance(organizations, dict) or len(organizations) > 1:
            raise ValueError()
        identity_org = None
        if organizations:
            identity_org = next(iter(organizations.values()))["id"]
            if str(UUID(identity_org)) != identity_org:
                raise ValueError()
        if "glamdb-member" in roles and identity_org is None:
            raise ValueError()
        return roles, identity_org
    except (ValueError, TypeError, AttributeError, KeyError):
        raise AuthError() from None


@dataclass(frozen=True)
class Principal:
    subject: str
    identity_organization: str | None
    roles: tuple[str, ...]
    expires_at: int
    organization_id: str | None = None
    token: str = field(default="", repr=False, compare=False)


class TokenVerifier:
    def __init__(self, settings, *, transport=None, observer=None):
        self.observer = observer
        self.settings = settings
        self.keys = None
        self.keys_until = 0
        verify = (
            ssl.create_default_context(cafile=str(settings.ca_file))
            if settings.ca_file
            else True
        )
        self.client = AsyncOAuth2Client(
            client_id=settings.client_id,
            client_secret=secret_file(settings.client_secret_file),
            token_endpoint_auth_method="client_secret_basic",
            verify=verify,
            timeout=settings.timeout_seconds,
            transport=transport,
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
            follow_redirects=False,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        await self.close()

    async def close(self):
        await self.client.aclose()

    async def verify(self, raw):
        started = time.perf_counter()
        try:
            try:
                if not self.keys or time.monotonic() >= self.keys_until:
                    response = await self.client.request(
                        "GET", self.settings.endpoint + "/certs", withhold_token=True
                    )
                    response.raise_for_status()
                    if len(response.content) > 65536:
                        raise ValueError()
                    keys = response.json()
                    if (
                        not isinstance(keys.get("keys"), list)
                        or not 1 <= len(keys["keys"]) <= 16
                    ):
                        raise ValueError()
                    self.keys = jwk.KeySet.import_key_set(keys)
                    self.keys_until = time.monotonic() + 30
            except Exception:
                raise AuthError(503, "UNAVAILABLE") from None
            try:
                decoded = jwt.decode(raw, self.keys, algorithms=["RS256"])
                claims = decoded.claims
                registry = jwt.JWTClaimsRegistry(
                    leeway=5,
                    iss={"essential": True, "value": self.settings.issuer},
                    aud={"essential": True, "value": self.settings.audience},
                    exp={"essential": True},
                    iat={"essential": True},
                    sub={"essential": True},
                )
                registry.validate(claims)
                if (
                    claims.get("typ") != "Bearer"
                    or type(claims["exp"]) is not int
                    or type(claims["iat"]) is not int
                ):
                    raise ValueError()
                if (
                    not 0
                    < claims["exp"] - claims["iat"]
                    <= self.settings.token_max_seconds
                    or claims["iat"] > time.time() + 5
                ):
                    raise ValueError()
                subject = str(UUID(claims["sub"]))
                if subject != claims["sub"]:
                    raise ValueError()
                roles, identity_org = policy_claims(claims)
            except Exception:
                raise AuthError() from None
        finally:
            finish_stage(self.observer, "jwt_validate", started)
        started = time.perf_counter()
        try:
            try:
                response = await self.client.introspect_token(
                    self.settings.endpoint + "/token/introspect",
                    token=raw,
                    token_type_hint="access_token",
                )
                if len(response.content) > 32768:
                    raise ValueError()
                if response.status_code == 400 and response.json().get("error") in (
                    "invalid_request",
                    "invalid_token",
                ):
                    # Keycloak reports a revoked organization context this way.
                    raise AuthError()
                response.raise_for_status()
                current = response.json()
            except AuthError:
                raise
            except Exception:
                raise AuthError(503, "UNAVAILABLE") from None
            if (
                current.get("active") is not True
                or current.get("sub") != subject
                or current.get("iss") != self.settings.issuer
            ):
                raise AuthError()
            audience = current.get("aud", [])
            if self.settings.audience not in (
                [audience] if isinstance(audience, str) else audience
            ):
                raise AuthError()
            current_roles, current_org = policy_claims(current)
            if current_org != identity_org or any(
                role not in current_roles for role in roles
            ):
                raise AuthError()
            return Principal(subject, identity_org, roles, claims["exp"], token=raw)
        finally:
            finish_stage(self.observer, "introspect", started)
