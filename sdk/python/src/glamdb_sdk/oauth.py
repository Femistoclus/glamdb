"""Optional Authlib token providers. Credentials remain in process memory only."""

from dataclasses import dataclass, field
import ipaddress
import math
import ssl
import threading
import time

import httpx

from .client import _bearer_token
from .errors import SDKError, ValidationError

_CODES = frozenset(
    {
        "invalid_client",
        "invalid_grant",
        "invalid_scope",
        "unauthorized_client",
        "unsupported_grant_type",
        "access_denied",
        "expired_token",
        "authorization_pending",
        "slow_down",
        "device_timeout",
        "invalid_response",
        "http_error",
        "transport_error",
        "reauthorization_required",
        "redirect_disallowed",
        "closed",
    }
)


class OAuthError(SDKError):
    """Sanitized OAuth failure; server descriptions, URLs and token bodies are omitted."""

    def __init__(self, code: str):
        self.code = code if code in _CODES else "invalid_response"
        super().__init__(f"OAuth token acquisition failed ({self.code})")


def _seconds(value: float, name: str, maximum: float = 900) -> float:
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or not 0 < value <= maximum
    ):
        raise ValidationError(
            f"{name} must be positive finite seconds, at most {maximum:g}"
        )
    return float(value)


def _endpoint(
    value: str, allow_insecure_loopback: bool, *, query: bool = False
) -> tuple[str, bool]:
    if type(allow_insecure_loopback) is not bool:
        raise ValidationError("allow_insecure_loopback must be an explicit boolean")
    try:
        url = httpx.URL(value)
        if (
            not url.host
            or url.username
            or url.password
            or url.fragment
            or (url.query and not query)
        ):
            raise ValueError()
        try:
            loopback = ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            loopback = url.host == "localhost"
        if url.scheme != "https" and not (
            url.scheme == "http" and loopback and allow_insecure_loopback
        ):
            raise ValueError()
        return str(url), loopback
    except (httpx.InvalidURL, ValueError, TypeError):
        raise ValidationError(
            "OAuth requires an absolute HTTPS URL without credentials, query or fragment; "
            "HTTP loopback requires allow_insecure_loopback=True for tests"
        ) from None


def _response(response):
    if response.is_redirect:
        raise OAuthError("redirect_disallowed")
    if (
        response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
    ):
        raise OAuthError("invalid_response")
    if not 200 <= response.status_code < 300:
        try:
            data = response.json()
        except ValueError:
            raise OAuthError("http_error") from None
        if not isinstance(data, dict) or not isinstance(data.get("error"), str):
            raise OAuthError("http_error")
    return response


class _OAuthProvider:
    def __init__(
        self,
        token_endpoint: str,
        client_id: str,
        client_secret: str | None = None,
        *,
        scope: str | None = None,
        timeout: float = 30,
        verify: bool | ssl.SSLContext = True,
        allow_insecure_loopback: bool = False,
        grant_type: str,
    ):
        endpoint, loopback = _endpoint(token_endpoint, allow_insecure_loopback)
        if (
            not isinstance(client_id, str)
            or not client_id
            or any(ord(c) < 32 for c in client_id)
        ):
            raise ValidationError(
                "OAuth client_id is required without control characters"
            )
        if scope is not None and (
            not isinstance(scope, str) or any(ord(c) < 32 for c in scope)
        ):
            raise ValidationError("OAuth scope must be text without control characters")
        if not isinstance(verify, (bool, ssl.SSLContext)) or (
            verify is False and not (loopback and allow_insecure_loopback)
        ):
            raise ValidationError(
                "OAuth TLS verification may be disabled only for an explicit loopback test"
            )
        if client_secret is not None and (
            not isinstance(client_secret, str) or not client_secret
        ):
            raise ValidationError("OAuth client_secret must be a nonempty string")
        self._timeout = _seconds(timeout, "timeout", maximum=300)
        self._allow_loopback = allow_insecure_loopback
        self._lock = threading.RLock()
        self._closed = False
        self._grant_type = grant_type
        try:
            from authlib.integrations.httpx_client import OAuth2Client
        except ImportError:
            raise ValidationError(
                "install glamdb-sdk[oauth] to use Authlib token providers"
            ) from None
        self._client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            leeway=5,
            token_endpoint=endpoint,
            grant_type=grant_type,
            timeout=self._timeout,
            verify=verify,
            follow_redirects=False,
            trust_env=False,
        )
        self._client.register_compliance_hook("access_token_response", _response)
        self._client.register_compliance_hook("refresh_token_response", _response)

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, *_):
        self.close()

    def _open(self):
        if self._closed:
            raise OAuthError("closed")

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                self._client.token = None
                self._client.client_secret = None
                self._client.close()

    @staticmethod
    def _invoke(function, *args, **kwargs):
        try:
            return function(*args, **kwargs)
        except OAuthError:
            raise
        except Exception as error:
            code = getattr(error, "error", None)
            if not isinstance(code, str) or code not in _CODES:
                code = (
                    "invalid_response"
                    if isinstance(error, (ValueError, TypeError, KeyError))
                    else "transport_error"
                )
            raise OAuthError(code) from None

    def _access(self) -> str:
        token = self._client.token
        if (
            not isinstance(token, dict)
            or str(token.get("token_type", "")).lower() != "bearer"
        ):
            raise OAuthError("invalid_response")
        try:
            return _bearer_token(token.get("access_token"))
        except ValidationError:
            raise OAuthError("invalid_response") from None

    def __call__(self) -> str:
        with self._lock:
            self._open()
            if not self._client.token:
                if self._grant_type != "client_credentials":
                    raise OAuthError("reauthorization_required")
                self._invoke(self._client.fetch_token, grant_type="client_credentials")
            elif not self._invoke(self._client.ensure_active_token):
                raise OAuthError("reauthorization_required")
            return self._access()


class ClientCredentialsTokenProvider(_OAuthProvider):
    """Callable service-account access token provider; Authlib owns acquisition/renewal."""

    def __init__(
        self, token_endpoint: str, client_id: str, client_secret: str, **kwargs
    ):
        if not isinstance(client_secret, str) or not client_secret:
            raise ValidationError(
                "a nonempty OAuth client_secret is required for client credentials"
            )
        super().__init__(
            token_endpoint,
            client_id,
            client_secret,
            grant_type="client_credentials",
            **kwargs,
        )


@dataclass(frozen=True, repr=False)
class DeviceAuthorization:
    """Display user_code and verification_uri explicitly; repr hides all fields."""

    user_code: str
    verification_uri: str
    verification_uri_complete: str | None
    _device_code: str = field(repr=False)
    _expires_at: float = field(repr=False)


class DeviceAuthorizationClient(_OAuthProvider):
    """Public native-device client. No password grant, browser session or token files."""

    def __init__(
        self,
        token_endpoint: str,
        device_authorization_endpoint: str,
        client_id: str,
        **kwargs,
    ):
        self._device_endpoint, _ = _endpoint(
            device_authorization_endpoint, kwargs.get("allow_insecure_loopback", False)
        )
        self._challenge = None
        self._poll_interval = 0
        self._next_poll = 0
        super().__init__(
            token_endpoint,
            client_id,
            grant_type="urn:ietf:params:oauth:grant-type:device_code",
            **kwargs,
        )

    def begin(self) -> DeviceAuthorization:
        with self._lock:
            self._open()
            self._client.token = None
            started = time.monotonic()
            data = {"client_id": self._client.client_id}
            if self._client.scope:
                data["scope"] = self._client.scope
            response = self._invoke(
                self._client.post, self._device_endpoint, data=data, auth=None
            )
            try:
                self._invoke(_response, response)
                value = self._invoke(response.json)
                if isinstance(value, dict) and "error" in value:
                    raise OAuthError(
                        value["error"]
                        if isinstance(value["error"], str)
                        else "invalid_response"
                    )
                if not isinstance(value, dict):
                    raise OAuthError("invalid_response")
                device_code, user_code = value.get("device_code"), value.get(
                    "user_code"
                )
                if not all(
                    isinstance(c, str)
                    and c
                    and len(c) <= 4096
                    and not any(ord(v) < 32 for v in c)
                    for c in (device_code, user_code)
                ):
                    raise OAuthError("invalid_response")
                try:
                    lifetime = _seconds(value.get("expires_in"), "device expires_in")
                    interval = _seconds(
                        value.get("interval", 5), "device interval", maximum=300
                    )
                    verification, _ = _endpoint(
                        value.get("verification_uri"), self._allow_loopback, query=True
                    )
                    complete = value.get("verification_uri_complete")
                    if complete is not None:
                        complete, _ = _endpoint(
                            complete, self._allow_loopback, query=True
                        )
                except ValidationError:
                    raise OAuthError("invalid_response") from None
                self._challenge = DeviceAuthorization(
                    user_code, verification, complete, device_code, started + lifetime
                )
                self._poll_interval = interval
                self._next_poll = time.monotonic() + interval
                return self._challenge
            finally:
                response.close()

    def poll(self, challenge: DeviceAuthorization, *, timeout: float = 300) -> None:
        """Wait for explicit user authorization; pending/slow_down follow RFC 8628."""
        timeout = _seconds(timeout, "device polling timeout")
        with self._lock:
            self._open()
            if (
                not isinstance(challenge, DeviceAuthorization)
                or challenge is not self._challenge
            ):
                raise ValidationError(
                    "device authorization is not current for this client"
                )
            end = min(challenge._expires_at, time.monotonic() + timeout)
            while True:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise OAuthError("device_timeout")
                time.sleep(min(max(0, self._next_poll - time.monotonic()), remaining))
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise OAuthError("device_timeout")
                try:
                    self._invoke(
                        self._client.fetch_token,
                        grant_type=self._grant_type,
                        device_code=challenge._device_code,
                        timeout=min(self._timeout, remaining),
                    )
                except OAuthError as error:
                    if error.code in ("authorization_pending", "slow_down"):
                        if error.code == "slow_down":
                            self._poll_interval += 5
                        self._next_poll = time.monotonic() + self._poll_interval
                        continue
                    self._challenge = None
                    raise
                self._challenge = None
                if time.monotonic() >= end:
                    self._client.token = None
                    raise OAuthError("device_timeout")
                self._access()
                return
