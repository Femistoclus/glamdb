"""Explicit deployment settings; credentials belong to the identity provider."""

from dataclasses import dataclass
import ipaddress
import json
import math
import os
from pathlib import Path
import stat
from urllib.parse import urlsplit

from .resources import CatalogError, private_output, text


def secret_file(path, *, minimum=16):
    path = private_output(path)
    try:
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_mode & 0o077
            or info.st_uid != os.geteuid()
        ):
            raise ValueError()
        with path.open("rb") as source:
            raw = source.read(4097)
        value = raw.decode("ascii").rstrip("\r\n")
        if not minimum <= len(value) <= 4096 or any(
            ord(c) < 33 or ord(c) > 126 for c in value
        ):
            raise ValueError()
        return value
    except (OSError, ValueError, UnicodeError):
        raise CatalogError(
            "INVALID_STATE",
            "secret must be an owned mode0600 regular ASCII file outside the UI",
        ) from None


def origin(value, *, test_mode=False, path=False):
    parsed = urlsplit(text(value, 2048))
    try:
        host = parsed.hostname
        port = parsed.port
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = host == "localhost"
        if (
            not host
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or (parsed.path not in ("", "/") and not path)
        ):
            raise ValueError()
        if parsed.scheme != "https" and not (test_mode and parsed.scheme == "http"):
            raise ValueError()
        if port is not None and not 1 <= port <= 65535:
            raise ValueError()
        if parsed.scheme == "http" and not local and not test_mode:
            raise ValueError()
    except ValueError:
        raise CatalogError(
            "INVALID_ARGUMENT", "HTTPS origin required outside explicit test mode"
        ) from None
    return value.rstrip("/")


@dataclass(frozen=True)
class SsoSettings:
    issuer: str
    client_id: str
    client_secret_file: Path
    audience: str = "glamdb-api"
    backchannel: str | None = None
    ca_file: Path | None = None
    test_mode: bool = False
    token_max_seconds: int = 90
    timeout_seconds: float = 2.0
    directory_client_id: str | None = None
    directory_secret_file: Path | None = None

    def __post_init__(self):
        if type(self.test_mode) is not bool:
            raise CatalogError("INVALID_ARGUMENT", "test_mode must be boolean")
        origin(self.issuer, test_mode=self.test_mode, path=True)
        if not urlsplit(self.issuer).path.endswith("/realms/glamdb"):
            raise CatalogError(
                "INVALID_ARGUMENT", "issuer must identify the glamdb realm"
            )
        if self.backchannel:
            origin(self.backchannel, test_mode=self.test_mode, path=True)
        text(self.client_id)
        text(self.audience)
        if (
            type(self.token_max_seconds) is not int
            or not 30 <= self.token_max_seconds <= 300
            or type(self.timeout_seconds) not in (int, float)
            or not math.isfinite(self.timeout_seconds)
            or not 0.1 <= self.timeout_seconds <= 10
        ):
            raise CatalogError(
                "INVALID_ARGUMENT", "identity time/resource bounds are invalid"
            )

    @property
    def endpoint(self):
        return (self.backchannel or self.issuer).rstrip(
            "/"
        ) + "/protocol/openid-connect"


@dataclass(frozen=True)
class Settings:
    sso: SsoSettings
    catalog: Path
    csrf_secret_file: Path
    site: Path
    theme: Path
    public_origin: str
    cerbos_target: str = "cerbos:3593"
    cerbos_ca_file: Path | None = None
    cerbos_plaintext: bool = False
    max_inflight: int = 32
    max_body_bytes: int = 4 * 1024 * 1024
    drain_timeout_seconds: float = 30
    backend_timeout_seconds: float = 30

    def __post_init__(self):
        origin(self.public_origin, test_mode=self.sso.test_mode)
        if (
            type(self.cerbos_plaintext) is not bool
            or type(self.max_inflight) is not int
            or type(self.max_body_bytes) is not int
            or not 1 <= self.max_inflight <= 128
            or not 1024 <= self.max_body_bytes <= 16 * 1024 * 1024
        ):
            raise CatalogError("INVALID_ARGUMENT", "gateway admission bounds invalid")
        for duration in (self.drain_timeout_seconds, self.backend_timeout_seconds):
            if (
                type(duration) not in (int, float)
                or not math.isfinite(duration)
                or not 0.1 <= duration <= 300
            ):
                raise CatalogError("INVALID_ARGUMENT", "gateway timeout bounds invalid")
        site = Path(self.site).resolve()
        for path in (
            self.catalog,
            self.csrf_secret_file,
            self.sso.client_secret_file,
            self.sso.directory_secret_file,
        ):
            if path is not None and Path(path).resolve().is_relative_to(site):
                raise CatalogError(
                    "INVALID_ARGUMENT",
                    "catalog and secrets must remain outside the published site tree",
                )
        if (
            self.cerbos_plaintext
            and not self.sso.test_mode
            and not self.cerbos_target.startswith(
                ("cerbos:", "127.0.0.1:", "localhost:")
            )
        ):
            raise CatalogError(
                "INVALID_ARGUMENT",
                "plaintext PDP must remain on the private deployment network",
            )

    @classmethod
    def load(cls, path):
        try:
            with Path(path).open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                raise ValueError()

            def pairs(items):
                result = {}
                for key, value in items:
                    if key in result:
                        raise ValueError()
                    result[key] = value
                return result

            data = json.loads(raw, object_pairs_hook=pairs)
            if data.pop("version") != 3:
                raise ValueError()
            sso = data.pop("sso")
            sso["client_secret_file"] = Path(sso["client_secret_file"])
            for key in ("ca_file", "directory_secret_file"):
                if sso.get(key):
                    sso[key] = Path(sso[key])
            for key in (
                "catalog",
                "csrf_secret_file",
                "site",
                "theme",
                "cerbos_ca_file",
            ):
                if data.get(key):
                    data[key] = Path(data[key]).absolute()
            settings = cls(sso=SsoSettings(**sso), **data)
        except (OSError, ValueError, TypeError, KeyError):
            raise CatalogError(
                "INVALID_ARGUMENT", "invalid version3 gateway configuration"
            ) from None
        return settings
