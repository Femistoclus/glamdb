"""Opt-in real Keycloak/Cerbos/OAuth2 Proxy checks on an explicit disposable fixture.

Set GLAMDB_IDENTITY_FIXTURE to a private fixture.json. No target discovery, saved
browser sessions, token files, traces, videos or raw credential diagnostics.
Provision automation clients with native organization membership, the API and
gateway audiences, and the optional organization scope. Remove their default
email client scope: service accounts without email must omit email_verified,
while the separate browser client's verified-email requirement stays enabled.
Database adapters in this identity fixture are explicitly disposable mocks.
Native engine/scanner code exists; a native public HTTP adapter is still needed
to run scripts/check-system.py against the native backend.
"""

import importlib.util
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import ssl
import stat
import subprocess
import sys
import time
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]


def checker():
    spec = importlib.util.spec_from_file_location(
        "glamdb_system_check", ROOT / "scripts/check-system.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_system_harness_requires_explicit_disposable_configuration():
    module = checker()
    with pytest.raises(module.CheckError) as error:
        module.load_config(None)
    assert error.value.code == "CONFIG_REQUIRED"


def test_system_harness_rejects_mock_by_default():
    module = checker()
    with pytest.raises(module.CheckError) as error:
        module.require_backend({"mock": True}, "native")
    assert error.value.code == "EXPLICIT_MOCK_REQUIRED"
    assert module.require_backend({"mock": True}, "mock") == "explicit_mock"
    assert module.require_backend({"mock": False}, "native") == "native"


def test_system_harness_rejects_unsafe_or_implicit_configuration(tmp_path):
    module = checker()
    path = tmp_path / "configuration.json"
    for value in (
        {"version": 1, "disposable": False},
        {
            "version": 1,
            "disposable": True,
            "base_url": "http://remote.example",
            "token_file": "token",
        },
        {
            "version": 1,
            "disposable": True,
            "base_url": "https://example.test",
            "token_file": "token",
            "verify": False,
        },
    ):
        path.write_text(json.dumps(value))
        path.chmod(0o600)
        with pytest.raises(module.CheckError):
            module.load_config(path)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "disposable": True,
                "base_url": "https://example.test",
                "token_file": "token",
            }
        )
    )
    path.chmod(0o644)
    with pytest.raises(module.CheckError) as error:
        module.load_config(path)
    assert error.value.code == "PRIVATE_FILE_REQUIRED"
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(module.CheckError) as error:
        module.load_config(link)
    assert error.value.code == "PRIVATE_FILE_REQUIRED"


def private_json(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        assert (
            stat.S_ISREG(info.st_mode)
            and info.st_uid == os.geteuid()
            and not info.st_mode & 0o077
        )
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            raw = source.read(131073)
        assert len(raw) <= 131072
        return json.loads(raw)
    finally:
        os.close(descriptor)


def loopback_url(value, *, https=False):
    parsed = urlsplit(value)
    assert parsed.scheme in (("https",) if https else ("http", "https"))
    assert (
        not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    )
    try:
        local = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        local = parsed.hostname == "localhost"
    assert local, "real-identity tests require explicitly disposable loopback endpoints"
    return value.rstrip("/")


class NativeIdentity:
    def __repr__(self):
        return "<explicit disposable native identity fixture; credentials redacted>"

    def __init__(self, path):
        self.path = Path(path).resolve()
        self.directory = self.path.parent
        self.data = private_json(self.path)
        self.settings = private_json(self.directory / "gateway.json")
        assert self.settings["sso"]["test_mode"] is True
        self.base = loopback_url(self.settings["public_origin"], https=True)
        self.issuer = loopback_url(self.settings["sso"]["issuer"], https=True)
        self.backchannel = loopback_url(
            self.settings["sso"].get("backchannel", self.issuer)
        )
        assert self.issuer.endswith("/realms/glamdb") and self.backchannel.endswith(
            "/realms/glamdb"
        )
        self.ca = self.directory / "tls.crt"
        self.tls = ssl.create_default_context(cafile=str(self.ca))
        self.http = httpx.Client(
            verify=self.tls, trust_env=False, timeout=5, follow_redirects=False
        )
        self.organizations = self.data["organizations"]
        self.databases = self.data["databases"]
        assert 3 <= len(self.organizations) <= 8 and 2 <= len(self.databases) <= 8
        for organization in self.organizations:
            for key in ("id", "identity_id", "user_id"):
                assert str(UUID(organization[key])) == organization[key]
        self.tokens = {}
        self._admin = None
        self.output = self.directory / "native-check-results"
        self.output.mkdir(mode=0o700, exist_ok=True)

    def close(self):
        self.tokens.clear()
        self.http.close()

    def token(self, index):
        __tracebackhide__ = True
        cached = self.tokens.get(index)
        if cached and time.monotonic() < cached[1]:
            return cached[0]
        from authlib.integrations.httpx_client import OAuth2Client

        organization = self.organizations[index]
        try:
            with OAuth2Client(
                organization["client_id"],
                organization["client_secret"],
                scope="openid organization:*",
                verify=self.tls,
                trust_env=False,
                timeout=5,
                follow_redirects=False,
            ) as client:
                value = client.fetch_token(
                    self.issuer + "/protocol/openid-connect/token",
                    grant_type="client_credentials",
                )
            token = value["access_token"]
            assert isinstance(token, str) and 0 < len(token) < 16384
            self.tokens[index] = (
                token,
                time.monotonic() + max(1, int(value.get("expires_in", 30)) - 5),
            )
            return token
        except Exception:
            raise AssertionError(
                "Native fixture token acquisition failed; credentials suppressed"
            ) from None

    def request(self, index, method, path, **kwargs):
        __tracebackhide__ = True
        headers = {
            "Authorization": "Bearer " + self.token(index),
            **kwargs.pop("headers", {}),
        }
        try:
            return self.http.request(
                method, self.base + path, headers=headers, **kwargs
            )
        except httpx.HTTPError:
            raise AssertionError(
                "Native public endpoint unavailable; request diagnostics suppressed"
            ) from None

    @property
    def admin(self):
        __tracebackhide__ = True
        if self._admin is None:
            from keycloak import KeycloakAdmin
            from apps.workbench_access.settings import secret_file

            settings = self.settings["sso"]
            self._admin = KeycloakAdmin(
                server_url=self.backchannel.split("/realms/", 1)[0] + "/",
                realm_name="glamdb",
                client_id=settings["directory_client_id"],
                client_secret_key=secret_file(settings["directory_secret_file"]),
                grant_type="client_credentials",
                verify=True,
                timeout=3,
                max_retries=0,
            )
        return self._admin

    def owned(self, index):
        return [
            database
            for database in self.databases
            if database["owner"] == self.organizations[index]["id"]
        ]

    def sdk(self, index, database):
        from glamdb_sdk import Client

        return Client(
            self.base + "/databases/" + database["id"],
            token_provider=lambda: self.token(index),
            verify=self.tls,
            timeout=10,
        )

    def operator(self, command=None, *, view="status", identity=None):
        __tracebackhide__ = True
        from authlib.integrations.httpx_client import OAuth2Client
        from apps.workbench_access.settings import secret_file

        sso = self.settings["sso"]
        try:
            with OAuth2Client(
                sso["directory_client_id"],
                secret_file(sso["directory_secret_file"]),
                verify=self.tls,
                trust_env=False,
                timeout=5,
                follow_redirects=False,
            ) as oauth:
                token = oauth.fetch_token(
                    self.issuer + "/protocol/openid-connect/token",
                    grant_type="client_credentials",
                )["access_token"]
            with httpx.Client(
                transport=httpx.HTTPTransport(
                    uds=str(self.directory / "operator.sock")
                ),
                base_url="http://localhost",
                headers={"Authorization": "Bearer " + token},
                timeout=15,
                follow_redirects=False,
            ) as client:
                if command is not None:
                    return client.post("/operator", json=command)
                return client.get(
                    "/operator",
                    params={"view": view, **({"id": identity} if identity else {})},
                )
        except Exception:
            raise AssertionError(
                "Native operator endpoint unavailable; credentials suppressed"
            ) from None

    def backend(self, database, method, path, **kwargs):
        __tracebackhide__ = True
        from apps.workbench_access.settings import secret_file

        try:
            with httpx.Client(
                trust_env=False, timeout=5, follow_redirects=False
            ) as client:
                return client.request(
                    method,
                    f"http://localhost:{database['port']}" + path,
                    headers={
                        "Authorization": "Bearer "
                        + secret_file(database["token_file"], minimum=1),
                        **kwargs.pop("headers", {}),
                    },
                    **kwargs,
                )
        except Exception:
            raise AssertionError(
                "Explicit disposable backend unavailable; credentials suppressed"
            ) from None

    def command(self, action, **params):
        return {"operation_id": str(uuid4()), "action": action, "params": params}

    def browser(self, mode="ui"):
        __tracebackhide__ = True
        public_key = subprocess.run(
            ["openssl", "x509", "-in", str(self.ca), "-pubkey", "-noout"],
            capture_output=True,
            check=True,
        ).stdout
        der = subprocess.run(
            ["openssl", "pkey", "-pubin", "-outform", "DER"],
            input=public_key,
            capture_output=True,
            check=True,
        ).stdout
        pin = base64.b64encode(hashlib.sha256(der).digest()).decode("ascii")
        script = self.output / "browser.mjs"
        script.write_text(BROWSER_SCRIPT)
        payload = {
            "root": str(ROOT),
            "fixture": str(self.path),
            "base": self.base,
            "issuer": self.issuer,
            "backchannel": self.backchannel,
            "directory_client_id": self.settings["sso"]["directory_client_id"],
            "directory_secret_file": self.settings["sso"]["directory_secret_file"],
            "pin": pin,
            "output": str(self.output),
            "mode": mode,
            "executable": os.environ.get("GLAMDB_CHROMIUM_EXECUTABLE"),
        }
        theme = Path(self.settings["theme"]).resolve()
        original_theme = None
        if theme.parent == self.directory and theme.name.startswith("native-check"):
            payload["runtime_theme_file"] = str(theme)
            original_theme = theme.read_bytes()
        try:
            result = subprocess.run(
                ["node", str(script)],
                input=json.dumps(payload),
                text=True,
                capture_output=True,
                timeout=180,
                cwd=ROOT,
            )
        finally:
            if original_theme is not None:
                theme.write_bytes(original_theme)
        try:
            value = json.loads(result.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise AssertionError(
                "Native browser check failed; diagnostics suppressed"
            ) from None
        assert result.returncode == 0 and value.get("status") == "pass", (
            f"Native browser failure: {value.get('stage','unknown')} / "
            f"{value.get('error_class','unknown')} / "
            f"{value.get('diagnostics', [])}"
        )
        return value


@pytest.fixture(scope="module")
def native_identity():
    configured = os.environ.get("GLAMDB_IDENTITY_FIXTURE")
    if not configured:
        pytest.skip(
            "requires explicit GLAMDB_IDENTITY_FIXTURE disposable infrastructure"
        )
    fixture = NativeIdentity(configured)
    try:
        yield fixture
    finally:
        fixture.close()


def test_native_organizations_isolate_every_database_and_empty_account(native_identity):
    fixture = native_identity
    counts = []
    for index, organization in enumerate(fixture.organizations):
        response = fixture.request(index, "GET", "/workbench/session")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")
        view = response.json()
        assert view["organization"]["id"] == organization["id"]
        actual = {database["id"] for database in view["databases"]}
        expected = {database["id"] for database in fixture.owned(index)}
        assert actual == expected
        counts.append(len(actual))
        for database in fixture.databases:
            result = fixture.request(
                index, "GET", f"/databases/{database['id']}/capabilities"
            )
            assert result.status_code == (200 if database["id"] in expected else 404)
            if result.status_code == 200:
                assert (
                    result.json()["mock"] is True
                )  # Only storage backends are explicit test doubles.
    assert 0 in counts and max(counts) >= 2


def test_native_api_never_redirects_credentials_or_accepts_forged_identity(
    native_identity,
):
    fixture = native_identity
    own = fixture.owned(0)[0]["id"]
    foreign = fixture.owned(1)[0]["id"]
    for route in ("/workbench/session", f"/databases/{own}/capabilities"):
        response = fixture.http.get(fixture.base + route)
        assert response.status_code in (401, 403)
        assert response.headers["content-type"].startswith("application/json")
        assert not response.is_redirect
    for route in ("/operator", "/metrics", "/capabilities"):
        assert fixture.request(0, "GET", route).status_code == 404
    forged = {
        "X-Forwarded-User": fixture.organizations[1]["user_id"],
        "X-Forwarded-Groups": "glamdb-operator",
        "X-GlamDB-Organization": fixture.organizations[1]["id"],
    }
    assert (
        fixture.request(
            0, "GET", f"/databases/{foreign}/capabilities", headers=forged
        ).status_code
        == 404
    )
    for path in (
        "%63apabilities",
        "collections%2f..%2fcapabilities",
        "%252e%252e/capabilities",
    ):
        assert fixture.request(0, "GET", f"/databases/{own}/{path}").status_code == 404
    response = fixture.request(
        0,
        "GET",
        "/workbench/session",
        headers={"Cookie": "__Host-glamdb-session=forged"},
    )
    assert response.status_code in (401, 403)


def test_native_jwt_bad_signature_and_invalid_bearer_fail_closed(native_identity):
    fixture = native_identity
    from joserfc import jwk, jwt

    keys = fixture.http.get(fixture.issuer + "/protocol/openid-connect/certs").json()
    decoded = jwt.decode(
        fixture.token(0), jwk.KeySet.import_key_set(keys), algorithms=["RS256"]
    )
    assert "email" not in decoded.claims and "email_verified" not in decoded.claims
    wrong = jwk.RSAKey.generate_key(2048)
    forged = jwt.encode(
        {"alg": "RS256", "kid": decoded.header["kid"]}, decoded.claims, wrong
    )
    for value in ("malformed.invalid.token", forged):
        response = fixture.http.get(
            fixture.base + "/workbench/session",
            headers={"Authorization": "Bearer " + value},
        )
        assert response.status_code in (401, 403)
        assert response.headers["content-type"].startswith("application/json")
        assert not response.is_redirect


def test_native_https_sdk_prefix_all_routes_and_arrow_roundtrip(native_identity):
    fixture = native_identity
    with fixture.sdk(0, fixture.owned(0)[0]) as client:
        result = checker().roundtrip(client, "mock")
    assert result["status"] == "pass" and result["backend"] == "explicit_mock"
    assert len(result["checks"]) == 16
    assert "get_collection" in result["checks"]


def test_native_organization_disable_and_enable_preserve_native_account_status(
    native_identity,
):
    fixture = native_identity
    organization = fixture.organizations[2]
    user = fixture.admin.get_user(organization["user_id"])
    disable = fixture.command("organization.disable", id=organization["id"])
    try:
        fixture.admin.update_user(organization["user_id"], {**user, "enabled": False})
        result = fixture.operator(disable)
        assert result.status_code == 200 and result.json()["state"] == "complete"
        assert (
            fixture.admin.get_organization(organization["identity_id"])["enabled"]
            is False
        )
        status = fixture.operator().json()
        assert (
            next(
                item
                for item in status["organizations"]
                if item["id"] == organization["id"]
            )["enabled"]
            == 0
        )
        assert fixture.request(2, "GET", "/workbench/session").status_code in (401, 403)
        replay = fixture.operator(disable)
        assert replay.status_code == 200 and replay.json() == result.json()
        enabled = fixture.operator(
            fixture.command("organization.enable", id=organization["id"])
        )
        assert enabled.status_code == 200 and enabled.json()["state"] == "complete"
        assert (
            fixture.admin.get_organization(organization["identity_id"])["enabled"]
            is True
        )
        assert fixture.admin.get_user(organization["user_id"])["enabled"] is False
    finally:
        fixture.admin.update_user(organization["user_id"], user)
        fixture.operator(
            fixture.command("organization.enable", id=organization["id"])
        ).raise_for_status()
        fixture.tokens.pop(2, None)
    assert fixture.request(2, "GET", "/workbench/session").status_code == 200


def test_native_strict_transfer_waits_for_authoritative_mock_outcome_and_replays_receipt(
    native_identity,
):
    """Native IdP/PDP/operator workflow; only backend outcomes are explicit mocks."""
    fixture = native_identity
    database = fixture.owned(0)[-1]
    first, second = fixture.organizations[:2]
    collection_id = str(uuid4())
    operation_id = str(uuid4())
    transfer = fixture.command(
        "database.transfer",
        id=database["id"],
        **{"from": first["id"], "to": second["id"]},
    )
    from glamdb_sdk import Field, Schema

    schema = Schema([Field("key", "string", False), Field("n", "int64", True)])
    path = f"/collections/{collection_id}/append"
    resolution = (
        f"/development/collections/{collection_id}/operations/{operation_id}:resolve"
    )
    admitted = False
    transfer_started = False
    try:
        with fixture.sdk(0, database) as client:
            assert (
                client.create_collection(
                    "transfer_" + uuid4().hex, schema, collection_id=collection_id
                ).outcome
                == "committed"
            )
            prepared = client.prepare_append(
                collection_id, [{"key": "owned", "n": 7}], operation_id=operation_id
            )
            unknown = fixture.backend(
                database,
                "POST",
                path,
                content=prepared.body,
                headers={
                    "Content-Type": "application/json",
                    "X-GlamDB-Mock-Scenario": "commit_unknown",
                },
            )
            assert (
                unknown.status_code == 503
                and unknown.json()["outcome"] == "commit_unknown"
            )
            admitted = True
            assert client.append(prepared).outcome == "commit_unknown"
        unresolved = fixture.operator(view="admissions", identity=database["id"])
        assert unresolved.status_code == 200 and any(
            item["operation_id"] == operation_id for item in unresolved.json()
        )
        transfer_started = True
        blocked = fixture.operator(transfer)
        assert (
            blocked.status_code == 409
            and blocked.json()["error"]["code"] == "BACKEND_UNCERTAIN"
        )
        receipt = fixture.operator(
            view="receipt", identity=transfer["operation_id"]
        ).json()
        assert receipt["state"] == "pending" and receipt["applied"] is False
        for index in (0, 1):
            assert (
                fixture.request(
                    index, "GET", f"/databases/{database['id']}/capabilities"
                ).status_code
                == 404
            )
        assert (
            fixture.request(
                0, "GET", f"/databases/{fixture.owned(0)[0]['id']}/capabilities"
            ).status_code
            == 200
        )
        assert (
            fixture.backend(
                database, "POST", resolution, json={"outcome": "committed"}
            ).status_code
            == 200
        )
        reconciled = fixture.operator(
            fixture.command("database.reconcile", id=database["id"])
        )
        assert reconciled.status_code == 200 and reconciled.json()["unresolved"] == 0
        completed = fixture.operator(transfer)
        assert completed.status_code == 200 and completed.json()["state"] == "complete"
        assert fixture.operator(transfer).json() == completed.json()
        assert (
            fixture.operator(view="receipt", identity=transfer["operation_id"]).json()
            == completed.json()
        )
        assert (
            fixture.request(
                0, "GET", f"/databases/{database['id']}/capabilities"
            ).status_code
            == 404
        )
        assert (
            fixture.request(
                1, "GET", f"/databases/{database['id']}/capabilities"
            ).status_code
            == 200
        )
        with fixture.sdk(1, database) as client:
            assert client.scan_json(collection_id).rows == [{"key": "owned", "n": 7}]
        conflict = {**transfer, "params": {**transfer["params"], "to": first["id"]}}
        assert fixture.operator(conflict).status_code == 409
    finally:
        if admitted:
            fixture.backend(
                database, "POST", resolution, json={"outcome": "committed"}
            ).raise_for_status()
            fixture.operator(
                fixture.command("database.reconcile", id=database["id"])
            ).raise_for_status()
        if transfer_started:
            # Complete the original receipt before reversing it; never edit the live catalog.
            fixture.operator(transfer).raise_for_status()
            fixture.operator(
                fixture.command(
                    "database.transfer",
                    id=database["id"],
                    **{"from": second["id"], "to": first["id"]},
                )
            ).raise_for_status()
    assert (
        fixture.request(
            0, "GET", f"/databases/{database['id']}/capabilities"
        ).status_code
        == 200
    )
    assert (
        fixture.request(
            1, "GET", f"/databases/{database['id']}/capabilities"
        ).status_code
        == 404
    )


def test_native_browser_all_pages_cookie_isolation_theme_and_logout(native_identity):
    result = native_identity.browser()
    assert result["pages"] == 8 and result["organizations"] == 3
    assert all(
        result[key]
        for key in (
            "secure_cookie",
            "empty_storage",
            "logo_bytes",
            "theme_preview",
            "database_switch",
            "csrf_rejected",
            "logout_revoked",
        )
    )
    if Path(native_identity.settings["theme"]).name.startswith("native-check"):
        assert result["runtime_theme"] is True


def test_native_password_reset_and_session_revocation(native_identity):
    organization = native_identity.organizations[1]
    try:
        result = native_identity.browser("account")
    finally:
        native_identity.admin.set_user_password(
            organization["user_id"], organization["password"], temporary=False
        )
        native_identity.admin.user_logout(organization["user_id"])
    assert all(
        result[key]
        for key in (
            "native_sessions",
            "native_account",
            "old_password_rejected",
            "new_password_accepted",
            "session_revoked",
            "password_restored",
        )
    )


@pytest.mark.parametrize("dependency", ("cerbos", "keycloak"))
def test_native_dependency_outage_fails_closed_and_restores_service(
    native_identity, dependency
):
    """Additional opt-in: only explicit disposable Podman connection/container names."""
    connection = os.environ.get("GLAMDB_IDENTITY_OUTAGE_CONNECTION")
    container = os.environ.get("GLAMDB_IDENTITY_OUTAGE_" + dependency.upper())
    if not connection or not container:
        pytest.skip(
            "requires explicit disposable outage connection and dependency container"
        )
    assert re.fullmatch(r"[a-zA-Z0-9_.-]{1,96}", connection)
    assert re.fullmatch(r"glamdb-[a-zA-Z0-9_.-]{1,96}", container)
    fixture = native_identity
    database = fixture.owned(0)[0]
    collection_id = str(uuid4())
    assert fixture.request(0, "GET", "/workbench/session").status_code == 200
    prefix = ["podman", "--connection", connection]

    def command(action):
        __tracebackhide__ = True
        result = subprocess.run(
            prefix + [action, container], capture_output=True, timeout=30
        )
        assert (
            result.returncode == 0
        ), "Explicit disposable dependency command failed; diagnostics suppressed"

    running = subprocess.run(
        prefix + ["inspect", "--format", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert running.returncode == 0 and running.stdout.strip() == "true"
    try:
        command("stop")
        for path in (
            "/workbench/session",
            f"/databases/{database['id']}/capabilities",
        ):
            response = fixture.request(0, "GET", path)
            assert response.status_code == 503 and not response.is_redirect
            assert response.headers["content-type"].startswith("application/json")
        response = fixture.request(
            0,
            "POST",
            f"/databases/{database['id']}/collections",
            json={
                "collection_id": collection_id,
                "name": "denied_" + uuid4().hex,
                "schema": {
                    "fields": [
                        {
                            "field_id": 1,
                            "name": "key",
                            "type": "string",
                            "nullable": False,
                        }
                    ]
                },
            },
        )
        assert response.status_code == 503 and not response.is_redirect
    finally:
        command("start")
        end = time.monotonic() + 90
        while time.monotonic() < end:
            try:
                if fixture.request(0, "GET", "/workbench/session").status_code == 200:
                    break
            except AssertionError:
                pass
            time.sleep(0.25)
        else:
            raise AssertionError(
                "Explicit disposable dependency did not recover; diagnostics suppressed"
            )
    assert (
        fixture.backend(database, "GET", f"/collections/{collection_id}").status_code
        == 404
    )


BROWSER_SCRIPT = r"""
import fs from 'node:fs';
import http from 'node:http';
import https from 'node:https';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const fixture=JSON.parse(fs.readFileSync(input.fixture,'utf8'));
const { chromium,expect }=await import(pathToFileURL(input.root+'/ui/node_modules/@playwright/test/index.mjs').href);
let stage='launch',browser;const diagnostics=[];
const safeCodes=new Set(['UNAVAILABLE','UNAUTHENTICATED','PERMISSION_DENIED','NOT_FOUND','INVALID_ARGUMENT','CONFLICT','RESOURCE_EXHAUSTED']);
function ensure(value){if(!value){const e=new Error();e.name='CheckError';throw e;}}
function request(url,method='GET',body=null,headers={}){
 return new Promise((resolve,reject)=>{
  const transport=new URL(url).protocol==='https:'?https:http;
  const req=transport.request(url,{method,headers,timeout:10000},res=>{
   const chunks=[];let length=0;
   res.on('data',part=>{length+=part.length;if(length>1048576){req.destroy();reject(new Error());}else chunks.push(part);});
   res.on('end',()=>{let value=null;try{value=JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{}resolve({status:res.statusCode,value});});
  });
  req.on('error',reject);req.on('timeout',()=>req.destroy());if(body)req.write(body);req.end();
 });
}
let controlToken=null;
async function admin(path,method='GET',value=null){
 if(!controlToken){
  const clientSecret=fs.readFileSync(input.directory_secret_file,'utf8').trim();
  const body=new URLSearchParams({grant_type:'client_credentials'}).toString();
  const response=await request(input.backchannel+'/protocol/openid-connect/token','POST',body,
   {'Content-Type':'application/x-www-form-urlencoded','Authorization':'Basic '+Buffer.from(input.directory_client_id+':'+clientSecret).toString('base64')});
  ensure(response.status===200&&response.value?.access_token);controlToken=response.value.access_token;
 }
 const base=input.backchannel.split('/realms/')[0]+'/admin/realms/glamdb';
 const response=await request(base+path,method,value===null?null:JSON.stringify(value),
  {'Authorization':'Bearer '+controlToken,'Content-Type':'application/json'});
 ensure(response.status>=200&&response.status<300);return response.value;
}
async function context(){return browser.newContext({viewport:{width:1440,height:1000},ignoreHTTPSErrors:false});}
async function credentials(page,organization,password=organization.password){
 await page.locator('input[name=username]').waitFor({state:'visible',timeout:30000});
 await page.locator('input[name=username]').fill(organization.login);
 if(!(await page.locator('input[name=password]').isVisible())){
  await page.locator('#kc-login').click();await page.locator('input[name=password]').waitFor({state:'visible'});
 }
 await page.locator('input[name=password]').fill(password);await page.locator('#kc-login').click();
}
async function login(page,organization,password=organization.password){
 await page.goto(input.base+'/');await credentials(page,organization,password);
 await page.waitForURL(url=>url.origin===new URL(input.base).origin&&url.pathname==='/',{timeout:30000});
 await page.getByRole('button',{name:'Sign out',exact:true}).waitFor({timeout:20000});
}
async function storageEmpty(page){return page.evaluate(()=>localStorage.length===0&&sessionStorage.length===0);}
async function session(page){return page.evaluate(async()=>{const r=await fetch('/workbench/session',{redirect:'error',cache:'no-store'});return {status:r.status,value:await r.json()};});}
async function signout(page,organization,ctx){
 await page.getByRole('button',{name:'Sign out',exact:true}).click();
 await page.locator('input[name=username]').waitFor({state:'visible',timeout:30000});
 ensure(!(await ctx.cookies(input.base)).some(c=>/^__Host-glamdb-session(?:_[0-9]+)?$/.test(c.name)));
 ensure((await admin('/users/'+organization.user_id+'/sessions')).length===0);
}
try{
 browser=await chromium.launch({headless:true,executablePath:input.executable||undefined,
  args:['--ignore-certificate-errors-spki-list='+input.pin]});
 if(input.mode==='account'){
  const organization=fixture.organizations[1],replacement='Native-'+crypto.randomBytes(24).toString('hex');
  const ctx=await context(),page=await ctx.newPage();let reset=false;
  const result={status:'pass'};
  try{
   stage='account_login';await login(page,organization);ensure(await storageEmpty(page));
   result.native_sessions=(await admin('/users/'+organization.user_id+'/sessions')).length>0;ensure(result.native_sessions);
   stage='native_account';const href=await page.getByRole('link',{name:'Account',exact:true}).getAttribute('href');
   ensure(href===input.issuer+'/account/');const account=await ctx.newPage();await account.goto(href);
   await expect(account.getByRole('heading').first()).toBeVisible();
   ensure(new URL(account.url()).origin===new URL(input.issuer).origin&&new URL(account.url()).pathname.startsWith('/realms/glamdb/account'));
   result.native_account=true;await account.close();
   stage='password_reset';await admin('/users/'+organization.user_id+'/reset-password','PUT',{type:'password',value:replacement,temporary:false});reset=true;
   await admin('/users/'+organization.user_id+'/logout','POST');
   stage='session_revoked';await expect.poll(async()=>{try{return (await session(page)).status;}catch{return 0;}},{timeout:15000}).toBe(401);
   await expect(page.getByRole('button',{name:'Sign in',exact:true})).toBeVisible({timeout:15000});
   ensure(await page.locator('#database-selector').count()===0);result.session_revoked=true;
   stage='old_password';const oldctx=await context(),oldpage=await oldctx.newPage();await oldpage.goto(input.base+'/');await credentials(oldpage,organization);
   await oldpage.locator('#input-error,#input-error-password,.kc-feedback-text,[role="alert"]').first().waitFor({state:'visible',timeout:15000});
   ensure(new URL(oldpage.url()).origin===new URL(input.issuer).origin);result.old_password_rejected=true;await oldctx.close();
   stage='new_password';const newctx=await context(),newpage=await newctx.newPage();await login(newpage,organization,replacement);
   ensure((await session(newpage)).value.organization.id===organization.id);result.new_password_accepted=true;await newctx.close();
  }finally{
   if(reset)await admin('/users/'+organization.user_id+'/reset-password','PUT',{type:'password',value:organization.password,temporary:false});
   await admin('/users/'+organization.user_id+'/logout','POST');await ctx.close();
  }
  result.password_restored=true;console.log(JSON.stringify(result));
 }else{
  const result={status:'pass',pages:0,organizations:0,secure_cookie:true,empty_storage:true,logo_bytes:true,
   theme_preview:false,runtime_theme:false,database_switch:false,csrf_rejected:false,logout_revoked:true};
  for(let index=0;index<fixture.organizations.length;index++){
   stage='login_'+index;const organization=fixture.organizations[index],owned=fixture.databases.filter(d=>d.owner===organization.id);
   const ctx=await context(),page=await ctx.newPage();const pageErrors=[];let bearerLeak=false;
   page.on('pageerror',error=>pageErrors.push(error.name));
   page.on('response',async response=>{
    const url=new URL(response.url());
    if(url.origin!==new URL(input.base).origin||!/^\/(workbench|databases)\//.test(url.pathname)||response.status()<400)return;
    const sample={stage,status:response.status(),code:null};
    diagnostics.push(sample);if(diagnostics.length>8)diagnostics.shift();
    try{const body=await response.json();if(safeCodes.has(body?.error?.code))sample.code=body.error.code;}catch{}
   });
   page.on('request',req=>{if(new URL(req.url()).origin===new URL(input.base).origin&&req.headers().authorization)bearerLeak=true;});
   await login(page,organization);const view=await session(page);ensure(view.status===200&&view.value.organization.id===organization.id);
   ensure(JSON.stringify(view.value.databases.map(d=>d.id).sort())===JSON.stringify(owned.map(d=>d.id).sort()));
   const sessions=(await ctx.cookies(input.base)).filter(c=>/^__Host-glamdb-session(?:_[0-9]+)?$/.test(c.name));
   ensure(sessions.length>0&&sessions.every(c=>c.secure&&c.httpOnly&&c.sameSite==='Lax'&&c.path==='/'));result.empty_storage&&=await storageEmpty(page);
   if(!owned.length){ensure(await page.locator('#database-selector').count()===0);await expect(page.getByText('No databases are available for this account.',{exact:true})).toBeVisible();}
   else{
    await page.locator('#database-selector').waitFor({state:'visible'});
    ensure(await page.locator('#database-selector option').count()===owned.length);
   }
   if(index===0){
    stage='all_pages';for(const tab of ['overview','data','schema','snapshots','storage','explain','operations','style']){
     await page.locator('nav [data-tab="'+tab+'"]').click();await expect(page.locator('nav [data-tab="'+tab+'"]')).toHaveAttribute('aria-current','page');
     await expect(page.locator('#view h1')).toBeVisible();result.pages++;
    }
    stage='theme_preview';const initial=await page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim());
    ensure(/^#[0-9a-f]{6}$/i.test(initial));await page.getByLabel('Color accent',{exact:true}).fill('#94155c');
    await expect.poll(()=>page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim())).toBe('#94155c');
    await page.getByRole('button',{name:'Reset preview',exact:true}).click();
    await expect.poll(()=>page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim())).toBe(initial);result.theme_preview=true;
    if(input.runtime_theme_file){
     stage='runtime_theme';const original=fs.readFileSync(input.runtime_theme_file);const changed=JSON.parse(original);
     try{
      changed.colors.accent='#bd1570';fs.writeFileSync(input.runtime_theme_file,JSON.stringify(changed));
      await expect.poll(()=>page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim()),{timeout:12000}).toBe('#bd1570');
     }finally{fs.writeFileSync(input.runtime_theme_file,original);}
     await expect.poll(()=>page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--color-accent').trim()),{timeout:12000}).toBe(initial);result.runtime_theme=true;
    }
    stage='brand_bytes';const theme=await page.evaluate(async()=>{const r=await fetch('/workbench/config',{redirect:'error'});if(!r.ok)throw new Error();return r.json();});
    for(const key of ['wordmark','mark']){
     const asset=theme.brand[key];ensure(/^assets\/brand\/[a-z]+\.png$/.test(asset));
     const digest=await page.evaluate(async asset=>{const r=await fetch('/'+asset);if(!r.ok)throw new Error();return [...new Uint8Array(await crypto.subtle.digest('SHA-256',await r.arrayBuffer()))].map(b=>b.toString(16).padStart(2,'0')).join('');},asset);
     ensure(digest===crypto.createHash('sha256').update(fs.readFileSync(input.root+'/ui/'+asset)).digest('hex'));
    }
    stage='database_switch';const original=await page.locator('#database-selector').inputValue();const alternate=owned.find(d=>d.id!==original);
    ensure(alternate);await page.locator('#database-selector').selectOption(alternate.id);await expect(page.locator('#database-selector')).toHaveValue(alternate.id);
    await expect(page.locator('.collection-list button').first()).toBeVisible();
    await page.locator('#database-selector').selectOption(original);await expect(page.locator('.collection-list button').first()).toBeVisible();result.database_switch=true;
    stage='browser_csrf';const csrf=await page.evaluate(async database=>{const r=await fetch('/databases/'+database+'/collections',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}',redirect:'error'});return {status:r.status,json:r.headers.get('content-type')?.startsWith('application/json')};},original);
    ensure(csrf.status===403&&csrf.json);result.csrf_rejected=true;
    stage='browser_write';const name='browser_'+crypto.randomUUID().replaceAll('-','');
    await page.getByRole('button',{name:'New collection',exact:true}).click();await page.getByLabel('Collection name').fill(name);await page.getByRole('button',{name:'Create collection',exact:true}).click();await expect(page.getByRole('dialog')).not.toBeVisible();
    await page.getByRole('button',{name:'Append rows',exact:true}).click();const dialog=page.getByRole('dialog');
    await dialog.getByLabel('Rows · JSON array').fill(JSON.stringify([{key:'same',label:'Native SSO browser',amount:'9007199254740993'},{key:'same',label:'Duplicate retained',amount:null}]));
    await dialog.getByRole('button',{name:'Append rows',exact:true}).click();await expect(dialog.getByText('Append committed',{exact:true})).toBeVisible();
    await dialog.getByRole('button',{name:'Close dialog',exact:true}).click();await page.getByRole('button',{name:'Run query',exact:true}).click();
    await expect(page.getByRole('table',{name:'Query results'}).locator('tbody tr')).toHaveCount(2);await expect(page.getByRole('cell',{name:'9007199254740993',exact:true})).toBeVisible();
    stage='browser_arrow';const download=page.waitForEvent('download');await page.getByRole('button',{name:'Arrow',exact:true}).click();const downloaded=await download;ensure(downloaded.suggestedFilename().endsWith('.arrows'));await downloaded.delete();
    stage='authenticated_screenshot';await page.screenshot({path:input.output+'/authenticated-workbench.png',fullPage:true});
   }
   ensure(!bearerLeak&&pageErrors.length===0);result.empty_storage&&=await storageEmpty(page);ensure(result.empty_storage);
   stage='logout_'+index;await signout(page,organization,ctx);result.organizations++;await ctx.close();
  }
  console.log(JSON.stringify(result));
 }
}catch(error){console.log(JSON.stringify({status:'fail',stage,error_class:error.name,diagnostics}));process.exitCode=1;}
finally{controlToken=null;if(browser)await browser.close();}
"""
