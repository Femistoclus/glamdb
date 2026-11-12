from dataclasses import replace
import json
from uuid import uuid4

import anyio
import httpx
import pytest
from starlette.testclient import TestClient

from apps.workbench_access.gateway import create_app
from apps.workbench_access.identity import AuthError, Principal
from apps.workbench_access.resources import initialize, ResourceCatalog
from apps.workbench_access.settings import Settings, SsoSettings
from .test_resources import organization, database, operation


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / "resources.sqlite"
    initialize(path)
    with ResourceCatalog(path) as catalog:
        first, second = organization(catalog), organization(catalog)
        own = database(catalog, first)
        subject = str(uuid4())
        identity = catalog.organization(first)["identity_id"]
    secret = tmp_path / "secret"
    secret.write_text("long-test-service-secret-thirty-two-characters")
    secret.chmod(0o600)
    site = tmp_path / "site"
    site.mkdir()
    (site / "index.html").write_text('<html data-access-gateway="true"></html>')
    theme = tmp_path / "theme.json"
    theme.write_text("{}")
    settings = Settings(
        SsoSettings(
            issuer="https://identity.example/realms/glamdb",
            client_id="gateway",
            client_secret_file=secret,
            test_mode=True,
        ),
        path,
        secret,
        site,
        theme,
        "https://testserver",
        cerbos_plaintext=True,
    )

    class Verifier:
        async def verify(self, token):
            if token != "valid":
                raise AuthError()
            return Principal(subject, identity, ("glamdb-member",), 9999999999)

        async def close(self):
            pass

    class Policy:
        unavailable = False

        async def allowed(self, p, action, db=None, org=None, **kwargs):
            if self.unavailable:
                raise AuthError(503, "UNAVAILABLE")
            return bool(
                db
                and p.organization_id == db["organization_id"]
                and org["enabled"]
                and db["enabled"]
                and (db["state"] == "active" or kwargs.get("probe_active"))
            )

        async def close(self):
            pass

    class Directory:
        def logout(self, subject):
            pass

    policy = Policy()
    seen = []

    async def backend(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    app = create_app(
        settings,
        verifier=Verifier(),
        authorizer=policy,
        directory=Directory(),
        backend_transport=httpx.MockTransport(backend),
        environment={"DATABASE_TOKEN": "backend-only-secret"},
    )
    return app, path, first, second, own, policy, seen


def test_same_policy_for_list_and_direct_route_fail_closed(setup):
    app, path, first, second, own, policy, seen = setup
    with TestClient(app, base_url="https://testserver") as client:
        headers = {"Authorization": "Bearer valid"}
        session = client.get("/workbench/session", headers=headers)
        assert session.status_code == 200 and session.json()["version"] == 3
        assert session.json()["databases"] == [{"id": own, "name": "Database"}]
        assert (
            client.get(
                f"/databases/{uuid4()}/capabilities", headers=headers
            ).status_code
            == 404
        )
        assert (
            client.get(f"/databases/{own}/capabilities", headers=headers).status_code
            == 200
        )
        assert seen[0].headers["authorization"] == "Bearer backend-only-secret"
        assert (
            "cookie" not in seen[0].headers
            and "x-forwarded-access-token" not in seen[0].headers
        )
        with ResourceCatalog(path) as catalog:
            catalog.apply(operation("organization.update", id=first, enabled=False))
        assert (
            client.get(f"/databases/{own}/capabilities", headers=headers).status_code
            == 404
        )
        policy.unavailable = True
        assert client.get("/workbench/session", headers=headers).status_code == 503
        assert "valid" not in client.get("/workbench/session", headers=headers).text


def test_cookie_csrf_and_mixed_credentials_and_encoded_path(setup):
    app, path, first, second, own, policy, seen = setup
    with TestClient(app, base_url="https://testserver") as client:
        headers = {"X-Forwarded-Access-Token": "valid"}
        client.cookies.set("__Host-glamdb-session_0", "opaque")
        response = client.get("/workbench/session", headers=headers)
        assert response.status_code == 200
        csrf = client.cookies.get("__Host-glamdb-csrf")
        assert csrf
        route = f"/databases/{own}/collections/{uuid4()}/scan"
        assert client.post(route, headers=headers, json={}).status_code == 403
        assert (
            client.post(
                route,
                headers={
                    **headers,
                    "X-CSRFToken": csrf,
                    "Origin": "https://testserver",
                },
                json={},
            ).status_code
            == 200
        )
        assert (
            client.post(
                route,
                headers={
                    **headers,
                    "X-CSRFToken": csrf,
                    "Origin": "https://evil.example",
                },
                json={},
            ).status_code
            == 403
        )
        assert (
            client.get(
                "/workbench/session", headers={"Authorization": "Bearer valid"}
            ).status_code
            == 401
        )
        assert (
            client.get(
                f"/databases/{own}/%63apabilities", headers=headers
            ).status_code
            == 404
        )


@pytest.mark.parametrize("matching", [True, False])
def test_plain_create_receipt_is_terminal_only_for_matching_definition(setup, matching):
    app, path, first, second, own, policy, seen = setup
    payload = {
        "collection_id": str(uuid4()),
        "name": "New collection",
        "schema": {
            "fields": [
                {"field_id": 1, "name": "id", "type": "int64", "nullable": False}
            ]
        },
    }

    async def backend(request):
        schema = {**payload["schema"], "schema_id": 0}
        return httpx.Response(
            201,
            json={
                **payload,
                "schema": schema,
                "name": payload["name"] if matching else "Different",
            },
        )

    app.app.state.gateway.backend_transport = httpx.MockTransport(backend)
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            f"/databases/{own}/collections",
            headers={"Authorization": "Bearer valid"},
            json=payload,
        )
        assert response.status_code == 201
    with ResourceCatalog(path) as catalog:
        assert len(catalog.unresolved(own)) == (0 if matching else 1)


def test_arrow_snapshot_and_integrity_headers_survive_proxy(setup):
    app, path, first, second, own, policy, seen = setup

    async def backend(request):
        return httpx.Response(
            200,
            content=b"arrow",
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-GlamDB-Snapshot-Id": "17",
                "X-Content-SHA256": "a" * 64,
                "X-GlamDB-Next-Cursor": "opaque-next-page",
                "Content-Encoding": "identity",
            },
        )

    app.app.state.gateway.backend_transport = httpx.MockTransport(backend)
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            f"/databases/{own}/collections/{uuid4()}/scan",
            headers={"Authorization": "Bearer valid"},
            json={},
        )
        assert response.headers["X-GlamDB-Snapshot-Id"] == "17"
        assert response.headers["X-Content-SHA256"] == "a" * 64
        assert response.headers["Content-Length"] == "5"
        assert response.headers["Content-Encoding"] == "identity"
        assert response.headers["X-GlamDB-Next-Cursor"] == "opaque-next-page"


@pytest.mark.anyio
async def test_drain_waits_for_stream_and_unknown_write_survives_restart(setup):
    app, path, first, second, own, policy, seen = setup
    from apps.workbench_access.admission import AdmissionGate
    from apps.workbench_access.resources import CatalogError

    with ResourceCatalog(path) as catalog:
        gate = AdmissionGate(catalog)
        db, org = catalog.assert_admission(own, first)
        entered = anyio.Event()
        release = anyio.Event()
        drained = anyio.Event()

        async def reader():
            async with gate.enter(own, first, (db["version"], org["version"])):
                entered.set()
                await release.wait()

        async def transfer():
            await gate.drain(own, first, str(uuid4()), 5, "operator")
            drained.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(reader)
            await entered.wait()
            tasks.start_soon(transfer)
            await anyio.sleep(0.02)
            assert not drained.is_set()
            release.set()
        assert drained.is_set()
        catalog.apply(operation("database.resume", id=own))
        db, org = catalog.assert_admission(own, first)
        write = {
            "id": str(uuid4()),
            "kind": "append",
            "operation_id": str(uuid4()),
            "collection_id": str(uuid4()),
        }
        async with gate.enter(own, first, (db["version"], org["version"]), write):
            pass
        with pytest.raises(CatalogError):
            await gate.drain(own, first, str(uuid4()), 1, "operator")
    with ResourceCatalog(path) as reopened:
        assert reopened.unresolved(own)[0]["outcome"] == "unknown"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "failure", ["start", "body", "read", "close", "write", "headers"]
)
async def test_proxy_failures_close_backend_and_release_admission(setup, failure):
    from starlette.requests import Request, ClientDisconnect
    from apps.workbench_access.admission import AdmissionGate

    app, path, first, second, own, policy, seen = setup
    gateway = app.app.state.gateway
    collection_id, operation_id = str(uuid4()), str(uuid4())

    class BackendStream(httpx.AsyncByteStream):
        close_calls = 0

        async def __aiter__(self):
            yield json.dumps(
                {"operation_id": operation_id, "outcome": "committed"}
            ).encode()
            if failure in ("read", "write"):
                raise httpx.ReadError("injected incomplete backend response")

        async def aclose(self):
            self.close_calls += 1
            if failure == "close":
                raise RuntimeError("injected backend close failure")

    upstream = BackendStream()

    async def backend(request):
        content_type = (
            b"application/json; label=\xe2\x98\x83"
            if failure == "headers"
            else b"application/json"
        )
        return httpx.Response(
            200, stream=upstream, headers={"content-type": content_type}
        )

    gateway.backend_transport = httpx.MockTransport(backend)
    path_suffix = (
        f"collections/{collection_id}/append"
        if failure == "write"
        else "collections"
    )
    route = f"/databases/{own}/{path_suffix}"
    scope = {
        "type": "http",
        "method": "POST" if failure == "write" else "GET",
        "scheme": "https",
        "server": ("testserver", 443),
        "path": route,
        "raw_path": route.encode(),
        "query_string": b"",
        "headers": [(b"authorization", b"Bearer valid")],
        "path_params": {"database_id": own, "path": path_suffix},
        "asgi": {"version": "3.0", "spec_version": "2.4"},
    }
    request_sent = False

    async def receive():
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {
                "type": "http.request",
                "body": json.dumps({"operation_id": operation_id}).encode(),
                "more_body": False,
            }
        return {"type": "http.disconnect"}

    async def send(message):
        if failure == "start" or (
            failure == "body" and message["type"] == "http.response.body"
        ):
            raise OSError("injected client disconnect")

    with ResourceCatalog(path) as catalog:
        gateway.catalog = catalog
        gateway.gate = AdmissionGate(catalog)
        try:
            if failure in ("write", "headers"):
                expected = httpx.ReadError if failure == "write" else UnicodeEncodeError
                with pytest.raises(expected):
                    await gateway.proxy(Request(scope, receive))
            else:
                response = await gateway.proxy(Request(scope, receive))
                expected = (
                    ClientDisconnect
                    if failure in ("start", "body")
                    else httpx.ReadError if failure == "read" else RuntimeError
                )
                with pytest.raises(expected):
                    await response(scope, receive, send)
                # ASGI cleanup may close the body again after a failed send.
                await response.body_iterator.aclose()
            assert gateway.gate.total == 0
            assert gateway.gate.active.get(own, 0) == 0
            assert upstream.close_calls == 1
            unresolved = catalog.unresolved(own)
            if failure == "write":
                assert len(unresolved) == 1
                assert unresolved[0]["operation_id"] == operation_id
                assert unresolved[0]["outcome"] == "unknown"
            else:
                assert unresolved == []
        finally:
            for client in gateway.clients.values():
                await client.aclose()


@pytest.mark.anyio
async def test_catalog_finish_failure_releases_memory_but_retains_durable_unknown(
    setup, monkeypatch
):
    import sqlite3
    from apps.workbench_access.admission import AdmissionGate

    app, path, first, second, own, policy, seen = setup
    with ResourceCatalog(path) as catalog:
        gate = AdmissionGate(catalog)
        db, org = catalog.assert_admission(own, first)
        write = {
            "id": str(uuid4()),
            "kind": "append",
            "operation_id": str(uuid4()),
            "collection_id": str(uuid4()),
        }
        finish = catalog.finish_write

        def failed_finish(*args):
            raise sqlite3.OperationalError("injected catalog I/O failure")

        monkeypatch.setattr(catalog, "finish_write", failed_finish)
        with pytest.raises(sqlite3.OperationalError):
            async with gate.enter(own, first, (db["version"], org["version"]), write):
                pass
        assert gate.total == 0 and gate.active.get(own, 0) == 0
        assert len(catalog.unresolved(own)) == 1
        monkeypatch.setattr(catalog, "finish_write", finish)
        catalog.finish_write(write["id"], "definitely_not_committed")
        await gate.drain(own, first, str(uuid4()), 0.2, "operator")


def test_operator_retry_cannot_reenable_after_superseding_disable(setup):
    app, path, first, second, own, policy, seen = setup
    gateway = app.app.state.gateway
    subject = str(uuid4())

    class OperatorVerifier:
        async def verify(self, token):
            if token != "valid":
                raise AuthError()
            return Principal(subject, None, ("glamdb-operator",), 9999999999)

        async def close(self):
            pass

    class Directory:
        enabled = False
        fail_enable = True
        enable_calls = 0

        def update(self, organization, *, enabled=None, name=None):
            self.enabled = enabled
            if enabled:
                self.enable_calls += 1
                if self.fail_enable:
                    self.fail_enable = False
                    raise RuntimeError("injected lost identity response")

    directory = Directory()

    async def allowed(principal, action, *args, **kwargs):
        return action == "manage" and "glamdb-operator" in principal.roles

    gateway.verifier = OperatorVerifier()
    gateway.directory = directory
    policy.allowed = allowed
    with ResourceCatalog(path) as catalog:
        catalog.apply(operation("organization.update", id=first, enabled=False))
        pending = operation("organization.enable", id=first)
        pending["expected_revision"] = catalog.revision
    headers = {"Authorization": "Bearer valid"}
    with TestClient(
        app, base_url="https://testserver", raise_server_exceptions=False
    ) as client:
        assert (
            client.post("/operator", headers=headers, json=pending).status_code == 500
        )
        disable = operation("organization.disable", id=first)
        assert (
            client.post("/operator", headers=headers, json=disable).status_code == 200
        )
        response = client.post("/operator", headers=headers, json=pending)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "CONFLICT"
        assert not directory.enabled and directory.enable_calls == 1
        receipt = client.get(
            "/operator",
            headers=headers,
            params={"view": "receipt", "id": pending["operation_id"]},
        )
        assert receipt.status_code == 200
        assert receipt.json()["state"] == "superseded"
        assert receipt.json()["superseded_by"] == disable["operation_id"]
    with ResourceCatalog(path) as catalog:
        assert not catalog.organization(first)["enabled"]


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["lost_response", "member_budget"])
async def test_native_effect_without_local_stage_keeps_original_workflow_pending(
    setup, failure
):
    from apps.workbench_access.admission import AdmissionGate
    from apps.workbench_access.lifecycle import Lifecycle
    from apps.workbench_access.resources import CatalogError

    app, path, first, second, own, policy, seen = setup

    class Directory:
        enabled = False
        fail = True

        def update(self, organization, *, enabled=None, name=None):
            self.enabled = enabled
            if self.fail:
                if failure == "member_budget":
                    raise CatalogError(
                        "RESOURCE_EXHAUSTED",
                        "injected member limit after native update",
                    )
                raise RuntimeError("injected lost native update response")

    directory = Directory()
    actor = Principal(str(uuid4()), None, ("glamdb-operator",), 9999999999)
    with ResourceCatalog(path) as catalog:
        catalog.apply(operation("organization.update", id=first, enabled=False))
        service = Lifecycle(
            catalog,
            AdmissionGate(catalog),
            directory,
            policy,
            app.app.state.gateway.settings,
        )
        request = operation("organization.enable", id=first)
        expected = CatalogError if failure == "member_budget" else RuntimeError
        with pytest.raises(expected):
            await service.run(request, actor)
        assert directory.enabled and not catalog.organization(first)["enabled"]
        assert catalog.receipt(request["operation_id"])["state"] == "pending"
        # A malformed retry must not rewrite the original unknown receipt.
        conflicting = {
            **request,
            "action": "organization.rename",
            "params": {"id": first, "name": "Different"},
        }
        with pytest.raises(CatalogError) as error:
            await service.run(conflicting, actor)
        assert error.value.code == "CONFLICT"
        assert catalog.receipt(request["operation_id"])["state"] == "pending"
        directory.fail = False
        result = await service.run(request, actor)
        assert result["state"] == "complete"
        assert catalog.organization(first)["enabled"]


@pytest.mark.anyio
async def test_fresh_assignment_stays_closed_until_policy_verification_succeeds(setup):
    from apps.workbench_access.admission import AdmissionGate
    from apps.workbench_access.lifecycle import Lifecycle
    from apps.workbench_access.resources import CatalogError

    app, path, first, second, own, policy, seen = setup

    class Policy:
        unavailable = True

        async def allowed(self, principal, action, db, org, **kwargs):
            if self.unavailable:
                raise AuthError(503, "UNAVAILABLE")
            return principal.organization_id == db["organization_id"] and bool(
                org["enabled"]
            )

    policy = Policy()
    actor = Principal(str(uuid4()), None, ("glamdb-operator",), 9999999999)
    with ResourceCatalog(path) as catalog:
        target = str(uuid4())
        catalog.apply(
            operation(
                "database.create",
                id=target,
                name="Unassigned",
                url="https://unassigned.example",
                token_env="DATABASE_TOKEN",
            )
        )
        service = Lifecycle(
            catalog,
            AdmissionGate(catalog),
            None,
            policy,
            app.app.state.gateway.settings,
        )
        request = operation("database.assign", id=target, organization_id=second)
        with pytest.raises(AuthError):
            await service.run(request, actor)
        assert catalog.database(target)["organization_id"] == second
        assert catalog.database(target)["state"] == "draining"
        with pytest.raises(CatalogError):
            catalog.assert_admission(target, second)
        assert catalog.receipt(request["operation_id"])["state"] == "pending"
        policy.unavailable = False
        assert (await service.run(request, actor))["state"] == "complete"
        assert catalog.assert_admission(target, second)[0]["state"] == "active"


def test_write_settings_preserves_body_and_checks_current_database_policy(setup):
    app, path, first, second, own, policy, seen = setup
    collection = str(uuid4())
    payload = {
        "expected_version": "0",
        "write_settings": {"parquet": {"compression": {"codec": "zstd", "level": 3}}},
    }
    actions = []
    allowed = policy.allowed

    async def record_action(principal, action, *args, **kwargs):
        actions.append(action)
        return await allowed(principal, action, *args, **kwargs)

    policy.allowed = record_action
    with TestClient(app, base_url="https://testserver") as client:
        route = f"/databases/{own}/collections/{collection}/write-settings"
        assert client.post(route, headers={"Authorization": "Bearer valid"}, json=payload).status_code == 200
        assert actions == ["write"]
        assert json.loads(seen[0].content) == payload
        assert seen[0].url.path == f"/collections/{collection}/write-settings"
        assert seen[0].headers["authorization"] == "Bearer backend-only-secret"
        with ResourceCatalog(path) as catalog:
            catalog.apply(operation("database.update", id=own, enabled=False))
        assert client.post(route, headers={"Authorization": "Bearer valid"}, json=payload).status_code == 404
        assert len(seen) == 1
