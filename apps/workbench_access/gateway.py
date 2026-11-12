"""Private OIDC/Cerbos adapter; all durable user-data writes belong to the backend."""

from contextlib import asynccontextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import re
import ssl
import sqlite3
import time
from urllib.parse import urlsplit
from uuid import uuid4

import anyio
import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse, FileResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from starlette_csrf import CSRFMiddleware
from keycloak.exceptions import KeycloakError
from requests.exceptions import RequestException

from .admission import AdmissionGate
from .authorization import Authorizer, classify
from .identity import AuthError, TokenVerifier, credentials, finish_stage
from .lifecycle import KeycloakDirectory, Lifecycle
from .resources import ResourceCatalog, CatalogError, catalog_lease, identifier
from .settings import secret_file


def error(status, code, message):
    return JSONResponse(
        {"error": {"code": code, "message": message}},
        status_code=status,
        headers={"Cache-Control": "private, no-store"},
    )


def receipt_schema(value):
    """Apply only v0 wire defaults when comparing create identities, not storage validation."""
    if not isinstance(value, dict) or value.keys() - {"schema_id", "fields"}:
        raise ValueError()
    schema_id = value.get("schema_id", 0)
    fields = value.get("fields")
    if (
        type(schema_id) is not int
        or not isinstance(fields, list)
        or not 1 <= len(fields) <= 64
    ):
        raise ValueError()
    normalized = []
    for index, item in enumerate(fields, 1):
        if not isinstance(item, dict) or item.keys() - {
            "field_id",
            "name",
            "type",
            "nullable",
        }:
            raise ValueError()
        field = {
            "field_id": item.get("field_id", index),
            "name": item["name"],
            "type": item["type"],
            "nullable": item.get("nullable", False),
        }
        if (
            type(field["field_id"]) is not int
            or type(field["nullable"]) is not bool
            or not isinstance(field["name"], str)
            or not isinstance(field["type"], str)
        ):
            raise ValueError()
        normalized.append(field)
    return {"schema_id": schema_id, "fields": normalized}


def operation_conflict(value):
    detail = value.get("error") if isinstance(value, dict) else None
    return isinstance(detail, dict) and detail.get("code") in (
        "OPERATION_CONFLICT",
        "IDEMPOTENCY_CONFLICT",
    )


def native_failure_receipt(value, status, *, operation_id=None):
    """Recognize the native ToJson terminal-failure envelope, never a bare HTTP error."""
    if not isinstance(value, dict) or operation_conflict(value):
        return False
    fields = {"outcome", "phase", "error"}
    if operation_id is not None:
        fields.add("operation_id")
        if value.get("operation_id") != operation_id:
            return False
    if value.keys() != fields or value.get("outcome") != "definitely_not_committed":
        return False
    if value.get("phase") not in (
        "validation",
        "binding",
        "data",
        "metadata",
        "catalog_transaction",
        "catalog_commit",
        "recording_failure",
        "reconciling",
    ):
        return False
    detail = value.get("error")
    if (
        not isinstance(detail, dict)
        or detail.keys() != {"code", "message", "retry_hint"}
        or not isinstance(detail.get("code"), str)
        or not isinstance(detail.get("message"), str)
        or detail.get("retry_hint")
        not in ("SameIdAfterReconcile", "NewOperationId", "FixInput", "OperatorAction")
    ):
        return False
    # Match the native serializer's ErrorStatus mapping. New/unknown error codes
    # stay unresolved until their contract is reviewed, including default-500 codes.
    statuses = {
        "INVALID_ARGUMENT": 400,
        "ALREADY_EXISTS": 409,
        "SCHEMA_MISMATCH": 409,
        "STALE_BASE": 409,
        "PRECONDITION_FAILED": 412,
        "RESOURCE_EXHAUSTED": 429,
        "CAPACITY_EXCEEDED": 429,
        "COLLECTION_NOT_FOUND": 404,
        "NOT_FOUND": 404,
        "RECOVERY_REQUIRED": 503,
        "DEPENDENCY_UNAVAILABLE": 503,
        "IO_ERROR": 500,
        "CORRUPTION": 500,
        "INTERNAL": 500,
        "INVALID_STATE": 500,
        "STATE_IN_USE": 500,
        "UNSUPPORTED_STORE_VERSION": 500,
        "DEADLINE_EXCEEDED": 500,
        "INTERRUPTED": 500,
    }
    return statuses.get(detail["code"]) == status


class JsonCSRF(CSRFMiddleware):
    def _get_error_response(self, request):
        return error(403, "PERMISSION_DENIED", "CSRF verification failed")


class CookieCSRF:
    def __init__(self, app, settings, gateway=None):
        self.gateway = gateway
        self.app = app
        self.origin = settings.public_origin
        self.protected = JsonCSRF(
            app,
            secret=secret_file(settings.csrf_secret_file, minimum=32),
            cookie_name="__Host-glamdb-csrf",
            cookie_secure=True,
            cookie_samesite="lax",
            header_name="x-csrftoken",
        )

    async def __call__(self, scope, receive, send):
        if self.gateway:
            return await self.gateway.instrument(self._dispatch, scope, receive, send)
        return await self._dispatch(scope, receive, send)

    async def _dispatch(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request = Request(scope)
        cookie = any(
            re.fullmatch(
                r"__Host-glamdb-session(?:_[0-9]+)?", part.strip().split("=", 1)[0]
            )
            for part in request.headers.get("cookie", "").split(";")
        )
        if (
            cookie
            and request.method not in ("GET", "HEAD", "OPTIONS")
            and request.headers.get("origin") != self.origin
        ):
            return await error(
                403, "PERMISSION_DENIED", "Request origin verification failed"
            )(scope, receive, send)
        return await (self.protected if cookie else self.app)(scope, receive, send)


class FencedStream(StreamingResponse):
    def __init__(self, *args, release, **kwargs):
        super().__init__(*args, **kwargs)
        self.release = release

    async def __call__(self, scope, receive, send):
        try:
            return await super().__call__(scope, receive, send)
        finally:
            await self.release()


class Gateway:
    def __init__(
        self,
        settings,
        *,
        verifier=None,
        authorizer=None,
        directory=None,
        backend_transport=None,
        environment=None,
        observer=None,
    ):
        self.settings = settings
        self.verifier = verifier
        self.authorizer = authorizer
        self.directory = directory
        self.backend_transport = backend_transport
        self.environment = os.environ if environment is None else environment
        self.catalog = None
        self.lease = None
        self.clients = {}
        self.events = None
        self.metrics = None
        self.observer = observer
        self._health_generation = 0

    def metric(self, name, *args):
        if self.metrics is not None:
            try:
                getattr(self.metrics, name)(*args)
            except Exception:
                pass

    def observe_stage(self, stage, seconds):
        self.metric("observe_stage", stage, seconds)
        if self.observer is not None:
            try:
                self.observer(stage, seconds)
            except Exception:
                pass

    def catalog_call(self, callback, *args, **kwargs):
        started = time.perf_counter()
        try:
            return callback(*args, **kwargs)
        finally:
            finish_stage(self.observe_stage, "catalog", started)

    def dependency_unavailable(self):
        self._health_generation += 1
        self.metric("set_ready", False)

    @staticmethod
    def mark_dependency(request, name):
        state = (
            request.scope.get("_glamdb_metric_state") if request is not None else None
        )
        if state is not None:
            state["dependencies"].add(name)

    async def allowed(
        self, request, principal, action, database=None, organization=None
    ):
        result = await self.authorizer.allowed(
            principal, action, database, organization
        )
        self.mark_dependency(request, "pdp")
        return result

    @staticmethod
    def metric_route(scope):
        path = scope.get("path", "")
        fixed = {
            "/workbench/session": "session",
            "/workbench/logout": "logout",
            "/operator": "admin",
        }
        if path in fixed:
            return fixed[path]
        if path.startswith("/databases/"):
            parts = path.split("/", 3)
            if len(parts) == 4:
                try:
                    return classify(scope.get("method", ""), "/" + parts[3]).metric
                except ValueError:
                    pass
        return "other"

    async def instrument(self, app, scope, receive, send):
        if scope["type"] != "http" or scope.get("path", "").rstrip("/") == "/metrics":
            return await app(scope, receive, send)
        route = self.metric_route(scope)
        method = scope.get("method", "OTHER")
        started = time.perf_counter()
        status = 500
        completed = False
        recorded = False
        state = {"dependencies": set(), "generation": self._health_generation}
        scope["_glamdb_metric_state"] = state
        self.metric("in_flight", route, 1)

        def finish(code):
            nonlocal recorded
            if recorded:
                return
            recorded = True
            self.metric("in_flight", route, -1)
            self.metric(
                "observe_http", route, method, code, time.perf_counter() - started
            )
            if code >= 500:
                self.dependency_unavailable()
            elif (
                completed
                and code < 400
                and state["dependencies"] == {"idp", "pdp"}
                and state["generation"] == self._health_generation
            ):
                self.metric("set_ready", True)

        async def measured_send(message):
            nonlocal status, completed
            if message["type"] == "http.response.start":
                value = message["status"]
                status = (
                    value if isinstance(value, int) and 100 <= value <= 599 else 500
                )
            await send(message)
            if message["type"] == "http.response.body" and not message.get(
                "more_body", False
            ):
                completed = True
                finish(status)

        try:
            return await app(scope, receive, measured_send)
        finally:
            finish(status if completed else 499)

    @asynccontextmanager
    async def lifespan(self, app):
        with catalog_lease(self.settings.catalog):
            self.catalog = ResourceCatalog(
                self.settings.catalog, test_mode=self.settings.sso.test_mode
            )
            self.gate = AdmissionGate(self.catalog, self.settings.max_inflight)
            self.verifier = self.verifier or TokenVerifier(
                self.settings.sso, observer=self.observe_stage
            )
            self.authorizer = self.authorizer or Authorizer(
                self.settings, observer=self.observe_stage
            )
            self.directory = self.directory or KeycloakDirectory(self.settings)
            self.lifecycle = Lifecycle(
                self.catalog, self.gate, self.directory, self.authorizer, self.settings
            )
            from .operational import OperationalEvents

            self.events = OperationalEvents()
            try:
                try:
                    from tools.monitoring.application_metrics import ApplicationMetrics
                    from tools.monitoring.logging_metrics import LoggingMetrics
                except ModuleNotFoundError as exc:
                    if exc.name not in (
                        "tools.monitoring",
                        "tools.monitoring.application_metrics",
                        "tools.monitoring.logging_metrics",
                    ):
                        raise
                else:
                    self.metrics = ApplicationMetrics()
                    if self.events.logger:
                        self.metrics.registry.register(
                            LoggingMetrics(self.events.logger.snapshot)
                        )
                    self.metric("set_ready", False)
                self.events.emit("runtime_started")
                yield
            finally:
                self.metric("set_ready", False)
                for client in self.clients.values():
                    await client.aclose()
                await self.verifier.close()
                await self.authorizer.close()
                self.events.close()
                self.catalog.close()

    async def principal(self, request):
        token, cookie = credentials(request.headers)
        principal = await self.verifier.verify(token)
        self.mark_dependency(request, "idp")
        if principal.identity_organization:
            try:
                org = self.catalog_call(
                    self.catalog.organization_by_identity,
                    principal.identity_organization,
                )
            except CatalogError:
                raise AuthError() from None
            principal = replace(principal, organization_id=org["id"])
        return principal, cookie

    async def authorize_database(self, principal, database_id, action, request=None):
        try:
            db = self.catalog_call(self.catalog.database, database_id)
            org = (
                self.catalog_call(self.catalog.organization, db["organization_id"])
                if db["organization_id"]
                else None
            )
        except CatalogError:
            raise CatalogError("NOT_FOUND", "resource not found") from None
        if not await self.allowed(request, principal, action, db, org):
            raise CatalogError("NOT_FOUND", "resource not found")
        if not org:
            raise CatalogError("NOT_FOUND", "resource not found")
        return db, org

    async def session(self, request):
        principal, _ = await self.principal(request)
        databases = []
        # A control-only principal has no data grants; it still uses the same PDP.
        for db in self.catalog_call(self.catalog.state)["databases"]:
            org = (
                self.catalog_call(self.catalog.organization, db["organization_id"])
                if db["organization_id"]
                else None
            )
            if await self.allowed(request, principal, "metadata", db, org):
                databases.append({"id": db["id"], "name": db["name"]})
        org = (
            self.catalog_call(self.catalog.organization, principal.organization_id)
            if principal.organization_id
            else None
        )
        return JSONResponse(
            {
                "version": 3,
                "revision": self.catalog.revision,
                "organization": (
                    {
                        "id": org["id"],
                        "name": org["name"],
                        "login": "Organization account",
                    }
                    if org
                    else None
                ),
                "subject": principal.subject,
                "databases": databases,
                "account_url": self.settings.sso.issuer + "/account/",
                "login_url": "/oauth2/start",
                "logout_url": "/workbench/logout",
            },
            headers={
                "Cache-Control": "private, no-store",
                "X-GlamDB-Access-Revision": str(self.catalog.revision),
            },
        )

    async def config(self, request):
        await self.principal(request)
        # Read runtime configuration on each poll; no binary rebuild or secret injection.
        raw = self.settings.theme.read_bytes()
        if len(raw) > 1024 * 1024:
            raise CatalogError("INVALID_STATE", "theme exceeds resource limit")
        return Response(
            raw,
            media_type="application/json",
            headers={
                "Cache-Control": "private, no-store",
                "X-GlamDB-Access-Revision": str(self.catalog.revision),
            },
        )

    async def logout(self, request):
        principal, cookie = await self.principal(request)
        await anyio.to_thread.run_sync(self.directory.logout, principal.subject)
        return JSONResponse(
            {"signed_out": True, "redirect": "/oauth2/sign_out?rd=%2F"},
            headers={"Cache-Control": "no-store"},
        )

    async def operator(self, request):
        principal, cookie = await self.principal(request)
        if cookie or not await self.allowed(request, principal, "manage"):
            raise AuthError(403, "PERMISSION_DENIED")
        if request.method == "GET":
            view = request.query_params.get("view", "status")
            if view == "status":
                result = self.catalog_call(self.catalog.state)
            elif view == "audit":
                result = self.catalog_call(self.catalog.audit)
            elif view == "receipt":
                result = self.catalog.receipt(request.query_params.get("id"))
            elif view == "admissions":
                result = self.catalog.unresolved(request.query_params.get("id"))
            else:
                raise CatalogError("INVALID_ARGUMENT", "unknown operator view")
        else:
            body = await self.body(request)
            try:
                command = json.loads(body)
            except (ValueError, UnicodeError):
                raise CatalogError(
                    "INVALID_ARGUMENT", "JSON command required"
                ) from None
            if not isinstance(command, dict):
                raise CatalogError("INVALID_ARGUMENT", "JSON object required")
            if command.get("action") == "database.reconcile":
                result = await self.reconcile(command, principal, request)
            else:
                result = await self.lifecycle.run(command, principal)
                self.events.mutation(result)
        return JSONResponse(result, headers={"Cache-Control": "private, no-store"})

    async def reconcile(self, command, principal, request):
        self.catalog.fields(command, ("operation_id", "action", "params"))
        self.catalog.fields(command["params"], ("id",))
        dbid = identifier(command["params"]["id"])
        identifier(command["operation_id"])
        db = self.catalog_call(self.catalog.database, dbid)
        resolved = 0
        for admission in self.catalog_call(self.catalog.unresolved, dbid):
            # Create has no durable operation-lookup contract yet; absence is never proof.
            if admission["kind"] != "append":
                continue
            target = f"{db['url']}/collections/{admission['collection_id']}/operations/{admission['operation_id']}"
            started = time.perf_counter()
            try:
                response = await self.backend(db).get(
                    target, headers=self.backend_headers(db, request)
                )
            finally:
                finish_stage(self.observe_stage, "proxy", started)
            if len(response.content) > 1024 * 1024:
                continue
            try:
                result = response.json()
            except ValueError:
                continue
            if not isinstance(result, dict):
                continue
            if operation_conflict(result):
                # Conflicting input says nothing about the bound operation.
                continue
            matching = result.get("operation_id") == admission["operation_id"]
            committed = (
                response.status_code == 200 and result.get("outcome") == "committed"
            )
            failed = native_failure_receipt(
                result, response.status_code, operation_id=admission["operation_id"]
            )
            if matching and (committed or failed):
                self.catalog_call(
                    self.catalog.finish_write, admission["id"], result["outcome"]
                )
                resolved += 1
        return {
            "operation_id": command["operation_id"],
            "resolved": resolved,
            "unresolved": len(self.catalog_call(self.catalog.unresolved, dbid)),
            "source": "backend-operation-outcomes",
        }

    async def body(self, request):
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > self.settings.max_body_bytes:
                raise CatalogError("BODY_TOO_LARGE", "request body limit exceeded")
            data.extend(chunk)
        return bytes(data)

    def backend(self, db):
        key = (db["id"], db["version"])
        # One client per active mapping; stale clients are closed during mapping edits/restart.
        if key not in self.clients:
            if len(self.clients) >= 256:
                raise CatalogError(
                    "RESOURCE_EXHAUSTED",
                    "backend connection catalog requires restart after repeated reconfiguration",
                )
            verify = (
                ssl.create_default_context(cafile=db["ca_file"])
                if db["ca_file"]
                else True
            )
            self.clients[key] = httpx.AsyncClient(
                verify=verify,
                timeout=self.settings.backend_timeout_seconds,
                follow_redirects=False,
                limits=httpx.Limits(
                    max_connections=self.settings.max_inflight,
                    max_keepalive_connections=4,
                ),
                transport=self.backend_transport,
            )
        return self.clients[key]

    def backend_headers(self, db, request):
        if db["token_file"]:
            if (
                Path(db["token_file"])
                .resolve()
                .is_relative_to(self.settings.site.resolve())
            ):
                raise CatalogError(
                    "INVALID_STATE",
                    "backend credentials must remain outside the published site tree",
                )
            token = secret_file(db["token_file"], minimum=1)
        else:
            token = self.environment.get(db["token_env"], "")
        if (
            not token
            or len(token) > 4096
            or any(ord(c) < 33 or ord(c) > 126 for c in token)
        ):
            raise CatalogError("INVALID_STATE", "backend credential unavailable")
        headers = {"Authorization": "Bearer " + token, "Accept-Encoding": "identity"}
        for name in ("accept", "content-type"):
            if name in request.headers:
                headers[name] = request.headers[name]
        return headers

    async def proxy(self, request):
        raw = request.scope.get("raw_path", b"").decode("ascii", errors="replace")
        if raw != request.url.path or "%" in raw or "\\" in raw:
            raise CatalogError("NOT_FOUND", "resource not found")
        try:
            dbid = identifier(request.path_params["database_id"])
            path = "/" + request.path_params["path"]
            route = classify(request.method, path)
        except (CatalogError, ValueError):
            raise CatalogError("NOT_FOUND", "resource not found") from None
        principal, _ = await self.principal(request)
        db, org = await self.authorize_database(principal, dbid, route.action, request)
        versions = (db["version"], org["version"])
        content = await self.body(request) if request.method == "POST" else None
        write = None
        if route.kind:
            try:
                body = json.loads(content)
                cid = identifier(
                    body["collection_id"]
                    if route.kind == "create"
                    else path.split("/")[2]
                )
                op = identifier(body["operation_id"]) if route.kind == "append" else cid
            except (TypeError, KeyError, ValueError):
                raise CatalogError(
                    "INVALID_ARGUMENT", "canonical write identity required"
                ) from None
            write = {
                "id": str(uuid4()),
                "kind": route.kind,
                "operation_id": op,
                "collection_id": cid,
            }
        client = self.backend(db)
        headers = self.backend_headers(db, request)
        target = (
            db["url"] + path + ("?" + request.url.query if request.url.query else "")
        )
        admission = self.gate.enter(dbid, principal.organization_id, versions, write)
        await admission.__aenter__()
        upstream = None
        released = False
        proxy_started = None

        async def release():
            nonlocal released
            if released:
                return
            released = True
            with anyio.CancelScope(shield=True):
                try:
                    if upstream is not None:
                        await upstream.aclose()
                finally:
                    if proxy_started is not None:
                        finish_stage(self.observe_stage, "proxy", proxy_started)
                    await admission.__aexit__(None, None, None)

        try:
            proxy_started = time.perf_counter()
            upstream = await client.send(
                client.build_request(
                    request.method, target, content=content, headers=headers
                ),
                stream=True,
            )
            if upstream.is_redirect:
                await upstream.aclose()
                raise CatalogError(
                    "BACKEND_UNAVAILABLE", "backend redirects are disabled"
                )
            output_headers = {
                name: value
                for name, value in upstream.headers.items()
                if name.lower()
                in (
                    "content-type",
                    "content-length",
                    "content-encoding",
                    "x-content-sha256",
                    "x-glamdb-next-cursor",
                    "x-glamdb-request-id",
                    "x-glamdb-snapshot-id",
                )
            }
            if write:
                chunks = bytearray()
                async for chunk in upstream.aiter_bytes():
                    if len(chunks) + len(chunk) > 16 * 1024 * 1024:
                        raise CatalogError(
                            "BACKEND_UNAVAILABLE", "backend outcome limit exceeded"
                        )
                    chunks.extend(chunk)
                await upstream.aclose()
                try:
                    outcome = json.loads(chunks)
                    identity_key = (
                        "operation_id" if route.kind == "append" else "collection_id"
                    )
                    expected = (
                        write["operation_id"]
                        if route.kind == "append"
                        else write["collection_id"]
                    )
                    if operation_conflict(outcome):
                        # A matching ID cannot make conflicting input authoritative.
                        pass
                    elif outcome.get(identity_key) == expected and outcome.get(
                        "outcome"
                    ) in ("committed", "definitely_not_committed"):
                        write["outcome"] = outcome["outcome"]
                    elif route.kind == "create" and native_failure_receipt(
                        outcome, upstream.status_code
                    ):
                        # Native failed creates have no collection_id field. This
                        # synchronous reply belongs to the already admitted request
                        # with its validated collection UUID; no ID/hash is inferred.
                        write["outcome"] = "definitely_not_committed"
                    elif (
                        route.kind == "create"
                        and upstream.status_code in (200, 201)
                        and outcome.get("outcome") is None
                        and "error" not in outcome
                        and outcome.get("collection_id") == write["collection_id"]
                        and outcome.get("name") == body.get("name")
                        and receipt_schema(outcome.get("schema"))
                        == receipt_schema(body.get("schema"))
                    ):
                        # The v0 protocol also defines a plain collection-info success.
                        # A partial/mismatched definition remains unresolved, never inferred.
                        write["outcome"] = "committed"
                except (ValueError, TypeError, AttributeError, KeyError):
                    pass
                await release()
                return Response(
                    bytes(chunks),
                    status_code=upstream.status_code,
                    headers=output_headers,
                )
        except BaseException:
            await release()
            raise

        async def stream():
            try:
                async for chunk in upstream.aiter_bytes():
                    yield chunk
            finally:
                await release()

        try:
            return FencedStream(
                stream(),
                release=release,
                status_code=upstream.status_code,
                headers=output_headers,
            )
        except BaseException:
            await release()
            raise


def create_app(settings, **dependencies):
    if dependencies and not settings.sso.test_mode:
        raise CatalogError(
            "INVALID_STATE", "dependency doubles require explicit test mode"
        )
    gateway = Gateway(settings, **dependencies)

    async def failure(request, exc):
        if isinstance(exc, AuthError):
            if exc.status == 503:
                gateway.dependency_unavailable()
            return error(exc.status, exc.code, str(exc))
        status = {
            "NOT_FOUND": 404,
            "CONFLICT": 409,
            "DRAIN_PENDING": 409,
            "BACKEND_UNCERTAIN": 409,
            "NOT_EMPTY": 409,
            "POLICY_NOT_READY": 503,
            "RESOURCE_EXHAUSTED": 429,
            "BODY_TOO_LARGE": 413,
            "BACKEND_UNAVAILABLE": 502,
            "INVALID_STATE": 503,
        }.get(exc.code, 422)
        if status == 503:
            gateway.dependency_unavailable()
        return error(status, exc.code, str(exc))

    async def unavailable(request, exc):
        gateway.dependency_unavailable()
        return error(502, "UNAVAILABLE", "Database service unavailable")

    async def service_unavailable(request, exc):
        gateway.dependency_unavailable()
        return error(503, "UNAVAILABLE", "Gateway dependency unavailable")

    async def index(request):
        await gateway.principal(request)
        raw = (settings.site / "index.html").read_text()
        raw = re.sub(
            r"<html\b(?![^>]*\bdata-access-gateway=)",
            '<html data-access-gateway="true"',
            raw,
            count=1,
        )
        return Response(
            raw, media_type="text/html", headers={"Cache-Control": "private, no-store"}
        )

    async def metrics(scope, receive, send):
        if not gateway.metrics:
            return await error(
                503, "UNAVAILABLE", "Metrics component is not installed"
            )(scope, receive, send)
        await gateway.metrics.metrics_app()(scope, receive, send)

    routes = [
        Route("/workbench/session", gateway.session),
        Route("/workbench/config", gateway.config),
        Route("/workbench/logout", gateway.logout, methods=["POST"]),
        Route("/operator", gateway.operator, methods=["GET", "POST"]),
        Route(
            "/databases/{database_id}/{path:path}",
            gateway.proxy,
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        ),
        Mount("/metrics", metrics),
        Route("/", index),
        Mount(
            "/assets", StaticFiles(directory=settings.site / "assets", check_dir=False)
        ),
    ]
    app = Starlette(
        routes=routes,
        lifespan=gateway.lifespan,
        exception_handlers={
            AuthError: failure,
            CatalogError: failure,
            httpx.HTTPError: unavailable,
            KeycloakError: service_unavailable,
            RequestException: service_unavailable,
            sqlite3.Error: service_unavailable,
        },
    )
    app.state.gateway = gateway
    return CookieCSRF(app, settings, gateway)
