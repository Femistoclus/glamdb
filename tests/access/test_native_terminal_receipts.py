"""Native ToJson receipt shapes through gateway admission, using explicit doubles.

The native serializer emits create failures without a collection UUID, and
append failure lookups retain their error HTTP status. These tests exercise
the gateway/catalog, not a native storage server or its durability.
"""

from copy import deepcopy
import json
from uuid import uuid4

import httpx
import pytest
from starlette.testclient import TestClient

from apps.workbench_access.identity import Principal
from apps.workbench_access.resources import ResourceCatalog
from .test_gateway import setup

CREATE_FAILURE = {
    "outcome": "definitely_not_committed",
    "phase": "binding",
    "error": {
        "code": "ALREADY_EXISTS",
        "message": "collection name already exists",
        "retry_hint": "FixInput",
    },
}


@pytest.mark.parametrize("kind", ["create", "append"])
def test_noncanonical_original_write_identity_never_reaches_backend(setup, kind):
    app, path, _, _, own, _, seen = setup
    if kind == "create":
        route = f"/databases/{own}/collections"
        payload = {"collection_id": "not-a-uuid", "name": "Invalid original"}
    else:
        route = f"/databases/{own}/collections/{uuid4()}/append"
        payload = {"operation_id": "not-a-uuid", "rows": [{"key": "original"}]}
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            route, headers={"Authorization": "Bearer valid"}, json=payload
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "INVALID_ARGUMENT"
    assert not seen
    with ResourceCatalog(path) as catalog:
        assert not catalog.unresolved(own)


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "plain_error",
        "wrong_status",
        "unknown_code",
        "bad_hint",
        "bad_phase",
        "unknown",
        "wrong_identity",
        "operation_conflict",
        "idempotency_conflict",
        "committed_conflict",
        "committed_idempotency_conflict",
    ],
)
def test_native_failed_create_uses_exact_terminal_envelope(setup, mutation):
    app, path, _, _, own, _, seen = setup
    payload = {
        "collection_id": str(uuid4()),
        "name": "New native collection",
        "schema": {"fields": [{"name": "key", "type": "string", "nullable": False}]},
    }
    envelope = deepcopy(CREATE_FAILURE)
    status = 409
    if mutation == "plain_error":
        envelope = {"error": envelope["error"]}
    elif mutation == "wrong_status":
        status = 200
    elif mutation == "unknown_code":
        envelope["error"]["code"] = "UNRECOGNIZED_FAILURE"
        status = 500
    elif mutation == "bad_hint":
        envelope["error"]["retry_hint"] = "retry-whenever"
    elif mutation == "bad_phase":
        envelope["phase"] = "complete"
    elif mutation == "unknown":
        envelope.update(outcome="commit_unknown", recovery_required=True)
        status = 503
    elif mutation == "wrong_identity":
        envelope["collection_id"] = str(uuid4())
    elif mutation in (
        "operation_conflict",
        "idempotency_conflict",
        "committed_conflict",
        "committed_idempotency_conflict",
    ):
        envelope["collection_id"] = payload["collection_id"]
        envelope["error"]["code"] = (
            "IDEMPOTENCY_CONFLICT"
            if "idempotency" in mutation
            else "OPERATION_CONFLICT"
        )
        if mutation.startswith("committed"):
            envelope["outcome"] = "committed"
            status = 200

    async def backend(request):
        seen.append(request)
        assert json.loads(request.content) == payload
        return httpx.Response(status, json=envelope)

    app.app.state.gateway.backend_transport = httpx.MockTransport(backend)
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            f"/databases/{own}/collections",
            headers={"Authorization": "Bearer valid"},
            json=payload,
        )
        assert response.status_code == status and response.json() == envelope
    with ResourceCatalog(path) as catalog:
        pending = catalog.unresolved(own)
        assert len(pending) == (0 if mutation is None else 1)
        if pending:
            assert pending[0]["collection_id"] == payload["collection_id"]
            assert pending[0]["outcome"] == "unknown"
    assert len(seen) == 1


@pytest.mark.parametrize(
    "code,disposition",
    [
        ("OPERATION_CONFLICT", "definitely_not_committed"),
        ("IDEMPOTENCY_CONFLICT", "definitely_not_committed"),
        ("OPERATION_CONFLICT", "committed"),
        ("IDEMPOTENCY_CONFLICT", "committed"),
    ],
)
def test_conflicting_append_response_never_establishes_the_original_outcome(
    setup, code, disposition
):
    app, path, _, _, own, _, seen = setup
    collection, operation = str(uuid4()), str(uuid4())
    payload = {"operation_id": operation, "rows": [{"key": "conflicting-content"}]}
    envelope = {
        "outcome": disposition,
        "operation_id": operation,
        "error": {
            "code": code,
            "message": "different content",
            "retry_hint": "FixInput",
        },
    }
    status = 200 if disposition == "committed" else 409

    async def backend(request):
        seen.append(request)
        assert json.loads(request.content) == payload
        return httpx.Response(status, json=envelope)

    app.app.state.gateway.backend_transport = httpx.MockTransport(backend)
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            f"/databases/{own}/collections/{collection}/append",
            headers={"Authorization": "Bearer valid"},
            json=payload,
        )
        assert response.status_code == status and response.json() == envelope
    with ResourceCatalog(path) as catalog:
        pending = catalog.unresolved(own)
        assert len(pending) == 1
        assert pending[0]["operation_id"] == operation
        assert pending[0]["outcome"] == "unknown"
    assert len(seen) == 1


@pytest.mark.parametrize(
    "status,code", [(412, "PRECONDITION_FAILED"), (409, "SCHEMA_MISMATCH")]
)
@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "missing_identity",
        "wrong_identity",
        "plain_error",
        "wrong_status",
        "unknown_code",
        "bad_hint",
        "bad_phase",
        "unknown",
        "operation_conflict",
        "idempotency_conflict",
        "committed_conflict",
        "committed_idempotency_conflict",
    ],
)
def test_native_failed_append_lookup_resolves_only_matching_authoritative_receipt(
    setup, status, code, mutation
):
    app, path, _, _, own, policy, seen = setup
    gateway = app.app.state.gateway
    original_verifier = gateway.verifier
    original_allowed = policy.allowed

    class Verifier:
        async def verify(self, token):
            if token == "operator":
                return Principal(str(uuid4()), None, ("glamdb-operator",), 9999999999)
            return await original_verifier.verify(token)

        async def close(self):
            await original_verifier.close()

    async def allowed(principal, action, *args, **kwargs):
        if action == "manage":
            return "glamdb-operator" in principal.roles
        return await original_allowed(principal, action, *args, **kwargs)

    gateway.verifier = Verifier()
    policy.allowed = allowed
    collection, operation = str(uuid4()), str(uuid4())
    payload = {"operation_id": operation, "rows": [{"key": "original-content"}]}
    envelope = {
        "outcome": "definitely_not_committed",
        "phase": "binding",
        "operation_id": operation,
        "error": {
            "code": code,
            "message": "native request did not publish",
            "retry_hint": "NewOperationId",
        },
    }
    if mutation == "missing_identity":
        del envelope["operation_id"]
    elif mutation == "wrong_identity":
        envelope["operation_id"] = str(uuid4())
    elif mutation == "plain_error":
        envelope = {"error": envelope["error"]}
    elif mutation == "wrong_status":
        status = 503
    elif mutation == "unknown_code":
        envelope["error"]["code"] = "UNRECOGNIZED_FAILURE"
        status = 500
    elif mutation == "bad_hint":
        envelope["error"]["retry_hint"] = None
    elif mutation == "bad_phase":
        envelope["phase"] = "complete"
    elif mutation == "unknown":
        envelope.update(outcome="commit_unknown", recovery_required=True)
        status = 503
    elif mutation in (
        "operation_conflict",
        "idempotency_conflict",
        "committed_conflict",
        "committed_idempotency_conflict",
    ):
        envelope["error"]["code"] = (
            "IDEMPOTENCY_CONFLICT"
            if "idempotency" in mutation
            else "OPERATION_CONFLICT"
        )
        status = 409
        if mutation.startswith("committed"):
            envelope["outcome"] = "committed"
            status = 200

    async def backend(request):
        seen.append(request)
        if request.method == "POST":
            assert json.loads(request.content) == payload
            # A transport failure after admission has no authoritative outcome.
            raise httpx.ReadError(
                "explicit lost native-protocol response", request=request
            )
        assert (
            request.url.path == f"/collections/{collection}/operations/{operation}"
        )
        return httpx.Response(status, json=envelope)

    gateway.backend_transport = httpx.MockTransport(backend)
    with TestClient(
        app, base_url="https://testserver", raise_server_exceptions=False
    ) as client:
        lost = client.post(
            f"/databases/{own}/collections/{collection}/append",
            headers={"Authorization": "Bearer valid"},
            json=payload,
        )
        assert lost.status_code >= 500
        before = gateway.catalog.unresolved(own)
        assert len(before) == 1 and before[0]["outcome"] == "unknown"
        assert before[0]["operation_id"] == operation
        command = {
            "operation_id": str(uuid4()),
            "action": "database.reconcile",
            "params": {"id": own},
        }
        reconciled = client.post(
            "/operator", headers={"Authorization": "Bearer operator"}, json=command
        )
        assert reconciled.status_code == 200
        result = reconciled.json()
        assert result["resolved"] == (1 if mutation is None else 0)
        assert result["unresolved"] == (0 if mutation is None else 1)
    with ResourceCatalog(path) as catalog:
        assert len(catalog.unresolved(own)) == (0 if mutation is None else 1)
    assert [request.method for request in seen] == ["POST", "GET"]
