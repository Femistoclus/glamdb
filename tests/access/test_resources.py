import json
from pathlib import Path
from uuid import uuid4
import pytest
from apps.workbench_access.resources import ResourceCatalog, initialize, CatalogError


def operation(action, **params):
    return {"operation_id": str(uuid4()), "action": action, "params": params}


@pytest.fixture
def catalog(tmp_path):
    path = tmp_path / "resources.sqlite"
    initialize(path)
    with ResourceCatalog(path) as catalog:
        yield catalog


def organization(catalog, **extra):
    identity = str(uuid4())
    catalog.apply(
        operation("organization.create", id=identity, name="Organization", **extra)
    )
    catalog.apply(
        operation("organization.activate", id=identity, identity_id=str(uuid4()))
    )
    return identity


def database(catalog, owner):
    identity = str(uuid4())
    catalog.apply(
        operation(
            "database.create",
            id=identity,
            organization_id=owner,
            name="Database",
            url="https://database.example",
            token_env="DATABASE_TOKEN",
        )
    )
    return identity


def test_schema_contains_no_password_columns_and_pending_org_cannot_admit(catalog):
    org = str(uuid4())
    catalog.apply(operation("organization.create", id=org, name="Pending"))
    db = database(catalog, org)
    with pytest.raises(CatalogError):
        catalog.admit_write(str(uuid4()), db, org, "append", str(uuid4()), str(uuid4()))
    columns = catalog.connection.execute("PRAGMA table_info(organizations)").fetchall()
    assert not {"password", "password_hash", "login"} & {row["name"] for row in columns}


def test_transfer_waits_for_authoritative_write_outcomes(catalog):
    first, second = organization(catalog), organization(catalog)
    db = database(catalog, first)
    request = str(uuid4())
    catalog.admit_write(request, db, first, "append", str(uuid4()), str(uuid4()))
    catalog.apply(operation("database.drain", id=db, expected_owner=first))
    with pytest.raises(CatalogError):
        catalog.apply(
            operation("database.transfer", id=db, **{"from": first, "to": second})
        )
    catalog.finish_write(request, "unknown")
    with pytest.raises(CatalogError):
        catalog.apply(
            operation("database.transfer", id=db, **{"from": first, "to": second})
        )
    catalog.finish_write(request, "committed")
    catalog.apply(
        operation("database.transfer", id=db, **{"from": first, "to": second})
    )
    assert catalog.database(db)["organization_id"] == second
    assert catalog.database(db)["state"] == "draining"
    catalog.apply(operation("database.resume", id=db))
    assert catalog.database(db)["state"] == "active"


def test_retry_is_idempotent_and_other_content_conflicts(catalog):
    request = operation("organization.create", id=str(uuid4()), name="Same")
    first = catalog.apply(request)
    assert catalog.apply({**request, "expected_revision": 0}) == first
    with pytest.raises(CatalogError):
        catalog.apply({**request, "params": {**request["params"], "name": "Other"}})


def test_disabled_org_and_stale_owner_cannot_admit(catalog):
    org = organization(catalog)
    db = database(catalog, org)
    catalog.apply(operation("organization.update", id=org, enabled=False))
    with pytest.raises(CatalogError):
        catalog.admit_write(str(uuid4()), db, org, "append", str(uuid4()), str(uuid4()))
    with pytest.raises(CatalogError):
        catalog.apply(operation("database.drain", id=db, expected_owner=str(uuid4())))


def test_reopen_retains_unknown_and_abandoned_admissions(catalog):
    org = organization(catalog)
    db = database(catalog, org)
    request = str(uuid4())
    catalog.admit_write(request, db, org, "append", str(uuid4()), str(uuid4()))
    with ResourceCatalog(catalog.path) as second:
        assert second.unresolved(db)[0]["id"] == request
        with pytest.raises(CatalogError):
            second.apply(operation("database.delete", id=db, expected_owner=org))


def test_strict_unknown_fields_and_secret_inputs_rejected(catalog):
    for value in [True, 42, {"password": "x"}]:
        with pytest.raises(CatalogError):
            catalog.apply(operation("organization.create", id=str(uuid4()), name=value))
    with pytest.raises(CatalogError):
        catalog.apply(
            operation(
                "organization.create",
                id=str(uuid4()),
                name="X",
                password_hash="forbidden",
            )
        )


def test_backend_origin_canonicalization_and_emergency_disable(catalog):
    owner = organization(catalog)
    first = database(catalog, owner)
    with pytest.raises(CatalogError):
        catalog.apply(
            operation(
                "database.create",
                id=str(uuid4()),
                name="Alias",
                url="https://DATABASE.example:443/",
                token_env="OTHER",
            )
        )
    catalog.admit_write(
        str(uuid4()), first, owner, "append", str(uuid4()), str(uuid4())
    )
    catalog.apply(operation("database.update", id=first, enabled=False))
    assert not catalog.database(first)["enabled"]
    with pytest.raises(CatalogError):
        catalog.apply(operation("database.update", id=first, enabled=True))


def test_pending_identity_cannot_be_bypassed_and_transfer_requires_target(catalog):
    pending = str(uuid4())
    catalog.apply(operation("organization.create", id=pending, name="Pending"))
    with pytest.raises(CatalogError):
        catalog.apply(
            operation("organization.update", id=pending, enabled=True, state="active")
        )
    first = organization(catalog)
    db = database(catalog, first)
    with pytest.raises(CatalogError):
        catalog.apply(
            operation("organization.activate", id=first, identity_id=str(uuid4()))
        )
    catalog.apply(operation("database.drain", id=db, expected_owner=first))
    with pytest.raises(CatalogError):
        catalog.apply(
            operation("database.transfer", id=db, **{"from": first, "to": None})
        )


def test_concurrent_retries_commit_one_audit_and_stale_revision_cannot_apply(catalog):
    from concurrent.futures import ThreadPoolExecutor

    request = operation("organization.create", id=str(uuid4()), name="Concurrent")
    request["expected_revision"] = 0
    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(lambda _: catalog.apply(request), range(32)))
    assert all(result == results[0] for result in results)
    assert catalog.revision == 1 and len(catalog.audit()) == 1
    stale = operation("organization.create", id=str(uuid4()), name="Stale")
    stale["expected_revision"] = 0
    with pytest.raises(CatalogError):
        catalog.apply(stale)
    assert catalog.revision == 1 and len(catalog.audit()) == 1


def test_workflow_revision_requires_integer_not_boolean(catalog):
    catalog.apply(operation("organization.create", id=str(uuid4()), name="First"))
    request = operation("organization.create", id=str(uuid4()), name="Second")
    request["expected_revision"] = True
    with pytest.raises(CatalogError):
        catalog.begin_workflow(request)


def test_native_database_endpoint_is_scoped_and_canonical(catalog):
    owner = organization(catalog)
    backend = str(uuid4())
    first = str(uuid4())
    catalog.apply(
        operation(
            "database.create", id=first, organization_id=owner, name="Native",
            url=f"https://DATABASE.example:443/databases/{backend}/", token_env="DATABASE_TOKEN",
        )
    )
    assert catalog.database(first)["url"] == f"https://database.example/databases/{backend}"
    with pytest.raises(CatalogError):
        catalog.apply(
            operation(
                "database.create", id=str(uuid4()), name="Alias",
                url=f"https://database.example/databases/{backend}", token_env="OTHER",
            )
        )
    for suffix in [
        "/admin", f"/databases/{backend}/collections", f"/databases/{backend}/../other",
        f"/databases/{backend.upper()}", "/databases/%2e%2e", "/databases/not-a-uuid",
    ]:
        with pytest.raises(CatalogError):
            catalog.apply(
                operation(
                    "database.create", id=str(uuid4()), name="Invalid",
                    url="https://database.example" + suffix, token_env="OTHER",
                )
            )
