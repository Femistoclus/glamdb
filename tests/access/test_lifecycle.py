from types import SimpleNamespace
from uuid import uuid4
import pytest
from apps.workbench_access.admission import AdmissionGate, child
from apps.workbench_access.identity import Principal
from apps.workbench_access.lifecycle import Lifecycle
from apps.workbench_access.resources import CatalogError
from .test_resources import catalog, organization, database, operation


@pytest.fixture
def anyio_backend():
    return "asyncio"


def lifecycle(catalog):
    class Directory:
        fail = False
        calls = []

        def ensure_organization(self, *args):
            self.calls.append(args)
            if self.fail:
                raise RuntimeError("injected native service outage")
            return str(uuid4())

        def update(self, *args, **kwargs):
            if self.fail:
                raise RuntimeError("injected native service outage")

        def delete(self, *args):
            pass

    class Policy:
        fail = False

        async def allowed(self, p, action, db, org, **kwargs):
            if self.fail:
                return False
            return p.organization_id == db["organization_id"] and bool(org["enabled"])

    directory = Directory()
    policy = Policy()
    service = Lifecycle(
        catalog,
        AdmissionGate(catalog),
        directory,
        policy,
        SimpleNamespace(drain_timeout_seconds=0.1, sso=SimpleNamespace(test_mode=True)),
    )
    actor = Principal(str(uuid4()), None, ("glamdb-operator",), 9999999999)
    return service, directory, policy, actor


@pytest.mark.anyio
async def test_create_identity_outage_keeps_disabled_pending_and_retry_stable(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    request = operation("organization.create", id=str(uuid4()), name="New", login="new")
    directory.fail = True
    with pytest.raises(RuntimeError):
        await service.run(request, actor)
    assert catalog.organization(request["params"]["id"])["state"] == "pending"
    assert not catalog.organization(request["params"]["id"])["enabled"]
    directory.fail = False
    first = await service.run(request, actor)
    assert await service.run(request, actor) == first
    assert len(catalog.state()["organizations"]) == 1
    with pytest.raises(CatalogError):
        await service.run(
            {**request, "params": {**request["params"], "login": "different"}}, actor
        )


@pytest.mark.anyio
async def test_disable_precedes_identity_outage(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = organization(catalog)
    request = operation("organization.disable", id=org)
    directory.fail = True
    with pytest.raises(RuntimeError):
        await service.run(request, actor)
    assert not catalog.organization(org)["enabled"]
    assert catalog.receipt(request["operation_id"])["state"] == "pending"
    directory.fail = False
    assert (await service.run(request, actor))["state"] == "complete"


@pytest.mark.anyio
async def test_transfer_policy_failure_keeps_closed_then_retry_resumes(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    first, second = organization(catalog), organization(catalog)
    db = database(catalog, first)
    request = operation("database.transfer", id=db, **{"from": first, "to": second})
    policy.fail = True
    with pytest.raises(CatalogError) as failure:
        await service.run(request, actor)
    assert failure.value.code == "POLICY_NOT_READY"
    assert (
        catalog.database(db)["state"] == "draining"
        and catalog.database(db)["organization_id"] == second
    )
    policy.fail = False
    result = await service.run(request, actor)
    assert result["applied"] and catalog.database(db)["state"] == "active"
    assert await service.run(request, actor) == result


@pytest.mark.anyio
async def test_crash_after_delete_can_finish_same_receipt(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = organization(catalog)
    db = database(catalog, org)
    request = operation("database.delete", id=db, expected_owner=org)
    finish = catalog.finish_workflow

    def fail(*args):
        raise RuntimeError("crash after durable mutation")

    catalog.finish_workflow = fail
    with pytest.raises(RuntimeError):
        await service.run(request, actor)
    catalog.finish_workflow = finish
    assert (await service.run(request, actor))["applied"]
    request = operation("organization.delete", id=org)
    catalog.finish_workflow = fail
    with pytest.raises(RuntimeError):
        await service.run(request, actor)
    catalog.finish_workflow = finish
    assert (await service.run(request, actor))["applied"]


@pytest.mark.anyio
async def test_emergency_disable_supersedes_pending_enable_without_regrant(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = organization(catalog)
    catalog.apply(operation("organization.update", id=org, enabled=False))
    request = operation("organization.enable", id=org)
    request["expected_revision"] = catalog.revision
    directory.fail = True
    with pytest.raises(RuntimeError):
        await service.run(request, actor)
    directory.fail = False
    await service.run(operation("organization.disable", id=org), actor)
    with pytest.raises(CatalogError):
        await service.run(request, actor)
    assert not catalog.organization(org)["enabled"]
    assert catalog.receipt(request["operation_id"])["state"] == "superseded"


@pytest.mark.anyio
async def test_detach_then_assign_verifies_and_reopens(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    first, second = organization(catalog), organization(catalog)
    db = database(catalog, first)
    await service.run(operation("database.detach", id=db, expected_owner=first), actor)
    await service.run(
        operation("database.assign", id=db, organization_id=second), actor
    )
    assert catalog.assert_admission(db, second)[0]["state"] == "active"


@pytest.mark.anyio
async def test_first_assignment_is_closed_until_policy_probes_complete(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = organization(catalog)
    db = str(uuid4())
    catalog.apply(
        operation(
            "database.create",
            id=db,
            name="Unassigned",
            url="https://unassigned.example",
            token_env="TOKEN",
        )
    )
    request = operation("database.assign", id=db, organization_id=org)
    policy.fail = True
    with pytest.raises(CatalogError):
        await service.run(request, actor)
    assert catalog.database(db)["state"] == "draining"
    with pytest.raises(CatalogError):
        catalog.assert_admission(db, org)
    policy.fail = False
    assert (await service.run(request, actor))["applied"]
    assert catalog.assert_admission(db, org)[0]["state"] == "active"


@pytest.mark.anyio
async def test_invalid_mapping_does_not_poison_future_corrected_command(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    identity = str(uuid4())
    with pytest.raises(CatalogError):
        await service.run(
            operation(
                "database.create",
                id=identity,
                name="DB",
                url="file:///bad",
                token_env="TOKEN",
            ),
            actor,
        )
    result = await service.run(
        operation(
            "database.create",
            id=identity,
            name="DB",
            url="https://backend.example",
            token_env="TOKEN",
        ),
        actor,
    )
    assert result["applied"]


@pytest.mark.anyio
async def test_unknown_native_effect_cannot_be_mislabeled_definite_failure(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = organization(catalog)
    catalog.apply(operation("organization.update", id=org, enabled=False))

    def partial(*args, **kwargs):
        raise CatalogError(
            "RESOURCE_EXHAUSTED", "native organization changed before member check"
        )

    directory.update = partial
    request = operation("organization.enable", id=org)
    with pytest.raises(CatalogError):
        await service.run(request, actor)
    assert catalog.receipt(request["operation_id"])["state"] == "pending"
    with pytest.raises(CatalogError):
        await service.run({**request, "params": {"id": str(uuid4())}}, actor)
    assert catalog.receipt(request["operation_id"])["state"] == "pending"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invalid",
    [
        {"operation_id": str(uuid4())},
        {"operation_id": str(uuid4()), "action": [], "params": {}},
    ],
)
async def test_malformed_command_is_validation_error_without_new_workflow(
    catalog, invalid
):
    service, directory, policy, actor = lifecycle(catalog)
    with pytest.raises(CatalogError) as error:
        await service.run(invalid, actor)
    assert error.value.code == "INVALID_ARGUMENT"
    assert (
        catalog.connection.execute("SELECT count(*) FROM workflows").fetchone()[0] == 0
    )


def test_native_disable_and_delete_discover_unbound_pending_identity():
    from apps.workbench_access.lifecycle import KeycloakDirectory

    org_id, native_id = str(uuid4()), str(uuid4())
    calls = []

    class Native:
        def get_organizations(self, query):
            return [{"id": native_id, "alias": org_id}]

        def get_organization(self, identity):
            assert identity == native_id
            return {
                "id": native_id,
                "alias": org_id,
                "name": "Pending",
                "enabled": True,
            }

        def update_organization(self, identity, payload):
            calls.append(("disable", identity, payload["enabled"]))

        def get_organization_members(self, identity, query):
            assert identity == native_id
            return []

        def delete_organization(self, identity):
            calls.append(("delete", identity))

    directory = object.__new__(KeycloakDirectory)
    directory.client = Native()
    pending = {"id": org_id, "identity_id": None}
    directory.update(pending, enabled=False)
    directory.delete(pending)
    assert calls == [("disable", native_id, False), ("delete", native_id)]


@pytest.mark.anyio
async def test_pending_create_can_be_disabled_then_deleted_without_reenabling(catalog):
    service, directory, policy, actor = lifecycle(catalog)
    org = str(uuid4())
    deleted = []
    directory.fail = True
    with pytest.raises(RuntimeError):
        await service.run(
            operation("organization.create", id=org, name="Pending", login="pending"),
            actor,
        )
    directory.fail = False
    directory.delete = lambda value: deleted.append(value["id"])
    await service.run(operation("organization.disable", id=org), actor)
    await service.run(operation("organization.delete", id=org), actor)
    assert deleted == [org]
    with pytest.raises(CatalogError):
        catalog.organization(org)
