"""Idempotent database ownership workflows and thin Keycloak administration."""

from dataclasses import replace
from uuid import UUID
import anyio

from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakGetError, KeycloakDeleteError, KeycloakError

from .admission import child
from .identity import Principal
from .resources import CatalogError, identifier, text, database_definition
from .settings import secret_file


class KeycloakDirectory:
    def __init__(self, settings):
        sso = settings.sso
        base = (sso.backchannel or sso.issuer).split("/realms/", 1)[0] + "/"
        self.client = KeycloakAdmin(
            server_url=base,
            realm_name="glamdb",
            client_id=sso.directory_client_id or sso.client_id,
            client_secret_key=secret_file(
                sso.directory_secret_file or sso.client_secret_file
            ),
            grant_type="client_credentials",
            verify=str(sso.ca_file) if sso.ca_file else True,
            timeout=sso.timeout_seconds,
            max_retries=0,
        )

    def ensure_organization(self, identity, name, login, enabled=True):
        # Native custom attributes make retries after an unknown create response discoverable.
        found = self.client.get_organizations({"q": f"glamdb_id:{identity}", "max": 2})
        if len(found) > 1:
            raise CatalogError("CONFLICT", "duplicate native identity mapping")
        if found:
            external = found[0]
            if external.get("alias") != identity:
                raise CatalogError("CONFLICT", "identity mapping changed")
            identity_id = external["id"]
        else:
            identity_id = self.client.create_organization(
                {
                    "name": name,
                    "alias": identity,
                    "enabled": False,
                    "attributes": {"glamdb_id": [identity]},
                }
            )
        user_id = self.client.get_user_id(login)
        if user_id:
            user = self.client.get_user(user_id)
            if user.get("attributes", {}).get("glamdb_managed_org") != [identity]:
                raise CatalogError(
                    "CONFLICT",
                    "login belongs to a different identity; choose another login",
                )
        else:
            user_id = self.client.create_user(
                {
                    "username": login,
                    "enabled": True,
                    "attributes": {"glamdb_managed_org": [identity]},
                    "requiredActions": ["UPDATE_PASSWORD"],
                }
            )
        memberships = self.client.get_user_organizations(user_id)
        if any(item["id"] != identity_id for item in memberships):
            raise CatalogError(
                "CONFLICT", "managed login has unexpected organization membership"
            )
        if not memberships:
            self.client.organization_user_add(user_id, identity_id)
        self.client.assign_realm_roles(
            user_id, [self.client.get_realm_role("glamdb-member")]
        )
        self.client.update_organization(
            identity_id,
            {**self.client.get_organization(identity_id), "enabled": enabled},
        )
        return identifier(identity_id)

    def resolve_identity(self, organization):
        if organization["identity_id"]:
            return organization["identity_id"]
        found = self.client.get_organizations(
            {"q": f"glamdb_id:{organization['id']}", "max": 2}
        )
        if not found:
            return None
        if len(found) != 1 or found[0].get("alias") != organization["id"]:
            raise CatalogError(
                "CONFLICT", "pending native identity mapping is ambiguous"
            )
        return identifier(found[0]["id"])

    def update(self, organization, *, enabled=None, name=None):
        identity_id = self.resolve_identity(organization)
        if identity_id is None:
            if enabled is False:
                return
            raise CatalogError(
                "CONFLICT", "native identity provisioning has not completed"
            )
        payload = {}
        if name is not None:
            payload["name"] = name
        if enabled is not None:
            payload["enabled"] = enabled
        self.client.update_organization(
            identity_id,
            {**self.client.get_organization(identity_id), **payload},
        )
        if enabled is not None:
            members = self.client.get_organization_members(identity_id, {"max": 129})
            if len(members) > 128:
                raise CatalogError(
                    "RESOURCE_EXHAUSTED",
                    "organization member budget exceeded; finish in Keycloak before retry",
                )
            for member in members:
                # Native account status remains authoritative. Re-enabling an organization
                # must not re-enable an account separately disabled in Keycloak.
                if not enabled:
                    self.client.user_logout(member["id"])

    def delete(self, organization):
        identity_id = self.resolve_identity(organization)
        if identity_id is None:
            return
        try:
            members = self.client.get_organization_members(identity_id, {"max": 129})
        except KeycloakGetError as error:
            if error.response_code == 404:
                return
            raise
        if len(members) > 128:
            raise CatalogError(
                "RESOURCE_EXHAUSTED", "organization member budget exceeded"
            )
        for member in members:
            user = self.client.get_user(member["id"])
            self.client.user_logout(member["id"])
            if user.get("attributes", {}).get("glamdb_managed_org") == [
                organization["id"]
            ]:
                self.client.delete_user(member["id"])
        try:
            self.client.delete_organization(identity_id)
        except KeycloakDeleteError as error:
            if error.response_code != 404:
                raise

    def logout(self, subject):
        self.client.user_logout(subject)


COMMANDS = {
    "organization.create": (("id", "name", "login"), ("enabled",)),
    "organization.rename": (("id", "name"), ()),
    "organization.disable": (("id",), ()),
    "organization.enable": (("id",), ()),
    "organization.delete": (("id",), ()),
    "database.create": (
        ("id", "name", "url"),
        ("organization_id", "token_env", "token_file", "ca_file", "enabled"),
    ),
    "database.update": (
        ("id",),
        ("name", "url", "token_env", "token_file", "ca_file", "enabled"),
    ),
    "database.assign": (("id", "organization_id"), ()),
    "database.detach": (("id", "expected_owner"), ()),
    "database.transfer": (("id", "from", "to"), ("name",)),
    "database.delete": (("id", "expected_owner"), ()),
}


class Lifecycle:
    def __init__(self, catalog, gate, directory, authorizer, settings):
        self.catalog = catalog
        self.gate = gate
        self.directory = directory
        self.authorizer = authorizer
        self.settings = settings
        self.lock = anyio.Lock()

    async def run(self, request, actor):
        try:
            return await self._run(request, actor)
        except CatalogError as error:
            if (
                isinstance(request, dict)
                and {"operation_id", "action", "params"} <= request.keys()
                and request.get("operation_id")
            ):
                try:
                    identifier(request["operation_id"])
                except CatalogError:
                    raise error from None
                stages = [
                    child(request["operation_id"], stage)
                    for stage in (
                        "intent",
                        "activate",
                        "mutation",
                        "disable",
                        "enable",
                        "rename",
                        "drain",
                        "delete",
                        "resume",
                    )
                ]
                self.catalog.fail_empty_workflow(request, stages, error.code)
            raise

    async def _run(self, request, actor):
        self.catalog.fields(
            request, ("operation_id", "action", "params"), ("expected_revision",)
        )
        action = request["action"]
        p = request["params"]
        operation = identifier(request["operation_id"])
        if not isinstance(action, str) or action not in COMMANDS:
            raise CatalogError("INVALID_ARGUMENT", "unsupported lifecycle action")
        required, optional = COMMANDS[action]
        self.catalog.fields(p, required, optional)
        identifier(p["id"])
        if "login" in p:
            import re

            if not isinstance(p["login"], str) or not re.fullmatch(
                r"[a-zA-Z0-9_.-]{1,64}", p["login"]
            ):
                raise CatalogError("INVALID_ARGUMENT", "invalid native username")
        if "name" in p:
            text(p["name"])
        for key in ("from", "to", "organization_id", "expected_owner"):
            if key in p and (
                p[key] is not None or key in ("from", "to", "organization_id")
            ):
                identifier(p[key])
        if action == "database.create":
            database_definition(p, test_mode=self.settings.sso.test_mode)
        if action == "database.update":
            candidate = {**self.catalog.database(p["id"]), **p}
            if "token_env" in p:
                candidate["token_file"] = None
            if "token_file" in p:
                candidate["token_env"] = None
            database_definition(candidate, test_mode=self.settings.sso.test_mode)
        async with self.lock:
            previous = self.catalog.begin_workflow(request)
            if previous:
                return previous

            def apply(stage, command, params):
                return self.catalog.apply(
                    {
                        "operation_id": child(operation, stage),
                        "action": command,
                        "params": params,
                    },
                    actor=actor.subject,
                )

            async def external(function, *args):
                self.catalog.mark_external(operation)
                return await anyio.to_thread.run_sync(function, *args)

            if action == "organization.create":
                apply(
                    "intent", "organization.create", {"id": p["id"], "name": p["name"]}
                )
                if type(p.get("enabled", True)) is not bool:
                    raise CatalogError("INVALID_ARGUMENT", "enabled must be boolean")
                identity_id = await external(
                    self.directory.ensure_organization,
                    p["id"],
                    p["name"],
                    p["login"],
                    p.get("enabled", True),
                )
                apply(
                    "activate",
                    "organization.activate",
                    {
                        "id": p["id"],
                        "identity_id": identity_id,
                        "enabled": p.get("enabled", True),
                    },
                )
            elif action.startswith("organization."):
                try:
                    organization = self.catalog.organization(p["id"])
                except CatalogError:
                    if action != "organization.delete":
                        raise
                    self.catalog.receipt(child(operation, "delete"))
                    organization = None
                if action == "organization.disable":
                    apply(
                        "disable",
                        "organization.update",
                        {"id": p["id"], "enabled": False},
                    )
                    await external(
                        lambda: self.directory.update(organization, enabled=False)
                    )
                elif action == "organization.enable":
                    if (
                        organization["state"] != "active"
                        or not organization["identity_id"]
                    ):
                        raise CatalogError("CONFLICT", "organization is not active")
                    await external(
                        lambda: self.directory.update(organization, enabled=True)
                    )
                    apply(
                        "enable",
                        "organization.update",
                        {"id": p["id"], "enabled": True},
                    )
                elif action == "organization.rename":
                    await external(
                        lambda: self.directory.update(organization, name=p["name"])
                    )
                    apply("rename", "organization.update", p)
                elif organization is not None:
                    if any(
                        db["organization_id"] == p["id"]
                        for db in self.catalog.state()["databases"]
                    ):
                        raise CatalogError(
                            "NOT_EMPTY",
                            "detach or transfer databases before deleting an organization",
                        )
                    apply(
                        "disable",
                        "organization.update",
                        {"id": p["id"], "enabled": False, "state": "deleting"},
                    )
                    try:
                        await external(
                            lambda: self.directory.update(organization, enabled=False)
                        )
                    except KeycloakError as error:
                        if error.response_code != 404:
                            raise
                    await external(self.directory.delete, organization)
                    apply("delete", "organization.delete", {"id": p["id"]})
            else:
                needs_drain = action in (
                    "database.transfer",
                    "database.detach",
                    "database.delete",
                ) or (action == "database.update" and bool(set(p) - {"id", "enabled"}))
                if needs_drain:
                    try:
                        db = self.catalog.database(p["id"])
                    except CatalogError:
                        if action != "database.delete":
                            raise
                        self.catalog.receipt(child(operation, "mutation"))
                        db = None
                    if db is not None:
                        owner = p.get(
                            "from", p.get("expected_owner", db["organization_id"])
                        )
                        await self.gate.drain(
                            p["id"],
                            owner,
                            operation,
                            self.settings.drain_timeout_seconds,
                            actor.subject,
                        )
                apply("mutation", action, p)
                if action in ("database.transfer", "database.assign"):
                    db = self.catalog.database(p["id"])
                    new = self.catalog.organization(
                        p.get("to", p.get("organization_id"))
                    )
                    source = Principal(
                        actor.subject,
                        None,
                        ("glamdb-member",),
                        actor.expires_at,
                        organization_id=p.get("from", ""),
                    )
                    target = replace(
                        source,
                        identity_organization=new["identity_id"],
                        organization_id=new["id"],
                    )
                    for permission in ("read", "write", "metadata"):
                        expected = bool(
                            db["enabled"]
                            and new["enabled"]
                            and new["state"] == "active"
                        )
                        if (
                            await self.authorizer.allowed(
                                source, permission, db, new, probe_active=True
                            )
                            or await self.authorizer.allowed(
                                target, permission, db, new, probe_active=True
                            )
                            != expected
                        ):
                            raise CatalogError(
                                "POLICY_NOT_READY",
                                "ownership changed but admission remains closed pending policy verification",
                            )
                if (
                    needs_drain and action not in ("database.detach", "database.delete")
                ) or action == "database.assign":
                    apply("resume", "database.resume", {"id": p["id"]})
            result = {
                "operation_id": operation,
                "action": action,
                "target": p["id"],
                "revision": self.catalog.revision,
                "state": "complete",
                "applied": True,
                "metadata_committed": True,
                "identity_complete": True,
            }
            self.catalog.finish_workflow(operation, result)
            return result
