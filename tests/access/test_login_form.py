from copy import deepcopy

import pytest

from apps.workbench_access.login_form import FLOW, configure_login_form


class Admin:
    def __init__(self):
        self.flows = [{"id": "builtin", "alias": "browser", "builtIn": True}]
        self.realm = {"browserFlow": "browser"}
        self.executions = {"browser": [
            {"id": "cookie", "providerId": "auth-cookie", "requirement": "ALTERNATIVE"},
            {"id": "org", "displayName": "GlamDB Organization", "authenticationFlow": True,
             "level": 0, "requirement": "ALTERNATIVE"},
            {"id": "credentials", "providerId": "auth-username-password-form", "requirement": "REQUIRED"},
            {"id": "otp", "providerId": "auth-otp-form", "requirement": "CONDITIONAL"},
        ]}
        self.client = {"id": "client", "clientId": "glamdb-workbench", "directAccessGrantsEnabled": False,
                       "authenticationFlowBindingOverrides": {"direct_grant": "original"}}
        self.others = []
        self.copied = []
        self.updates = []

    def get_realm(self, name):
        assert name == "glamdb"
        return deepcopy(self.realm)

    def get_authentication_flows(self):
        return deepcopy(self.flows)

    def copy_authentication_flow(self, payload, flow_alias):
        self.flows.append({"id": "combined", "alias": payload["newName"], "builtIn": False})
        self.executions[payload["newName"]] = deepcopy(self.executions[flow_alias])
        self.copied.append(flow_alias)

    def get_authentication_flow_executions(self, alias):
        return deepcopy(self.executions[alias])

    def update_authentication_flow(self, identity, payload):
        next(flow for flow in self.flows if flow["id"] == identity).update(payload)

    def update_authentication_flow_executions(self, payload, flow_alias):
        next(item for item in self.executions[flow_alias] if item["id"] == payload["id"]).update(payload)
        self.updates.append((flow_alias, payload["id"]))

    def get_client_id(self, name):
        assert name == "glamdb-workbench"
        return self.client["id"]

    def get_client(self, identity):
        return deepcopy(next(item for item in [self.client, *self.others] if item["id"] == identity))

    def get_clients(self):
        return [deepcopy(item) for item in [self.client, *self.others]]

    def update_client(self, identity, payload):
        assert identity == self.client["id"]
        self.client = deepcopy(payload)

    def custom_mfa(self):
        self.flows.append({"id": "custom", "alias": "required-mfa", "builtIn": False})
        self.executions["required-mfa"] = deepcopy(self.executions["browser"])
        self.executions["required-mfa"][-1]["requirement"] = "REQUIRED"


def test_combined_login_preserves_mfa_and_other_bindings_and_is_repeatable():
    admin = Admin()
    source = deepcopy(admin.executions["browser"])
    configure_login_form(admin)
    configure_login_form(admin)
    assert admin.copied == ["browser"]
    assert admin.executions["browser"] == source
    assert admin.executions[FLOW][1]["requirement"] == "DISABLED"
    assert admin.executions[FLOW][2]["requirement"] == "REQUIRED"
    assert admin.executions[FLOW][3]["requirement"] == "CONDITIONAL"
    assert admin.client["authenticationFlowBindingOverrides"] == {
        "direct_grant": "original", "browser": "combined"}
    assert admin.client["directAccessGrantsEnabled"] is False


def test_passwordless_flow_is_not_rebound_or_copied():
    admin = Admin()
    admin.executions["browser"] = [item for item in admin.executions["browser"] if item["id"] != "credentials"]
    with pytest.raises(ValueError):
        configure_login_form(admin)
    assert not admin.copied
    assert admin.client["authenticationFlowBindingOverrides"] == {"direct_grant": "original"}


@pytest.mark.parametrize("client_override", [False, True])
def test_custom_mfa_flow_requires_explicit_review_before_changes(client_override):
    admin = Admin()
    admin.custom_mfa()
    if client_override:
        admin.client["authenticationFlowBindingOverrides"]["browser"] = "custom"
    else:
        admin.realm["browserFlow"] = "required-mfa"
    before = deepcopy((admin.client, admin.executions))
    with pytest.raises(ValueError, match="custom"):
        configure_login_form(admin)
    assert not admin.copied
    assert (admin.client, admin.executions) == before
    assert admin.executions["required-mfa"][-1]["requirement"] == "REQUIRED"


def test_unrelated_existing_alias_is_not_modified_or_bound():
    admin = Admin()
    admin.flows.append({"id": "unrelated", "alias": FLOW, "builtIn": False})
    admin.executions[FLOW] = deepcopy(admin.executions["browser"])
    before = deepcopy(admin.executions)
    with pytest.raises(ValueError):
        configure_login_form(admin)
    assert admin.executions == before
    assert not admin.updates
    assert "browser" not in admin.client["authenticationFlowBindingOverrides"]


@pytest.mark.parametrize("sharing", ["other-client", "realm", "other-binding"])
def test_owned_flow_cannot_be_modified_when_shared(sharing):
    admin = Admin()
    configure_login_form(admin)
    admin.executions[FLOW][1]["requirement"] = "ALTERNATIVE"
    before = deepcopy(admin.executions[FLOW])
    if sharing == "other-client":
        admin.others.append({"id": "other", "authenticationFlowBindingOverrides": {"browser": "combined"}})
    elif sharing == "realm":
        admin.realm["browserFlow"] = FLOW
    else:
        admin.client["authenticationFlowBindingOverrides"]["direct_grant"] = "combined"
    with pytest.raises(ValueError):
        configure_login_form(admin)
    assert admin.executions[FLOW] == before


USER = "00112233-4455-4677-8899-aabbccddeeff"
ORG = "11112233-4455-4677-8899-aabbccddeeff"


class CredentialAdmin:
    def __init__(self):
        self.user = {
            "id": USER, "username": "owner", "enabled": True,
            "emailVerified": True, "requiredActions": ["UPDATE_PASSWORD"],
            "attributes": {"glamdb_managed_org": [ORG], "other": ["preserved"]},
        }
        self.memberships = [{"id": "native-org", "alias": ORG}]
        self.policy = "length(14) and digits(1)"
        self.calls = []
        self.conflict = None
        self.fail_reset = False
        self.username_editable = False
        self.fail_rename = False

    def get_user(self, identity):
        assert identity == USER
        return deepcopy(self.user)

    def get_user_id(self, login):
        return self.conflict or (USER if self.user["username"] == login else None)

    def get_user_organizations(self, identity):
        assert identity == USER
        return deepcopy(self.memberships)

    def get_realm(self, realm):
        assert realm == "glamdb"
        return {"passwordPolicy": self.policy, "editUsernameAllowed": self.username_editable}

    def update_realm(self, realm, payload):
        assert realm == "glamdb"
        if "passwordPolicy" in payload:
            self.policy = payload["passwordPolicy"]
            self.calls.append(("policy", self.policy))
        if "editUsernameAllowed" in payload:
            self.username_editable = payload["editUsernameAllowed"]
            self.calls.append(("username-editable", self.username_editable))

    def set_user_password(self, identity, password, temporary):
        assert identity == USER
        assert self.policy == ""
        if self.fail_reset:
            raise RuntimeError("password reset unavailable")
        self.calls.append(("password", password, temporary))
        self.user["requiredActions"].remove("UPDATE_PASSWORD")

    def update_user(self, identity, payload):
        assert identity == USER
        if not self.username_editable:
            raise ValueError("error-user-attribute-read-only")
        if self.fail_rename:
            raise RuntimeError("rename unavailable")
        self.user.update(deepcopy(payload))
        self.calls.append(("user", deepcopy(payload)))

    def user_logout(self, identity):
        assert identity == USER
        self.calls.append(("logout", identity))


def test_demo_credentials_preserve_account_and_restore_realm_password_policy():
    from apps.workbench_access.login_form import configure_demo_credentials

    admin = CredentialAdmin()
    original = deepcopy(admin.user)
    assert configure_demo_credentials(admin, USER) == {"demo_login": "hello"}
    assert admin.user == {**original, "username": "hello", "requiredActions": []}
    assert admin.policy == "length(14) and digits(1)"
    assert admin.username_editable is False
    assert admin.calls == [
        ("policy", ""), ("password", "world", False),
        ("policy", "length(14) and digits(1)"),
        ("username-editable", True), ("user", {"username": "hello"}),
        ("username-editable", False), ("logout", USER),
    ]


def test_failed_demo_password_reset_restores_policy_without_renaming_account():
    from apps.workbench_access.login_form import configure_demo_credentials

    admin = CredentialAdmin()
    admin.fail_reset = True
    with pytest.raises(RuntimeError, match="password reset unavailable"):
        configure_demo_credentials(admin, USER)
    assert admin.policy == "length(14) and digits(1)"
    assert admin.user["username"] == "owner"
    assert admin.calls == [("policy", ""), ("policy", "length(14) and digits(1)")]


def test_failed_demo_rename_restores_the_read_only_username_setting():
    from apps.workbench_access.login_form import configure_demo_credentials

    admin = CredentialAdmin()
    admin.fail_rename = True
    with pytest.raises(RuntimeError, match="rename unavailable"):
        configure_demo_credentials(admin, USER)
    assert admin.user["username"] == "owner"
    assert admin.username_editable is False
    assert admin.policy == "length(14) and digits(1)"
    assert admin.calls[-2:] == [("username-editable", True), ("username-editable", False)]


@pytest.mark.parametrize("invalid", ["unmanaged", "disabled", "membership", "collision"])
def test_demo_credentials_refuse_unrelated_or_disabled_accounts_before_changes(invalid):
    from apps.workbench_access.login_form import configure_demo_credentials

    admin = CredentialAdmin()
    if invalid == "unmanaged":
        admin.user["attributes"] = {}
    elif invalid == "disabled":
        admin.user["enabled"] = False
    elif invalid == "membership":
        admin.memberships = []
    else:
        admin.conflict = "other-user"
    before = deepcopy(admin.user)
    with pytest.raises(ValueError):
        configure_demo_credentials(admin, USER)
    assert admin.user == before
    assert admin.calls == []
