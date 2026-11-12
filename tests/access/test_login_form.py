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
