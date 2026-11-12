"""Configure combined workbench sign-in and optional, explicit demo credentials."""

from uuid import UUID

FLOW = "glamdb-workbench-browser"
# Temporary public demonstration credentials; used only with --demo-user-id.
DEMO_LOGIN = "hello"
DEMO_PASSWORD = "world"


def _credentials_form(executions):
    if not any(item.get("providerId") == "auth-username-password-form" and
               item.get("requirement") == "REQUIRED" for item in executions):
        raise ValueError("the browser flow has no required combined credentials form; inspect it before migration")


def _exclusive_flow(admin, flow, realm, client_id):
    if flow.get("builtIn") or flow.get("description") != f"GlamDB combined login; owner={client_id}":
        raise ValueError("the existing workbench flow has unknown ownership; inspect it before migration")
    if any(value == flow["alias"] for key, value in realm.items() if key.endswith("Flow")):
        raise ValueError("the workbench flow is shared with the realm")
    for item in admin.get_clients():
        client = admin.get_client(item["id"])
        for binding, identity in client.get("authenticationFlowBindingOverrides", {}).items():
            if identity == flow["id"] and (item["id"] != client_id or binding != "browser"):
                raise ValueError("the workbench flow is shared with another client or binding")


def configure_login_form(admin):
    client_id = admin.get_client_id("glamdb-workbench")
    if not client_id:
        raise ValueError("workbench client does not exist")
    client = admin.get_client(client_id)
    realm = admin.get_realm("glamdb")
    flows = admin.get_authentication_flows()
    matching = next((item for item in flows if item["alias"] == FLOW), None)
    current = client.get("authenticationFlowBindingOverrides", {}).get("browser")
    browser = next((item for item in flows if item["alias"] == "browser" and item.get("builtIn")), None)
    # Custom flows can enforce additional MFA or organization policy. Leave
    # them unchanged until an operator has reviewed that policy explicitly.
    managed = matching is not None and current == matching["id"]
    if (browser is None or (not managed and
            (realm.get("browserFlow") != "browser" or current not in (None, browser["id"])))):
        raise ValueError("a custom browser flow requires manual review before changing the login form")
    if matching is None:
        _credentials_form(admin.get_authentication_flow_executions("browser"))
        admin.copy_authentication_flow({"newName": FLOW}, "browser")
        matching = next(item for item in admin.get_authentication_flows() if item["alias"] == FLOW)
        matching = {**matching, "description": f"GlamDB combined login; owner={client_id}"}
        admin.update_authentication_flow(matching["id"], matching)
    _exclusive_flow(admin, matching, realm, client_id)
    executions = admin.get_authentication_flow_executions(FLOW)
    _credentials_form(executions)
    # Preserve cookie SSO, conditional MFA and other existing browser policy.
    # Disable only identity-first organization discovery in the copied flow.
    organizations = [item for item in executions if item.get("authenticationFlow") and
                     item.get("level", 0) == 0 and
                     item.get("displayName", "").lower().endswith("organization")]
    if not organizations:
        organizations = [item for item in executions if item.get("providerId") == "organization"]
    for item in organizations:
        if item.get("requirement") != "DISABLED":
            admin.update_authentication_flow_executions({**item, "requirement": "DISABLED"}, FLOW)
    bindings = {**client.get("authenticationFlowBindingOverrides", {}), "browser": matching["id"]}
    if client.get("authenticationFlowBindingOverrides") != bindings:
        admin.update_client(client_id, {**client, "authenticationFlowBindingOverrides": bindings})
    return {"client": "glamdb-workbench", "browser_flow": FLOW, "credentials_form": "combined"}


def configure_demo_credentials(admin, user_id):
    user_id = str(UUID(str(user_id)))
    user = admin.get_user(user_id)
    managed = user.get("attributes", {}).get("glamdb_managed_org", [])
    if not user.get("enabled") or len(managed) != 1:
        raise ValueError("demo credentials require an enabled managed organization account")
    organization_id = str(UUID(managed[0]))
    memberships = admin.get_user_organizations(user_id)
    if len(memberships) != 1 or memberships[0].get("alias") != organization_id:
        raise ValueError("demo account must belong to its managed organization only")
    existing = admin.get_user_id(DEMO_LOGIN)
    if existing and existing != user_id:
        raise ValueError("the demo login already belongs to another account")
    realm = admin.get_realm("glamdb")
    policy = realm.get("passwordPolicy", "")
    # Keycloak validates administrative password resets against the realm policy.
    # Permit this one explicitly requested short demo password, then restore it.
    try:
        if policy:
            admin.update_realm("glamdb", {"passwordPolicy": ""})
        admin.set_user_password(user_id, DEMO_PASSWORD, temporary=False)
    finally:
        if policy:
            admin.update_realm("glamdb", {"passwordPolicy": policy})
    if user["username"] != DEMO_LOGIN:
        editable = realm.get("editUsernameAllowed", False)
        try:
            if not editable:
                admin.update_realm("glamdb", {"editUsernameAllowed": True})
            admin.update_user(user_id, {"username": DEMO_LOGIN})
        finally:
            if not editable:
                admin.update_realm("glamdb", {"editUsernameAllowed": False})
    admin.user_logout(user_id)
    return {"demo_login": DEMO_LOGIN}


def main():
    import argparse
    import json
    from pathlib import Path
    from keycloak import KeycloakAdmin
    from .settings import origin, secret_file

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--admin", required=True)
    parser.add_argument("--password-file", required=True, type=Path)
    parser.add_argument("--ca-file", type=Path)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--demo-user-id", type=UUID,
                        help="Explicitly set this managed demo account to the temporary hello/world credentials")
    args = parser.parse_args()
    origin(args.server, test_mode=args.test_mode)
    admin = KeycloakAdmin(server_url=args.server.rstrip("/") + "/", username=args.admin,
                          password=secret_file(args.password_file), realm_name="master",
                          user_realm_name="master", verify=str(args.ca_file) if args.ca_file else True,
                          timeout=10, max_retries=0)
    admin.get_realms()
    admin.connection.realm_name = "glamdb"
    result = configure_login_form(admin)
    if args.demo_user_id:
        result.update(configure_demo_credentials(admin, args.demo_user_id))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
