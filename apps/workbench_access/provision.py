"""One-time native Keycloak realm/client setup; never imports legacy passwords."""

import argparse
import json
import os
from pathlib import Path
import secrets

from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakGetError, KeycloakPostError

from .resources import CatalogError, private_output
from .settings import origin, secret_file


def write_secret(path, value):
    path = private_output(path)
    descriptor = os.open(
        path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "wb") as output:
        output.write(value if isinstance(value, bytes) else value.encode("ascii"))
        output.flush()
        os.fsync(output.fileno())


def provision(admin, *, workbench_origin, output, test_mode=False):
    origin(workbench_origin, test_mode=test_mode)
    output = private_output(output).absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(
        (output / name).exists()
        for name in (
            "gateway.secret",
            "workbench.secret",
            "control.secret",
            "cookie.secret",
            "csrf.secret",
        )
    ):
        raise CatalogError(
            "ALREADY_EXISTS",
            "provisioning never overwrites existing client secrets; rotate in Keycloak",
        )
    realm = {
        "realm": "glamdb",
        "enabled": True,
        "organizationsEnabled": True,
        "registrationAllowed": False,
        "resetPasswordAllowed": True,
        "editUsernameAllowed": False,
        "verifyEmail": True,
        "sslRequired": "none" if test_mode else "external",
        "bruteForceProtected": True,
        "permanentLockout": False,
        "failureFactor": 5,
        "maxFailureWaitSeconds": 900,
        "waitIncrementSeconds": 60,
        "minimumQuickLoginWaitSeconds": 10,
        "accessTokenLifespan": 60,
        "ssoSessionIdleTimeout": 1800,
        "ssoSessionMaxLifespan": 28800,
        "revokeRefreshToken": True,
        "refreshTokenMaxReuse": 0,
        "passwordPolicy": "length(14) and digits(1) and lowerCase(1) and upperCase(1) and specialChars(1) and notUsername",
    }
    try:
        admin.create_realm(realm)
    except KeycloakPostError as error:
        if error.response_code != 409:
            raise
        # Never reset policy on an existing live realm through this bootstrap command.
        raise CatalogError(
            "ALREADY_EXISTS",
            "realm already exists; review native configuration and rotate clients explicitly",
        ) from None
    admin.connection.realm_name = "glamdb"
    for name in ("glamdb-member", "glamdb-operator"):
        admin.create_realm_role({"name": name})
    # Keycloak represents disabled unmanaged attributes as null, not an enum string.
    profile = admin.get_realm_users_profile()
    profile["unmanagedAttributePolicy"] = None
    for attr in profile["attributes"]:
        attr.pop("required", None)
        if attr["name"] == "email":
            attr["required"] = {"roles": ["user"]}
        if attr["name"] == "username":
            attr["permissions"]["edit"] = ["admin"]
            attr["validations"]["length"] = {"min": 1, "max": 64}
    profile["attributes"].append(
        {
            "name": "glamdb_managed_org",
            "multivalued": False,
            "permissions": {"view": ["admin"], "edit": ["admin"]},
            "validations": {"length": {"max": 36}},
        }
    )
    admin.update_realm_users_profile(profile)
    scopes = {scope["name"]: scope for scope in admin.get_client_scopes()}
    scope = admin.get_client_scope(scopes["organization"]["id"])
    mapper = scope["protocolMappers"][0]
    mapper["config"]["addOrganizationId"] = "true"
    mapper["config"]["addOrganizationAttributes"] = "false"
    admin.update_mapper_in_client_scope(
        scopes["organization"]["id"], mapper["id"], mapper
    )
    common = {
        "protocol": "openid-connect",
        "enabled": True,
        "directAccessGrantsEnabled": False,
        "defaultClientScopes": ["basic", "profile", "email", "roles", "web-origins"],
        "optionalClientScopes": ["organization"],
        "protocolMappers": [
            {
                "name": "glamdb-api-audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": "glamdb-api",
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                    "introspection.token.claim": "true",
                },
            },
            {
                "name": "glamdb-gateway-audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": "glamdb-gateway",
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                    "introspection.token.claim": "true",
                },
            },
        ],
    }
    clients = {}
    for name in ("glamdb-workbench", "glamdb-sdk", "glamdb-gateway", "glamdb-control"):
        client = {
            **common,
            "clientId": name,
            "publicClient": name == "glamdb-sdk",
            "standardFlowEnabled": name == "glamdb-workbench",
            "serviceAccountsEnabled": name in ("glamdb-gateway", "glamdb-control"),
            "attributes": {
                "pkce.code.challenge.method": "S256",
                "oauth2.device.authorization.grant.enabled": str(
                    name == "glamdb-sdk"
                ).lower(),
            },
            "redirectUris": (
                [workbench_origin + "/oauth2/callback"]
                if name == "glamdb-workbench"
                else []
            ),
            "webOrigins": [workbench_origin] if name == "glamdb-workbench" else [],
        }
        if name != "glamdb-sdk":
            client["secret"] = secrets.token_urlsafe(32)
        if name == "glamdb-control":
            client["defaultClientScopes"] = [
                *common["defaultClientScopes"],
                "kc-admin-api",
            ]
        client["protocolMappers"] = [
            *common["protocolMappers"],
            {
                "name": "client-audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": name,
                    "access.token.claim": "true",
                    "id.token.claim": "false",
                    "introspection.token.claim": "true",
                },
            },
        ]
        identity = admin.create_client(client)
        if name == "glamdb-workbench":
            from .login_form import configure_login_form

            configure_login_form(admin)
        clients[name] = identity
        if name != "glamdb-sdk":
            write_secret(
                output / (name.removeprefix("glamdb-") + ".secret"), client["secret"]
            )
    management = admin.get_client_id("realm-management")
    for name, roles in {
        "glamdb-control": [
            "manage-users",
            "manage-organizations",
            "view-organizations",
            "query-organizations",
            "view-realm",
        ]
    }.items():
        account = admin.get_client_service_account_user(clients[name])
        admin.assign_client_role(
            account["id"],
            management,
            [admin.get_client_role(management, role) for role in roles],
        )
        if name == "glamdb-control":
            admin.assign_realm_roles(
                account["id"], [admin.get_realm_role("glamdb-operator")]
            )
    write_secret(output / "cookie.secret", secrets.token_bytes(32))
    write_secret(output / "csrf.secret", secrets.token_urlsafe(48))
    return {
        "realm_created": True,
        "clients": list(clients),
        "credentials_directory": str(output),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--workbench-origin", required=True)
    parser.add_argument("--admin", required=True)
    parser.add_argument("--password-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--ca-file", type=Path)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--private-admin-origin")
    args = parser.parse_args()
    try:
        origin(args.server, test_mode=args.test_mode)
        admin = KeycloakAdmin(
            server_url=args.server.rstrip("/") + "/",
            username=args.admin,
            password=secret_file(args.password_file),
            realm_name="master",
            user_realm_name="master",
            verify=str(args.ca_file) if args.ca_file else True,
            timeout=5,
            max_retries=0,
        )
        # Obtain the bootstrap token in master before selecting the new realm.
        admin.get_realms()
        if args.private_admin_origin:
            origin(args.private_admin_origin, test_mode=args.test_mode)
            master = admin.get_realm("master")
            admin.update_realm(
                "master",
                {
                    "attributes": {
                        **master.get("attributes", {}),
                        "frontendUrl": args.private_admin_origin,
                    }
                },
            )
            # The issuer changed; let the native SDK obtain a fresh master token.
            admin = KeycloakAdmin(
                server_url=args.server.rstrip("/") + "/",
                username=args.admin,
                password=secret_file(args.password_file),
                realm_name="master",
                user_realm_name="master",
                verify=str(args.ca_file) if args.ca_file else True,
                timeout=5,
                max_retries=0,
            )
            admin.get_realms()
        print(
            json.dumps(
                provision(
                    admin,
                    workbench_origin=args.workbench_origin,
                    output=args.output,
                    test_mode=args.test_mode,
                )
            )
        )
    except Exception as error:
        code = error.code if isinstance(error, CatalogError) else "IDENTITY_UNAVAILABLE"
        parser.exit(
            1,
            json.dumps(
                {
                    "error": {
                        "code": code,
                        "message": "Provisioning did not complete; inspect native administrative state before retry",
                    }
                }
            )
            + "\n",
        )


if __name__ == "__main__":
    main()
