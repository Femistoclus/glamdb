# Combined Keycloak login form

New realms provisioned by `apps.workbench_access.provision` already show the
username and password together. Updating workbench assets alone does not change
the login flow of an existing Keycloak client.

## Migrate an existing client

Rebuild the gateway image from the current source and use the existing
[SSO deployment](SSO_OPERATIONS.md). Prepare the current Keycloak administrator's
password in a private mode0600 file owned by the gateway's container UID
(10001 by default). With the configured `dc` compose function:

```sh
dc run --rm --no-deps \
  -v "$ACCESS_ROOT/private/keycloak-admin.password:/admin.password:ro" \
  gateway python -m apps.workbench_access.login_form \
  --server https://keycloak-admin:18080 \
  --admin YOUR_ADMIN --password-file /admin.password \
  --ca-file /etc/glamdb/pki/caddy-root.crt
```

Use the equivalent configured `podman compose` command for a Podman deployment.
The command copies the standard browser flow to `glamdb-workbench-browser`,
disables organization identification before password entry in that copy, and
binds it only to `glamdb-workbench`. Membership, roles, MFA, SSO cookies and the
disabled password grant remain in effect. Repeating the command reuses the
client-owned flow.

Custom realm/client flows, an unknown copy with the same name, or a flow shared
with another client/binding require manual review; the command refuses to
change them. It also refuses a form without the required username/password
execution. The built-in realm browser flow remains available for rollback.

Use a new private browser session to check both fields, successful/failed login,
password recovery, MFA and the authorized database list. Existing SSO sessions
can skip the form. To roll back, clear the client's Browser flow override under
Clients → glamdb-workbench → Advanced → Authentication flow overrides.

## Optional demonstration account

Adding `--demo-user-id EXISTING_USER_UUID` explicitly sets a selected managed
Keycloak account to the public temporary `hello` / `world` credentials.
Normal provisioning and migration leave this mode disabled.

The account must be enabled, managed and a member of its sole organization;
the command refuses a login owned by another user. It preserves the user's
UUID, roles, attributes and membership and revokes that user's sessions.
Temporary realm password-policy and rename settings are restored in `finally`;
inspect them before retrying an interrupted administrative command. Verify the
demonstration login in a fresh browser session.
