# Operating GlamDB access with Keycloak and Cerbos

The access deployment provides native Keycloak accounts and organizations,
OAuth2 Proxy browser sessions with a private ephemeral Valkey store,
Cerbos policy checks, Caddy HTTPS, and a private
GlamDB ownership catalog. It requires existing database HTTP servers. Registering
a database connects one such server; deleting its mapping never deletes its data.
The native HTTP server is available; register its explicit database scope. A
running identity stack, SDK checks against an explicitly selected mock, or a
successful image build does not establish production identity integration.

Run commands from the checkout root. The single-host instructions target Linux
with Docker Engine and Docker Compose v2. The gateway image uses Python 3.13.11.
They do not install Docker, create DNS records, supply SMTP credentials, provision
database servers, or send notifications to an external chat.

## 1. Components and trust boundaries

The request paths are:

```text
browser / SDK -> public Caddy HTTPS -> OAuth2 Proxy -> private gateway
                                                       |-> private Keycloak
                                                       |-> private Cerbos
                                                       `-> existing database server

operator CLI -> owner-only Unix socket -> gateway lifecycle -> native Keycloak
                                         `-> SQLite ownership/audit/receipts

OAuth2 Proxy -> private ephemeral Valkey session store
```

Only Caddy publishes TCP 80/443 and UDP 443. Its private Keycloak administrative
listener is published on host **127.0.0.1:18080**, with a separate internal-CA
certificate for `keycloak-admin`. PostgreSQL, Valkey 6379, Keycloak 8080/9000, Cerbos
3592/3593, OAuth2 Proxy 4180, gateway 8080, Caddy 9095 metrics, and the operator
socket have no public port mapping. Keep the host firewall closed to them.

The public identity hostname accepts `/realms/glamdb`, its descendants, and
`/resources/*`. It rejects the master realm and `/admin`. `KC_HOSTNAME_ADMIN`
alone does not restrict access: the Caddy routes and absence of published
Keycloak ports provide that restriction. Do not add a second ingress that
forwards every Keycloak path.

Caddy deletes caller-supplied identity/proxy headers before proxying. OAuth2
Proxy supplies the browser access token; the gateway verifies its signature,
issuer, API audience, access-token type, lifetime, current native organization
and roles, and performs live introspection. It then reads current catalog
attributes and asks Cerbos. Browser cookies plus an Authorization header are
rejected. There is no Basic, shared-password, static-group, or mock fallback.

The edge rejects percent-encoded database paths before OAuth2 Proxy can
normalize/redirect them. Percent encoding in query parameters remains valid.
Canonical UUID data paths require no encoded path characters.

The gateway's TCP listener is private, but remains a sensitive authenticated
control surface. The CLI uses its mode0600 Unix socket. Do not expose TCP 8080
as a convenient alternative entry point. A network peer alone receives no
administrative grant: control requests also require the operator bearer role
and policy authorization; browser cookies cannot invoke them.

The Compose networks reserve `172.30.80.0/24`, `172.30.81.0/24` and
`172.30.82.0/24`. Check for host/VPN/container overlap before using them. If you
change them, update Caddy's fixed IPs, Keycloak's trusted proxy address, and
OAuth2 Proxy's trusted proxy address together. Never replace those restrictions
with an unreviewed trust-all setting.

### Pinned runtime

The [Compose file](../../deploy/access/compose.yaml) and
[Dockerfile](../../deploy/access/Dockerfile) pin multi-platform image indexes:

- Keycloak 26.8.2: `sha256:723b1d6e254c0fc96d5cffec860c7e0744d6a3277822b865bb538cf891575e41`.
- PostgreSQL 18.6: `sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42`.
- OAuth2 Proxy 7.15.6: `sha256:c3415e8660af553f5e18f5d90db050614c02493c0ac6260ac59442f6d9b12048`.
- [Valkey 9.1.3](https://github.com/valkey-io/valkey/releases/tag/9.1.3) Alpine 3.24: `sha256:bf09d58e96d6b4524a15396b65b242bbe18beebbcc15a04d8ee75ddac62fae0b`.
- Cerbos 0.57.0: `sha256:e5e3b150b3cb0090975605254f255164567d67e233634199fb20972e3eaff300`.
- Caddy 2.11.9 Alpine: `sha256:0bbb055bda6793c999bde78b1b5ce3e89f829fb031af2cf50056ea1246559fee`.
- Python 3.13.11 slim Bookworm: `sha256:20080e807bfc404f8450b185cf0fc95d553462673598549613735f70a5b4d5d0`.

Keycloak, OAuth2 Proxy, Cerbos and Caddy use Apache-2.0; PostgreSQL uses the
PostgreSQL License, Valkey uses [BSD-3-Clause](https://github.com/valkey-io/valkey/blob/9.1.3/COPYING),
and Python uses the PSF license. Preserve the upstream notices
when redistributing images and review the licenses of transitive packages.
No paid service, enterprise policy feature or hosted control plane is required.

[requirements.in](../../apps/workbench_access/requirements.in) records the direct
Python dependencies. [requirements.txt](../../apps/workbench_access/requirements.txt)
locks the Linux CPython 3.13 closure with release hashes, including Authlib,
joserfc, python-keycloak, the official Cerbos SDK, starlette-csrf,
prometheus-client and python-json-logger. Installation requires binary artifacts
and hashes. Do not replace this with an unpinned installation during recovery.

## 2. Host, DNS, disk and service identity

Use a dedicated Linux host or an explicitly isolated namespace on a known host.
Do not point these commands at an existing unknown installation. Allow SSH and
public TCP 80/443; allow UDP 443 if HTTP/3 is wanted. Check the cloud firewall,
host firewall, router forwarding and IPv6 rules independently. An AAAA record
that points elsewhere can break clients and certificate issuance even when A
is correct.

Create A/AAAA records for two names, for example `workbench.example.com` and
`login.example.com`. Both point to the Caddy host. Caddy obtains public
certificates using ACME, requiring correct DNS, access to its public challenge
ports, working outbound DNS/HTTPS, and a real operator email. If a hosting proxy
terminates TLS first, design its forwarding/trust and certificate renewal before
using this example; do not silently relax Secure cookies or TLS verification.

Reserve persistent local disk for PostgreSQL, the SQLite access catalog,
encrypted off-host backups, Caddy certificate state, the database stores and
observability. Container memory limits are examples rather than capacity
qualification. Start with enough memory for the entire assembled stack, not
only the access services. Keep disk space for PostgreSQL WAL, SQLite journals,
image upgrades and restores as well as steady-state data.

Run the gateway and administrative utility containers with the same numeric
UID/GID as the Linux service account owning catalog and secrets. Mode0600 plus
an arbitrary image UID does not work: the gateway checks the file owner against
its effective UID and rejects symlinks, multiple hard links and group/world
permissions. A supplementary group or mode0640 is insufficient.

For a new host, choose an unused UID/GID, then provision the account and paths
as a host administrator. These examples use 10001; choose another if occupied:

```sh
sudo useradd --system --uid 10001 --user-group --home-dir /var/lib/glamdb \
  --create-home --shell /usr/sbin/nologin glamdb
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access/private
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access/private/secrets
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access/private/pki
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access/state
sudo install -d -m 700 -o glamdb -g glamdb /var/lib/glamdb/access/state/backups
```

Keep a reviewed checkout at `/opt/glamdb`, readable by that account. Preserve
the supplied logo PNGs byte-for-byte and use the shipped runtime `ui/config/theme.json`
for the pink theme. The image does not copy the UI; Compose mounts the reviewed
UI directory read-only. Never place credentials, the catalog, database stores,
backup files, identity exports or generated private configuration under `ui/`.

The remaining shell examples run as the service account in `/opt/glamdb`.
Give that account only the intentionally chosen runtime permissions. Docker
daemon access is effectively host administration; a dedicated host or rootless
deployment can reduce the shared-host impact. Rootless UID mapping and bind
mount ownership require separate validation; the example does not assume they
are identical to a rootful Docker host.

```sh
ACCESS_ROOT=/var/lib/glamdb/access
umask 077
cp -n deploy/access/compose.env.example "$ACCESS_ROOT/compose.env"
cp -n deploy/access/gateway.example.json "$ACCESS_ROOT/private/gateway.json"
```

Edit `compose.env`: set actual numeric UID/GID, absolute directories, DNS names
and ACME email. It contains no secrets. Edit `gateway.json` to use the same
public Workbench origin and public `/realms/glamdb` issuer. Retain the private
`https://keycloak-admin:18080/realms/glamdb` backchannel, `/state/resources.sqlite`,
the mounted secret paths and `/etc/glamdb/pki/caddy-root.crt` CA. Test-only HTTP
and `test_mode` must remain absent in production.

Define a convenience function; use this same project and env file throughout:

```sh
dc() {
  docker compose --env-file "$ACCESS_ROOT/compose.env" \
    -f deploy/access/compose.yaml "$@"
}
```

## 3. Initial secrets, session authentication and PostgreSQL

Generate initial secrets into new files without putting values in arguments,
shell history or command output. The following refuses to overwrite files:

```sh
python3 - <<'PY'
import hashlib
import os
from pathlib import Path
import secrets
root = Path('/var/lib/glamdb/access/private')
bootstrap = secrets.token_urlsafe(48)
session_password = secrets.token_urlsafe(48)
values = {
    'postgres.password': secrets.token_urlsafe(48),
    'bootstrap.password': bootstrap,
    'keycloak.env': 'KC_DB_PASSWORD=' + secrets.token_urlsafe(48) + '\n'
        + 'KC_BOOTSTRAP_ADMIN_USERNAME=bootstrap-admin\n'
        + 'KC_BOOTSTRAP_ADMIN_PASSWORD=' + bootstrap + '\n',
    'secrets/session-store.password': session_password,
    'secrets/session-store.acl': 'user default off\n'
        + 'user oauth2-proxy on #' + hashlib.sha256(session_password.encode()).hexdigest()
        + ' ~* +@connection +get +set +getrange +mget +mset +msetnx +del +exists +pttl +pexpire +eval +evalsha +script|load +info\n',
    'oauth2-proxy.env': 'OAUTH2_PROXY_REDIS_PASSWORD=' + session_password + '\n',
}
for name, value in values.items():
    fd = os.open(root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as target:
        target.write(value + ('\n' if not value.endswith('\n') else ''))
        target.flush()
        os.fsync(target.fileno())
PY
```

Record these secrets in the operator's secret manager through a protected
editor/import facility. Do not `cat` them into a captured terminal, paste them
into an issue, or save them in build/CI logs. The Keycloak env file is a runtime
secret file, despite its `.env` extension. A full `docker compose config` can
print resolved `env_file` values; use **`dc config --quiet`** for validation.

The session store has one dedicated native ACL user; its ACL contains a password
hash and grants only connection, session and refresh-lock commands. The health
probe reads `session-store.password` into the native CLI's process environment.
OAuth2 Proxy 7.15.6 accepts its Redis password through the private env file; it
does not provide a Redis password-file option. The Redis URL contains no
credentials. Runtime administrators can inspect container environments, so
keep those inspection results private just like the existing Keycloak env file.
The proxy mounts only its own client and cookie keys, rather than the control
client's secrets. All three session-auth files must match and remain owned
mode0600 files outside the UI tree.

The `glamdb-observability` network is shared with the independently deployed
logging, monitoring and alerting services. Create it once if it does not exist,
or set `GLAMDB_OBSERVABILITY_NETWORK` to the reviewed existing equivalent:

```sh
docker network inspect glamdb-observability >/dev/null 2>&1 || \
  docker network create glamdb-observability
dc config --quiet
dc pull postgres keycloak cerbos oauth2-proxy caddy session-store
dc build gateway
dc up -d postgres
dc ps postgres
```

Docker BuildKit honors `deploy/access/Dockerfile.dockerignore`. For a direct
Podman build, explicitly select it; some Podman remote builders otherwise use
the repository root's intentionally restrictive ignore file:

```sh
podman build --ignorefile deploy/access/Dockerfile.dockerignore \
  -f deploy/access/Dockerfile -t glamdb-access-gateway:local .
```

The optional telemetry Python modules are included when present. The stable
`tools/verification/README.md` keeps the tools COPY valid in a standalone access
checkout. An access-only image remains usable without the telemetry components;
its `/metrics` endpoint returns 503 instead of fabricating measurements.

After PostgreSQL reports healthy, create a separate nonsuperuser role and DB.
Use the password from `KC_DB_PASSWORD` through the protected secret manager.
The `\password` prompt does not echo the password or put it in SQL arguments:

```sh
dc exec postgres psql -U postgres -d postgres
```

At the `psql` prompt:

```sql
CREATE ROLE keycloak LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
\password keycloak
CREATE DATABASE keycloak OWNER keycloak;
\q
```

The `postgres` superuser is for installation/backup, not Keycloak's application
connection. Do not set `POSTGRES_HOST_AUTH_METHOD=trust`. PostgreSQL 18 stores
its version-specific data below `/var/lib/postgresql`; the Compose volume mounts
that parent directory. Replacing the image tag is not a PostgreSQL major-version
upgrade procedure.

## 4. Private native administration and realm bootstrap

Start Keycloak, Cerbos and Caddy before the browser proxy and gateway. This
allows the native bootstrap to complete before any user is admitted:

```sh
dc up -d keycloak cerbos caddy
dc ps
dc exec caddy caddy validate --config /etc/caddy/Caddyfile
```

Keycloak's healthcheck uses Bash `/dev/tcp` against private
`127.0.0.1:9000/health/ready`. The official image intentionally lacks curl.
Wait for `healthy`; a running process or PostgreSQL `pg_isready` alone does not
prove the realm or application is usable. First startup performs a Keycloak
build and can take longer than subsequent starts.

Copy only Caddy's public internal-CA certificate to the gateway's private PKI
directory. Preserve Caddy's `/data` volume: it also holds private keys and ACME
state. Do not copy its private CA key into the gateway or onto operator laptops.

```sh
dc exec -T caddy cat /data/caddy/pki/authorities/local/root.crt \
  > "$ACCESS_ROOT/private/pki/caddy-root.crt"
chmod 600 "$ACCESS_ROOT/private/pki/caddy-root.crt"
```

Create the realm and clients through the official Keycloak Python SDK:

```sh
dc run --rm --no-deps \
  -v "$ACCESS_ROOT/private/bootstrap.password:/bootstrap.password:ro" \
  -v "$ACCESS_ROOT/private/secrets:/bootstrap-secrets" \
  gateway python -m apps.workbench_access.provision \
  --server https://keycloak-admin:18080 \
  --private-admin-origin https://keycloak-admin:18080 \
  --workbench-origin https://workbench.example.com \
  --admin bootstrap-admin --password-file /bootstrap.password \
  --ca-file /etc/glamdb/pki/caddy-root.crt --output /bootstrap-secrets
```

Use your actual Workbench origin. `--private-admin-origin` sets the **master
realm** frontend URL to the private administrative origin and then obtains a
fresh master token. This is necessary for the native Admin Console to use the
private master login endpoints. The **glamdb realm issuer stays public** at
`https://login.example.com/realms/glamdb`. Never change that issuer to the
private name to work around a connectivity problem.

Bootstrap creates `glamdb-workbench`, public device client `glamdb-sdk`, and
separate confidential `glamdb-gateway` and `glamdb-control` clients. Access-token
audiences include the API, gateway and respective client. The native organization
mapper includes the organization UUID; unmanaged user attributes cannot supply
tenant identity. The control service account receives only its configured
native organization/user management roles plus `glamdb-operator`; it has no
database ownership grant. Client secrets, cookie key and CSRF key are new owned
mode0600 files. The command refuses existing realm/output secrets rather than
resetting a running realm. If bootstrap fails after partial native changes,
inspect that state through private native administration before a controlled
retry; do not delete an established realm to make bootstrap succeed.

### Open the native Admin Console over SSH

On the operator workstation, transfer `caddy-root.crt` securely and compare its
SHA-256 fingerprint with the value obtained on the host through the SSH session.
For CLI tools, pass it with `--cacert`/`--ca-file`. For a browser, explicitly
import/trust it in a dedicated operator browser profile or approved certificate
store after checking that fingerprint. No deployment command installs trust
into the operating system automatically. The certificate is not a public web
PKI certificate; `curl -k` and bypassing browser warnings are not a trust setup.

Map `keycloak-admin` to `127.0.0.1` in the workstation's chosen local name resolver
(for example an explicitly edited hosts entry), then keep this tunnel running:

```sh
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:18080:127.0.0.1:18080 operator@your-host
```

Open **https://keycloak-admin:18080/admin/master/console/** in that trusted
profile. Preserve the hostname and port: using `https://localhost:18080` fails
hostname/issuer matching. The public login hostname's `/admin` and master realm
must remain 404, including while this tunnel is open.

Create a permanent named administrator in the master realm, assign the native
master `admin` role, set its credentials, and configure MFA. Confirm a fresh
private-console login with that account before retiring the bootstrap account.
Remove both `KC_BOOTSTRAP_ADMIN_*` variables from the private env file, delete
the temporary bootstrap account in Keycloak and remove its password file from
the runtime host once the permanent credential is safely held. Recreate
Keycloak with the reviewed env file and wait for healthy again. Keep a separate
audited emergency access procedure; do not publish the master realm for recovery.

## 5. Email, passwords, sessions and MFA

In the **glamdb** realm, configure Realm settings -> Email with the actual SMTP
host, port, sender, encryption and authentication. Supply credentials through
the approved secret process and test delivery to an operator-controlled mailbox.
Permit outbound SMTP only to the intended relay. Set SPF/DKIM/DMARC and the
sender/domain policy with the mail operator. Monitor relay errors and limits.
Neither example hostnames nor a successful SMTP connection prove real delivery.

Self-registration is off. Realm bootstrap requires verified email and enables
password reset, brute-force protection, a password policy, short access tokens
and refresh-token rotation. It does **not** enforce MFA automatically. Configure
Keycloak's native OTP/WebAuthn policy, required actions and browser flow for the
desired member/operator requirements; test both enrollment and recovery. Do not
implement a separate password/reset UI in the Workbench.

Organization creation creates a managed native user with `UPDATE_PASSWORD` and
no usable password. In Users, set its intended email and verified status after
verifying the address, then either send the native update-password action email
or set a temporary credential through Keycloak's Credentials tab. With SMTP
unavailable, use the approved out-of-band initial-password process and require
an update; do not weaken email verification or add a password grant. Shared
organization credentials should use a mailbox/MFA recovery process controlled
by that organization, not a mailbox belonging to a departing individual.

Members use the Keycloak Account Console from the Workbench account link.
Native password reset, session review/logout, credential replacement and MFA
are handled there or by an administrator. When an individual is disabled in
Keycloak, that native status remains authoritative. Disabling an organization
closes catalog access, disables the native organization and logs out its
members; enabling it does **not** re-enable individually disabled accounts.

The browser uses Authorization Code + S256 PKCE with nonce checking. Session
cookies are Secure, HttpOnly, SameSite=Lax, scoped to `/`, without a Domain
attribute, and use the `__Host-` prefix. The separate gateway CSRF cookie is
readable by the UI to supply its CSRF header. Do not confuse that required CSRF
cookie with an exposed access token. Tokens and organization passwords must
never enter localStorage/sessionStorage, URLs, screenshots, HAR, traces or logs.

### Shared refresh state and ephemeral sessions

OAuth2 Proxy uses its native Redis session store with Valkey on a dedicated
internal `session` network. Only the proxy and store join that network; port 6379
has no host mapping. The browser carries a short session ticket. OAuth2 Proxy
encrypts the access/refresh/ID tokens in Valkey with the per-session ticket
secret, obtains the native Redis refresh lock and reloads the latest session
after obtaining it. Concurrent requests and already-issued stale-cookie
requests then share the refreshed state. Keep Keycloak refresh-token rotation
enabled with maximum reuse 0; switching back to cookie-only sessions would
reintroduce refresh races. See the upstream
[session-store contract](https://oauth2-proxy.github.io/oauth2-proxy/configuration/session_storage/).
The pinned proxy uses a two-second refresh-lock lease. Qualify token-endpoint
latency and concurrent refreshes under the intended load; the native regression
does not establish consistency through arbitrary upstream stalls or failovers.

This store is intentionally ephemeral. RDB snapshots and AOF are disabled,
`/data` is tmpfs, core/crash/slow/ACL logs are disabled, and the ACL does not
allow export, persistence, configuration, replication or monitor commands.
Logs hide user data. Never run `MONITOR`, export keys, take RDB/AOF dumps or
include session memory in support bundles/backups. Back up only the reviewed
configuration and static authentication files through the existing secret
manager. Session contents must not be restored from backups.

A store restart, replacement or loss invalidates its tickets and requires a
new Workbench sign-in. An existing native Keycloak SSO session may complete
that sign-in without another password prompt. Losing application sessions does
not revoke native Keycloak sessions or operator/SDK credentials; use the native
revocation procedures when that is required. Store failure denies browser
access; it does not enable cookie-only or Basic fallback. Bearer SDK requests
retain their separate signed-token/introspection path.

The example caps stored data at 64 MiB with `noeviction` and the container at 128 MiB.
Exhaustion rejects session writes instead of silently evicting arbitrary users.
Qualify these limits for the actual number and size of sessions. The store is
co-located on a private single-host bridge; a remote store requires an explicit
verified TLS design, not exposure of this private plaintext endpoint.

For planned static session-password rotation, stop `oauth2-proxy` and
`session-store`, replace the password, its ACL hash and `oauth2-proxy.env`
together through the protected secret process, retaining owner/mode0600, then
recreate both services. Do not put a replacement password in a command argument
or Redis URL. This discards current Workbench sessions; users sign in again.
Validate a fresh login and a due-time concurrent refresh after the change.

The gateway checks revocation through live native introspection for each
protected request. Access tokens are configured for 60 seconds; the gateway
rejects lifetimes above its configured maximum (90 seconds by default). That
is not a promise of instantaneous revocation on every upstream change: measure
actual native session/logout/client-disable behavior in the deployed release.
Dependency failure denies access. JWT key caching is not an authorization or
introspection-result cache. Data already downloaded cannot be recalled.

## 6. Initialize or migrate the ownership catalog

Choose exactly one of initialization and legacy conversion. Never point a new
binary at an absent catalog and expect it to create an empty replacement.

For a new deployment:

```sh
dc run --rm --no-deps gateway python -m apps.workbench_access.admin \
  --catalog /state/resources.sqlite init
```

For conversion, stop the old gateway and its writer first. Retain its consistent
encrypted offline backup. Put a private **copy** of the old mode0600 catalog in
`$ACCESS_ROOT/legacy/authorization.sqlite`, owned by the service account. Do not
use a hard link. Choose one namespace UUID, record it in the migration record
and use it for this migration attempt. The target catalog and mapping output
must not exist:

```sh
dc run --rm --no-deps -v "$ACCESS_ROOT/legacy:/legacy" \
  gateway python -m apps.workbench_access.admin \
  --catalog /state/resources.sqlite migrate \
  --legacy /legacy/authorization.sqlite \
  --namespace 10000000-0000-4000-8000-000000000001 \
  --mapping /state/migration.json
```

The source database is opened read-only, but its directory must permit creation
of the owner lease; that is why the private copy mount above is not `:ro`.
The converter checks the legacy application/schema identity and integrity,
derives stable new organization/database UUIDs within the namespace, preserves
ownership, aliases, sanitized audit and read-only receipts, and writes pending
organization activation commands. It never selects or imports legacy password
columns. The original database/backup can still contain old hashes and must
remain protected until its separately approved retention/deletion step.

Review `migration.json` privately. Before activation, ensure imported backend
origins, CA paths and server credential sources are valid inside the container.
Use explicit lifecycle updates where a legacy host path needs a new mounted
path. Legacy users need native credentials/reset, verified email and the chosen
MFA process. A pending organization has no data access; do not manually flip its
SQLite state. Apply each exact `activation_command` through the online CLI
after starting the new gateway. The workflow fills in the native organization
UUID and only then activates its mapping. An originally disabled organization
stays disabled. Confirm the old Basic/static routes and old listener are stopped
before admitting users to the new hostname.

## 7. Startup and readiness gates

After realm bootstrap and catalog initialization/conversion:

```sh
dc up -d session-store gateway oauth2-proxy
dc ps
dc exec gateway python -m apps.workbench_access.admin \
  --config /etc/glamdb/gateway.json --socket /state/operator.sock status
```

The CLI acquires a short control token through Authlib and calls the owner-only
Unix socket. It prints catalog metadata, so capture it only in a private
operator terminal. Successful authenticated status checks the identity and
policy path as well as catalog access; it does not prove any registered backend
can accept a write. A complete readiness decision also requires:

1. Public TLS chains and hostnames validate from a client outside the host.
2. Keycloak private health is ready, PostgreSQL and the authenticated session
   store are healthy, and Cerbos loads the reviewed policies without errors.
3. Unauthenticated `/workbench/session` and `/databases/...` return JSON 401
   without a login HTML redirect. Invalid bearer access returns a JSON error;
   an authenticated foreign database is concealed as 404.
4. Public `/operator`, `/metrics`, `/v0`, `/admin`, `/ping`, `/ready` and public
   identity `/admin` and `/realms/master` return 404. A forged identity header
   gives no access. No backend/PDP/IdP management port is reachable publicly.
5. A real member signs in, sees only owned databases, reads metadata and scans
   through the registered backend, then logs out. Verify reset/MFA and native
   session revocation using disposable accounts.
6. When telemetry components are installed, private gateway `/metrics` returns
   actual measurements. Readiness starts false and becomes true after an
   observed successful authenticated IdP/PDP path. A scrape alone must not turn
   it healthy; a 503 when the component is absent is deliberate.

Startup dependencies and healthchecks reduce initial connection errors; they
are not a restart supervisor for later cross-service failures. Container
`restart: unless-stopped` restarts a terminated process, not every unhealthy
container. Investigate IdP/PDP/database failures rather than configuring an
authentication bypass.

Inspect process logs locally with `dc logs --tail 100 SERVICE`, protecting any
captured output. Access request logging is disabled, including Caddy HTTP error
logs that can include OAuth callback queries. Keep TLS/startup diagnostics and
bounded runtime journals. The authoritative ownership audit is SQLite, not an
optional operational event or a log search.

If a crash leaves `/state/operator.sock`, first prove that the previous process
and all its containers are stopped. The launcher intentionally rejects an
existing socket. Remove only that verified stale socket, then restart. Never
delete the owner lease or edit a live catalog to bypass ownership; removing a
lock pathname can create a second independent lock and corrupt the safety model.

## 8. Routine lifecycle commands

All online operations use the private socket, a fresh control token, current
server-owned policy attributes and a JSON command file. Save command files
outside the UI, mode0600. A command contains a canonical lowercase UUID
`operation_id`, an action, params, and optionally `expected_revision`. Read
`status` first and set that revision when making an operator decision. Distinct
commands for the same resource can conflict while an earlier workflow is pending.

Example `$ACCESS_ROOT/state/create-organization.json` for a new catalog:

```json
{
  "operation_id": "20000000-0000-4000-8000-000000000001",
  "expected_revision": 0,
  "action": "organization.create",
  "params": {
    "id": "30000000-0000-4000-8000-000000000001",
    "name": "Analytics",
    "login": "analytics"
  }
}
```

```sh
dc exec gateway python -m apps.workbench_access.admin \
  --config /etc/glamdb/gateway.json --socket /state/operator.sock \
  apply --file /state/create-organization.json
```

The login accepts 1-64 ASCII letters, digits, `.`, `_` and `-`. Organization ID,
native organization ID and login are different identities. Empty organizations
are valid. Use Keycloak for their credentials; there are no GlamDB password
commands.

To register an existing private backend, first provision its distinct bearer
token through the secret manager into
`$ACCESS_ROOT/private/secrets/analytics-backend.secret`, mode0600 and owned by
the service UID. Mount its private CA in the PKI directory when needed. Put a
new operation ID and current revision around these action/params:

```json
{
  "operation_id": "20000000-0000-4000-8000-000000000002",
  "action": "database.create",
  "params": {
    "id": "40000000-0000-4000-8000-000000000001",
    "name": "Sales",
    "organization_id": "30000000-0000-4000-8000-000000000001",
    "url": "https://analytics-backend.internal.example.com",
    "token_file": "/run/secrets/glamdb/analytics-backend.secret",
    "ca_file": "/etc/glamdb/pki/backend-ca.crt"
  }
}
```

Backend URLs identify exactly one database. For the native managed server use
`https://host/databases/<canonical-lowercase-UUID>`; origin-only URLs remain
available for explicit mock adapters. Credentials, queries, fragments and other
path prefixes are rejected. Different mappings cannot alias the same canonical
endpoint. HTTPS verification is required; loopback HTTP is a deliberate local
topology only.
Keep backend ports private and authenticated so they cannot bypass organization
policy. Do not expose the backend's administrative APIs through a data path.

The supported action/params combinations are:

- `organization.create`: `id`, `name`, `login`; optional `enabled`.
- `organization.rename`: `id`, `name`.
- `organization.disable`, `organization.enable`, `organization.delete`: `id`.
- `database.create`: `id`, `name`, `url`; optional `organization_id`, `enabled`,
  and one of `token_file`/`token_env`, plus optional `ca_file`. A credential
  source is required even when a mapping is disabled or unassigned.
- `database.update`: `id` plus reviewed changes to name, backend endpoint, credential
  source, CA or `enabled`. Switching credential sources replaces the previous
  selection. A pure `{id, enabled:false}` is the emergency disable form.
- `database.assign`: `id`, `organization_id`, for an unassigned mapping.
- `database.detach`, `database.delete`: `id`, `expected_owner`; use JSON `null`
  when deliberately addressing an unowned mapping.
- `database.transfer`: `id`, `from`, `to`; optional new `name` to resolve an
  owner-local name collision.
- `database.reconcile`: `id`, using its own operation UUID; see the next section.

Organization deletion requires no database mapping still owned by that
organization. Transfer/detach those explicitly. Native managed users are
removed as part of deletion; unrelated native accounts are not silently
deleted. Mapping deletion affects routing/ownership metadata only.

Inspect receipts and audit without rerunning the action:

```sh
dc exec gateway python -m apps.workbench_access.admin \
  --config /etc/glamdb/gateway.json --socket /state/operator.sock \
  receipt 20000000-0000-4000-8000-000000000001
dc exec gateway python -m apps.workbench_access.admin \
  --config /etc/glamdb/gateway.json --socket /state/operator.sock audit
```

The same operation UUID and command content resumes/returns its receipt;
changed content under that UUID conflicts. A client timeout is not evidence
that nothing happened. Check the receipt, native state and admission state,
then retry the original file. Use a new UUID for a reviewed corrected command
after a terminal no-side-effect failure. An emergency disable supersedes older
pending grant workflows; replaying one cannot restore access. Do not discard
receipts/audit to make a retry succeed.

### Strict database transfer and uncertain writes

A transfer first durably closes admission, waits for admitted requests and
streams to finish, and rejects unresolved backend writes. It then changes
ownership, verifies source-deny/target policy behavior and resumes admission.
Catalog drain state and unresolved writes survive restart. Aborting a socket,
timing out or cancelling an Arrow stream is not an authoritative write outcome.

`DRAIN_PENDING` means admission remains closed while work is still active.
`BACKEND_UNCERTAIN` means a write outcome still needs authoritative resolution.
`POLICY_NOT_READY` can mean ownership changed but admission intentionally remains
closed. Keep the same transfer operation file; never manually set state active
or delete admission rows.

```sh
dc exec gateway python -m apps.workbench_access.admin \
  --config /etc/glamdb/gateway.json --socket /state/operator.sock \
  admissions 40000000-0000-4000-8000-000000000001
```

Apply a separate command with action `database.reconcile` and
`params: {"id":"40000000-0000-4000-8000-000000000001"}`. Reconciliation queries
each unresolved append's original backend operation identity. Only a matching
`committed` or `definitely_not_committed` response clears that admission. Missing
operations, malformed responses, network failures and an unknown outcome keep
it closed. Collection creation has no authoritative operation-lookup contract
here and remains unresolved; escalate to the backend's recovery procedure.
After authoritative reconciliation, retry the original transfer. Never label
an unknown write failed just to unblock an ownership move.

## 9. Browser and SDK access

Members open `https://workbench.example.com`. Caddy sends them to native
Keycloak login; the Workbench selector lists only their current organization's
databases. The Workbench retains its configurable pink theme and supplied local
logos. Account management remains the native Keycloak Account Console; it is
not a second GlamDB account administration UI.

Use `glamdb-sdk[oauth]` for human device authorization. The public SDK client
does not contain a secret and has device authorization enabled. Request
`openid profile email organization:*`, show only the intended user code and
verification URL to the user, and keep token acquisition/renewal in process
memory through the SDK's `DeviceAuthorizationClient`. See the
[SDK guide](../../sdk/python/README.md) for the complete API. Direct data URLs
use `/databases/<database-UUID>/...`; never send backend credentials to the
browser or substitute an unscoped backend URL.

For automation, create a **separate confidential client** in the glamdb realm
with service accounts enabled and direct/password/standard browser grants off.
Use the native client/audience mappers for `glamdb-api`, `glamdb-gateway` and
that client's own ID, including introspection claims. Assign `glamdb-member`
to its service-account user and add that native user to exactly one native
organization. Add the optional native organization client scope and request
`openid organization:*` when acquiring the token. **Remove the default email
client scope for this automation client.** A service account has no human
verified mailbox; issuing `email_verified:false` makes the strict native
OAuth2 Proxy bearer path reject it. Keep email verification enabled for the
Workbench and human SDK flows. Do not use an insecure email-verification flag,
a fake email, or a bearer bypass to work around this.

Use Keycloak's native membership API/console to add the service-account user.
A hardcoded organization claim mapper is insufficient: the gateway compares
the signed access token with live introspection and current native membership.
Do not give automation `glamdb-operator`, realm-management roles, the gateway's
introspection secret or the control client's secret.

An automation program can read its private client secret once and pass an
Authlib-backed token provider to the SDK:

```python
from pathlib import Path
from glamdb_sdk import Client
from glamdb_sdk.oauth import ClientCredentialsTokenProvider

with ClientCredentialsTokenProvider(
    'https://login.example.com/realms/glamdb/protocol/openid-connect/token',
    'analytics-automation',
    Path('/run/secrets/analytics-automation.secret').read_text().strip(),
    scope='openid organization:*',
) as tokens:
    with Client('https://workbench.example.com/databases/40000000-0000-4000-8000-000000000001',
                token_provider=tokens) as database:
        print(database.list_collections().items)
```

Use the current SDK constructor described in its guide when integrating other
options, and preserve TLS verification. Do not log the provider, token response
or exception's remote body. Create separate clients per automation workload so
rotation/revocation does not require sharing one credential across tenants.

## 10. Resource limits, retention and operations

The gateway defaults to 32 admitted requests, a 4 MiB request-body limit, a
30-second backend timeout and a 30-second drain timeout. Settings bound admitted
work; deployment memory/PID limits bound the process. Increase them only after
measuring representative request/Arrow sizes, concurrent streams, CPU/RAM and
backend behavior. A long stream can delay transfer; an uncertain write can
block it beyond the drain timeout by design.

The catalog limits organizations and database mappings to 128 each. Native
organization membership administration is bounded at 128 members; finish larger
native membership changes deliberately rather than treating a partial result as
success. Pending lifecycle workflows are bounded at 256 and unresolved write
admissions at 4096. Repeated backend remapping can exhaust the gateway's bounded
client cache and require a reviewed restart. Audit and operation receipts are
permanent records; there is no automatic delete/TTL policy for them. Back up
and capacity-plan their growth rather than pruning them with ad hoc SQL.

The Compose json-file journal limit is 10 MiB x 3 files per service. Size limits
are per container and do not cap PostgreSQL, Caddy data or the access catalog.
Set disk alerts and hosting quotas for those separately. Keep Keycloak native
event/audit retention appropriate for your incident and privacy policy. Passwords,
access/refresh tokens and raw callback URLs are not acceptable telemetry.

Runtime log delivery uses a bounded 1 MiB nonblocking buffer. Under sustained
backpressure, the runtime can discard events; it must not hold a database
request indefinitely waiting for a collector. Monitor producer/runtime/collector
loss and delivery errors. This is not an audit-delivery guarantee.

For the assembled release, deploy the reviewed logging, monitoring and alerting
components as independent services attached to `glamdb-observability`. Configure
their actual retained volume paths, size/time retention and secret files. The
gateway uses optional standard stderr logging and Prometheus-client adapters
when those components are present. Runtime journals own rotation; no separate
GlamDB file rotation owner is required.

### Connect access-container stderr to Alloy and Loki

The access Compose uses Docker `json-file`. The logging Compose defaults to a
journal source, so apply the access-specific Docker source override below.
It replaces the journal/machine-ID mounts with the **current gateway container's
directory only**, mounted read-only, and drops the unused journal supplementary
group. It grants no Docker socket and does not mount all containers' logs.
The native source decodes the Docker envelope and drops stdout before the
shared finite JSON projection, preserving the machine-readable CLI stdout.

These commands require an assembled checkout containing
`deploy/logging/compose.yaml`, its `config.alloy` and
`sources/docker-json.alloy`, plus Docker Compose >=2.24.4 (`!override` support;
Compose 2.40.3 was used for configuration validation). Run after the gateway is
started, as the host runtime administrator. Use a new dedicated observability
directory and its quota/retention policy:

```sh
OBS_ROOT=/var/lib/glamdb/observability
sudo install -d -m 700 -o 10001 -g 10001 "$OBS_ROOT/loki"
sudo install -d -m 700 -o root -g root "$OBS_ROOT/alloy"
export GLAMDB_LOKI_DATA_DIR="$OBS_ROOT/loki"
export GLAMDB_ALLOY_STATE_DIR="$OBS_ROOT/alloy"
export GLAMDB_ALLOY_UID=0 GLAMDB_ALLOY_GID=0
export GLAMDB_ENVIRONMENT=production
# Required during base-file interpolation, then removed by the override.
export GLAMDB_JOURNAL_GID=0 GLAMDB_JOURNAL_MATCHES=unused-docker-source
GATEWAY_CONTAINER_ID=$(dc ps -q gateway)
test -n "$GATEWAY_CONTAINER_ID" || exit 1
GLAMDB_GATEWAY_LOG_FILE=$(docker inspect --format '{{.LogPath}}' "$GATEWAY_CONTAINER_ID")
test -f "$GLAMDB_GATEWAY_LOG_FILE" || exit 1
test "$(basename "$GLAMDB_GATEWAY_LOG_FILE")" = "$GATEWAY_CONTAINER_ID-json.log" || exit 1
export GLAMDB_GATEWAY_LOG_DIRECTORY="$(dirname "$GLAMDB_GATEWAY_LOG_FILE")"
docker compose -f deploy/logging/compose.yaml \
  -f deploy/access/logging.docker.override.yaml config --quiet
docker compose -f deploy/logging/compose.yaml \
  -f deploy/access/logging.docker.override.yaml up -d
```

The example collector runs as root because rootful Docker log directories are
normally owner-only. Its capabilities remain dropped, filesystem read-only,
and the sole application-directory mount read-only. A deliberately provisioned
read ACL and collector UID can replace this if verified against the actual
runtime, including rotated files. Do not make every container directory public
to avoid configuring ownership. The Loki data directory remains owned by its
native UID10001, independently of the gateway UID.

Save the nonsecret environment settings in a protected operator env file and use
it consistently. **Gateway recreation changes its container ID/log directory**:
repeat the ID/path discovery, update that saved setting, and recreate Alloy with
the same two Compose files. An ordinary restart retaining the container ID does
not change the mount. Runtime log rotation stays inside the mounted directory;
Alloy state persists separately. Do not bind only one log file inode or add a
second logrotate owner.

Generate a real authenticated gateway request or lifecycle action, then confirm
its known request/event ID appears in Loki through Grafana Explore. Check Alloy
targets/delivery counters and Loki readiness from the private network; a running
collector alone does not prove collection. If the image lacks the optional
logging adapter, rebuild the assembled image first: no operational JSON events
are invented by the access-only image. Add other application containers through
equally explicit source mounts; do not accidentally collect unrelated workloads.

For monitoring and alerting, after their own private Grafana password,
Prometheus target/override and receiver setup, their versioned entry points are:

```sh
docker compose --env-file "$OBS_ROOT/monitoring.env" \
  -f deploy/monitoring/compose.yml up -d
docker compose --env-file "$OBS_ROOT/alerting.env" \
  -f deploy/alerting/compose.yml up -d
```

The env files are operator-created and private. Add reviewed host exporters and
the Prometheus/Alertmanager wiring according to those component guides. Merely
starting services does not establish expected target coverage or alert delivery.
These manifests belong to the assembled release; an access-only checkout can
operate independently with telemetry explicitly unavailable.

Grafana is initially **operator only**. Its default example listener is host
`127.0.0.1:3000`; Alertmanager is `127.0.0.1:9093` and Prometheus is
`127.0.0.1:9090`. Access those through SSH loopback forwarding. Do not publish
Loki, Alloy, Prometheus, exporters or Alertmanager as an alternative public
entry point. Use either the monitoring Grafana instance or the optional logging
one, not both on the same port. Grafana folders and the Viewer role do not
isolate other organizations' underlying metrics/log queries. If tenants later
need telemetry access, design real data-source/tenant isolation first.

For the default private listeners, keep this operator tunnel running:

```sh
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:3000:127.0.0.1:3000 \
  -L 127.0.0.1:9090:127.0.0.1:9090 \
  -L 127.0.0.1:9093:127.0.0.1:9093 operator@your-host
```

Open `http://127.0.0.1:3000` for Grafana, `http://127.0.0.1:9090` for operator
Prometheus queries and `http://127.0.0.1:9093` for Alertmanager alerts/silences.
HTTP stays on the loopback endpoints carried inside the SSH tunnel. Keep each
service's configured root/external URL consistent with the address being used,
especially for SSO. Do not publish the unauthenticated Alertmanager UI/API.

Optional Grafana SSO uses a separate native Keycloak confidential client, exact
private Grafana root/callback URL and reviewed operator-role mapping. Keep it
off the member Workbench client. Prove anonymous access is disabled, operator
revocation works and users cannot self-select an administrator role. Retain
an audited emergency Grafana account according to local policy.

The four monitoring states are exclusive OK/WARN/ALARM/NODATA. Missing or stale
measurements are not recovery. The expected target inventory, scrape/evaluation
errors, exporter health, alert evaluation and delivery errors require their own
checks. Example CPU/disk/error thresholds are configurable starting values,
not an SLO. Follow the observability components' source-backed rules and
dashboards rather than guessing metric names in a new gateway-specific generator.

Run the independent observer on a **different host/fault domain**, not another
container on the same server. Arrange a private authenticated route to the
intended pipeline health endpoints and maintain an explicit expected inventory.
Test host loss, stopped Prometheus/Alertmanager and a failed observer scrape.
An observer that cannot reach the pipeline must report unavailable/NODATA,
not OK. Keep its notification path independent enough to detect a failed main
pipeline. The main host cannot report its own total outage by itself.

Alertmanager's native Telegram receiver needs an explicitly assigned bot token
and chat ID in private files plus outbound HTTPS. The supplied configuration
does not discover a chat or authorize sending to one. Validate routing with a
local test peer before assigning a real destination. Native delivery provides
current observed state, timestamps, retries/repeats and delivery-error metrics;
it does not guarantee delivery of every intermediate transition across arbitrary
restarts. Do not recreate a custom outbox to imply that guarantee exists.

## 11. Consistent backup and restore

Valkey holds ephemeral browser session tickets on a dedicated private network;
it uses neither RDB nor AOF persistence. Exclude it from backup/restore. Losing
the store requires fresh sign-in; restoring old tickets must not revive revoked
sessions. Keep its authentication material in the secret manager independently
of PostgreSQL/catalog backups, and recreate an empty session store during restore.

Back up at least PostgreSQL's Keycloak database and role information, the access
catalog including receipts/audit/admissions, Caddy certificate/CA state, reviewed
configuration/policy revisions, private secret-manager references and required
client/backend credentials. Database stores require their own native consistent
backup procedure. The access catalog contains no user rows and is not a backup
of a store.

A realm JSON export is useful for inspection or selected configuration transfer.
It is **not** the PostgreSQL backup: native exports have consistency and coverage
limits, including sessions, events, workflow state and revoked-token state. Stop
all Keycloak nodes before an export requiring a consistent view, and follow the
[native export limitations](https://www.keycloak.org/server/importExport).

For a simple coordinated maintenance backup:

1. Close public admission with the reviewed maintenance/firewall procedure;
   retain SSH/private administrative access. Tell users when sessions/streams
   will end. Stop OAuth2 Proxy and the gateway and verify their owning processes
   have exited. Retain unresolved admissions; shutdown is not write resolution.
2. Stop Keycloak after allowing native administrative writes to finish. Keep
   PostgreSQL running for its native dump. No other node may mutate this realm.
3. Run offline catalog backup and native PostgreSQL dumps into new private
   paths. Use a unique timestamp/operator backup ID rather than overwriting.
4. Snapshot Caddy data/config and retained configuration/secret references with
   the approved encrypted backup facility. Record digests, schema/revision,
   image indexes, native DB version and backup cut time in a private manifest.
5. Restart through readiness gates before opening public admission. Send
   encrypted copies off-host, verify checksums and perform a restore rehearsal.

Example backup commands after selecting a new backup directory:

```sh
umask 077
dc stop oauth2-proxy gateway
dc stop keycloak
dc run --rm --no-deps gateway python -m apps.workbench_access.admin \
  --catalog /state/resources.sqlite backup \
  --output /state/backups/access-maintenance-001.sqlite
dc exec -T postgres pg_dump -U postgres -d keycloak --format=custom \
  > "$ACCESS_ROOT/state/backups/keycloak-maintenance-001.dump"
dc exec -T postgres pg_dumpall -U postgres --globals-only \
  > "$ACCESS_ROOT/state/backups/roles-maintenance-001.sql"
dc up -d keycloak cerbos caddy gateway oauth2-proxy
```

The catalog backup acquires the owner lease, creates a new mode0600 file through
SQLite backup, checks it and fsyncs the file and parent directory. Do not use a
plain copy of a live SQLite file. `pg_dumpall --globals-only` contains password
verifiers and administrative role definitions: protect it like a secret and
review it before restoring roles on another server.

### Restore without restoring revoked access

Restore into a new isolated project/namespace, new volumes and new catalog path;
keep its public ingress closed. Do not replace a live catalog in place, reuse
a running owner lock, or point a newer Keycloak version at the only old backup.
Use the recorded image/config/policy versions first. Recreate the dedicated
Keycloak role/database with current approved credentials, then restore the
custom dump using native PostgreSQL tools. For an empty `keycloak` database
owned by the recreated role, one reviewed command is:

```sh
dc exec -T postgres pg_restore -U postgres -d keycloak --no-owner \
  --role=keycloak --exit-on-error --single-transaction \
  < "$ACCESS_ROOT/state/backups/keycloak-maintenance-001.dump"
```

This command is for the isolated restore target selected by `dc`, not the live
service. Restore the catalog backup as a new mode0600 single-link file owned by
the runtime UID. Inspect its identity/schema/integrity and unresolved admissions
before starting the gateway. Restore server stores independently and preserve
their authoritative operation outcomes so uncertain admissions can be reconciled.

**A backup can predate revocations and transfers.** Before reconnecting users,
compare the restored state with the latest retained security change/audit record
and native identity state. Reapply account/client disables, organization disables,
ownership moves and backend revocations newer than the backup. Revoke restored
sessions/refresh grants and rotate affected client/cookie/backend credentials.
If that reconciliation cannot be established, keep the affected resources
disabled and public admission closed. An old catalog plus old IdP snapshot must
not silently regrant a former owner's database access. An application rollback
does not authorize rolling back identity/authorization state.

Test that known disabled users remain denied, transferred databases reject the
old owner, pending/unknown writes keep transfer closed, private admin remains
private and a fresh current member works. Only then complete the readiness gate
and reopen. Record the measured restore time and data/security cut, rather than
claiming an untested RTO/RPO.

## 12. Rotation, upgrades and rollback

Rotate one trust relationship at a time and test fresh acquisition plus old
credential denial. Store the prior secret only for the bounded planned rollback
window; never reactivate a credential revoked for compromise.

- **Workbench client secret:** use native Keycloak credentials/rotation for
  `glamdb-workbench`, replace its owned mode0600 file atomically and recreate
  OAuth2 Proxy. Test a new login and refresh. Follow the native overlap policy
  if configured; do not assume two secrets are valid by default.
- **Gateway/control secrets:** rotate each native client separately, replace
  the corresponding file, recreate the gateway, and test data introspection and
  authenticated CLI status. Do not keep running an instance holding an old
  in-memory secret after declaring rotation complete.
- **Cookie key:** replace with 32 random binary bytes using the same private
  file discipline as bootstrap, then recreate OAuth2 Proxy. Existing sessions
  require login again. A CSRF key change similarly needs a gateway restart and
  fresh browser state. Never weaken cookie flags during that transition.
- **Backend bearer credential/CA:** update the actual server credential first
  according to its supported overlap/revocation procedure, then change the
  private token/CA file and apply a reviewed `database.update` so admission is
  drained and mapping/version changes are explicit. Restart if necessary to
  discard existing clients. Check old bearer rejection at the private backend.
- **Keycloak signing keys:** use native key-provider/key rotation. Keep retired
  verification keys for the intended lifetime of noncompromised issued tokens,
  test JWKS rollover and do not extend the gateway's token lifetime to mask an
  unexpected failure. Handle a compromised key with the incident procedure.
- **PostgreSQL role password:** use native `\password`, update `KC_DB_PASSWORD`
  in the private env file and recreate Keycloak after the planned maintenance
  cut. Changing the file alone does not change a database role's password.
- **Private CA:** establish the new Caddy/private PKI and explicit trust on
  gateway/operators in a controlled overlap, verify hostname/fingerprints, then
  retire old trust. Replacing only the gateway CA file or deleting Caddy data
  breaks backchannel access and does not constitute rotation.

Secret-file replacement must preserve owner/mode, avoid symlinks/hardlinks and
use a new temporary file plus fsync/rename in the same protected directory.
Use the approved secret manager/host tooling; do not put values in CLI options.
Protect file backups and editor swap files as well as the final pathname.

For an upgrade, read the exact upstream release notes and supported database
matrix; verify the new index digest and Python hashes from official sources.
Update one reviewed component at a time, rehearse with a private restored copy,
and run identity/browser/lifecycle/transfer/outage tests with real services.
Check PostgreSQL and Keycloak migration/rollback restrictions before the first
new binary touches persistent state. Preserve compatible pre-upgrade backups
and images. A database schema migration can make a simple image downgrade
unsafe. Never point an older binary at newly migrated state without explicit
upstream compatibility evidence.

Prefer rolling back application/configuration while retaining current
revocations, ownership, receipts and native identity state. If a native database
restore is necessary, perform the security reconciliation above. A broken SSO
deployment should fail closed; a rollback must not revive the removed Basic
gateway or switch production traffic to a mock adapter.

## 13. Kubernetes deployment variant

Kubernetes is a separate deployment design, not `docker compose` translated
mechanically. Use the same reviewed image indexes, native realm/client policy,
private administration and current storage/HTTP limitations. This repository's
single-host Compose proof does not certify an untested cluster configuration.

Use a dedicated namespace and default-deny ingress/egress NetworkPolicies.
Create one public TLS edge for Workbench and the permitted glamdb identity
paths. Keycloak HTTP/management, PostgreSQL, Valkey, PDP, gateway, operator API and
metrics use ClusterIP/private networks only. A Service named `keycloak-admin`
can select Caddy's port18080 for the existing private origin; it has no public
Ingress/LoadBalancer. Operator access is an explicitly bound loopback
`kubectl port-forward service/keycloak-admin 18080:18080` plus the same checked
CA/name setup. Do not publish master endpoints through the login Ingress.

Separate public and private edge listeners and retain Caddy's header stripping.
Restrict Keycloak ingress to the chosen edge and configure its trusted proxy
addresses for the actual cluster topology. Compose's fixed subnet addresses
are not valid pod addresses. Where pod addresses change, combine the smallest
practical trusted proxy CIDR with enforced NetworkPolicy and a controlled edge
workload; a broad CIDR by itself permits forged proxy headers from other pods.

Allow only the required flows: edge -> OAuth2 Proxy/Keycloak; OAuth2 Proxy ->
gateway, its private Valkey session service and public issuer through the edge;
gateway -> private Keycloak/Cerbos/
registered backends; Keycloak -> PostgreSQL/approved SMTP; scrapers -> intended
private metrics. Add DNS, certificate-renewal and explicitly approved telemetry
egress. Keep the independent observer outside the cluster's failure domain.

Run one gateway replica with `Recreate` rollout and an exclusive persistent
volume supporting the required local SQLite locking/fsync semantics. Do not
share its catalog across replicas or assume an RWX network filesystem has the
same lock/durability behavior as local storage. Keep `/state/operator.sock` on
that gateway's private state; use `kubectl exec` for the authenticated CLI,
not a public operator service. PostgreSQL needs its own supported persistent
storage/backup or an intentionally selected managed PostgreSQL service; Keycloak
uses the dedicated role and verified supported connection configuration.

Set runtime `runAsUser`/`runAsGroup` to the catalog/secret owner, drop capabilities,
use `readOnlyRootFilesystem` where the component supports it, set requests/limits
and a sufficient termination grace period, and mount writable tmp/state only
where needed. Keycloak image build/runtime requirements differ from the read-only
Python adapter; build an optimized native image in CI if choosing that pattern.
Use native health endpoints for probes and an authenticated synthetic check for
end-to-end access readiness. Do not give a liveness probe the power to bypass
an identity outage or delete durable uncertainty state.

Run the pinned Valkey session service privately, with the same authenticated
ACL, no RDB/AOF, no eviction, memory/client limits and dump restrictions as the
single-host configuration. Its writable directory can be a bounded memory-backed
emptyDir; do not attach a persistent data volume or restore old browser tickets.
Allow only OAuth2 Proxy and an authorized health probe to reach it. A lost store
requires fresh sign-in. Supply its ACL/password and the OAuth2 Proxy password
environment through reviewed Secret projections; apply the actual container UID
and namespace mapping rather than assuming the image can read root-owned files.

Kubernetes Secret projections normally belong to root. `defaultMode: 0600`
and `fsGroup` alone do **not** satisfy the gateway's current-UID owner check.
Use a reviewed CSI driver with explicit file owner support, or a narrowly scoped
init container that copies projected secrets into a private memory-backed
emptyDir and sets owner UID/GID and mode0600 before the nonroot gateway starts.
Mount that prepared directory read-only in the main container. Initial catalog
and PVC directory ownership need the same deliberate provisioning. Plan how
rotation recopies/restarts consumers; projected secret updates alone do not
refresh a private copy or an in-memory client secret. Keep root-init privileges
separate from the application container and do not print values.

Use a separately controlled Job for one-time native realm bootstrap, with the
official Python SDK, explicit private CA and a temporary bootstrap credential.
Do not run it on every application pod start. Gate its execution on the intended
empty/disposable identity target and retain no populated Secret YAML in Git.
Perform the same private-master/public-glamdb issuer checks, SMTP/MFA setup,
catalog migration, policy checks and security-aware restore rehearsal as on
the single host.

## 14. Verification and boundaries

For local gateway/SDK behavior, run the current focused suites rather than a
historical test count:

```sh
.venv/bin/python -m pytest tests/access sdk/python/tests -q
git diff --check
```

Real identity tests require an explicitly provisioned disposable loopback fixture:

```sh
GLAMDB_IDENTITY_FIXTURE=/private/test-fixture/fixture.json \
  .venv/bin/python -m pytest tests/access/test_real_identity.py -q
```

That fixture uses real native identity/PDP/proxy components, but may deliberately
register disposable mock databases for authorization tests. Such a result must
be labelled accordingly. Do not reuse production accounts or point the suite
at a guessed host.

The browser refresh and ephemeral-store restart cases are in
`tests/access/test_real_refresh.py`. They require the native Redis session
deployment, the same explicit fixture and browser installation. The restart
case additionally requires `GLAMDB_IDENTITY_SESSION_STORE_CONNECTION` and
`GLAMDB_IDENTITY_SESSION_STORE_CONTAINER` naming the inspected disposable Podman
resources. Outage cases similarly need explicit dependency settings. Never point
those settings at an unknown or production container. When forwarding PDP only
on `127.0.0.1`, use that exact IPv4 address in the fixture; `localhost` can also
resolve to an unbound IPv6 listener.

The system harness requires a private mode0600 configuration with
`version:1`, `disposable:true`, `base_url`, optional `ca_file`, and exactly one
of `token_file` or an `oauth` object containing `token_url`, `client_id`,
`client_secret_file`, `scope`. Paths are relative to that config. It creates a
fresh collection on the explicitly designated disposable backend:

```sh
.venv/bin/python scripts/check-system.py --config /private/system-check.json
```

Its default is the **native** backend. An explicit `--backend mock` is a separate
development check and cannot prove native integration. Run the default proof
against a configured native database endpoint and retain any unsupported-feature
result; do not relabel a mock result to make this gate green. Native engine/
storage/scanner validation remains distinct from the network and access checks.

The harness also checks the server's advertised capability profile. An explicit
`false` for a required metadata/profile feature (`stats`, `snapshots`, `files`,
`explain`, `pagination`, `zero_column_projection`) does not produce a passing
qualification. It runs the supported create/resolve/collection/schema/append,
idempotent lookup, fixed-view JSON/filter and latest/historical Arrow checks,
and verifies that each missing feature actually returns HTTP501 with typed
`UNSUPPORTED_OPERATION`. It then reports `status:"blocked"`,
`code:"UNSUPPORTED_CAPABILITIES"`, the exact `missing_capabilities` list and
completed `checks`, and exits **2**. `get_collection` is visible among those
completed checks. Do not translate that partial evidence into full qualification.

A claimed-supported feature that fails, a missing feature returning success or
a different error/status, and malformed/nonboolean feature flags are failures
(exit **1**). Boolean-looking strings or numbers are not feature flags. Servers
without the legacy `features` object retain the full existing route checks;
absence is not treated as a declaration that metadata is unavailable. A complete
successful profile exits **0**. Default native mode rejects a mock identity
before creating a collection, regardless of its feature flags.

Native terminal-failure envelopes are distinct from HTTP/transport errors.
A recognized synchronous failed-create receipt can clear its own admission
using the original validated collection UUID, even when the native receipt
has no collection-ID field. Append reconciliation accepts a matching original
operation UUID and the exact terminal-failure envelope at its native error
status (for example 412/409), as well as committed results. Missing/mismatched
identity, unknown outcomes, unrecognized error codes, incorrect status/phase/
retry hints and bare HTTP errors remain unresolved. This does not add an
authoritative create lookup or permit guessing the outcome of a lost create
response; that uncertainty still keeps transfer closed.

Before public activation, record executed commands and environment, real-service
versus mock boundaries, browser login/logout/MFA/reset/revoke behavior, foreign
UUID/encoded-path/header denial, dependency outages, strict-transfer uncertainty,
backup/restore reconciliation and measured resource use. Report any unperformed
DNS/TLS/SMTP/notification or native transport step plainly. No successful smoke
test alone qualifies engine durability or an untested production deployment.

Useful upstream references are the [Keycloak hostname guide](https://www.keycloak.org/server/hostname),
[reverse-proxy guide](https://www.keycloak.org/server/reverseproxy),
[Keycloak administration](https://www.keycloak.org/docs/26.8.2/server_admin/index.html),
[OAuth2 Proxy Keycloak OIDC configuration](https://oauth2-proxy.github.io/oauth2-proxy/configuration/providers/keycloak_oidc/),
[Cerbos configuration](https://docs.cerbos.dev/cerbos/latest/configuration/),
and [Caddy automatic HTTPS](https://caddyserver.com/docs/automatic-https).
For changes, consult documentation/source for the pinned release: a current
documentation page can describe a later feature.
