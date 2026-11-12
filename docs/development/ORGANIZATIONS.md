# Organizations and database ownership

Keycloak owns users, organization membership, passwords, MFA and sessions.
OAuth2 Proxy handles browser OIDC login; Cerbos evaluates access policies.
The private GlamDB gateway adapts those identities to configured database HTTP
servers. It never starts a mock automatically or implements a storage writer.
See [the operating guide](SSO_OPERATIONS.md) for the complete host, TLS, SMTP,
credentials, deployment, backup and recovery procedures.

## Identity and data boundaries

- Each organization and database has a canonical UUID. Organization identity is
  independent of its login. The catalog maps its UUID to the native Keycloak
  organization UUID; changing a login does not move any database.
- Initially each organization has one managed shared account. Additional native
  personal or service accounts can join the same organization. The initial
  policy accepts exactly one organization per member principal.
- Native Keycloak Admin/Account Console handles passwords, email verification,
  account enablement, MFA, sessions and recovery. GlamDB has no password endpoint,
  password hashes, authentication database or refresh service.
- SQLite stores exclusive ownership, configuration references, transactional
  audit, lifecycle receipts and unresolved write admissions. Credentials remain
  separate owned files. Backend origins are canonicalized and unique: two
  database UUIDs cannot alias the same configured origin.
- All database lists and direct URLs use the same Cerbos policy with current
  server-owned attributes. Members can read/write only their active organization's
  active databases. Control principals manage ownership; that role alone grants
  no row access. Unknown and foreign databases both return 404.

Creating an organization provisions its native account and membership, then
activates the catalog mapping. Its operator still initializes password/email
through Keycloak. A disabled native user remains disabled when an organization
is re-enabled. Organization disable closes local data access before native
organization/session changes; account status remains Keycloak-authoritative.
Deletion requires owned databases to be detached or transferred first. Native
personal/service accounts are retained; only the organization's marked managed
shared account is deleted with it.

## Request authentication

Browser requests pass through Caddy and OAuth2 Proxy. Authorization Code with
PKCE and nonce verification creates a Secure, HttpOnly, SameSite=Lax session
cookie. Access/refresh tokens never enter browser storage or product URLs.
Cookie mutations use `starlette-csrf`, a separate readable CSRF cookie and an
exact Origin check. Bearer requests do not depend on browser CSRF cookies.
Mixed session-cookie/Bearer credentials, conflicting forwarded tokens and
unrecognized authentication schemes are rejected.

The gateway verifies RS256 signatures with joserfc, exact issuer/audience,
expiry/issued-at bounds, access-token type, canonical subject and native
membership. Authlib performs live introspection for **every** admitted API
request. Current introspection roles and organization must still match the
signed grant. Cerbos decisions are not cached. A bounded public-key cache is
separate from grant decisions. IdP/PDP failures deny access and make dependency
readiness false; no alternate password or mock path is enabled.

The supplied realm uses 60-second access tokens; the gateway's default maximum
accepted lifetime is 90 seconds including configuration tolerance. JWT time
validation allows five seconds of clock leeway, while live native introspection
also checks expiry/revocation. This is a contract for **new admissions**, not a
promise to retract already downloaded data. Requests admitted before revocation
may finish. The UI polls identity/allowlists every two seconds and clears rows,
dialogs and controllers on identity changes or authentication/service failures.
It cannot erase previously exported data.

SDK clients use direct Bearer tokens or an explicit callable token provider.
Authlib helpers support client credentials and human Device Authorization.
Mutating calls are sent once; a 401 does not automatically replay a write.
Machine accounts use native membership and omit the irrelevant `email` client
scope; browser email verification remains strict. API authentication failures
are JSON without HTML login pages or redirects.

## Explicit route actions

Database requests retain `/databases/<database-UUID>` throughout JSON and Arrow
calls. The following suffixes are the current v0 HTTP adapter contract:

- Metadata: GET `/capabilities`, `/collections`,
  `/collections:resolve`, `/collections/<UUID>`, and its `/schema`, `/stats`,
  `/snapshots`, `/files`, `/files/<opaque-ID>` and `/operations/<UUID>` suffixes.
- Metadata: POST `/collections/<UUID>/explain`.
- Read: POST `/collections/<UUID>/scan` and its `/open` and `/cancel` suffixes.
  Arrow is selected by `Accept` on the scan route. Snapshot, checksum, length
  and continuation response headers are preserved.
- Write: POST `/collections`, `/collections/<UUID>/append` and
  `/collections/<UUID>/write-settings` (version-checked Parquet compression settings).

Unknown methods/routes are denied. Encoded database paths are rejected before
OAuth proxy normalization; percent-encoded query values remain supported.
Client identity headers are stripped at ingress. Only safe content negotiation
headers and the server-side database credential reach the backend. Browser
cookies and the incoming OIDC token never become backend credentials.

## Administrative receipts and partial effects

Use the mode0600 operator Unix socket and an authenticated `glamdb-operator`
principal. Caddy blocks the operator HTTP route. The local CLI offers `status`,
`audit`, `receipt`, `admissions` and `apply --file`; account administration remains
native. Every mutation supplies an operation UUID, action, parameters and
optionally the expected catalog revision. Exact same-content retries reuse the
receipt. Reusing an operation UUID with different content conflicts.

Cross-service operations persist intent and an external-call marker before
calling Keycloak. Native timeout/partial effects remain pending rather than
being reported as rolled back. Catalog changes use deterministic child receipts.
An unfinished workflow fences conflicting changes to that resource. Emergency
disable may supersede a pending grant; replaying the older operation cannot
re-enable it. Side-effect-free validation failures do not poison a resource
forever. The operating guide explains receipt inspection and safe retries.

## Strict database transfer

1. Persistently close admission for the database and increment its version.
2. Wait for all admitted requests and response streams. A stale policy decision
   cannot enter after this fence because admission rechecks current versions.
3. Require terminal backend outcomes for all admitted writes. Disconnection,
   timeout, process restart, cancellation or operation-lookup 404 is not proof
   that a write was cancelled.
4. Change owner and append audit in the same catalog transaction. The database
   remains draining while policies are checked against the new ownership.
5. Verify the old principal is denied and the new principal has the expected
   access, then reopen admission. Assignment of an unowned database also starts
   closed until these checks succeed. Disabled resources remain denied.

A drain timeout or unresolved write leaves a pending receipt and closed
admission. An uncertain append can be reconciled only using a matching terminal
backend operation outcome. A matching synchronous v0 create response is terminal;
wire schema defaults are compared conservatively. An interrupted create lacks an
authoritative lookup contract in the current adapter, so its unresolved record
keeps strict transfer blocked. No operator command simply marks it cancelled.
Do not edit or clear live admission records to bypass this rule.

## Migration, monitoring and validation

The offline migration reads only explicit non-password columns from a legacy
catalog. A supplied namespace produces stable organization/database UUIDs and
an explicit alias map. Ownership/audit/read-only receipts are retained; old
credential-dependent content hashes are not imported. Organizations remain
disabled and pending until native provisioning completes, including originally
disabled organizations. Original catalog backups stay offline and are never a
runtime authentication fallback.

Logs use the optional standard stderr adapter; runtime journals own rotation.
Transactional audit remains authoritative. Optional Prometheus client metrics
expose bounded route/method/status/stage series on a private endpoint; missing
components return unavailable rather than fabricated healthy values. Workbench
is the database UI, Keycloak the account UI, and private Grafana/Alertmanager the
operator interfaces. Grafana folders do not provide tenant data isolation.

The native HTTP server and embedded scanner are implemented. A native backend
URL includes its canonical `/databases/<UUID>` scope; an origin-only URL remains
available for the explicit mock adapter. The gateway keeps that scope on every
metadata, scan and write request. Test identity/policy doubles do not qualify a
production identity deployment or storage durability.
See [validation](ORGANIZATIONS_VALIDATION.md) for exact commands and boundaries.
