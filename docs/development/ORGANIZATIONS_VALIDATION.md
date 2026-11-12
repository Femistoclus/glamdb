# Organization access validation

The checks below run on macOS arm64 with Python 3.13, Chromium 1194,
Playwright 1.56.1, Caddy 2.11.9, Keycloak 26.8.2, OAuth2 Proxy 7.15.6,
Cerbos 0.57.0, PostgreSQL 18.6 and Valkey 9.1.3. Public integration requests use real HTTPS
with a disposable certificate. Identity/policy services are actual upstream
processes, while the three database HTTP adapters are explicitly disposable
in-memory mocks. No production password, warehouse or Telegram destination is
used. Product launchers never start a mock automatically.

## Repeatable commands

```sh
bash scripts/bootstrap-workbench.sh
bash scripts/check-workbench.sh
.venv/bin/python -m pytest tests/access -ra

# Explicit provisioned disposable identity infrastructure, not target discovery:
GLAMDB_IDENTITY_FIXTURE=/private/fixture.json \
  .venv/bin/python -m pytest tests/access/test_real_identity.py -ra

# Default native-system qualification rejects adapters that report mock=true:
.venv/bin/python scripts/check-system.py --config /private/disposable-system.json

# A separate explicit protocol mode cannot certify native storage:
.venv/bin/python scripts/check-system.py \
  --config /private/disposable-system.json --backend mock
```

The operator guide describes the private fixture requirements. Outage cases need
additional explicit connection/container settings, accept only task-scoped
loopback resources, and restore the inspected running service in `finally`.
Without infrastructure configuration the native tests skip explicitly; ordinary
unit tests do not require credentials or reach an external identity host.

## Executed replacement checks

- SDK: direct Bearer and Authlib token providers, real HTTP/HTTPS requests,
  Device Authorization pending/slow-down/refresh, concurrency, total deadlines,
  exact database prefixes, Arrow ownership and once-per-call writes.
- Gateway: actual JOSE cryptography and live-introspection transport, signature,
  issuer/audience/type/time rejection, changed roles/membership, same policy for
  list and direct resource, CSRF/Origin and mixed-credential denial.
- Ownership: stable UUIDs, unique canonical backend endpoints, disabled/pending states,
  concurrent/stale revisions and content-bound retries, external-effect markers,
  emergency-disable superseding, restart-persistent uncertain writes and audit.
- Fencing: admitted reads/streams, unknown write resolution, failed policy probes,
  detach/assignment, send-start/body/read/close/header-construction failures and
  catalog-finalization errors. Exceptions release in-memory admission while
  preserving durable uncertainty.
- Telemetry: actual prometheus_client exposition, fixed 1850 HTTP/stage samples
  plus bounded logging samples, actual in-flight/backpressure/cancellation,
  observed dependency readiness and no per-user/database/request labels.

The live native identity suite passed **15 cases**, including one Cerbos outage,
one Keycloak outage, concurrent browser refresh after 31 seconds with stale
in-flight tickets, and fresh sign-in after losing the ephemeral session store.
It used three organizations with
2/1/0 databases, actual native memberships and fresh access tokens. Checks covered
foreign UUIDs, encoded paths, forged headers, JSON API errors, the prefixed SDK
and Arrow, transfer while an append outcome was unknown, terminal reconciliation,
receipt replay and restoration of original ownership. All three organizations
finished active with zero unresolved admissions.

The session store used the exact authenticated health command and restricted ACL;
persistence/export/administration commands were denied, AOF and RDB saves were
zero, and its writable directory was empty tmpfs. Refresh-token rotation stayed
enabled with maximum reuse zero. Native account sessions were logged out after
testing. The two-second upstream refresh-lock lease still requires production
latency/stall qualification; these tests do not prove arbitrary-delay consistency.

The test policy endpoint explicitly used the IPv4 address of its IPv4-only SSH
tunnel. Earlier runs recorded intermittent PDP transport failures before the
server handler; those failures remain unqualified historical evidence. A closed
IPv6 probe reproduces connection refusal, but the exact address selected by the
earlier failed calls was not retained, so it is not claimed as their proven cause.

Chromium visited all eight Workbench pages, including Style Studio, both populated
and empty organization selectors, browser create/append/Arrow, native Account
Console, native password change/session revocation and logout. The runtime theme
file changed and reloaded without rebuilding, then was restored byte-for-byte.
The supplied PNGs matched their original hashes. Only the authenticated dummy
database UI was captured; no HAR, token/cookie dump, video or login trace was saved.
The separate UI suite also exercises late responses after identity/collection
changes, dialogs, responsive layout and the standalone browser demo.

Protocol corrections were observed failing before their fixes: the proxy lost
Arrow's snapshot header, failed to recognize plain successful create receipts
with schema defaults, and redirected encoded paths before gateway validation.
The corrected public flow passed the system harness's **16 route checks**.
The same explicitly mock target was rejected by the default native mode.

Capability-profile regressions run the real network SDK and CLI against an
explicit disposable mock with test-only capability/unsupported-response
middleware. They verify that supported create/collection/append/history/JSON/
Arrow work still executes, while actual typed501 responses for advertised
missing metadata/profile features produce `blocked` / `UNSUPPORTED_CAPABILITIES`
and exit2 with the exact missing list. Legacy complete profiles still pass;
claimed-supported failures, contradictory replies and malformed flags fail.
Default native mode rejects the mock before writes. These tests do not execute
a native storage server or qualify a partial feature profile.

Native-protocol receipt regressions use explicit backend transport doubles
through the actual gateway/catalog. They first reproduced the unresolved
failed-create and 412/409 append-lookup cases, then verified terminal admission
cleanup. Mismatched/missing IDs, unknown outcomes, incorrect native status/
phase/retry hints, unknown codes and bare errors preserve durable uncertainty.
Both `OPERATION_CONFLICT` and `IDEMPOTENCY_CONFLICT` are rejected before any
terminal inference, including legacy wrappers claiming a matching ID and a
committed or failed outcome. A conflicting request says nothing about the bound
operation. Legitimate 409 schema failures and 412 precondition failures retain
their separate authoritative-outcome contract. New-main conflict regressions
were observed failing before the gateway guard, then passing with the SDK's
corresponding conflict/failure-envelope tests.
The fixtures describe native serializer shapes, not actual native execution.

## Deployment and persistence evidence

The production Python image built from the exact 46-package hash lock; binary
installation and nonroot `pip check` passed. Native Compose parsing, native Caddy
configuration/CEL validation and the Docker JSON Alloy override passed. Separate
bounded containers exercised the actual private-admin hostname topology, native
realm provisioning, master/private and glamdb/public issuers, Admin Console HTML,
public administration denial, strict browser email verification and native OAuth
API responses. All of those disposable deployment containers/volumes were removed.

Actual PostgreSQL `pg_dump -Fc` and `pg_restore --exit-on-error` into a separate
owned database preserved the realm/organization/user counts (1/3/9). The original
database was not overwritten. Catalog backup/migration tests verify private files,
stable mapping, disabled activation and absence of copied password columns.
A realm export is not treated as a complete PostgreSQL/session backup.

## Local diagnostic costs

A 300-request sequential metadata trial used the same explicitly mock backend
route before and after the migration. The older cached Basic/Caddy path measured
226.7 requests/s, p95 8.05 ms and p99 33.93 ms. The new complete HTTPS path, with
native identity/policy services in a local 4-CPU/4-GiB VM, measured 59.2 requests/s,
p95 29.00 ms and p99 35.22 ms. Each request performed live introspection and a
fresh PDP decision; no grant cache was introduced.

Actual component observer p95/p99 values were 0.31/0.48 ms for local JWT
validation, 16.41/29.95 ms for native introspection, and 4.51/6.21 ms for Cerbos.
Gateway CPU increased by 1.57 s during the 300 full requests; its sampled RSS
went from 29,232 to 55,312 KiB. This excludes other processes and is not a
whole-stack memory comparison. The key cache was warm, the host was not CPU
isolated, and VM/SSH networking is part of the new path. These observations show
its measured additional cost; they are not a production RPS promise or native
storage benchmark. Logging/metrics costs are measured separately in their
validation guides.

A separate 16-worker metadata trial completed 600/600 requests with HTTP 200,
204.4 requests/s, p95 105.36 ms and p99 129.65 ms. It used the same mandatory
live checks and explicit mock backend, with the same diagnostic limitations.

## Qualification limits

The embedded engine, backend, scanner and native HTTP server exist on `main`.
Identity and mock-protocol checks do not prove the assembled production identity
deployment, native data durability, power-loss behavior or throughput.
No public DNS/ACME certificate, SMTP server, real notification destination or
Kubernetes cluster was configured. The complete assembled production Compose
runtime remains an operator deployment gate even though its real components,
image, topology and source configurations were exercised separately.

Live introspection and local admission fencing cover new requests. Already
downloaded data cannot be retracted; admitted streams may finish and prevent a
strict transfer from completing. An interrupted create without an authoritative
backend outcome remains blocked. Backups must be restored under maintenance
and reconciled against subsequent revocations before public access reopens.
