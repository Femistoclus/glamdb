# GlamDB documentation

Start with the repository [quick start](../README.md#get-started). GlamDB is an
experimental local columnar database; it is not intended for production use.
Physical power-loss durability has not been qualified.

## Clients and data

| Guide | Purpose |
| --- | --- |
| [CLI](../apps/cli/README.md) | Build the standalone client, manage databases, read and write data. |
| [Python SDK](../sdk/python/README.md) | Typed requests, JSON/Arrow scans, exact retries and OAuth access. |
| [Workbench](WORKBENCH.md) | Use the browser UI, inspect data and recover pending requests. |
| [HTTP API](HTTP_SERVER.md) | Routes, authentication, limits, pagination and write outcomes. |
| [Embedded C++ contract](STORAGE_CONTRACT.md) | Negotiate contracts 1.0–1.5 and use the installed Engine facade. |
| [Scalar types](SCALAR_TYPES.md) | Exact decimal/date values, timestamps and format compatibility. |
| [Scans](SCANNING.md) | Fixed snapshots, predicates, projection, pruning and buffer ownership. |
| [Read views](READ_VIEWS.md) | Capture coherent per-store snapshot vectors for repeated native scans. |
| [Scan splits](SCAN_SPLITS.md) | Plan and schedule bounded, repeatable native file-range scans. |
| [Scan negotiation](SCAN_NEGOTIATION.md) | Negotiate typed pushdown, residual filters, projections and safe limits. |
| [Metadata](METADATA.md) | Snapshot history, statistics, files, footers and explain. |
| [KV collections](KV_MODE.md) | Upsert, delete, lookup, compaction and explicit append migration. |
| [Write sessions](WRITE_SESSIONS.md) | Durable multipart native append/upsert staging, retry, commit and cleanup. |
| [Bulk append](BULK_INGESTION.md) | Replayable streaming ingestion through C++, HTTP, SDK and CLI. |

## Installation and operations

| Guide | Purpose |
| --- | --- |
| [Build and install](BUILDING.md) | Toolchain, pinned dependencies, installation and CMake consumers. |
| [Architecture](ARCHITECTURE.md) | Store ownership, publication, recovery and supported boundaries. |
| [Backups](BACKUP.md) | Quiesced whole-store copy and restore requirements. |
| [Database deletion](DATABASE_DROP.md) | Exact-identity managed deletion and interrupted cleanup. |
| [Collection limits](COLLECTION_PROFILES.md) | Configure 1–64 collections and fixed inventories. |
| [Retained history](SCALABLE_HISTORY.md) | Configure operation, metadata and recovery budgets. |
| [Snapshot retention](SNAPSHOT_RETENTION.md) | Explicit C++ maintenance for append collections. |
| [Snapshot archive](SNAPSHOT_ARCHIVE.md) | Hot window, archived reads, maintenance and verified-checkpoint open. |
| [Shared resources](SHARED_RESOURCES.md) | Process memory classes, device admission and ownership. |
| [Scan sessions](SCAN_SESSIONS.md) | Configure cursor quotas, replay, expiry and HTTP reader admission. |
| [Physical inventory](STORAGE_FOOTPRINT.md) | Inspect current store allocation and retained files. |
| [Organizations](ORGANIZATIONS.md) | Identity, database ownership and gateway route permissions. |
| [SSO operations](SSO_OPERATIONS.md) | Deploy and operate Keycloak, OAuth2 Proxy and Cerbos. |
| [Combined login](COMBINED_LOGIN.md) | Migrate an existing Keycloak client login form. |
| [Monitoring](MONITORING.md) | Private metrics, collectors and Grafana dashboards. |
| [Metric coverage](MONITORING_COVERAGE.md) | Measurement scopes and unavailable sources. |
| [Logging](LOGGING.md) | Structured events, retention and collection. |
| [Read server logs](READING_SERVER_LOGS.md) | Sampling, errors and correlation commands. |
| [Alerting](../ALERTING.md) | Four-state rules and Telegram delivery. |
| [Linux observability quick start](../deploy/observability/QUICKSTART.ru.md) | Single-host deployment instructions in Russian. |
| [Public browser demo](PUBLIC_DEMO.md) | Package and preview the isolated static UI. |

## Workloads

The [workload package](../tools/workload/README.md) provides data generation,
load journals and finite Locust campaigns. Use the [TPC-H guide](TPC_H.md) for
eight-table append/KV workflows and the [ClickBench guide](../tools/workload/CLICKBENCH.md)
for all 43 scan/reduction workloads. Reductions run in the client; neither
benchmark provides a native SQL endpoint or an official database score.
