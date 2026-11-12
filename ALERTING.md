# GlamDB alerting

Prometheus evaluates **OK, WARN, ALARM and NODATA**. Alertmanager sends one
combined current-state summary daily at **10:00 Europe/Moscow** through its
native Telegram receiver. State changes between summaries do not notify. There is no
GlamDB webhook, delivery daemon or SQLite outbox. These rules depend on the
metric and recording-rule contracts in [monitoring](MONITORING.md).

This configuration is an operational starting point, not an SLO or proof of
engine durability. Missing or unsupported sources remain visible as NODATA.

## Versions and upstream interfaces

[versions.lock.json](deploy/alerting/versions.lock.json) pins official
Alertmanager **0.35.0** and Prometheus **3.13.5**, their container digests and the
Darwin arm64 archive checksums. Both use Apache-2.0. Download the exact release
for the host platform and verify its published checksum before execution.

The pinned [Telegram configuration](https://github.com/prometheus/alertmanager/blob/v0.35.0/notify/telegram/config.go)
provides `bot_token_file`, `chat_id_file`, `timeout` and an empty `parse_mode`.
The [receiver implementation](https://github.com/prometheus/alertmanager/blob/v0.35.0/notify/telegram/telegram.go)
owns requests, errors, token-file reads and Telegram message truncation.
Prometheus owns [alerting rules](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/);
Alertmanager owns grouping, deduplication, repeats, inhibition and silences.

## State and source contracts

[checks.json](deploy/alerting/checks.json) contains the fixed expected check
inventory, selectors, thresholds, freshness windows and holds. Run:

```sh
python3 tools/alerting/generate.py
```

Each configured identity has exactly one `glamdb_alert_state` value:

- **0 OK:** a fresh, finite, valid measurement below WARN.
- **1 WARN:** the warning threshold is reached, below ALARM.
- **2 ALARM:** the alarm threshold is reached.
- **3 NODATA:** a required source is missing, stale, invalid or unsupported.

`glamdb_alert_value` exists only while its underlying inputs pass their guards.
Raw sample timestamps are checked; a recently evaluated recording or instant
query does not refresh an old source. Fixed inventory entries survive target
removal. A newly healthy source must pass the configured OK hold before it can
notify. `send_resolved: false` prevents an expired or disappearing series from
creating a recovery notification. Rule-evaluation errors have a separate check
and an independent observer; an evaluator failure is not a healthy reading.

The default evaluation interval is 15 seconds and the source window is 90
seconds. Holds are 2 minutes for OK/WARN/NODATA and 1 minute for ALARM. Initial
healthy checks can appear as OK in the daily summary; this is an observed starting state. Example
thresholds include CPU 80/95%, disk 20/10% remaining, HTTP errors 1/5% with at
least 100 requests in five minutes. Tune these to actual quotas and traffic.

The application HTTP error and latency checks use the gateway's standard
Prometheus client metrics. The separate Caddy outer-edge checks include requests
rejected by OAuth2 Proxy before reaching the application, including 401/403,
429/503 and 502/504. Configure both `glamdb-gateway` and `glamdb-edge` targets.
Native metrics come from the live private `glamdb-core` endpoint, not a CLI
snapshot file. `core_availability` monitors that endpoint's scrape reachability;
`core_readiness` separately monitors `glamdb_server_ready`, aggregated from
`Engine::GetHealth` for the open databases. The gauge is 1 only when at least one
database is open and all are write-ready. No open database, any health lookup
failure, or any database not ready yields 0, even when `/metrics` remains
reachable. Missing or stale readiness data, or a failed core scrape, yields
NODATA rather than recovery. Both checks notify NODATA by default. Existing
native admission, uncertain-commit and operation-error checks notify WARN and
ALARM; their NODATA remains visible and muted by default.
System checks use node_exporter and kubelet/cAdvisor metrics.
Container checks require real namespace/pod/container labels, exporter
availability and fresh timestamps on the raw CPU/memory samples. Preserve
Prometheus's `honor_timestamps: true` for cAdvisor/kubelet: their raw statistics
carry the collector sample time. `container_last_seen` is a scrape-time clock
in [cAdvisor 0.60.6](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go#L127)
and is not a statistics-freshness source.
Unlabelled Compose containers do not establish Kubernetes pod coverage. The exact anonymous-memory check remains NODATA because the
configured standard collector does not expose that field; RSS and working set
are not substituted. The reserved administrative-anomaly source also remains
NODATA until an actual producer exists.

Duplicate the finite inventory for each intended scope. Change `cluster`,
namespace, pod, node and metric selectors together. The shipped `pod="glamdb"`
is an example, not automatic discovery. Set `notify_nodata: true` for required
sources; `false` keeps NODATA visible but suppresses its Telegram notification.

## Configure Telegram secrets

1. Create a dedicated bot through Telegram's verified **@BotFather** and retain
   its token privately. Give it permission to send to the intended destination.
2. Verify the numeric destination chat ID, including the negative sign for
   groups/channels. Use Telegram's official
   [getUpdates documentation](https://core.telegram.org/bots/api#getupdates) and
   [update-delivery FAQ](https://core.telegram.org/bots/faq#getting-updates) with
   an operator-controlled API client. Do not use the bot's user ID as the chat
   ID or remove an unrelated webhook. This repository has no Telegram API client.
3. Create the two mounted files with hidden token entry:

   ```sh
   python3 tools/alerting/configure.py
   ```

The command writes `artifacts/alerting/secrets/telegram_bot_token` and
`telegram_chat_id`. The ignored directory is 0700; files are 0444 so the non-root
Alertmanager container can read Compose's read-only file-backed secret mounts.
Protect host/runtime access: Compose file secrets are not an encrypted vault.
A managed secret store can supply the same mounted paths. Set an absolute
`ALERTING_SECRETS_DIR` for another location and pass it to `configure.py` with
`--secrets-dir`. Keep tokens out of `.env`, arguments, URLs, UI assets, logs,
screenshots and shared backups. Custom alert annotations must contain no secrets
or user payloads; notifications use plain text.

## Start and verify

Configure actual scrape inventories in the monitoring deployment first. The
private monitoring network must exist. Do not use the disposable mock adapter
as a production metric source.

```sh
docker compose -f deploy/alerting/compose.yml config --quiet
docker compose -f deploy/alerting/compose.yml up -d
docker compose -f deploy/monitoring/compose.yml \
  -f deploy/alerting/monitoring.override.yml config --quiet
docker compose -f deploy/monitoring/compose.yml \
  -f deploy/alerting/monitoring.override.yml up -d prometheus
```

The override selects `prometheus-alerting.yml`, mounts generated state rules and
preserves monitoring's configured retention. It depends on the monitoring
package's static Prometheus configuration and Alertmanager scrape job.

Open [Alertmanager](http://localhost:9093) and [Prometheus](http://localhost:9090).
Published ports bind to loopback. Keep administration, metrics and receiver APIs
on a trusted private network; expose the UIs only through the configured access
boundary. In Prometheus `/targets`, check the application, outer edge, exporters,
Prometheus and Alertmanager. In `/rules`, verify rule health. Query
`glamdb_alert_state` and confirm one state per configured check.

Only after verifying the destination, an operator can send a deliberate setup
alert through the native receiver:

```sh
amtool --alertmanager.url=http://localhost:9093 alert add \
  alertname=GlamDBCheckState check=telegram_setup cluster=local service=alerting state=ALARM \
  --annotation=summary='Manual Telegram setup test' \
  --annotation=observed_at="$(date +%s)"
```

The setup alert obeys the same daily 10:00 Moscow delivery window. A setup alert
that expires before the window will not be delivered; use the loopback tests
for immediate receiver validation. Automated tests never send to the real chat.
Expire the setup alert in Alertmanager afterwards. A successful Telegram
API response means acceptance by Telegram, not that a person read the message.

## Delivery, noise and restart semantics

Alertmanager keeps one group across services and opens the native
`daily-summary` interval only from **10:00 to 10:01 Europe/Moscow**. The group
is checked once per minute, allowing one scheduled snapshot in that minute.
Outside it, even WARN/ALARM/NODATA transitions do not notify. The snapshot is the
current set of active checks that have passed their rule holds, not a history of
all incidents during the previous day. Existing `notify_nodata=false` exclusions
and silences still apply; omitted checks remain visible in the dashboards.

The 12-hour `repeat_interval` is shorter than one day so small timer-phase
changes cannot suppress the next day's unchanged summary. The active interval
prevents a separate 12-hour reminder. Keep the one-minute interval duration and
`group_interval: 1m` together; increasing the interval length can permit multiple
updates, while increasing the polling period can miss the daily window. Change
`start_time`, `end_time` and `location` together to move the scheduled minute.
The existing 120-hour notification-log retention is sufficient for this cadence.
Alertmanager and the local VM must be running during the scheduled minute.
A missed window is not replayed automatically; this uses native time intervals,
not a persistent calendar-job service.

NODATA inhibits other states of the same identity; ALARM inhibits OK/WARN.
Use time-bounded silences with narrow stable labels, an owner and a reason.
A silence cannot retract a request already in flight.

The contract is **delivery of the current observed state**, including its Unix
observation timestamp in the summary header. Notifications can be
coalesced by grouping, inhibited, silenced, truncated at Telegram's limit or
repeated. Observation time is not a claim that the state is still current when
the chat reads it. In particular:

- The persistent Alertmanager volume retains silences and its notification log.
  Active alerts and in-flight retries are not an event archive. Prometheus must
  resend current active alerts after restart.
- This release retries Telegram errors, including 429, 5xx and request timeout,
  using the standard Alertmanager retry pipeline. Its Telegram receiver does
  **not** apply `retry_after` as a durable chat-wide deadline. Failed in-flight
  attempts can extend past the active minute; this is not an exactly-once daily
  delivery guarantee. There is no separate delivery daemon or chat pacer.
- The configured Telegram timeout is fifteen seconds. Failed attempts are visible
  in `alertmanager_notification_requests_failed_total{integration="telegram"}`;
  final failed notifications appear in `alertmanager_notifications_failed_total`.
  Also inspect requests, notification latency and suppressed-notification counts.
- A request accepted by Telegram followed by a lost response can be retried and
  duplicated. No exactly-once or every-intermediate-event guarantee is made.
- **The old durable SQLite outbox, latest-per-identity ordering, expiry policy,
  persisted backoff and send-time revalidation of delayed OK are gone.** An
  already queued/in-flight OK can arrive after its observation is superseded.
  Its timestamp remains visible; explicit healthy PromQL and resolved
  suppression prevent missing data from manufacturing a new OK.

The application deliberately adopts the native Alertmanager contract. It does
not promise equivalence to the removed outbox or delivery-time recovery query.
A compact daily summary uses Telegram HTML with bold Database / API, CPU,
Memory, Disk, Network, Logs, Monitoring / delivery and Other headings. Empty
groups are omitted, and a blank line separates groups. Each check reaching the
receiver appears once with its four-state value and a readable label; common scope is
shown once in the header and differing scope stays on the check's line. Dynamic
labels are HTML-escaped after bounding their display length (48 characters for
check/common scope, 32 for differing scope). The observation timestamp remains
in the header. Unknown checks appear under Other.

The complete rendered HTML is limited to 3,800 UTF-8 bytes, below the native
receiver's 4,096-character truncation boundary. If the full inventory is larger,
the message explicitly reports that the list is too large, includes totals for
all four states, and directs the reader to Alertmanager / Grafana. It does not
silently cut checks or HTML tags. The full inventory remains in those UIs.
A batch of 50 checks across services is covered by a native receiver test
without message truncation. Native template tests cover every current Linux
check, unknown checks, empty groups, hostile labels and oversized inventories.
A separate native
test keeps the real one-minute production cadence, shortens only the repeat
horizon, and verifies one snapshot followed by silence through closure of the
window. The tests use their own processes and a loopback Telegram peer.

Tune grouping, holds, repeats and routing for Telegram rate limits; inspect
failures instead of assuming that every short transition reaches the chat.

## Independent pipeline observation

Install [external-observer/prometheus.yml](deploy/alerting/external-observer/prometheus.yml)
and [its rules](deploy/alerting/external-observer/rules.yml) on **another host and
fault domain**. Use the pinned Prometheus image/binary and persistent, bounded
storage there. Replace all reserved `.invalid` addresses with private reachable
main Prometheus/Alertmanager endpoints and an **independent Alertmanager and
notification path**. Mount both files together; `rules.yml` is relative to the
observer configuration. Configure credentials/TLS for that private path as
required by the environment. With the verified pinned binary installed, an
example observer command is:

```sh
prometheus --config.file=/etc/glamdb-observer/prometheus.yml \
  --storage.tsdb.path=/var/lib/glamdb-observer \
  --storage.tsdb.retention.time=7d --storage.tsdb.retention.size=1GB \
  --web.listen-address=127.0.0.1:9091
```

Create those directories for the service account and supervise the process with
the host service manager. The size flag bounds retained blocks; leave additional
space for the WAL/head. Supply credentials through protected files.

The observer detects failed or removed scrape targets, failed rule evaluations,
and missing/stale evaluation timestamps. Its notifications do not depend on the
main Alertmanager. Verify loss of each main process and check the independent
route before relying on it. The observer and its receiver also need an external
dead-man check or another operational notification path. Processes on one host
cannot prove physical fault-domain separation.

## Reload, rotate and migrate

Regenerate and validate changed thresholds before reloading Prometheus and
Alertmanager with SIGHUP. Invalid replacement configurations are rejected;
inspect `/rules`, `prometheus_config_last_reload_successful` and
`alertmanager_config_last_reload_successful`. Changing an identity creates a
new hold and initial-state notification.

For token compromise, revoke it through BotFather and replace both local files:

```sh
python3 tools/alerting/configure.py --rotate
docker compose -f deploy/alerting/compose.yml up -d --force-recreate alertmanager
```

Recreation is required for atomically replaced host files mounted by Compose.
The chat destination is read from the file and cannot be changed by alert labels.

When upgrading an older deployment, choose a maintenance window, stop its
`alert-transport` service, inspect/finish or intentionally discard pending
outbox work, and retain an offline backup if needed. Do not run both delivery
paths concurrently. Start the new native receiver and verify current states.
The removed `telegram-outbox` volume is not migrated or deleted by this change;
it will not be replayed. Remove obsolete webhook bearer material from the secret
store after the switch. A rollback must be an explicit configuration/version
change; it cannot reconstruct transitions lost while the old outbox was stopped.

To pause native notifications, stop Alertmanager or apply scoped silences.
Stopping it pauses routing and its UI as well. To stop the local stack, use
`docker compose -f deploy/alerting/compose.yml down` without `-v`; retain the
notification log and silences. The base monitoring configuration can run without
this alerting override.

## Executable validation

Use official checksum-verified tools on PATH, or pass all four binary paths:

```sh
python3 tools/alerting/check.py
```

The checker validates deterministic generation, state and independent-observer
PromQL scenarios, both service configurations, secret setup, and real native
Prometheus/Alertmanager subprocesses. It tests all four states, missing/stale
inputs, fixed file-based destination, observation timestamps, 429/5xx/timeouts,
restart, retry loss/current-state resend, grouping, repeats, inhibition, silences
and rapid coalescing. A separate local observer continues notifying after its
main Prometheus and Alertmanager processes stop. An actual many-to-many rule
error verifies evaluator-fault detection.

Direct Python discovery skips subprocess tests unless `PROMETHEUS_BIN` and
`ALERTMANAGER_BIN` are provided. The full checker requires them. Telegram and
synthetic exporters are explicit loopback protocol peers; the Caddy replay is a
captured outer-edge scrape. No automated test contacts Telegram. These tests do
not provide a native storage HTTP/scanner integration, physical host-failure
proof, or a production chat-delivery test.
