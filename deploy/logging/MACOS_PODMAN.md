# Native macOS logs with Podman Loki

This profile collects real native JSONL events. The producer must already emit
schema-1 records into a dedicated directory with bounded application rotation;
its active file is `glamdb.jsonl`, with numbered backups such as `glamdb.1.jsonl`.
Only the exact active filename is discovered, every five seconds. This retries
startup if the producer has not created the file yet. Alloy's file tailer follows
rotation; discovering each renamed backup separately could replay old records.
A rescan while the path is absent can remove its saved positions; when the path
returns, active-file records may replay. See the
[Alloy file-source behavior](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/#component-behavior).
Startup messages in an unrelated stdout file are not a replacement for this
source. The collector applies the same allowlist, validation, field projection
and trusted labels as the Linux journal profile.

An existing rootful Podman machine and `glamdb-observability` network are
prerequisites. The installer checks that the named connection selects that
machine's rootful socket. It never installs or invokes Docker, changes the
default connection, or changes other running containers. Supply the official
`alloy-darwin-arm64.zip` release archive; its SHA-256 must match
`versions.lock.json`. The pinned download is
<https://github.com/grafana/alloy/releases/download/v1.21.3/alloy-darwin-arm64.zip>.

```sh
python3 tools/logging/install-macos-podman.py \
  --machine glamdb-observability-e2e \
  --connection glamdb-observability-e2e-root \
  --alloy-archive artifacts/logging-tools/alloy.zip
```

Alloy 1.21.3 runs as the current user through `org.glamdb.alloy`, listening only
on `127.0.0.1:12345`. The binary, merged config and persistent positions live
below `~/.local/share/glamdb/live/logging`. Logs are read from
`~/.local/share/glamdb/live/logs` by default; `--log-directory` changes only that
source. Go's `GOMEMLIMIT=192MiB` is a soft runtime target, not an OS memory quota.
Daemon stdout/stderr go to `/dev/null`, avoiding an unbounded diagnostic file;
use readiness, metrics and Alloy's local component UI for runtime diagnosis.
Application log retention belongs to the producer.

Loki 3.7.9 runs as UID/GID 10001 in `glamdb-logging-loki-1`, using the pinned
image digest. Its HTTP endpoint is published only on `127.0.0.1:3100`; network
alias `loki` serves other trusted members of `glamdb-observability`. The service
has a 1 GiB memory limit, 1.5 CPUs, 256 PIDs, a read-only root filesystem and a
64 MiB temporary filesystem. Its own container diagnostic log is capped at
10 MiB. No warehouse or application credentials are mounted.

The VM's `/var/lib/glamdb/logging/loki.ext4` is a preallocated **1 GiB** image
mounted at `/var/lib/glamdb/logging/loki`. The filesystem has no reserved-block
percentage; ext4 metadata leaves less than 1 GiB available for Loki data. The
installer never formats an existing image. WAL, chunks, indexes and compactor
state all reside on this one filesystem. The 168-hour retention policy remains
asynchronous; filling the filesystem causes ingestion failure and can lose
best-effort operational events after collector retries are exhausted.

The Quadlet binds the container lifecycle to
`var-lib-glamdb-logging-loki.mount`, requires the mount and checks `mountpoint`
before each start. Stopping that mount stops the container. A failed mount
cannot silently redirect writes into the larger VM root filesystem. Loki's
90% WAL disk guard sees this dedicated filesystem. Authentication remains
disabled within the private observability network; do not publish these ports
on external interfaces.

Inspect or restart only these services:

```sh
curl --fail http://127.0.0.1:3100/ready
curl --fail http://127.0.0.1:12345/-/ready
curl --fail http://127.0.0.1:12345/metrics
launchctl kickstart -k gui/$(id -u)/org.glamdb.alloy
podman machine ssh glamdb-observability-e2e \
  sudo systemctl restart glamdb-logging-loki.service
podman machine ssh glamdb-observability-e2e \
  sudo df -B1 /var/lib/glamdb/logging/loki
```

Check actual runtime metric names before configuring rules; unavailable metrics
must remain unavailable rather than being replaced with constant zero. An
initial file source can have no targets until a real matching log file exists.
Successful ingestion is established only by querying real schema-1 application
events from `/loki/api/v1/query_range`, then observing file-read and sent-entry
counters. Readiness alone does not establish this path. Collector restart keeps
positions. Rotation while the collector is unavailable, truncation or deletion
can cause unrecoverable gaps: the active-file source does not replay numbered
backups. A missing positions file can replay active-file records. This is a
best-effort operational log pipeline, not exactly-once delivery.

In pinned Alloy 1.21.3, `loki_source_file_read_bytes_total` is a checkpoint gauge.
The tailer refreshes it when processing an entry after the ten-second interval,
and on orderly shutdown. On a quiet source it can lag lines already sent to
Loki. Use the actual read-line and sent-entry counters plus a Loki query to
verify ingestion. See the
[pinned tailer implementation](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/loki/source/file/tailer.go#L219-L249).

Quadlet service and install semantics follow the
[Podman systemd documentation](https://docs.podman.io/en/latest/markdown/podman-systemd.unit.5.html).
