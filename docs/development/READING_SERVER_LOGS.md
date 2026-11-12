# Reading server logs

The native `glamdb` server writes one JSON event per line to **stderr**.
Stdout still contains the machine-readable listening address. Keep both streams
separate. For a short local debugging session:

```sh
export GLAMDB_TOKEN='your-local-token'
glamdb --data-dir /absolute/path/to/server --port 8080 \
  --log-success-sample-every 1 --log-max-success-per-second 1000 \
  2>server.jsonl
```

The data directory needs an existing canonical parent. The shell redirection
above has no size limit: stop it after debugging. In production send stderr to
journald or your container runtime and configure its quotas/rotation; see
[deployment and retention](LOGGING.md#runtime-journal-and-collection).

Read events with `jq` (ordinary startup/CLI diagnostics may also appear on stderr):

```sh
# Follow structured events, omitting non-JSON diagnostics.
tail -F server.jsonl | jq --unbuffered -Rc 'fromjson? | select(.schema == "1")'

# Errors and denied/cancelled requests.
jq -Rc 'fromjson? | select(.level == "error" or .level == "warn")' server.jsonl

# Follow a core event through its HTTP completion using its generated ID.
jq -Rc --arg id 'paste-request_id-here' \
  'fromjson? | select(.request_id == $id)' server.jsonl
```

| Event | Meaning |
| --- | --- |
| `runtime_started` | Runtime initialization began; this alone does not mean the listener is ready. |
| `runtime_stopped` | The process runtime finished; `outcome` reflects its exit result, not a durability guarantee. |
| `request_completed` | One HTTP session finished: operation, duration in microseconds and HTTP status. A cancelled transport can have status 0 or an attempted response status. |
| `storage_failed` | The core observed an I/O failure or invalid stored data on an instrumented operation path. |
| `recovery_required` | The core requires recovery after corruption, an uncertain commit, or failed reconciliation/reopening. `outcome=unknown` means the commit needs authoritative reconciliation. |

Native numeric fields are JSON **strings**, including `schema`, `status_code`
and `duration_us`. `timestamp` is Unix nanoseconds. `request_id` is generated
internally. During a request it links synchronous core events to the HTTP
completion; client headers cannot choose it. Startup/shutdown core events use
a separate runtime context (`operation=other`) and have no HTTP completion.
No raw URL, headers, credentials, exception text or stored values enter
structured events.

By default, successful requests are sampled **1 in 100**, with at most **20**
sampled successes per second. Errors, denials, cancellations and lifecycle/core
events bypass that sampling, but bounded queues or sink failures can still lose
events. Logs are diagnostic, not an audit trail or request counter. A successful
HTTP write means the bytes were sent by the server, not that the client consumed
them. A failed connection does not establish whether a database commit happened;
use the operation outcome API. A permanently blocked stderr can delay graceful
shutdown.

`glamdb-admin` also logs `create_store`, `inspect_store`, `scan` (`read`) and
`quiesce_store`. Its `status_code` is the process exit code, not HTTP status;
stdout remains the command result. Embedded library users do not start a logger
implicitly. The Python adapter is a separate utility, not the native server's
logging implementation.
