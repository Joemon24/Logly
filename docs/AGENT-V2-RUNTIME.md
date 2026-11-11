# Agent v2 runtime (Milestone A)

Status: **runnable executable, v1 untouched.** `agent/cmd/agent-v2` wires the
Phase 3 authentication collector, the Phase 2 SQLite outbox, and the Phase 4
Kafka dispatcher into one process. The v1 executable
`agent/cmd/agent/main.go` and `agent/pipeline` are unchanged.

Package: `agent/runtimev2`
Executable: `agent/cmd/agent-v2`
Tests: `agent/runtimev2/runtime_test.go`

## Configuration

All settings come from the `LOGLY_V2_` environment. Required values are
rejected when empty; there are no broker, topic, or database defaults.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `LOGLY_V2_LOG_PATH` | no | `/var/log/auth.log` | Authentication log to tail. |
| `LOGLY_V2_SOURCE_ID` | no | `auth-log` | Stable logical source identity. |
| `LOGLY_V2_DB_PATH` | yes | — | SQLite outbox path. |
| `LOGLY_V2_AGENT_ID` | yes | — | Persistent agent identity. Must match the database. |
| `LOGLY_V2_AGENT_TYPE` | yes | — | Agent type stored in events. |
| `LOGLY_V2_AGENT_VERSION` | no | `2.0.0` | Agent version stored in events. |
| `LOGLY_V2_KAFKA_BROKERS` | yes | — | Comma-separated `host:port` list. |
| `LOGLY_V2_KAFKA_TOPIC` | yes | — | v2 topic, for example `ids.events.v2`. |
| `LOGLY_V2_CAPACITY` | no | `10000` | Maximum pending outbox events. |
| `LOGLY_V2_POLL_INTERVAL` | no | `1s` | Collector and dispatcher poll period. |
| `LOGLY_V2_PUBLISH_TIMEOUT` | no | `10s` | Bound on one Kafka publish, including acks. |
| `LOGLY_V2_ACK_TIMEOUT` | no | `5s` | Bound on one SQLite acknowledgement. |
| `LOGLY_V2_SHUTDOWN_TIMEOUT` | no | `30s` | Drain bound after SIGINT/SIGTERM. |
| `LOGLY_V2_STATS_INTERVAL` | no | `30s` | Counter log interval; final counters are always logged. |
| `LOGLY_V2_MAX_RECORDS_PER_POLL` | no | `256` | Records consumed per collector poll. |
| `LOGLY_V2_HOSTNAME` | no | `os.Hostname()` | Hostname attached to events. |

```bash
LOGLY_V2_LOG_PATH=/var/log/auth.log \
LOGLY_V2_DB_PATH=/var/lib/logly/outbox.db \
LOGLY_V2_AGENT_ID=linux-01 \
LOGLY_V2_AGENT_TYPE=linux \
LOGLY_V2_KAFKA_BROKERS=127.0.0.1:19092 \
LOGLY_V2_KAFKA_TOPIC=ids.events.v2 \
go run ./cmd/agent-v2
```

## Runtime shape

- One collector goroutine, one dispatcher goroutine, one stats ticker. No
  per-event goroutines and no unbounded queues.
- The collector persists events and checkpoints in the same SQLite
  transaction. A persistence failure returns `ErrBackpressure` or `ErrStorage`
  and never advances the checkpoint.
- The dispatcher reads bounded `Pending` batches and calls `Ack` only after
  broker confirmation. Pending rows survive a crash or an outage.
- The Kafka message key is `<agent_id>/<source_id>`, so one source keeps its
  order in one partition. `event_id` remains the payload deduplication key.
- The Python consumer separately deduplicates on `event_id`.

Startup logs the restored pending count and oldest pending age. Forward
progress is logged every `LOGLY_V2_STATS_INTERVAL`:
`pending_events`, `oldest_pending_age`, `batches`, `delivered`, `acknowledged`,
`publish_failures`, `ack_failures`, `retries`, `failures`, `last_error`.

## Shutdown and failures

SIGINT/SIGTERM cancels the runtime. The collector stops before reading a new
poll. An in-flight publish may finish within `PublishTimeout` and is
acknowledged within `AckTimeout`. Remaining events stay in SQLite. `Run`
returns when both goroutines exit or after `ShutdownTimeout` (reported as
`ErrShutdownTimeout`).

Component failures are logged with a stable `reason` and an operator `hint`:

| Reason | Meaning |
|---|---|
| `rejected_record` | A malformed/oversized sshd record stopped the checkpoint. Fix or rotate the log. |
| `coverage_gap` | An offline rotation lost unread input; the gap is reported, not hidden. |
| `storage_failure` | The outbox write failed; the checkpoint was not advanced. |
| `kafka_unavailable` | Publish failed; events stay in the outbox. |
| `outbox_ack_failure` | Delivery succeeded but the row was not removed; duplicates are possible. |
| `outbox_read_failure` | The dispatcher could not read SQLite. |
| `corrupt_storage` | A stored payload or database is damaged. |

Kafka and SQLite outages are retried by the dispatcher with bounded
exponential backoff. Rejected records are fatal so they stay visible.

## Development fixture

`agent/runtimev2/testdata/auth.log` has one IPv4 failure and one IPv6 success.
macOS fixture tests exercise parsing, persistence, dispatch, restart, and
shutdown, but **they do not prove Linux file-identity, rotation, or production
auth.log behavior**. Linux behavior is covered by the collector's unit tests
and must be re-checked on the target host.

```bash
cd agent
go test -race ./runtimev2 -count=1
```

## Manual local run

```bash
cd agent
go build -o /tmp/agent-v2 ./cmd/agent-v2
printf '%s\n' \
  'Nov 10 10:00:01 web-01 sshd[100]: Failed password for alice from 203.0.113.9 port 50000 ssh2' \
  > /tmp/auth.log

LOGLY_V2_LOG_PATH=/tmp/auth.log \
LOGLY_V2_DB_PATH=/tmp/logly-outbox.db \
LOGLY_V2_AGENT_ID=linux-local \
LOGLY_V2_AGENT_TYPE=linux \
LOGLY_V2_KAFKA_BROKERS=localhost:19092 \
LOGLY_V2_KAFKA_TOPIC=ids.events.v2 \
/tmp/agent-v2
```

Stop with Ctrl-C. Start it again with the same database and topic: any pending
events are delivered before new records.

## Limitations

- One source per process. Multiple sources run as multiple processes (or a
  future multi-source runtime); each has its own source ID.
- Capacity must be at least the batch the collector submits in one poll.
  `MaxRecordsPerPoll` bounds that batch, so keep `LOGLY_V2_CAPACITY` well above
  it; otherwise backpressure cannot drain and the checkpoint stays put.
- macOS cannot validate Linux inode-based rotation; see the note above.
- Counters are structured logs, not a metrics endpoint.
- No replay-from-database tool; a corrupt payload stops delivery for operator
  inspection.
- At-least-once delivery only. Consumers must deduplicate on `event_id`.
