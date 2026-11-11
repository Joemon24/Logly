# Kafka dispatcher (Phase 4)

Status: **dispatcher package and tests only.** It drains the Phase 2 SQLite
outbox into Kafka. It is not wired into the v1 runtime, Python consumer, React
app, ML/LLM code, or deployment files.

Package: `agent/dispatcher`
Tests: `agent/dispatcher/*_test.go`
Dependencies: `agent/events`, `agent/storage` (read-only addition:
`Store.OutboxStats`), `github.com/segmentio/kafka-go`

## Delivery contract

Delivery is **at-least-once**, not exactly-once.

1. `storage.Pending(ctx, limit)` reads up to `BatchSize` rows in outbox `seq`
   order. The limit is bounded by `storage.MaxPendingLimit` (1000).
2. Each row is published as one Kafka record:
   - key = the stored `event_id`,
   - value = the exact stored JSON bytes (no unmarshal, remarshal, or event
     reconstruction).
3. The producer must wait for broker acknowledgement
   (`RequiredAcks=RequireAll`, synchronous writes, `MaxAttempts=1`).
4. Only after acknowledgement does the dispatcher call `storage.Ack` for that
   batch.
5. A crash between 3 and 4, or a failed SQLite acknowledgement, leaves the rows
   in the outbox. They are published again on retry and may appear twice.

The Python consumer must deduplicate on `event_id`.

## Configuration

`dispatcher.New` needs a `Store` and a `Producer`. `dispatcher.NewKafkaProducer`
needs an explicit broker list and topic:

```go
producer, err := dispatcher.NewKafkaProducer(dispatcher.KafkaConfig{
    Brokers: []string{"localhost:19092"},
    Topic:   "ids.events.v2",
})
defer producer.Close()

d, err := dispatcher.New(dispatcher.Config{
    Store:    store,
    Producer: producer,
})

err = d.Run(ctx) // blocks until ctx is cancelled
```

There are deliberately **no default brokers or topic**. Empty values, missing
ports, and wildcard listen addresses (`0.0.0.0`, `::`, `*`) are rejected. Topic
auto-creation is disabled so a typo cannot create an unintended topic.

Dispatcher defaults:

| Field | Default | Meaning |
|---|---|---|
| `BatchSize` | 100 | Maximum rows per `Pending` call (1..1000). |
| `PollInterval` | 1s | Delay when the outbox is empty. |
| `PublishTimeout` | 10s | Bound on one producer call, including acks. |
| `AckTimeout` | 5s | Bound on one SQLite acknowledgement. |
| `RetryBase` | 250ms | First backoff after a failed batch. |
| `RetryMax` | 30s | Backoff cap. |

## Retry and shutdown

`Run` retries publish, acknowledgement, and transient storage failures with
bounded exponential backoff plus jitter. It resets the backoff after a
successful batch. Corrupt payloads, corrupt databases, unsupported schemas, and
invalid limits are returned instead of retried forever.

Shutdown:

1. Cancel the context passed to `Run`.
2. `Run` stops reading new batches.
3. A batch already read is allowed to finish publishing within
   `PublishTimeout`, and a confirmed batch is acknowledged within `AckTimeout`.
4. Any unacknowledged rows remain durable in SQLite.
5. Close the producer, then the store.

`Run` returns the context error (`context.Canceled`) after a graceful stop.

## Ordering

The dispatcher has **one worker and one in-flight publish**, so:

- Batches are read in outbox insertion order and published sequentially.
- Kafka keys are hashed to partitions, so records sharing an `event_id` key also
  share a partition.
- Within this process, a failed batch is retried before the next batch, so
  retries do not reorder later batches.
- Producer-internal retries are disabled (`MaxAttempts=1`); the dispatcher owns
  retries.

Limits:

- Global order across different sources/keys is **not** guaranteed because they
  can map to different partitions.
- A retry after an uncertain acknowledgement can duplicate a record; it does
  not remove the original.
- Multiple dispatcher processes or multiple workers are not used or supported
  in this phase. Running more than one would weaken ordering between batches.
- Kafka itself can still redeliver or lose ordering under broker failover or
  consumer rebalance. Deduplicate on `event_id` and use per-key order only as
  the practical guarantee.

## Metrics

`Dispatcher.Stats()` returns a consistent snapshot:

| Field | Meaning |
|---|---|
| `Pending`, `PendingKnown` | Last observed outbox count. |
| `OldestPendingAge` | Age of the oldest stored event. |
| `Delivered` | Broker-confirmed publishes (may exceed unique events). |
| `Acknowledged` | Rows removed from the outbox. |
| `PublishFailures`, `AckFailures`, `Failures` | Error counters. |
| `Retries` | Backoff cycles entered by `Run`. |
| `Batches` | Batches read from the outbox. |
| `LastError` | Most recent delivery/storage error. |

`storage.OutboxStats(ctx)` is the read-only query behind the backlog fields.

## Tests

`go test -race ./dispatcher ./storage` covers:

- successful publish and acknowledgement
- exact payload bytes and `event_id` key
- broker unavailable, retained events, and bounded retries
- producer timeout
- duplicate publish after a failed acknowledgement
- acknowledgement failure after successful Kafka delivery
- graceful in-flight completion and cancelled idle shutdown
- batch size bounds and cross-batch ordering
- restart with pending events and unchanged payloads
- explicit Kafka config validation, including wildcard rejection

The Kafka integration test is opt-in and requires a running broker.

## Manual Kafka verification

Start the local single-broker environment:

```bash
docker compose up -d kafka
```

Create the topic once:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:19092 \
  --create --if-not-exists \
  --topic ids.events.v2 \
  --partitions 3 \
  --replication-factor 1
```

Run the integration test. Brokers are explicit; there is no fallback value:

```bash
cd agent
LOGLY_KAFKA_INTEGRATION=1 \
LOGLY_KAFKA_BROKERS=localhost:19092 \
go test ./dispatcher -run TestKafkaIntegration -v -count=1
```

The test creates a unique topic, publishes through the real producer, reads the
two messages back, and verifies the key and payload bytes. It does not clear the
`ids.events.v2` topic.

Inspect keys and payloads in the running environment:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic ids.events.v2 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=' | '
```

Each line is `<event_id> | <stored JSON payload>`. The key must equal the
payload's `event_id` field, and the JSON must be byte-identical to the outbox
row.

## Limitations

- At-least-once only; duplicate delivery is expected inside the
  publish/acknowledge window.
- Single worker; throughput is one batch at a time.
- No dead-letter queue: a corrupt stored payload stops `Run` for operator
  action rather than being deleted.
- `Delivered` counts publishes, not unique events.
- No metrics endpoint or Prometheus exporter; counters are in-process.
- No automatic topic management, TLS/SASL configuration, or broker discovery.
- Not connected to the collector or v1 runtime yet.
