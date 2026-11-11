# Python consumer schema-v2 support (Milestone B)

Status: **v2 support added to the existing consumer; v1 behavior kept.** The
consumer still reads one Kafka topic and writes Elasticsearch. No framework
migration was made.

Files:
- `central-server-py/api/schema_v2.py` — contract validation and deterministic ID.
- `central-server-py/api/kafka_consumer.py` — v2 branch, deduplication,
  rejection handling, per-message offset commit.
- `central-server-py/tests/test_schema_v2.py`
- `central-server-py/tests/test_delivery.py`

## Message handling

One Kafka record is either:

1. a schema-v2 event object (`"schema_version": 2`),
2. a v1 event or list of events (unchanged path), or
3. malformed JSON / an invalid event.

For v2:

- `validate_event_v2` checks the required agent/collection/raw fields,
  category/source/timestamp vocabularies, raw-evidence size, heartbeat
  provenance, and **recomputes the canonical `event_id`**. The recomputation
  uses the same canonical JSON document and SHA-256 as `agent/events`; the Go
  fixtures in `agent/events/testdata` are used as compatibility tests.
- `storage_id` and the Elasticsearch `_id` are the `event_id`.
- Before feature computation the consumer atomically claims the event with
  `create(index="ids-events", id=event_id)` and a claim document containing
  `processed: false` and `claimed_at`. This replaces the old non-atomic
  `exists` check-then-insert.
- If the claim already exists and is `processed: true`, the delivery is a
  duplicate: features, failure streaks, and alert cooldowns are skipped.
  Incident analysis is still repeated because it is idempotent. Documents
  written before claims existed (no `processed` flag but with the canonical
  `storage_id`) are also treated as complete duplicates.
- If the claim exists with `processed: false` and is newer than 120 seconds,
  another consumer is still working on the event. The consumer raises
  `EventInFlight` and does not commit the offset; a supervised restart retries
  the delivery.
- A claim older than 120 seconds is treated as a crashed consumer and taken
  over by rewriting the claim.
- The final event document is written with `processed: true` in the bulk, after
  the claim.
- `raw_event` and `collection` remain in the stored document as evidence and
  provenance.
- Authentication is recognized for both the v1 `auth` and v2 `authentication`
  categories. IPv4 and IPv6 come from `source.ip`; user/outcome parsing already
  understands `raw_event.data`.

## Crash safety and offsets

Auto-commit stays disabled. For every Kafka message:

1. claim each v2 event,
2. process events and buffer Elasticsearch documents,
3. `helpers.bulk(...)`,
4. refresh `ids-events`, evaluate incident windows, and write incidents,
5. only then commit that message's offset (`offset + 1`).

A crash or Elasticsearch failure before or during the bulk leaves the offset
uncommitted, so the message is replayed. Because document IDs are stable and the
claim is atomic, replay cannot duplicate features, documents, or evidence. If
the event document was written but incident analysis failed, replay sees
`processed: true`, skips feature work, re-runs incident analysis, and commits.
This is at-least-once processing with idempotent storage, not exactly-once
delivery.

## Malformed messages and rejects

Malformed JSON is converted to a sentinel by the deserializer instead of
crashing the consumer. Invalid v2 events, unsupported schema versions, and
non-object events are written to the `ids-rejected` index and then committed.
The rejection `_id` is derived from `topic:partition:offset:kind:position`, so a
retry overwrites the same rejection document. If the rejection write itself
fails, the offset is not committed and the message is retried after restart.
No rejection is silently dropped and no infinite loop is created.

## Known limitation: Elasticsearch outage

When Elasticsearch is unreachable, the claim, bulk write, or incident analysis
raises and the consumer exits **without committing**. Under the Docker Compose
service that is recovered by `restart: unless-stopped`; under a manual run,
restart the consumer after Elasticsearch returns. The event is replayed from
Kafka. There is no in-process retry loop, so a failed process is visible rather
than silently spinning.

A crashed consumer leaves a `processed: false` claim behind. Replay within 120
seconds is deliberately deferred; after that window the claim is taken over.
This trades up to 120 seconds of recovery latency for protection against two
consumers processing the same event at once.

## Known limitation: incident window cost

Incident analysis issues window queries per authentication event. This is
intentionally simple and stateless, but it is more expensive than keeping
in-process counters and does not scale linearly with event rate. It can be
batched or cached later if throughput requires it.

## Tests

```bash
cd central-server-py
python -m unittest discover -s tests -v
```

Coverage:

- every Go v2 fixture validates and its `event_id` matches the Python
  recomputation
- tampered raw evidence or position changes the ID and is rejected
- event-ID format, missing fields, non-UTC timestamps
- IPv6 source and omitted byte offset
- v2 event is claimed before the bulk write and commits only after incident
  analysis
- a `processed: true` duplicate skips features, still runs idempotent incident
  analysis, and commits
- a fresh in-flight claim stops without committing; a stale claim is taken over
- a partial bulk write followed by replay creates no second document
- an incident-analysis failure never commits
- invalid v2 payloads and malformed JSON are rejected and committed
- claim failure and rejection-write failure never commit
- existing v1 batching and storage-failure tests still pass

## Compatibility

v1 events keep their `topic:partition:offset:position` storage IDs and existing
feature/detection path. Nothing in the v1 pipeline, Go agent, or deployment
files was changed for this milestone.
