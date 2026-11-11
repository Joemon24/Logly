# Agent durable outbox (Phase 2)

Status: **storage package and tests only.** The Phase 3 authentication-log
collector (`agent/collector`, see `AGENT-AUTH-COLLECTOR.md`) writes to this
package and the Phase 4 Kafka dispatcher (`agent/dispatcher`, see
`AGENT-KAFKA-DISPATCHER.md`) drains it, but nothing in the v1 runtime pipeline
uses it yet. Python, Docker, deployment files, and v1 code are unchanged.

Package: `agent/storage`
Tests: `agent/storage/*_test.go`
Schema model: `agent/events` (schema v2, unchanged)

## Driver

`modernc.org/sqlite` v1.38.2 is a maintained, pure-Go SQLite driver. It avoids
CGO and supports Linux amd64/arm64 and Apple Silicon. The package imports the
driver normally, so `sql.Open("sqlite", ...)` is registered. The storage package
was cross-compiled with `CGO_ENABLED=0` for linux/amd64, linux/arm64, and
darwin/arm64.

## Database layout

The database contains four tables. Event payloads are stored as JSON bytes in
the `outbox` table; source positions live in `source_checkpoints`; the active
incarnation of each source lives in `source_generations`; identity and the
heartbeat counter live in the single-row `agent_metadata` table.

```sql
CREATE TABLE agent_metadata (
    id                 INTEGER PRIMARY KEY CHECK (id = 1),
    agent_id           TEXT NOT NULL,
    agent_type         TEXT NOT NULL,
    agent_version      TEXT NOT NULL,
    generation         TEXT NOT NULL,
    heartbeat_sequence INTEGER NOT NULL DEFAULT 0 CHECK (heartbeat_sequence >= 0),
    created_at         TEXT NOT NULL
);

CREATE TABLE source_checkpoints (
    source_id     TEXT NOT NULL,
    generation    TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    byte_offset   INTEGER,          -- NULL for sources without byte offsets
    record_number INTEGER NOT NULL CHECK (record_number > 0),
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (source_id, generation)
);

CREATE TABLE outbox (
    seq               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    source_id         TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    record_number     INTEGER NOT NULL,
    byte_offset       INTEGER,      -- NULL when the event has no byte offset
    payload           BLOB NOT NULL,
    stored_at         TEXT NOT NULL
);

CREATE TABLE source_generations (
    source_id           TEXT NOT NULL,
    generation          TEXT NOT NULL,
    source_type         TEXT NOT NULL,
    generation_sequence INTEGER NOT NULL CHECK (generation_sequence > 0),
    active              INTEGER NOT NULL CHECK (active IN (0, 1)),
    created_at          TEXT NOT NULL,
    retired_at          TEXT,       -- NULL while active
    PRIMARY KEY (source_id, generation),
    UNIQUE (source_id, generation_sequence)
);

CREATE INDEX outbox_source_position
    ON outbox (source_id, source_generation, record_number);

CREATE UNIQUE INDEX source_generations_one_active
    ON source_generations (source_id) WHERE active = 1;
```

`seq` is the insertion order and the deterministic replay order. It is never
reused, even after acknowledgement. `event_id` is unique.

`PRAGMA user_version` records the schema version. Version 2 is the current
layout. Version 0/1 databases (the original Phase 2 schema without
`source_generations`) are upgraded in place on `Open`: every generation that
already has a checkpoint is inserted with an increasing
`generation_sequence`, ordered by its checkpoint's `updated_at`, and the most
recently updated generation becomes active. An unknown newer version returns
`ErrUnsupportedSchema` instead of being modified.

## Transaction boundaries

### `Append(ctx, batch)`

One `BEGIN IMMEDIATE` transaction covers the entire batch:

1. Validate every event with `events.Event.Validate()`, including the 64 KiB
   raw-evidence limit and the recomputed event ID.
2. Reject events whose `agent.agent_id` or `agent.generation` differs from the
   identity in `agent_metadata`.
3. Bind or validate the event's source generation. First use of a previously
   unseen source binds `collection.generation` as generation sequence 1 inside
   the same transaction. Later events must match the persisted active
   generation; a different generation returns `ErrSourceGenerationMismatch`
   until `TransitionSourceGeneration` is called. See "Source generations".
4. For each event ID, compare with the stored payload:
   - same ID and same bytes: idempotent duplicate, no second row;
   - same ID and different bytes: `ErrEventIDConflict`.
5. Check the source checkpoint using the strict ordering described below. The
   same check applies within the batch, so an out-of-order batch fails.
6. Insert the original serialized bytes and advance each source checkpoint to
   the last position in the batch.
7. Enforce capacity before inserting additional rows.

Any failure rolls the whole transaction back, including checkpoint changes and
first-use generation bindings. There is no partial batch.

### `AdvanceCheckpoint(ctx, sourceID, sourceType, generation, position)`

Advances a source checkpoint without inserting an outbox row. It is for records
the collector deliberately did not emit, such as unrelated authentication-log
lines. One transaction:

1. Require the generation to be the active one for the source, reusing the same
   validation as `Append`.
2. Apply the strict checkpoint ordering below.
3. Update `source_checkpoints.updated_at` and commit.

It does not touch the outbox or consume outbox capacity, so an ignored line can
still be checkpointed while the outbox is full. It must never be used to step
over a malformed, oversized, or otherwise rejected security record; the
collector stops on those before calling it.

### `Ack(ctx, eventIDs)`

Deletes outbox rows in one transaction. The caller must call `Ack` **only after
Kafka confirms delivery**. This package cannot verify delivery and does not
track Kafka offsets. Acknowledging an unknown ID is a no-op, so retried
acknowledgements after a crash are safe. `Ack` never changes source
checkpoints.

Acknowledged events are gone. Replaying an old position after acknowledgement
is rejected by the checkpoint with `ErrStalePosition`, wrapped by
`ErrCheckpointConflict`, not silently deduplicated. The collector must resume
reading after the stored checkpoint. Kafka consumers should still deduplicate on
`event_id` for the at-least-once window.

## Source generations

A generation identifies one incarnation of a logical source, such as a rotated
file or a new boot ID. It is persisted, not inferred from the event stream.

- `ActiveGeneration(ctx, sourceID)` returns the active generation, its source
  type, and its sequence, or `found == false` for a source never seen.
- `TransitionSourceGeneration(ctx, sourceID, sourceType, expectedGeneration,
  nextGeneration)` performs an explicit rotation. `expectedGeneration` must be
  the current active generation, which prevents two writers from rotating the
  same source from a stale view. The first initialization uses `""`.
- Repeating a transition with `expectedGeneration == nextGeneration ==` the
  active generation is an idempotent no-op. This is how a process restart keeps
  the same generation; restart is not a rotation.
- A generation that was ever active cannot be activated again. It remains in
  `source_generations` with `active = 0` and a `retired_at` timestamp, so
  rollback and accidental reuse return `ErrGenerationReuse`.
- A stale expected generation, a source-type change, or an attempt to
  initialize an already-bound source returns `ErrGenerationConflict`.
- `Append` binds a first-use generation automatically, but it never rotates an
  established source. Rotation must be an explicit transition before events
  from the new incarnation are accepted.
- The generation transition and the events/checkpoints that depend on it are
  separate transactions by design. Perform the transition first, then append
  events for the new generation; a crash between them leaves the new
  generation active with no checkpoint, which is safe. Never append a new
  generation before its transition succeeds.

### `NextHeartbeatSequence(ctx)`

A single atomic SQL statement:

```sql
UPDATE agent_metadata SET heartbeat_sequence = heartbeat_sequence + 1
WHERE id = 1 RETURNING heartbeat_sequence;
```

The first call returns `1`; the value survives restarts and concurrent calls
see distinct values. The collector must use this value as the heartbeat
event's `collection.position.record_number` before computing its event ID. The
store does not fabricate an event or a file offset.

### `Open`

`Open` verifies the file with `PRAGMA quick_check`, migrates the schema to the
current `PRAGMA user_version`, creates missing tables, verifies the durability
pragmas, then initializes or checks `agent_metadata` in a transaction. A stored
`agent_id` that differs from the config returns `ErrAgentIdentityMismatch`
instead of silently mixing two installations in one outbox. An empty stored
generation returns `ErrCorrupt`. `agent_type` and `agent_version` are refreshed
on open; `generation` is created once and never changes. `Open` also fails with
`ErrDurability` if WAL, `synchronous=FULL`, or the 5000 ms busy timeout is not
actually active on the connection.

## Durability policy

- Journal mode: WAL (`journal_mode=WAL`).
- Synchronization: `synchronous=FULL`. A committed transaction is durable
  across process crashes and normal power loss, subject to the filesystem and
  hardware honoring flush semantics.
- Busy timeout: 5000 ms (`busy_timeout(5000)`).
- Transaction lock: `_txlock=immediate`, so a write transaction takes the
  write lock at `BEGIN` instead of failing during an upgrade.
- Connection pool: one open and one idle connection per `Store`. This
  serializes writes within a process and keeps per-connection pragmas fixed.
  Multiple processes can still open the same file; SQLite's busy timeout and
  write lock serialize them. A long-running writer can still return
  `SQLITE_BUSY`; callers should retry.
- File permissions: the database file is created and forced to `0600`; a
  missing parent directory is created with `0700`. WAL/SHM sidecars are
  tightened to `0600` on a best-effort basis when they exist. SQLite normally
  inherits the main file's mode.
- `Open` verifies the effective `journal_mode`, `synchronous`, and
  `busy_timeout` values and fails with `ErrDurability` if they differ from this
  policy.
- `Open` runs `quick_check`, not a full `integrity_check`. Use an offline
  backup/verification procedure if stronger checking is required.
- The database must live on a local filesystem that supports SQLite WAL and
  durable synchronous writes. Network filesystems are not supported.
- `Append` with an empty batch is a no-op.

## Ordering and replay

`Pending(ctx, limit)` returns up to `limit` rows in ascending `seq` order. The
limit must be in `1..MaxPendingLimit` (`MaxPendingLimit` is 1000); anything else
returns `ErrInvalidLimit`. There is no unbounded read, so a dispatcher cannot
accidentally load the whole outbox. Before returning a row, the store unmarshals
the payload, checks that the embedded `event_id` matches the row, and validates
the event again. A damaged payload returns `ErrCorruptPayload` and is not
replayed.

Replay returns the stored payload bytes unchanged. It does not regenerate IDs
or timestamps. This is what keeps events stable across process restarts,
network retries, and dispatcher restarts.

## Duplicate, checkpoint, size, and position rules

| Situation | Result |
|---|---|
| Event ID already pending with identical bytes | Idempotent success; no new row |
| Event ID already present with different bytes | `ErrEventIDConflict` |
| Event ID already acknowledged (row deleted) and position at/below checkpoint | `ErrStalePosition` (`ErrCheckpointConflict`) |
| Event position equal to or behind the source checkpoint | `ErrStalePosition` (`ErrCheckpointConflict`) |
| Byte offset and record number disagree about direction | `ErrCheckpointConflict` (not `ErrStalePosition`) |
| Batch positions not strictly increasing | `ErrCheckpointConflict` |
| Same source ID with offsets in one event and none in another | `ErrCheckpointConflict` |
| Event generation differs from the active generation | `ErrSourceGenerationMismatch` |
| Explicit transition to a retired generation | `ErrGenerationReuse` |
| Explicit transition with a stale expectation or changed source type | `ErrGenerationConflict` |
| Event raw evidence over `events.MaxRawEvidenceBytes` (64 KiB) | `ErrOversizedEvent` |
| Invalid event, ID mismatch, non-UTC timestamp, etc. | `ErrInvalidEvent` |
| Agent ID/generation differs from the database identity | `ErrAgentIdentityMismatch` |
| Pending count would exceed capacity | `ErrCapacityReached` |
| `Pending` limit outside `1..MaxPendingLimit` | `ErrInvalidLimit` |

### Authoritative checkpoint ordering

A position must advance strictly within one `(source_id, generation)`:

- **Byte-based sources** (`byte_offset` present): both `byte_offset` and
  `record_number` must increase. If one increases while the other stays equal
  or decreases, the position is contradictory and returns
  `ErrCheckpointConflict`. If both are equal or lower, it is stale and returns
  `ErrStalePosition`.
- **Record-number sources** (`byte_offset` NULL, such as journald): only
  `record_number` is authoritative and must increase. Equal or lower returns
  `ErrStalePosition`.
- Mixing byte-offset and record-number positions for one source is
  contradictory (`ErrCheckpointConflict`).

Gaps are allowed. The ordering is monotonic, not contiguous, because a collector
may legitimately skip source records that produce no event. The same rule is
applied within one batch.

A checkpoint is keyed by `(source_id, generation)`. A new generation starts a
fresh checkpoint, but it must be activated explicitly first (see "Source
generations"). Events in a retired generation are rejected even when their
position would otherwise be valid.

## Oversized and rejected records

`agent/events` rejects raw evidence larger than 64 KiB. `Append` checks the same
limit before validation and returns `ErrOversizedEvent`; the entire batch rolls
back. The store does **not** have a rejection ledger or policy engine.

The collector must not silently advance the checkpoint past a rejected record.
`agent/collector` stops with a typed rejection and leaves the checkpoint at the
previous record. It advances only recognized irrelevant records (through
`AdvanceCheckpoint`) or validated events (through `Append`). Never hash, store,
or send an oversized payload as a normal event.

## Capacity and backpressure

`Config.Capacity` is the maximum number of pending events. Zero selects
`DefaultCapacity` (10,000); negative values are rejected. The store never
evicts old events. When the outbox is full, `Append` returns
`ErrCapacityReached` and the collector must stop reading or apply backpressure
until the dispatcher acknowledges events. Duplicate submissions do not consume
additional capacity.

## Errors

Use `errors.Is` with these sentinels:

- `ErrDiskFull` — SQLite `SQLITE_FULL` or an OS `ENOSPC`; the transaction was
  rolled back, retry after freeing space.
- `ErrCorrupt` — SQLite `SQLITE_CORRUPT`/`SQLITE_NOTADB` or a failed
  `quick_check`; stop and escalate rather than discarding data.
- `ErrCorruptPayload` — a stored payload cannot be decoded, no longer matches
  its event ID, or no longer validates.
- `ErrEventIDConflict` — same ID with different bytes.
- `ErrCheckpointConflict` — position, ordering, source-type, or generation
  conflict.
- `ErrStalePosition` — position at or behind the checkpoint (also wrapped by
  `ErrCheckpointConflict`). Recognisable after acknowledgement so a collector
  can treat an already-checkpointed record as already collected.
- `ErrSourceGenerationMismatch` — the event's generation is not the active one.
- `ErrGenerationConflict` — stale transition expectation, source-type change,
  or invalid initialization.
- `ErrGenerationReuse` — an attempt to activate a retired generation.
- `ErrCapacityReached` — pending capacity would be exceeded.
- `ErrAgentIdentityMismatch` — config or event belongs to another agent.
- `ErrOversizedEvent` — raw evidence exceeds the schema limit.
- `ErrInvalidEvent` — schema-v2 validation failed.
- `ErrInvalidConfig` — missing path/identity or negative capacity.
- `ErrInvalidLimit` — `Pending` limit outside `1..MaxPendingLimit`.
- `ErrUnsupportedSchema` — database `PRAGMA user_version` is newer than this
  binary supports.
- `ErrDurability` — WAL, `synchronous=FULL`, or busy timeout is not active.

## Recovery assumptions

- SQLite's atomic commit, WAL, and `synchronous=FULL` are trusted for
  committed transactions.
- A crash before commit leaves no outbox row and no checkpoint change.
- A crash after commit leaves the event pending. It will be replayed and may be
  redelivered, hence at-least-once delivery.
- A checkpoint is always at or ahead of pending events from the same source
  because they are written in the same transaction.
- A first-use generation binding is rolled back with the batch that created it,
  so a failed append never leaves a phantom active generation.
- An explicit transition commits before the first event of the new generation.
  A crash in between leaves an active generation with no checkpoint; the
  collector simply starts that generation from its first record. A crash before
  the transition leaves the old generation active, so new-generation events are
  rejected until the transition is retried.
- Acknowledgement happens after Kafka confirmation and only removes outbox
  rows. It never rewinds a checkpoint.
- Corrupt payloads and corrupt databases are surfaced to the caller. The code
  never deletes or rewrites a suspect event automatically.
- The agent identity is tied to the database file. Deleting the file creates a
  new `generation` and a new outbox.

## Limits and remaining integration work

Known limits:

- One database file per agent; no encryption at rest and no backup tooling.
- No automatic retention or compaction beyond row deletion on acknowledgement.
- Payloads are validated against schema v2 only; no forward-compatible reader.
- The capacity check is per `Store` instance/process, backed by the database
  count; cross-process writers still serialize through SQLite.
- No rejection ledger, metrics, or alerting.
- `Ack` is manual; the Phase 4 dispatcher owns the Kafka-confirmation decision.
- Heartbeat sequence is a logical counter only. The collector must embed it in
  a heartbeat event and persist that event through the outbox.
- Generation rotation is manual. The store does not detect rotation from file
  metadata; the collector must call `TransitionSourceGeneration` when it
  observes a new incarnation.
- Migration assumes at most one active generation per source and that
  `updated_at` reflects checkpoint recency. It cannot reconstruct rotation order
  beyond that ordering.
- `Pending` returns at most `MaxPendingLimit` events per call; a dispatcher
  drains the outbox in repeated batches.
- Busy timeouts can still surface `SQLITE_BUSY` under long contention.

The authentication-log collector in `agent/collector` covers the collection,
identity, checkpoint, rejection-policy, and bounded-read parts of Phase 3. The
Kafka dispatcher in `agent/dispatcher` provides `Pending` → Kafka → `Ack` after
broker confirmation, bounded retry/backoff, in-process counters (pending,
oldest pending age, acknowledgements, failures), graceful shutdown, and an
opt-in real-Kafka integration test. See `AGENT-KAFKA-DISPATCHER.md`.

Remaining integration work:

1. Wire the collector, dispatcher, and store lifecycle into the agent runtime
   with explicit Kafka broker/topic configuration.
2. Add a metrics endpoint/exporter and rejection ledger.
3. Connect the Python consumer and deduplicate on `event_id`.

## Test coverage

`go test -race ./storage` covers:

- atomic event and checkpoint persistence across multiple sources
- full rollback for invalid, conflicting, oversized, and over-capacity batches
- idempotent duplicate submission and conflicting duplicate payloads
- replay across close/reopen with byte-identical payloads
- deterministic pending order, limits, and partial acknowledgement
- pending count and oldest stored-at statistics
- capacity enforcement and reuse after acknowledgement
- strict byte-offset and record-number checkpoint progression, including
  duplicate, stale, skipped, contradictory, and mixed positions
- explicit generation lifecycle: first-use binding, restart idempotence,
  rotation, stale expectations, source-type changes, and retired-generation
  reuse
- rollback of first-use generation bindings with a failed batch
- schema migration/backfill from version 0 and rejection of a newer version
- bounded `Pending` limits, including rejection of zero, negative, and
  oversized limits
- post-acknowledgement replay returning `ErrStalePosition` without reinsertion
- checkpoint conflicts, new generations, and no-byte-offset sources
- concurrent appends, concurrent first-use generation binding, concurrent
  transitions (one winner), and concurrent heartbeat sequence allocation
- persistent identity/generation and version refresh
- persistent monotonic heartbeat sequence
- active WAL/FULL/busy-timeout pragmas and the single-connection pool
- database file and directory permissions
- corrupt stored payload rejection
- corrupt database rejection on open
- disk-full error mapping
