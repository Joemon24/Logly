# Agent durable outbox (Phase 2)

Status: **storage package and tests only.** Nothing in the v1 runtime pipeline
uses this package yet. Kafka, Python, Docker, deployment files, and v1 code are
unchanged.

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

The database contains three tables. Event payloads are stored as JSON bytes in
the `outbox` table; source positions live in `source_checkpoints`; identity and
the heartbeat counter live in the single-row `agent_metadata` table.

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

CREATE INDEX outbox_source_position
    ON outbox (source_id, source_generation, record_number);
```

`seq` is the insertion order and the deterministic replay order. It is never
reused, even after acknowledgement. `event_id` is unique.

## Transaction boundaries

### `Append(ctx, batch)`

One `BEGIN IMMEDIATE` transaction covers the entire batch:

1. Validate every event with `events.Event.Validate()`, including the 64 KiB
   raw-evidence limit and the recomputed event ID.
2. Reject events whose `agent.agent_id` or `agent.generation` differs from the
   identity in `agent_metadata`.
3. For each event ID, compare with the stored payload:
   - same ID and same bytes: idempotent duplicate, no second row;
   - same ID and different bytes: `ErrEventIDConflict`.
4. Check the source checkpoint. A new position must be strictly after the
   stored position. Positions are ordered by `byte_offset` when present and by
   `record_number` otherwise. Mixing the two forms for one source is rejected.
   The same check applies within the batch, so an out-of-order batch fails.
5. Insert the original serialized bytes and advance each source checkpoint to
   the last position in the batch.
6. Enforce capacity before inserting additional rows.

Any failure rolls the whole transaction back, including checkpoint changes.
There is no partial batch.

### `Ack(ctx, eventIDs)`

Deletes outbox rows in one transaction. The caller must call `Ack` **only after
Kafka confirms delivery**. This package cannot verify delivery and does not
track Kafka offsets. Acknowledging an unknown ID is a no-op, so retried
acknowledgements after a crash are safe. `Ack` never changes source
checkpoints.

Acknowledged events are gone. Replaying an old position after acknowledgement
is rejected by the checkpoint (`ErrCheckpointConflict`), not silently
deduplicated. The collector must resume reading after the stored checkpoint.
Kafka consumers should still deduplicate on `event_id` for the at-least-once
window.

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

`Open` verifies the file with `PRAGMA quick_check`, creates missing tables,
then initializes or checks `agent_metadata` in a transaction. A stored
`agent_id` that differs from the config returns `ErrAgentIdentityMismatch`
instead of silently mixing two installations in one outbox. `agent_type` and
`agent_version` are refreshed on open; `generation` is created once and never
changes.

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
- `Open` runs `quick_check`, not a full `integrity_check`. Use an offline
  backup/verification procedure if stronger checking is required.
- The database must live on a local filesystem that supports SQLite WAL and
  durable synchronous writes. Network filesystems are not supported.
- `Append` with an empty batch is a no-op.

## Ordering and replay

`Pending(ctx, limit)` returns rows in ascending `seq` order. `limit <= 0`
returns all pending events. Before returning a row, the store unmarshals the
payload, checks that the embedded `event_id` matches the row, and validates the
event again. A damaged payload returns `ErrCorruptPayload` and is not replayed.

Replay returns the stored payload bytes unchanged. It does not regenerate IDs
or timestamps. This is what keeps events stable across process restarts,
network retries, and dispatcher restarts.

## Duplicate, checkpoint, size, and position rules

| Situation | Result |
|---|---|
| Event ID already pending with identical bytes | Idempotent success; no new row |
| Event ID already present with different bytes | `ErrEventIDConflict` |
| Event ID already acknowledged (row deleted) and position at/below checkpoint | `ErrCheckpointConflict` |
| Event position equal to or behind the source checkpoint | `ErrCheckpointConflict` |
| Batch positions not strictly increasing | `ErrCheckpointConflict` |
| Same source ID with offsets in one event and none in another | `ErrCheckpointConflict` |
| New `collection.generation` | Starts a fresh checkpoint |
| Event raw evidence over `events.MaxRawEvidenceBytes` (64 KiB) | `ErrOversizedEvent` |
| Invalid event, ID mismatch, non-UTC timestamp, etc. | `ErrInvalidEvent` |
| Agent ID/generation differs from the database identity | `ErrAgentIdentityMismatch` |
| Pending count would exceed capacity | `ErrCapacityReached` |

Sources such as journald without byte positions store `byte_offset = NULL` and
advance the checkpoint by `record_number`. File/audit sources use byte offsets.
A checkpoint is keyed by `(source_id, generation)`, so a new source incarnation
does not conflict with the old one.

## Oversized and rejected records

`agent/events` rejects raw evidence larger than 64 KiB. `Append` checks the same
limit before validation and returns `ErrOversizedEvent`; the entire batch rolls
back. The store does **not** have a rejection ledger or policy engine.

The future collector must not silently advance the checkpoint past a rejected
record. It must either stop and alert for operator action, or durably record a
rejection decision (source ID, generation, position, reason) before advancing.
Never hash, store, or send an oversized payload as a normal event.

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
- `ErrCheckpointConflict` — position, ordering, or source-type conflict.
- `ErrCapacityReached` — pending capacity would be exceeded.
- `ErrAgentIdentityMismatch` — config or event belongs to another agent.
- `ErrOversizedEvent` — raw evidence exceeds the schema limit.
- `ErrInvalidEvent` — schema-v2 validation failed.
- `ErrInvalidConfig` — missing path/identity or negative capacity.

## Recovery assumptions

- SQLite's atomic commit, WAL, and `synchronous=FULL` are trusted for
  committed transactions.
- A crash before commit leaves no outbox row and no checkpoint change.
- A crash after commit leaves the event pending. It will be replayed and may be
  redelivered, hence at-least-once delivery.
- A checkpoint is always at or ahead of pending events from the same source
  because they are written in the same transaction.
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
- `Ack` is manual; a future dispatcher owns the Kafka-confirmation decision.
- Heartbeat sequence is a logical counter only. The collector must embed it in
  a heartbeat event and persist that event through the outbox.
- Busy timeouts can still surface `SQLITE_BUSY` under long contention.

Phase 3, not started here, must:

1. Make the collector use `Store.Identity()` and `NextHeartbeatSequence()`.
2. Replace the item-at-a-time send path with: `Pending` → Kafka → `Ack` only
   after broker confirmation.
3. Add retry/backoff for `ErrDiskFull`, `SQLITE_BUSY`, and capacity pressure.
4. Add metrics for pending count, oldest pending age, ack failures, and
   checkpoint lag.
5. Define and implement the oversized-record rejection policy.
6. Add graceful shutdown that stops collection before closing the store.
7. Add an integration test with the real Kafka client and the v2 event model.

## Test coverage

`go test -race ./storage` covers:

- atomic event and checkpoint persistence across multiple sources
- full rollback for invalid, conflicting, oversized, and over-capacity batches
- idempotent duplicate submission and conflicting duplicate payloads
- replay across close/reopen with byte-identical payloads
- deterministic pending order, limits, and partial acknowledgement
- capacity enforcement and reuse after acknowledgement
- checkpoint conflicts, new generations, and no-byte-offset sources
- concurrent appends and concurrent heartbeat sequence allocation
- persistent identity/generation and version refresh
- persistent monotonic heartbeat sequence
- active WAL/FULL/busy-timeout pragmas and the single-connection pool
- database file and directory permissions
- corrupt stored payload rejection
- corrupt database rejection on open
- disk-full error mapping
