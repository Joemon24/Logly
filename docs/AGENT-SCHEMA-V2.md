# Logly agent event schema v2

Status: **model and tests only.** The runtime pipeline still emits the v1 envelope
from `agent/sender/schema.go`; the v1 types are unchanged. This package is not
wired into Kafka or the central server.

Package: `agent/events`
Fixtures: `agent/events/testdata/*.json`

## Event JSON

The canonical shape is shown by the fixtures. Example
(`agent/events/testdata/authentication.json`):

```json
{
  "schema_version": 2,
  "event_id": "evt2_fe35cc8f42afd77febeb9f24205a671d4e4599cbbbf6669907b8e158fefbdb8c",
  "timestamp": "2026-11-09T10:15:30.123456789Z",
  "ingest_timestamp": "2026-11-09T10:15:30.163456789Z",
  "timestamp_source": "log",
  "sequence": 1,
  "agent": {
    "agent_id": "linux-01",
    "agent_type": "linux",
    "agent_version": "2.0.0",
    "generation": "agent-gen-7f3a9c2e"
  },
  "host": {
    "hostname": "web-01",
    "os": "ubuntu",
    "os_version": "24.04",
    "kernel": "6.8.0-45-generic"
  },
  "event": {
    "category": "authentication",
    "action": "ssh_login",
    "outcome": "failure"
  },
  "source": {
    "entity_type": "ip",
    "ip": "203.0.113.9"
  },
  "target": {
    "entity_type": "user",
    "id": "1000",
    "name": "alice"
  },
  "raw_event": {
    "format": "text",
    "data": "Failed password for alice from 203.0.113.9 port 50000 ssh2\n"
  },
  "collection": {
    "source_id": "auth-log",
    "source_type": "file",
    "path": "/var/log/auth.log",
    "generation": "source-gen-4b8d1e6a",
    "position": {
      "byte_offset": 1024,
      "record_number": 7
    }
  }
}
```

## Fields

| Field | Required | Meaning |
|---|---|---|
| `schema_version` | yes | Always `2`. |
| `event_id` | yes | Deterministic ID; see below. |
| `timestamp` | yes | Event time in UTC, RFC3339Nano. |
| `ingest_timestamp` | yes | Agent read time in UTC, RFC3339Nano. |
| `timestamp_source` | yes | `log` when parsed from the record, `ingest` when the agent clock was used. |
| `sequence` | yes | Caller-assigned ordering value. Persistence and monotonicity are a later phase. |
| `agent.agent_id` | yes | Stable agent identity. |
| `agent.agent_type` | yes | For example `linux`. |
| `agent.agent_version` | yes | Agent build version. |
| `agent.generation` | yes | Persistent agent installation identity. Must survive restarts. |
| `host.hostname` | optional | Host name when available. |
| `host.os`, `host.os_version`, `host.kernel` | optional | Host context when available. |
| `event.category` | yes | One of the six categories below. |
| `event.action` | yes | Action string, for example `ssh_login`, `sudo_command`, `process_exec`. |
| `event.outcome` | yes | Outcome string, for example `success`, `failure`, `unknown`, `modified`. |
| `source` | optional | Actor that caused the event. Omitted when unavailable. |
| `target` | optional | Object affected by the event. Omitted when unavailable. |
| `source.entity_type` / `target.entity_type` | yes when present | `user`, `ip`, `host`, `process`, `file`, `session`. |
| `source/target.id`, `.name`, `.ip`, `.path`, `.pid` | optional | Only emitted when known. |
| `raw_event.format` | yes | `text`, `json`, or `audit`. |
| `raw_event.data` | yes | Exact evidence consumed by the parser. Newlines are preserved. Maximum 64 KiB. |
| `collection.source_id` | yes | Stable logical source ID, for example `auth-log`. |
| `collection.source_type` | yes | `file`, `journald`, `audit`, or `agent`. |
| `collection.path` | optional | Filesystem path when applicable. Must be absent for heartbeats. |
| `collection.generation` | yes | Persistent source-incarnation identity. |
| `collection.position.byte_offset` | optional | Record start offset. Omitted for heartbeats and sources without byte positions. |
| `collection.position.record_number` | yes | 1-based record ordinal within the source generation. Logical occurrence number for heartbeats. |

### Categories

- `authentication`
- `sudo`
- `account`
- `process`
- `file_integrity`
- `heartbeat`

### Optional fields

`host` sub-fields, `source`, `target`, entity sub-fields,
`collection.path`, and `collection.position.byte_offset` are omitted when
empty/unavailable. Required fields are always emitted. A zero byte offset is
still emitted when it is explicitly known; omitting the field is different from
stating offset `0`.

## Event ID derivation

ID format:

```
evt2_<64 lowercase hex characters>
```

Derivation:

1. Compute `raw_sha256 = hex(SHA-256(raw_event.data))`.
2. Build this canonical JSON object. Field order and names are fixed by the Go
   struct in `agent/events/id.go`:

   ```json
   {
     "agent_id": "...",
     "agent_generation": "...",
     "source_id": "...",
     "source_type": "file",
     "source_generation": "...",
     "byte_offset": 1024,
     "record_number": 7,
     "raw_sha256": "..."
   }
   ```

   `byte_offset` is omitted from the canonical object when the event has no byte
   offset, as with heartbeats. An omitted offset and an explicit offset `0`
   therefore produce different IDs.

3. `event_id = "evt2_" + hex(SHA-256(canonical JSON))`.

The ID must be exactly `evt2_` followed by 64 lowercase hexadecimal characters.
Uppercase hex, a different prefix, or a different length is invalid.

`Event.Validate()` recomputes this ID and rejects any mismatch. This is an
internal consistency check, **not cryptographic authentication**: anyone who can
rewrite the event can also recompute the ID. Authenticity must come from
authenticated transport or a separate signature.

### Identical repeated log lines

Position, not content alone, defines identity:

- The same line at record positions 7 and 8 produces **different** IDs.
- Re-reading the same line at the same position produces the **same** ID, so
  retries and replay are deduplicable.
- If bytes at the same position change (in-place rewrite), `raw_sha256` changes
  and the ID changes.

### Generation semantics

- `agent.generation` must be created once and persisted across agent restarts.
  It changes only when the agent identity is deliberately reset.
- `collection.generation` identifies one source incarnation. It must survive
  agent restarts and change when the source is replaced (new inode/file,
  journal boot, or equivalent).
- `record_number` is 1-based within the generation for collected records.
- If a generation is regenerated on every restart, replayed records get new IDs
  and deduplication fails. Persisting generations is part of the future
  checkpoint/outbox phase and is **not implemented here**.

### Heartbeat provenance and stable IDs

Heartbeats are agent-generated, not file records. They use explicit provenance:

- `event.category` = `heartbeat`
- `collection.source_type` = `agent`
- `collection.source_id` = a stable logical ID such as `agent-heartbeat`
- `collection.path` absent
- `collection.position.byte_offset` absent
- `collection.position.record_number` = a logical occurrence number
- `timestamp_source` = `ingest`
- `source` and `target` absent

`record_number` must be unique per heartbeat occurrence and stable when the same
event is retried or replayed. The recommended value is the heartbeat timestamp's
UTC Unix nanoseconds (`timestamp.UnixNano()`), which needs no file offset and no
new storage. A persisted monotonic heartbeat counter is equally valid. Do not
reset it to `1` on every restart without persisting the counter: distinct
heartbeats could then collide.

The future durable outbox should persist the finalized event, including its
`event_id` and provenance, and replay those bytes unchanged. It must not
recompute a new heartbeat ID on replay. This keeps IDs stable across retries and
restarts without fabricated file offsets.

## Raw evidence limit and rejected records

`MaxRawEvidenceBytes` is `64 * 1024` (64 KiB). Both `Event.Validate()` and
`GenerateEventID()` reject larger `raw_event.data`.

The future collector must check the size before assigning an ID or appending to
the outbox. Oversized records must not be silently skipped:

- Do not advance the source checkpoint past a rejected record.
- Either stop and alert for operator action, or durably record a rejection
  decision (source, generation, position, reason) before advancing.
- Never hash, store, or send the oversized payload as a normal event.

This preserves at-least-once recovery: a crash or restart re-reads the rejected
position instead of losing it.

## Timestamps

`time.Time` fields are encoded by the standard library as RFC3339Nano. The
model requires UTC (`Z`) values. Fractional seconds are omitted only when they
are zero; trailing zeros are trimmed by RFC3339Nano.

## Validation

`Event.Validate()` checks:

- schema version and exact lowercase event ID format
- UTC non-zero timestamps
- valid timestamp source, category, source type, raw format, entity types
- required agent/source/generation identifiers
- positive record number, non-negative optional byte offset/PID
- `raw_event.data` at most 64 KiB
- explicit heartbeat provenance (agent source, ingest time, no path, no byte
  offset, no source/target)
- that the recomputed deterministic ID equals `event_id`

The ID comparison is an internal consistency check, not cryptographic
authentication. Validation does not verify semantic action/outcome vocabularies,
whether the raw evidence matches the parsed fields, or whether the event is
truthful.

## Limitations

- Not wired into the runtime, Kafka, or the Python consumer. Schema v1 types
  remain authoritative until a later migration.
- Generations and positions are caller-supplied; no persistence, checkpoint, or
  outbox exists yet.
- IDs are only as stable as the supplied agent/source generations and positions.
- Sources without byte offsets omit `collection.position.byte_offset` and rely
  on `record_number`.
- Heartbeats rely on a unique logical `record_number` (recommended UnixNano).
  Clock rollback can theoretically reuse an occurrence number; a persisted
  counter avoids that.
- Raw evidence over 64 KiB is rejected; collector-side rejection/checkpoint
  behavior is documented but not implemented.
- Action and outcome are free-form strings; there is no enforced vocabulary.
- No deduplication is performed by this package. Consumers must deduplicate on
  `event_id` if replay can occur.
- The ID is an internal consistency check only, not an authenticity proof.
- No schema registry or cross-version reader is included; v1 is not converted.
- Clock skew between event and ingest clocks is not rejected.
- ID uniqueness assumes globally unique `agent_id` + `agent.generation` and a
  stable `collection.source_id` + `collection.generation`.
- The canonical ID document is encoded with Go's `encoding/json`. Identifiers
  should be ASCII-safe so other implementations produce identical bytes.
- v2 keeps the Python-compatible `raw_event` and `entity_type` names. It still
  differs from v1 in its flattened `host`, `sequence`, and `collection` blocks,
  so Python and dashboards need an explicit migration before v2 is wired in.

## Tests

- `event_test.go`: validation, ID mismatch rejection, uppercase ID rejection,
  raw-size boundary rejection, heartbeat provenance and stable IDs, category
  support, RFC3339Nano output, optional field omission, JSON round-trip.
- `id_test.go`: determinism, repeated-line distinction, omitted-vs-zero byte
  offset, oversized payload rejection, coordinate sensitivity, required inputs,
  exact ID format, `ComputeEventID` equivalence.
- `contract_test.go`: every category fixture decodes strictly, validates,
  recomputes its ID, has all required keys, uses Python-compatible `raw_event`
  and `entity_type` names, uses canonical UTC RFC3339Nano timestamps, verifies
  heartbeat provenance, and round-trips to equivalent JSON.
