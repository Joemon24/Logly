# Linux authentication-log collector (Phase 3)

Status: **collector package and tests only.** It reads a local file and persists
schema-v2 events through the durable outbox/checkpoint package. It is not wired
into the v1 runtime pipeline, and there is no Kafka dispatcher or Python/React
integration yet.

Package: `agent/collector`
Tests: `agent/collector/*_test.go`
Dependencies: `agent/events`, `agent/storage` (adds `Store.AdvanceCheckpoint`;
no schema changes)

## API

```go
store, _ := storage.Open(storage.Config{...})

collector, _ := collector.New(collector.Config{
    Store:    store,
    Path:     "/var/log/auth.log",
    SourceID: "auth-log",          // default
    Host:     events.Host{Hostname: "web-01"},
})

err := collector.Run(ctx)          // poll forever; defer collector.Close()
// or: n, err := collector.Poll(ctx)  // one bounded pass
```

`Config` fields:

| Field | Default | Meaning |
|---|---|---|
| `Store` | required | Durable outbox and checkpoint store. |
| `Path` | `/var/log/auth.log` | Log file. Made absolute by `New`. |
| `SourceID` | `auth-log` | Stable logical source identity. |
| `PollInterval` | 1s | Wait between `Run` polls. |
| `MaxRecordsPerPoll` | 256 | Bound on records consumed per `Poll`. |
| `Host` | hostname from `os.Hostname` | Host context attached to every event. |
| `Now` | `time.Now` | Ingest clock; injectable for tests. |

`Poll` returns the number of events persisted by that call. That value can be 0
even when the call made durable progress, because ignored lines advance the
checkpoint without emitting an event. Use the stored checkpoint to observe
physical progress. `Run` retries `ErrSourceIO` and `ErrBackpressure`; other
errors stop it so they stay observable. A missing file is treated as no work and
retried.

## Reading and checkpoint semantics

The collector reads newline-delimited records from a byte offset saved in the
SQLite `source_checkpoints` table.

- Only complete, newline-terminated lines become records during normal tailing.
  A trailing partial line stays unread until its newline arrives. While
  draining a rotated file, a final unterminated line is treated as complete.
- `record_number` is the physical ordinal of the **complete input line** within
  a source generation. Ignored lines consume a record number exactly like
  relevant lines; only relevant SSH records also produce an event. The event
  `sequence` is the same physical ordinal.
- A blank or unrelated line is not an event. It advances the durable checkpoint
  through `storage.Store.AdvanceCheckpoint`, which is a checkpoint-only
  transaction, so a restart does not rescan it.
- `byte_offset` is the start of the record's line. A relevant event advances the
  checkpoint through `Append` in the same transaction that stores the event; an
  ignored line advances it through `AdvanceCheckpoint`.
- On restart, the collector seeks to the checkpoint offset and skips exactly one
  line: the last consumed record, relevant or ignored. This prevents
  re-emitting an event with a new ingest timestamp and prevents re-advancing an
  ignored record. It then resumes at the next physical line.
- If `Append` or `AdvanceCheckpoint` fails for any reason, the in-memory
  position is not advanced. A failed `Append` keeps the exact event (same ID and
  payload) pending for retry; a failed ignored-line advance is simply retried
  from the durable checkpoint.
- A capacity failure keeps the event pending until space is acknowledged.
  Checkpoint-only advances do not consume outbox capacity.
- An empty or missing file never resets a checkpoint.

Event IDs are derived from the agent identity, source identity/generation, byte
offset, record number, and exact raw line, so re-reading the same record at the
same position yields the same ID.

## File identity and generation policy

File identity is `(device, inode)` from `stat(2)`, so renames are distinguished
from replacement files. Generations are persisted by storage; retired
generations are never reused.

| Situation | Policy |
|---|---|
| First sight of a file | Explicit transition from no generation to `file-<devhex>-<inohex>`. |
| Agent or collector restart | Resume the active generation and its checkpoint; restart is not a rotation. |
| Path now names a different inode while running | Drain the still-open old descriptor to EOF, then explicitly transition to the new file's `file-<devhex>-<inohex>` generation and start at record 1. |
| Path names a different inode after a restart | Search `path.1` through `path.9` for the old inode and drain its unread records; if it is not found, activate the new generation and return a `*CoverageGapError`. |
| Same inode shrinks below the last observed size or the current offset | Explicit truncation transition to `file-<devhex>-<inohex>-t<next-sequence>`, then restart at offset 0 and record 1. |
| Transition target was ever active or already has a checkpoint | `storage.ErrGenerationReuse`; collection stops rather than rewinding. |
| Other process changes the active generation | `storage.ErrSourceGenerationMismatch`; collection stops rather than writing into the wrong incarnation. |

Offsets are never silently reset inside a generation. A truncation creates a
new generation and therefore a fresh checkpoint.

### Rotation while the collector is running

The collector keeps the old file descriptor open. When the path gets a new
inode, it first reads all remaining newline-terminated records (and a final
unterminated line) from the old descriptor, then closes it, performs the
generation transition, and reads the replacement from offset 0. Unread records
in the rotated file are preserved as long as its descriptor is still valid,
which is the normal rename-based logrotate case.

### Rotation or truncation while the collector is stopped

On startup, if the path has a different inode from the active generation, the
collector searches the plain rotated candidates `path.1` through `path.9` for the
old inode. If found, it resumes that generation, drains its unread records to
EOF (including a final unterminated line), and then activates the replacement
generation through the normal rotation path. If the old inode is not found, the
collector still activates the replacement generation so a later restart is not
blocked, but it returns a `*CoverageGapError` once. That error unwraps to
`ErrCoverageGap` and reports the old generation, last checkpoint, and searched
paths. `Run` stops on it; a restart resumes the replacement generation.

The search intentionally covers only plain `.1`–`.9` files. Compressed rotations
(`.gz`), a deleted old file, or a different naming scheme cannot be read as text
and produce a coverage-gap error rather than a silent resume. A `copytruncate`
that happened while stopped and whose size has already regrown past the
checkpoint cannot be detected, especially when the checkpoint offset is 0.
Prefer rename/create log rotation, or coordinate a checkpoint reset with
`copytruncate`. These limits are deliberate: guessing would risk duplicating or
reusing positions and generations.

## Rejection policy

Rejected records stop the collector with a `*RejectedRecordError`. The error
carries source ID, path, generation, byte offset, next record number, a reason,
and unwraps to `ErrRejectedRecord`.

| Record | Result |
|---|---|
| Unrelated/authless line | Ignored; consumes the next physical record number, advances the checkpoint through `AdvanceCheckpoint`, and produces no event. |
| Oversized line (> `events.MaxRawEvidenceBytes`, 64 KiB including newline) | `ErrOversizedRecord` and `ErrRejectedRecord`; checkpoint not advanced. |
| Bounded SSH authentication line that cannot be parsed | Stored as `ssh_unparsed` with unknown outcome and no source or target. The outbox write advances the checkpoint atomically. |
| Line that parses but fails event validation/ID derivation | `ErrRejectedRecord`; checkpoint not advanced. |

A rejection is evaluated before the checkpoint is advanced, so an ignored line
can be checkpointed while the oversized security record after it stops
collection at its own position. Unparsed SSH evidence is searchable by action.
Invalid UTF-8 runs in this evidence are replaced with `?` to keep IDs stable
through JSON encoding.

There is no rejection ledger. The operator must inspect the error and either fix
the source, rotate/truncate it (which creates a new generation), or explicitly
remove/reinitialize the source state. The collector never skips a rejected
record or advances the checkpoint past it.

## Parsing coverage

The parser recognizes these messages at the start of a line or immediately
after a recognized OpenSSH syslog prefix:

- `Failed password|publickey|keyboard-interactive/pam for [invalid|illegal user] USER from IP port N ssh2`
- `Accepted password|publickey|keyboard-interactive/pam for USER from IP port N ssh2`
- `Invalid user USER from IP port N`

IPv4 and IPv6 addresses and the port are validated. Usernames can contain
spaces. Login messages with more than one plausible endpoint become unparsed
evidence. Parsed events use:

- category `authentication`
- action `ssh_login` (or `ssh_invalid_user` for an `Invalid user` notice)
- outcome `success` or `failure`
- `source`: IP entity
- `target`: user entity
- exact raw line in `raw_event.data`
- log timestamp when recognized (RFC3339 or syslog), otherwise ingest time

Timestamp parsing understands RFC3339Nano prefixes and `Jan _2 15:04:05`; the
syslog year is inferred from the ingest clock (future dates roll back a year).
Other sshd messages, such as disconnects and PAM session notices, are ignored.

## Backpressure

`storage.ErrCapacityReached` becomes `ErrBackpressure`. The event that could not
be persisted is retained in memory with its original payload and retried before
reading new data. `Run` waits `PollInterval` and retries; a caller using `Poll`
can wait for acknowledgements and call it again. Nothing is dropped.

Recognized ignored lines do not require outbox space. They continue to advance
the checkpoint while the outbox is full, so a full outbox does not turn
irrelevant traffic into a stalled source. A relevant event after them still
blocks until capacity is acknowledged; no security event is skipped.

## Storage failures

Any `Append` or `AdvanceCheckpoint` failure other than capacity becomes
`ErrStorage`, wrapping the original error. The checkpoint does not move; a
failed event is retained for an exact retry, and a failed ignored-line advance
is retried from the durable checkpoint. `Run` stops on `ErrStorage` so a
corrupt, closed, or full-disk database is not retried silently.

## Tests

`go test -race ./collector` covers:

- IPv4/IPv6 failed, accepted, publickey, invalid-user, and unrelated lines
- multiple appended lines, empty files, missing files, partial lines, and
  64 KiB boundary/oversized lines
- long irrelevant stretches crossing poll boundaries, with physical record
  numbers and a checkpoint-only advance for every ignored line
- restart following ignored records without rescanning or re-advancing them
- relevant/ignored interleaving with physical ordinals and deterministic IDs
- simulated crashes immediately before and after checkpoint-only persistence
- rename-based rotation while running and offline recovery from `path.1`
- offline rotation without a recoverable old file, including the coverage-gap
  error and the already-activated replacement generation
- repeated truncation into distinct `-t<N>` generations
- outbox capacity backpressure with interleaved ignored records, acknowledgement,
  and exact retry without lost or duplicated security events
- unparsed SSH evidence after ignored records, with continued collection
  and restart recovery
- oversized records without checkpoint movement past the rejection
- injected storage failures followed by an identical-payload retry
- temporary source I/O failure and recovery

## Manual Linux verification

1. Build and run the unit suite:

   ```sh
   cd agent
   go test -race ./collector -count=1
   ```

2. Read a real auth log without touching the production outbox. The gated test
   creates a throwaway database and polls a limited batch:

   ```sh
   sudo LOGLY_AUTH_LOG=/var/log/auth.log \
     LOGLY_MANUAL_DB=/tmp/logly-check/outbox.db \
     go test ./collector -run TestManualAuthLog -v -count=1
   ```

3. Exercise restart and rotation against a disposable copy:

   ```sh
   mkdir -p /tmp/logly-check
   cp /var/log/auth.log /tmp/logly-check/auth.log
   # run the gated test with LOGLY_AUTH_LOG=/tmp/logly-check/auth.log
   # then:
   mv /tmp/logly-check/auth.log /tmp/logly-check/auth.log.1
   printf '%s\n' \
     'Nov  9 10:15:30 web-01 sshd[123]: Failed password for alice from 203.0.113.9 port 50000 ssh2' \
     > /tmp/logly-check/auth.log
   # run again with LOGLY_MANUAL_DB=/tmp/logly-check/outbox.db, then inspect:
   sqlite3 /tmp/logly-check/outbox.db \
     'select event_id, source_generation, record_number, byte_offset from outbox;'
   sqlite3 /tmp/logly-check/outbox.db \
     'select source_id, generation, generation_sequence, active from source_generations;'
   ```

   The gated test logs the database path. This offline rotation verifies that a
   restart finds the old inode at `auth.log.1` and drains its unread records
   before activating the replacement generation (covered by
   `TestOfflineRotationRecoversRotatedFile`). If the rotated file is removed or
   moved outside `.1`–`.9`, the restart returns `ErrCoverageGap` once and then
   proceeds with the replacement (covered by
   `TestOfflineRotationWithoutRecoverableFileReportsCoverageGap`).

4. Verify backpressure by opening a store with `Capacity: 1`, writing two lines,
   polling once, and confirming the second event is not lost. `Ack` the first
   event and poll again.

5. Append a malformed SSH authentication line followed by a valid one. The
   collector must persist an `ssh_unparsed` record and the subsequent event,
   then resume at the next record after restart.

## Known limitations

- Offline recovery searches only plain `path.1`–`path.9`. Compressed rotations,
  deleted old files, and other naming schemes produce a one-time
  `ErrCoverageGap`; their unread bytes are not recoverable.
- A coverage gap is reported even when the old generation had no checkpoint, so
  a crash between generation binding and the first read is not hidden.
- A late write to a rotated inode after that descriptor has been drained and
  closed is not collected. Normal rename/create rotation with a reopened writer
  is the supported policy.
- `copytruncate` regrowth before the next poll can defeat size-based truncation
  detection. Rename/create rotation is the supported policy.
- `Close` releases the descriptor; an unpersisted in-memory event is re-read
  from the durable checkpoint after restart.
- Device/inode reuse is theoretically ambiguous; if a retired generation is
  offered for a reused inode, the store rejects it and collection stops.
- Incomplete final lines are deliberately deferred until a newline, except while
  draining a rotated file.
- Record numbers count physical complete lines, so event `sequence` values have
  gaps wherever ignored lines occur. There is no separate emitted-event counter.
- Each ignored line is checkpointed in its own small transaction. This keeps
  crash semantics exact but is not batched; a very high rate of irrelevant lines
  costs one commit per line.
- There is no rejection ledger, metrics endpoint, inotify integration, or Kafka
  dispatch in this phase.
- Only SSH authentication messages are parsed; sudo, PAM session, and other auth
  log records are ignored.
- The collector is single-goroutine per source and is not wired into
  `agent/pipeline`.
