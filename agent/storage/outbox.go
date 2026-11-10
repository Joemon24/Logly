package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"agent/events"
)

// OutboxEntry is one pending event. Payload is the exact serialized event that
// was stored. Replay must send these bytes unchanged.
type OutboxEntry struct {
	EventID string
	Payload []byte
}

// Append atomically stores a batch of validated events and advances the
// source checkpoint for every source in the batch.
//
// There is no partial success: if any event is invalid, duplicates an existing
// ID with different payload, conflicts with a checkpoint, or would exceed the
// configured capacity, nothing is written.
func (s *Store) Append(ctx context.Context, batch []events.Event) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mapError(err)
	}
	defer tx.Rollback()

	pending, err := pendingCount(ctx, tx)
	if err != nil {
		return err
	}

	type sourceKey struct {
		id         string
		generation string
	}
	states := make(map[sourceKey]*checkpointState)
	activeGenerations := make(map[string]SourceGeneration)
	seen := make(map[string][]byte, len(batch))
	newEvents := 0
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for i := range batch {
		event := batch[i]
		if err := s.validateEvent(event); err != nil {
			return err
		}
		if err := ensureActiveGeneration(ctx, tx, activeGenerations,
			event.Collection.SourceID, event.Collection.SourceType,
			event.Collection.Generation); err != nil {
			return err
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("storage: encode event %q: %w", event.EventID, err)
		}

		if previous, ok := seen[event.EventID]; ok {
			if !bytes.Equal(previous, payload) {
				return fmt.Errorf("%w: event %q appears with different payloads in one batch",
					ErrEventIDConflict, event.EventID)
			}
			continue
		}
		seen[event.EventID] = payload

		key := sourceKey{id: event.Collection.SourceID, generation: event.Collection.Generation}
		state := states[key]
		if state == nil {
			state, err = loadCheckpoint(ctx, tx, key.id, key.generation)
			if err != nil {
				return err
			}
			states[key] = state
		}

		stored, err := storedPayload(ctx, tx, event.EventID)
		if err != nil {
			return err
		}
		if stored != nil {
			if !bytes.Equal(stored, payload) {
				return fmt.Errorf("%w: event %q is already stored with a different payload",
					ErrEventIDConflict, event.EventID)
			}
			// Idempotent duplicate. It normally sits at or behind the
			// checkpoint, but accepting it also repairs a missing checkpoint.
			if err := state.accept(event.Collection.SourceType, event.Collection.Position, true); err != nil {
				return err
			}
			continue
		}

		if err := state.accept(event.Collection.SourceType, event.Collection.Position, false); err != nil {
			return err
		}

		if pending+newEvents+1 > s.capacity {
			return fmt.Errorf("%w: %d pending + %d new would exceed capacity %d",
				ErrCapacityReached, pending, newEvents+1, s.capacity)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO outbox
			 (event_id, source_id, source_generation, record_number, byte_offset, payload, stored_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			event.EventID,
			event.Collection.SourceID,
			event.Collection.Generation,
			int64(event.Collection.Position.RecordNumber),
			nullableInt64(event.Collection.Position.ByteOffset),
			payload,
			now); err != nil {
			return mapError(err)
		}
		newEvents++
	}

	for key, state := range states {
		if !state.changed {
			continue
		}
		if err := state.save(ctx, tx, key.id, key.generation, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Store) validateEvent(event events.Event) error {
	if len(event.Raw.Data) > events.MaxRawEvidenceBytes {
		return fmt.Errorf("%w: event %q raw_event.data is %d bytes (max %d)",
			ErrOversizedEvent, event.EventID, len(event.Raw.Data), events.MaxRawEvidenceBytes)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if event.Agent.AgentID != s.identity.AgentID || event.Agent.Generation != s.identity.Generation {
		return fmt.Errorf("%w: event agent %q/%q does not match store %q/%q",
			ErrAgentIdentityMismatch,
			event.Agent.AgentID, event.Agent.Generation,
			s.identity.AgentID, s.identity.Generation)
	}
	if event.Collection.Position.RecordNumber > math.MaxInt64 {
		return fmt.Errorf("%w: event %q record_number exceeds the SQLite INTEGER range",
			ErrInvalidEvent, event.EventID)
	}
	return nil
}

// Pending returns up to limit stored events in insertion order. The limit must
// be positive and no larger than MaxPendingLimit; this prevents an accidental
// unbounded dispatcher read. Each payload is validated before it is returned; a
// damaged payload yields ErrCorruptPayload instead of being replayed.
func (s *Store) Pending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	if limit < 1 || limit > MaxPendingLimit {
		return nil, fmt.Errorf("%w: got %d, want 1..%d",
			ErrInvalidLimit, limit, MaxPendingLimit)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, event_id, payload FROM outbox ORDER BY seq LIMIT ?`, limit)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var (
			seq     int64
			eventID string
			payload []byte
		)
		if err := rows.Scan(&seq, &eventID, &payload); err != nil {
			return nil, mapError(err)
		}
		if err := validateStoredPayload(eventID, payload); err != nil {
			return nil, fmt.Errorf("%w: event %q (seq %d): %v", ErrCorruptPayload, eventID, seq, err)
		}
		entries = append(entries, OutboxEntry{EventID: eventID, Payload: payload})
	}
	if err := rows.Err(); err != nil {
		return nil, mapError(err)
	}
	return entries, nil
}

// Ack deletes delivered events. The caller must only call it after Kafka has
// confirmed delivery; this package cannot verify delivery. Unknown IDs are
// ignored, which makes retries after a crash safe. Ack does not move source
// checkpoints.
func (s *Store) Ack(ctx context.Context, eventIDs []string) (int, error) {
	if len(eventIDs) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, mapError(err)
	}
	defer tx.Rollback()

	statement, err := tx.PrepareContext(ctx, `DELETE FROM outbox WHERE event_id = ?`)
	if err != nil {
		return 0, mapError(err)
	}
	defer statement.Close()

	acked := 0
	for _, eventID := range eventIDs {
		result, err := statement.ExecContext(ctx, eventID)
		if err != nil {
			return 0, mapError(err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return 0, mapError(err)
		}
		acked += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, mapError(err)
	}
	return acked, nil
}

func validateStoredPayload(eventID string, payload []byte) error {
	var event events.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	if event.EventID != eventID {
		return fmt.Errorf("stored event_id %q does not match row event_id %q", event.EventID, eventID)
	}
	if err := event.Validate(); err != nil {
		return err
	}
	return nil
}

func pendingCount(ctx context.Context, q rowQueryer) (int, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&count); err != nil {
		return 0, mapError(err)
	}
	return count, nil
}

func storedPayload(ctx context.Context, q rowQueryer, eventID string) ([]byte, error) {
	var payload []byte
	err := q.QueryRowContext(ctx, `SELECT payload FROM outbox WHERE event_id = ?`, eventID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, mapError(err)
	}
	return payload, nil
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
