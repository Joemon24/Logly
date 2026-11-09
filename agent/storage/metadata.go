package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"agent/events"
)

// Checkpoint is the last durable position for one source generation.
type Checkpoint struct {
	SourceID     string
	SourceType   events.SourceType
	Generation   string
	ByteOffset   *int64
	RecordNumber uint64
	UpdatedAt    time.Time
}

// Checkpoint returns the stored checkpoint for a source generation. The bool is
// false when no record has been committed for that generation yet.
func (s *Store) Checkpoint(ctx context.Context, sourceID, generation string) (Checkpoint, bool, error) {
	var (
		sourceType   string
		byteOffset   sql.NullInt64
		recordNumber int64
		updatedAt    string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT source_type, byte_offset, record_number, updated_at
		 FROM source_checkpoints WHERE source_id = ? AND generation = ?`,
		sourceID, generation).
		Scan(&sourceType, &byteOffset, &recordNumber, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, mapError(err)
	}

	parsed, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("%w: invalid checkpoint timestamp %q", ErrCorrupt, updatedAt)
	}

	checkpoint := Checkpoint{
		SourceID:     sourceID,
		SourceType:   events.SourceType(sourceType),
		Generation:   generation,
		RecordNumber: uint64(recordNumber),
		UpdatedAt:    parsed,
	}
	if byteOffset.Valid {
		value := byteOffset.Int64
		checkpoint.ByteOffset = &value
	}
	return checkpoint, true, nil
}

// NextHeartbeatSequence atomically increments and returns the persistent
// heartbeat counter. The first call returns 1, and the value survives restarts.
func (s *Store) NextHeartbeatSequence(ctx context.Context) (uint64, error) {
	var sequence int64
	err := s.db.QueryRowContext(ctx,
		`UPDATE agent_metadata SET heartbeat_sequence = heartbeat_sequence + 1
		 WHERE id = ? RETURNING heartbeat_sequence`, agentMetaRow).
		Scan(&sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: agent metadata row is missing", ErrCorrupt)
	}
	if err != nil {
		return 0, mapError(err)
	}
	if sequence < 1 {
		return 0, fmt.Errorf("%w: heartbeat sequence is %d", ErrCorrupt, sequence)
	}
	return uint64(sequence), nil
}

// checkpointState is the in-transaction view of one source checkpoint.
type checkpointState struct {
	exists       bool
	sourceType   events.SourceType
	byteOffset   *int64
	recordNumber uint64
	changed      bool
}

// accept advances the state to position. Unless the event is an idempotent
// duplicate, the position must be strictly after the existing checkpoint.
func (st *checkpointState) accept(sourceType events.SourceType, position events.Position, duplicate bool) error {
	if !st.exists {
		st.set(sourceType, position)
		return nil
	}
	if st.sourceType != sourceType {
		return fmt.Errorf("%w: source type changed from %q to %q",
			ErrCheckpointConflict, st.sourceType, sourceType)
	}

	after, err := positionAfter(position, st)
	if err != nil {
		return err
	}
	if !after {
		if duplicate {
			return nil
		}
		return fmt.Errorf("%w: position %s is not after stored position %s",
			ErrCheckpointConflict, positionString(position), positionString(events.Position{
				ByteOffset: st.byteOffset, RecordNumber: st.recordNumber,
			}))
	}
	st.set(sourceType, position)
	return nil
}

func (st *checkpointState) set(sourceType events.SourceType, position events.Position) {
	st.exists = true
	st.sourceType = sourceType
	st.recordNumber = position.RecordNumber
	st.byteOffset = nil
	if position.ByteOffset != nil {
		value := *position.ByteOffset
		st.byteOffset = &value
	}
	st.changed = true
}

func positionAfter(next events.Position, current *checkpointState) (bool, error) {
	if (next.ByteOffset == nil) != (current.byteOffset == nil) {
		return false, fmt.Errorf("%w: source mixes byte-offset and record-number positions",
			ErrCheckpointConflict)
	}
	if next.ByteOffset != nil {
		return *next.ByteOffset > *current.byteOffset, nil
	}
	return next.RecordNumber > current.recordNumber, nil
}

func positionString(position events.Position) string {
	if position.ByteOffset != nil {
		return fmt.Sprintf("byte_offset=%d", *position.ByteOffset)
	}
	return fmt.Sprintf("record_number=%d", position.RecordNumber)
}

func loadCheckpoint(ctx context.Context, q rowQueryer, sourceID, generation string) (*checkpointState, error) {
	var (
		sourceType   string
		byteOffset   sql.NullInt64
		recordNumber int64
	)
	err := q.QueryRowContext(ctx,
		`SELECT source_type, byte_offset, record_number
		 FROM source_checkpoints WHERE source_id = ? AND generation = ?`,
		sourceID, generation).
		Scan(&sourceType, &byteOffset, &recordNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return &checkpointState{}, nil
	}
	if err != nil {
		return nil, mapError(err)
	}

	state := &checkpointState{
		exists:       true,
		sourceType:   events.SourceType(sourceType),
		recordNumber: uint64(recordNumber),
	}
	if byteOffset.Valid {
		value := byteOffset.Int64
		state.byteOffset = &value
	}
	return state, nil
}

func (st *checkpointState) save(ctx context.Context, tx *sql.Tx, sourceID, generation, now string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO source_checkpoints
		 (source_id, generation, source_type, byte_offset, record_number, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (source_id, generation) DO UPDATE SET
			source_type = excluded.source_type,
			byte_offset = excluded.byte_offset,
			record_number = excluded.record_number,
			updated_at = excluded.updated_at`,
		sourceID, generation, string(st.sourceType),
		nullableInt64(st.byteOffset), int64(st.recordNumber), now)
	if err != nil {
		return mapError(err)
	}
	return nil
}
