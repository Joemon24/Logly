package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"agent/events"
)

// SourceGeneration is the active incarnation of one logical source.
//
// Sequence starts at 1 and increases on every explicit rotation. Retired
// generations remain in the database so they can never be activated again.
type SourceGeneration struct {
	SourceID    string
	SourceType  events.SourceType
	Generation  string
	Sequence    uint64
	ActiveSince time.Time
}

// ActiveGeneration returns the active generation for a logical source. The bool
// is false when the source has never been initialized.
func (s *Store) ActiveGeneration(ctx context.Context, sourceID string) (SourceGeneration, bool, error) {
	return loadActiveGeneration(ctx, s.db, sourceID)
}

// TransitionSourceGeneration explicitly rotates a source from its current
// generation to nextGeneration.
//
// expectedGeneration must equal the current active generation. Pass "" only for
// the first initialization of a source. Repeating a transition with the current
// generation and nextGeneration equal is an idempotent no-op, so process
// restarts do not rotate the source.
//
// A generation that was ever active (or that already has a checkpoint) cannot
// be activated again; this returns ErrGenerationReuse. Concurrent callers are
// serialized by the write transaction, and a stale expectedGeneration returns
// ErrGenerationConflict.
func (s *Store) TransitionSourceGeneration(
	ctx context.Context,
	sourceID string,
	sourceType events.SourceType,
	expectedGeneration, nextGeneration string,
) (SourceGeneration, error) {
	if sourceID == "" || nextGeneration == "" || !sourceType.Valid() {
		return SourceGeneration{}, fmt.Errorf(
			"%w: transition requires source_id, a valid source_type, and a next generation",
			ErrInvalidConfig)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceGeneration{}, mapError(err)
	}
	defer tx.Rollback()

	active, found, err := loadActiveGeneration(ctx, tx, sourceID)
	if err != nil {
		return SourceGeneration{}, err
	}
	if !found {
		if expectedGeneration != "" {
			return SourceGeneration{}, fmt.Errorf(
				"%w: source %q has no active generation, expected %q",
				ErrGenerationConflict, sourceID, expectedGeneration)
		}
		hasHistory, err := sourceHasGenerations(ctx, tx, sourceID)
		if err != nil {
			return SourceGeneration{}, err
		}
		if hasHistory {
			return SourceGeneration{}, fmt.Errorf(
				"%w: source %q has generation history but no active generation",
				ErrCorrupt, sourceID)
		}
		hasCheckpoint, err := sourceHasCheckpoint(ctx, tx, sourceID)
		if err != nil {
			return SourceGeneration{}, err
		}
		if hasCheckpoint {
			return SourceGeneration{}, fmt.Errorf(
				"%w: source %q has a checkpoint but no generation history",
				ErrCorrupt, sourceID)
		}
		next, err := insertInitialGeneration(ctx, tx, sourceID, sourceType, nextGeneration)
		if err != nil {
			return SourceGeneration{}, err
		}
		if err := tx.Commit(); err != nil {
			return SourceGeneration{}, mapError(err)
		}
		return next, nil
	}

	if active.SourceType != sourceType {
		return SourceGeneration{}, fmt.Errorf(
			"%w: source %q has type %q, not %q",
			ErrGenerationConflict, sourceID, active.SourceType, sourceType)
	}
	if expectedGeneration == "" {
		return SourceGeneration{}, fmt.Errorf(
			"%w: source %q already has active generation %q",
			ErrGenerationConflict, sourceID, active.Generation)
	}
	if expectedGeneration != active.Generation {
		return SourceGeneration{}, fmt.Errorf(
			"%w: source %q active generation is %q, expected %q",
			ErrGenerationConflict, sourceID, active.Generation, expectedGeneration)
	}
	if nextGeneration == active.Generation {
		// Restart/repeat: the persisted active generation is unchanged.
		if err := tx.Commit(); err != nil {
			return SourceGeneration{}, mapError(err)
		}
		return active, nil
	}

	exists, isActive, err := generationStatus(ctx, tx, sourceID, nextGeneration)
	if err != nil {
		return SourceGeneration{}, err
	}
	if exists {
		if isActive {
			return SourceGeneration{}, fmt.Errorf(
				"%w: generation %q is unexpectedly already active for source %q",
				ErrCorrupt, nextGeneration, sourceID)
		}
		return SourceGeneration{}, fmt.Errorf(
			"%w: generation %q for source %q was already retired",
			ErrGenerationReuse, nextGeneration, sourceID)
	}
	used, err := sourceGenerationUsed(ctx, tx, sourceID, nextGeneration)
	if err != nil {
		return SourceGeneration{}, err
	}
	if used {
		return SourceGeneration{}, fmt.Errorf(
			"%w: generation %q for source %q already has a checkpoint",
			ErrGenerationReuse, nextGeneration, sourceID)
	}
	if active.Sequence >= math.MaxInt64 {
		return SourceGeneration{}, fmt.Errorf(
			"%w: source %q generation sequence overflow", ErrGenerationConflict, sourceID)
	}

	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		`UPDATE source_generations SET active = 0, retired_at = ?
		 WHERE source_id = ? AND generation = ? AND active = 1`,
		nowText, sourceID, active.Generation); err != nil {
		return SourceGeneration{}, mapError(err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO source_generations
		 (source_id, generation, source_type, generation_sequence, active, created_at, retired_at)
		 VALUES (?, ?, ?, ?, 1, ?, NULL)`,
		sourceID, nextGeneration, string(sourceType), int64(active.Sequence+1), nowText); err != nil {
		return SourceGeneration{}, mapError(err)
	}
	if err := tx.Commit(); err != nil {
		return SourceGeneration{}, mapError(err)
	}

	return SourceGeneration{
		SourceID:    sourceID,
		SourceType:  sourceType,
		Generation:  nextGeneration,
		Sequence:    active.Sequence + 1,
		ActiveSince: now,
	}, nil
}

// ensureActiveGeneration binds a first-use source inside an Append transaction
// or validates that the event belongs to the persisted active generation.
// Generation changes after first use require TransitionSourceGeneration.
func ensureActiveGeneration(
	ctx context.Context,
	tx *sql.Tx,
	cache map[string]SourceGeneration,
	sourceID string,
	sourceType events.SourceType,
	generation string,
) error {
	if active, ok := cache[sourceID]; ok {
		if active.SourceType != sourceType {
			return fmt.Errorf("%w: source %q has type %q, not %q",
				ErrGenerationConflict, sourceID, active.SourceType, sourceType)
		}
		if active.Generation != generation {
			return fmt.Errorf("%w: source %q active generation is %q, event has %q",
				ErrSourceGenerationMismatch, sourceID, active.Generation, generation)
		}
		return nil
	}

	active, found, err := loadActiveGeneration(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if !found {
		hasHistory, err := sourceHasGenerations(ctx, tx, sourceID)
		if err != nil {
			return err
		}
		if hasHistory {
			return fmt.Errorf(
				"%w: source %q has generation history but no active generation",
				ErrCorrupt, sourceID)
		}
		hasCheckpoint, err := sourceHasCheckpoint(ctx, tx, sourceID)
		if err != nil {
			return err
		}
		if hasCheckpoint {
			return fmt.Errorf(
				"%w: source %q has a checkpoint but no active generation",
				ErrCorrupt, sourceID)
		}

		active, err = insertInitialGeneration(ctx, tx, sourceID, sourceType, generation)
		if err != nil {
			return err
		}
	}

	if active.SourceType != sourceType {
		return fmt.Errorf("%w: source %q has type %q, not %q",
			ErrGenerationConflict, sourceID, active.SourceType, sourceType)
	}
	if active.Generation != generation {
		return fmt.Errorf("%w: source %q active generation is %q, event has %q",
			ErrSourceGenerationMismatch, sourceID, active.Generation, generation)
	}

	cache[sourceID] = active
	return nil
}

func loadActiveGeneration(ctx context.Context, q rowQueryer, sourceID string) (SourceGeneration, bool, error) {
	var (
		generation string
		sourceType string
		sequence   int64
		createdAt  string
	)
	err := q.QueryRowContext(ctx,
		`SELECT generation, source_type, generation_sequence, created_at
		 FROM source_generations WHERE source_id = ? AND active = 1`, sourceID).
		Scan(&generation, &sourceType, &sequence, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceGeneration{}, false, nil
	}
	if err != nil {
		return SourceGeneration{}, false, mapError(err)
	}

	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return SourceGeneration{}, false, fmt.Errorf(
			"%w: invalid active generation timestamp %q", ErrCorrupt, createdAt)
	}
	if generation == "" || sequence < 1 || !events.SourceType(sourceType).Valid() {
		return SourceGeneration{}, false, fmt.Errorf(
			"%w: invalid active generation row for source %q", ErrCorrupt, sourceID)
	}
	return SourceGeneration{
		SourceID:    sourceID,
		SourceType:  events.SourceType(sourceType),
		Generation:  generation,
		Sequence:    uint64(sequence),
		ActiveSince: parsed,
	}, true, nil
}

func insertInitialGeneration(
	ctx context.Context,
	tx *sql.Tx,
	sourceID string,
	sourceType events.SourceType,
	generation string,
) (SourceGeneration, error) {
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO source_generations
		 (source_id, generation, source_type, generation_sequence, active, created_at, retired_at)
		 VALUES (?, ?, ?, 1, 1, ?, NULL)`,
		sourceID, generation, string(sourceType), nowText); err != nil {
		return SourceGeneration{}, mapError(err)
	}
	return SourceGeneration{
		SourceID:    sourceID,
		SourceType:  sourceType,
		Generation:  generation,
		Sequence:    1,
		ActiveSince: now,
	}, nil
}

func generationStatus(ctx context.Context, q rowQueryer, sourceID, generation string) (bool, bool, error) {
	var active int
	err := q.QueryRowContext(ctx,
		`SELECT active FROM source_generations WHERE source_id = ? AND generation = ?`,
		sourceID, generation).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, mapError(err)
	}
	return true, active == 1, nil
}

func sourceHasGenerations(ctx context.Context, q rowQueryer, sourceID string) (bool, error) {
	return rowExists(ctx, q,
		`SELECT 1 FROM source_generations WHERE source_id = ? LIMIT 1`, sourceID)
}

func sourceHasCheckpoint(ctx context.Context, q rowQueryer, sourceID string) (bool, error) {
	return rowExists(ctx, q,
		`SELECT 1 FROM source_checkpoints WHERE source_id = ? LIMIT 1`, sourceID)
}

func sourceGenerationUsed(ctx context.Context, q rowQueryer, sourceID, generation string) (bool, error) {
	return rowExists(ctx, q,
		`SELECT 1 FROM source_checkpoints WHERE source_id = ? AND generation = ? LIMIT 1`,
		sourceID, generation)
}

func rowExists(ctx context.Context, q rowQueryer, query string, args ...any) (bool, error) {
	var one int
	err := q.QueryRowContext(ctx, query, args...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, mapError(err)
	}
	return true, nil
}
