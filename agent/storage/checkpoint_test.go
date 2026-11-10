package storage

import (
	"context"
	"errors"
	"testing"

	"agent/events"
)

func TestCheckpointProgressionAuthoritativeOrdering(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	fileFirst := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{fileFirst}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{fileFirst}); err != nil {
		t.Fatalf("duplicate pending event: %v", err)
	}

	samePosition := withRaw(t, fileFirst, "different raw evidence at the same position\n")
	if err := store.Append(ctx, []events.Event{samePosition}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("same-position new event = %v, want ErrStalePosition", err)
	}

	staleOffset := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1", 1, int64Ptr(90))
	if err := store.Append(ctx, []events.Event{staleOffset}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("lower offset = %v, want ErrStalePosition", err)
	}

	contradictory := []struct {
		name   string
		record uint64
		offset int64
	}{
		{"offset advances but record does not", 1, 200},
		{"offset retreats while record advances", 2, 50},
		{"offset stays equal while record advances", 2, 100},
	}
	for _, test := range contradictory {
		t.Run(test.name, func(t *testing.T) {
			event := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1",
				test.record, int64Ptr(test.offset))
			err := store.Append(ctx, []events.Event{event})
			if !errors.Is(err, ErrCheckpointConflict) {
				t.Fatalf("error = %v, want ErrCheckpointConflict", err)
			}
			if errors.Is(err, ErrStalePosition) {
				t.Fatalf("contradictory position was misclassified as stale: %v", err)
			}
		})
	}

	bothBehind := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1", 1, int64Ptr(50))
	if err := store.Append(ctx, []events.Event{bothBehind}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("both coordinates behind checkpoint = %v, want ErrStalePosition", err)
	}

	// Gaps are allowed: ordering is monotonic, not contiguous.
	skipped := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1", 4, int64Ptr(400))
	if err := store.Append(ctx, []events.Event{skipped}); err != nil {
		t.Fatalf("skipped forward position: %v", err)
	}
	checkpoint, ok, err := store.Checkpoint(ctx, "file-order", "gen-1")
	if err != nil || !ok {
		t.Fatalf("checkpoint: ok=%v err=%v", ok, err)
	}
	if checkpoint.RecordNumber != 4 || checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != 400 {
		t.Fatalf("checkpoint after skipped record = %+v", checkpoint)
	}

	afterSkip := []struct {
		name   string
		record uint64
		offset int64
	}{
		{"offset retreats while record advances after checkpoint", 5, 300},
		{"offset advances but record stays equal after checkpoint", 4, 500},
	}
	for _, test := range afterSkip {
		t.Run(test.name, func(t *testing.T) {
			event := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1",
				test.record, int64Ptr(test.offset))
			err := store.Append(ctx, []events.Event{event})
			if !errors.Is(err, ErrCheckpointConflict) || errors.Is(err, ErrStalePosition) {
				t.Fatalf("error = %v, want contradictory ErrCheckpointConflict", err)
			}
		})
	}

	mixed := testEvent(t, store, "file-order", events.SourceTypeFile, "gen-1", 5, nil)
	if err := store.Append(ctx, []events.Event{mixed}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("mixed offset mode = %v, want ErrCheckpointConflict", err)
	}

	journalFirst := testEvent(t, store, "journal-order", events.SourceTypeJournald, "boot-1", 10, nil)
	if err := store.Append(ctx, []events.Event{journalFirst}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{journalFirst}); err != nil {
		t.Fatalf("duplicate journal event: %v", err)
	}

	journalSamePosition := withRaw(t, journalFirst, "different journal evidence\n")
	if err := store.Append(ctx, []events.Event{journalSamePosition}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("same record number = %v, want ErrStalePosition", err)
	}
	journalLower := testEvent(t, store, "journal-order", events.SourceTypeJournald, "boot-1", 9, nil)
	if err := store.Append(ctx, []events.Event{journalLower}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("lower record number = %v, want ErrStalePosition", err)
	}

	journalSkipped := testEvent(t, store, "journal-order", events.SourceTypeJournald, "boot-1", 13, nil)
	if err := store.Append(ctx, []events.Event{journalSkipped}); err != nil {
		t.Fatalf("skipped journal record: %v", err)
	}
	journalBehind := testEvent(t, store, "journal-order", events.SourceTypeJournald, "boot-1", 12, nil)
	if err := store.Append(ctx, []events.Event{journalBehind}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("journal record behind checkpoint = %v, want ErrStalePosition", err)
	}

	journalNext := testEvent(t, store, "journal-order", events.SourceTypeJournald, "boot-1", 14, nil)
	if err := store.Append(ctx, []events.Event{journalNext}); err != nil {
		t.Fatalf("journal record after checkpoint: %v", err)
	}
	journalCheckpoint, ok, err := store.Checkpoint(ctx, "journal-order", "boot-1")
	if err != nil || !ok {
		t.Fatalf("journal checkpoint: ok=%v err=%v", ok, err)
	}
	if journalCheckpoint.RecordNumber != 14 || journalCheckpoint.ByteOffset != nil {
		t.Fatalf("journal checkpoint = %+v", journalCheckpoint)
	}
}

func TestAdvanceCheckpointIsAtomicGenerationAwareAndMonotonic(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	generation, err := store.TransitionSourceGeneration(ctx,
		"advance-source", events.SourceTypeFile, "", "gen-1")
	if err != nil {
		t.Fatal(err)
	}
	if generation.Generation != "gen-1" {
		t.Fatalf("generation = %+v", generation)
	}

	// A checkpoint can exist without an outbox event.
	first := events.Position{ByteOffset: int64Ptr(0), RecordNumber: 1}
	if err := store.AdvanceCheckpoint(ctx,
		"advance-source", events.SourceTypeFile, "gen-1", first); err != nil {
		t.Fatalf("first advance: %v", err)
	}
	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("checkpoint-only advance inserted %d events", len(entries))
	}
	checkpoint, found, err := store.Checkpoint(ctx, "advance-source", "gen-1")
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%v err=%v", found, err)
	}
	if checkpoint.RecordNumber != 1 || checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != 0 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}

	// Ordering matches Append: both coordinates must move forward.
	if err := store.AdvanceCheckpoint(ctx, "advance-source", events.SourceTypeFile, "gen-1",
		events.Position{ByteOffset: int64Ptr(0), RecordNumber: 2}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("contradictory advance = %v, want ErrCheckpointConflict", err)
	}
	if err := store.AdvanceCheckpoint(ctx, "advance-source", events.SourceTypeFile, "gen-1",
		events.Position{ByteOffset: int64Ptr(5), RecordNumber: 1}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("contradictory offset advance = %v, want ErrCheckpointConflict", err)
	}
	if err := store.AdvanceCheckpoint(ctx, "advance-source", events.SourceTypeFile, "gen-1",
		events.Position{ByteOffset: int64Ptr(0), RecordNumber: 1}); !errors.Is(err, ErrStalePosition) {
		t.Fatalf("stale advance = %v, want ErrStalePosition", err)
	}

	// The generation must be the active one.
	if err := store.AdvanceCheckpoint(ctx, "advance-source", events.SourceTypeFile, "gen-2",
		events.Position{ByteOffset: int64Ptr(10), RecordNumber: 2}); !errors.Is(err, ErrSourceGenerationMismatch) {
		t.Fatalf("inactive generation advance = %v, want ErrSourceGenerationMismatch", err)
	}
	if err := store.AdvanceCheckpoint(ctx, "advance-source", events.SourceTypeAudit, "gen-1",
		events.Position{ByteOffset: int64Ptr(10), RecordNumber: 2}); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("source type change = %v, want ErrGenerationConflict", err)
	}

	next := events.Position{ByteOffset: int64Ptr(10), RecordNumber: 2}
	if err := store.AdvanceCheckpoint(ctx,
		"advance-source", events.SourceTypeFile, "gen-1", next); err != nil {
		t.Fatalf("second advance: %v", err)
	}

	store = reopen(t, store, cfg)
	checkpoint, found, err = store.Checkpoint(ctx, "advance-source", "gen-1")
	if err != nil || !found {
		t.Fatalf("checkpoint after reopen: found=%v err=%v", found, err)
	}
	if checkpoint.RecordNumber != 2 || checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != 10 {
		t.Fatalf("checkpoint after reopen = %+v", checkpoint)
	}
}

func TestAdvanceCheckpointDoesNotConsumeOutboxCapacity(t *testing.T) {
	cfg := testConfig(t)
	cfg.Capacity = 1
	store := openTestStore(t, cfg)
	ctx := context.Background()

	event := testEvent(t, store, "capacity-source", events.SourceTypeFile, "gen-1", 1, int64Ptr(0))
	if err := store.Append(ctx, []events.Event{event}); err != nil {
		t.Fatal(err)
	}

	// The outbox is full, but advancing past an ignored record must still work.
	if err := store.AdvanceCheckpoint(ctx, "capacity-source", events.SourceTypeFile, "gen-1",
		events.Position{ByteOffset: int64Ptr(10), RecordNumber: 2}); err != nil {
		t.Fatalf("advance while full: %v", err)
	}
	checkpoint, found, err := store.Checkpoint(ctx, "capacity-source", "gen-1")
	if err != nil || !found || checkpoint.RecordNumber != 2 {
		t.Fatalf("checkpoint = %+v found=%v err=%v", checkpoint, found, err)
	}

	second := testEvent(t, store, "capacity-source", events.SourceTypeFile, "gen-1", 3, int64Ptr(20))
	if err := store.Append(ctx, []events.Event{second}); !errors.Is(err, ErrCapacityReached) {
		t.Fatalf("append after advance = %v, want ErrCapacityReached", err)
	}
}

func TestDuplicateAfterAcknowledgementIsStale(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	event := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{event}); err != nil {
		t.Fatal(err)
	}
	acked, err := store.Ack(ctx, []string{event.EventID})
	if err != nil || acked != 1 {
		t.Fatalf("ack: acked=%d err=%v", acked, err)
	}

	err = store.Append(ctx, []events.Event{event})
	if !errors.Is(err, ErrStalePosition) || !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("post-ack replay = %v, want ErrStalePosition and ErrCheckpointConflict", err)
	}
	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("post-ack replay reinserted %d events", len(entries))
	}

	store = reopen(t, store, cfg)
	err = store.Append(ctx, []events.Event{event})
	if !errors.Is(err, ErrStalePosition) {
		t.Fatalf("post-ack replay after reopen = %v, want ErrStalePosition", err)
	}
}
