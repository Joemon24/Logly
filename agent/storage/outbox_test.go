package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/events"
)

func TestAppendPersistsEventsAndCheckpoints(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	auth1 := testEvent(t, store, "auth-log", events.SourceTypeFile, "auth-gen-1", 1, int64Ptr(100))
	auth2 := testEvent(t, store, "auth-log", events.SourceTypeFile, "auth-gen-1", 2, int64Ptr(200))
	audit := testEvent(t, store, "audit-log", events.SourceTypeAudit, "audit-gen-1", 1, int64Ptr(0))

	if err := store.Append(ctx, []events.Event{auth1, auth2, audit}); err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, entries, auth1, auth2, audit)

	checkpoint, ok, err := store.Checkpoint(ctx, "auth-log", "auth-gen-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("auth checkpoint missing")
	}
	if checkpoint.SourceType != events.SourceTypeFile {
		t.Fatalf("source type = %q", checkpoint.SourceType)
	}
	if checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != 200 || checkpoint.RecordNumber != 2 {
		t.Fatalf("unexpected auth checkpoint: %+v", checkpoint)
	}

	checkpoint, ok, err = store.Checkpoint(ctx, "audit-log", "audit-gen-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != 0 {
		t.Fatalf("unexpected audit checkpoint: %+v ok=%v", checkpoint, ok)
	}

	if _, ok, err := store.Checkpoint(ctx, "auth-log", "missing"); err != nil || ok {
		t.Fatalf("missing checkpoint reported ok=%v err=%v", ok, err)
	}
}

func TestAppendRollsBackOnFailure(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		batch    func(t *testing.T, store *Store) []events.Event
		want     error
	}{
		{
			name: "invalid event",
			batch: func(t *testing.T, store *Store) []events.Event {
				first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
				second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
				second.Raw.Format = "binary"
				return []events.Event{first, second}
			},
			want: ErrInvalidEvent,
		},
		{
			name: "checkpoint conflict",
			batch: func(t *testing.T, store *Store) []events.Event {
				first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
				second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
				return []events.Event{first, second}
			},
			want: ErrCheckpointConflict,
		},
		{
			name: "event id conflict",
			batch: func(t *testing.T, store *Store) []events.Event {
				first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
				second := first
				second.Host.Hostname = "other-host"
				return []events.Event{first, second}
			},
			want: ErrEventIDConflict,
		},
		{
			name: "oversized event",
			batch: func(t *testing.T, store *Store) []events.Event {
				first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
				second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
				second.Raw.Data = strings.Repeat("x", events.MaxRawEvidenceBytes+1)
				return []events.Event{first, second}
			},
			want: ErrOversizedEvent,
		},
		{
			name:     "capacity",
			capacity: 1,
			batch: func(t *testing.T, store *Store) []events.Event {
				first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
				second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
				return []events.Event{first, second}
			},
			want: ErrCapacityReached,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Capacity = test.capacity
			store := openTestStore(t, cfg)
			ctx := context.Background()

			err := store.Append(ctx, test.batch(t, store))
			if !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}

			entries, err := store.Pending(ctx, MaxPendingLimit)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("rolled-back batch left %d pending events", len(entries))
			}
			if _, ok, err := store.Checkpoint(ctx, "auth-log", "gen-1"); err != nil || ok {
				t.Fatalf("rolled-back batch left a checkpoint: ok=%v err=%v", ok, err)
			}
		})
	}
}

func TestDuplicateSubmissionIsIdempotent(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()
	event := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))

	if err := store.Append(ctx, []events.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{event}); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}
	if err := store.Append(ctx, []events.Event{event, event}); err != nil {
		t.Fatalf("duplicate events in one batch: %v", err)
	}

	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("pending = %d, want 1", len(entries))
	}

	conflict := event
	conflict.Host.Hostname = "other-host"
	if err := store.Append(ctx, []events.Event{conflict}); !errors.Is(err, ErrEventIDConflict) {
		t.Fatalf("expected ErrEventIDConflict, got %v", err)
	}
}

func TestReplayAcrossReopenReturnsOriginalPayload(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	eventsToStore := []events.Event{
		testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100)),
		testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200)),
	}
	if err := store.Append(ctx, eventsToStore); err != nil {
		t.Fatal(err)
	}

	before, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes := make([][]byte, len(before))
	for i, entry := range before {
		beforeBytes[i] = append([]byte(nil), entry.Payload...)
	}

	store = reopen(t, store, cfg)
	after, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(beforeBytes) {
		t.Fatalf("pending after reopen = %d, want %d", len(after), len(beforeBytes))
	}
	for i, entry := range after {
		if !bytes.Equal(entry.Payload, beforeBytes[i]) {
			t.Fatalf("payload %d changed across reopen", i)
		}
		want, err := json.Marshal(eventsToStore[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(entry.Payload, want) {
			t.Fatalf("payload %d is not the original serialized event", i)
		}
		if entry.EventID != eventsToStore[i].EventID {
			t.Fatalf("event id %d changed", i)
		}
		if entry.AgentID != store.Identity().AgentID ||
			entry.SourceID != eventsToStore[i].Collection.SourceID {
			t.Fatalf("delivery identity %d = %q/%q, want %q/%q", i,
				entry.AgentID, entry.SourceID,
				store.Identity().AgentID, eventsToStore[i].Collection.SourceID)
		}
	}
}

func TestPendingOrderLimitAndPartialAck(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
	third := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 3, int64Ptr(300))
	if err := store.Append(ctx, []events.Event{first, second, third}); err != nil {
		t.Fatal(err)
	}

	limited, err := store.Pending(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, limited, first, second)

	acked, err := store.Ack(ctx, []string{second.EventID})
	if err != nil {
		t.Fatal(err)
	}
	if acked != 1 {
		t.Fatalf("acked = %d, want 1", acked)
	}

	remaining, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, remaining, first, third)

	checkpoint, ok, err := store.Checkpoint(ctx, "auth-log", "gen-1")
	if err != nil || !ok {
		t.Fatalf("checkpoint after ack: ok=%v err=%v", ok, err)
	}
	if checkpoint.RecordNumber != 3 {
		t.Fatalf("ack changed checkpoint to %d", checkpoint.RecordNumber)
	}

	acked, err = store.Ack(ctx, []string{"evt2_" + strings.Repeat("0", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if acked != 0 {
		t.Fatalf("unknown ack = %d, want 0", acked)
	}
}

func TestOutboxStatsTracksPendingCountAndOldestAge(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	empty, err := store.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Pending != 0 || !empty.OldestStoredAt.IsZero() {
		t.Fatalf("empty stats = %+v", empty)
	}

	before := time.Now().Add(-time.Second)
	first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
	if err := store.Append(ctx, []events.Event{first, second}); err != nil {
		t.Fatal(err)
	}

	stats, err := store.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 2 {
		t.Fatalf("pending = %d, want 2", stats.Pending)
	}
	if stats.OldestStoredAt.Before(before) || stats.OldestStoredAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("oldest stored_at = %v, want current time", stats.OldestStoredAt)
	}

	if _, err := store.Ack(ctx, []string{first.EventID}); err != nil {
		t.Fatal(err)
	}
	stats, err = store.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.OldestStoredAt.IsZero() {
		t.Fatalf("stats after partial ack = %+v", stats)
	}
}

func TestCapacityLimit(t *testing.T) {
	cfg := testConfig(t)
	cfg.Capacity = 2
	store := openTestStore(t, cfg)
	ctx := context.Background()

	first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
	third := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 3, int64Ptr(300))

	if err := store.Append(ctx, []events.Event{first}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{second}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{third}); !errors.Is(err, ErrCapacityReached) {
		t.Fatalf("expected ErrCapacityReached, got %v", err)
	}

	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, entries, first, second)
	checkpoint, ok, err := store.Checkpoint(ctx, "auth-log", "gen-1")
	if err != nil || !ok {
		t.Fatalf("checkpoint: ok=%v err=%v", ok, err)
	}
	if checkpoint.RecordNumber != 2 {
		t.Fatalf("checkpoint record = %d, want 2", checkpoint.RecordNumber)
	}

	// A duplicate does not consume additional capacity.
	if err := store.Append(ctx, []events.Event{second}); err != nil {
		t.Fatalf("duplicate at capacity: %v", err)
	}

	if _, err := store.Ack(ctx, []string{first.EventID}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []events.Event{third}); err != nil {
		t.Fatalf("append after ack: %v", err)
	}
}

func TestCheckpointConflictsAndNoByteOffsetSources(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	fileOne := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	fileTwo := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
	if err := store.Append(ctx, []events.Event{fileOne, fileTwo}); err != nil {
		t.Fatal(err)
	}

	lower := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 3, int64Ptr(150))
	if err := store.Append(ctx, []events.Event{lower}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("expected lower offset conflict, got %v", err)
	}

	mixed := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 4, nil)
	if err := store.Append(ctx, []events.Event{mixed}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("expected mixed position conflict, got %v", err)
	}

	newGeneration := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-2", 1, int64Ptr(0))
	if err := store.Append(ctx, []events.Event{newGeneration}); !errors.Is(err, ErrSourceGenerationMismatch) {
		t.Fatalf("rotation without an explicit transition should fail, got %v", err)
	}
	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "gen-1", "gen-2"); err != nil {
		t.Fatalf("transition generation: %v", err)
	}
	if err := store.Append(ctx, []events.Event{newGeneration}); err != nil {
		t.Fatalf("new generation should start fresh after transition: %v", err)
	}

	journalTen := testEvent(t, store, "journal", events.SourceTypeJournald, "boot-1", 10, nil)
	journalEleven := testEvent(t, store, "journal", events.SourceTypeJournald, "boot-1", 11, nil)
	if err := store.Append(ctx, []events.Event{journalTen, journalEleven}); err != nil {
		t.Fatal(err)
	}

	checkpoint, ok, err := store.Checkpoint(ctx, "journal", "boot-1")
	if err != nil || !ok {
		t.Fatalf("journal checkpoint: ok=%v err=%v", ok, err)
	}
	if checkpoint.ByteOffset != nil {
		t.Fatalf("journal checkpoint byte offset = %v, want nil", *checkpoint.ByteOffset)
	}
	if checkpoint.RecordNumber != 11 {
		t.Fatalf("journal checkpoint record = %d, want 11", checkpoint.RecordNumber)
	}

	journalNine := testEvent(t, store, "journal", events.SourceTypeJournald, "boot-1", 9, nil)
	if err := store.Append(ctx, []events.Event{journalNine}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("expected record-number conflict, got %v", err)
	}
	journalTwelve := testEvent(t, store, "journal", events.SourceTypeJournald, "boot-1", 12, nil)
	if err := store.Append(ctx, []events.Event{journalTwelve}); err != nil {
		t.Fatalf("append next journal record: %v", err)
	}
}

func TestConcurrentAppendAndHeartbeatSequence(t *testing.T) {
	cfg := testConfig(t)
	cfg.Capacity = 1000
	store := openTestStore(t, cfg)
	ctx := context.Background()

	const (
		writers        = 8
		eventsPerBatch = 10
	)
	batches := make([][]events.Event, writers)
	for writer := 0; writer < writers; writer++ {
		sourceID := fmt.Sprintf("source-%d", writer)
		for record := 1; record <= eventsPerBatch; record++ {
			batches[writer] = append(batches[writer], testEvent(t, store, sourceID,
				events.SourceTypeFile, "gen-1", uint64(record), int64Ptr(int64(record*100))))
		}
	}

	var wg sync.WaitGroup
	appendErrs := make(chan error, writers)
	for writer := 0; writer < writers; writer++ {
		batch := batches[writer]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Append(ctx, batch); err != nil {
				appendErrs <- err
			}
		}()
	}
	wg.Wait()
	close(appendErrs)
	for err := range appendErrs {
		t.Fatalf("concurrent append: %v", err)
	}

	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != writers*eventsPerBatch {
		t.Fatalf("pending = %d, want %d", len(entries), writers*eventsPerBatch)
	}
	for writer := 0; writer < writers; writer++ {
		sourceID := fmt.Sprintf("source-%d", writer)
		checkpoint, ok, err := store.Checkpoint(ctx, sourceID, "gen-1")
		if err != nil || !ok {
			t.Fatalf("%s checkpoint: ok=%v err=%v", sourceID, ok, err)
		}
		if checkpoint.RecordNumber != eventsPerBatch {
			t.Fatalf("%s record = %d", sourceID, checkpoint.RecordNumber)
		}
	}

	const (
		heartbeatWorkers = 10
		perWorker        = 10
	)
	results := make(chan uint64, heartbeatWorkers*perWorker)
	heartbeatErrs := make(chan error, heartbeatWorkers)
	for worker := 0; worker < heartbeatWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sequence, err := store.NextHeartbeatSequence(ctx)
				if err != nil {
					heartbeatErrs <- err
					return
				}
				results <- sequence
			}
		}()
	}
	wg.Wait()
	close(results)
	close(heartbeatErrs)
	for err := range heartbeatErrs {
		t.Fatalf("heartbeat sequence: %v", err)
	}

	seen := make(map[uint64]bool, heartbeatWorkers*perWorker)
	for sequence := range results {
		if sequence < 1 || sequence > heartbeatWorkers*perWorker {
			t.Fatalf("heartbeat sequence out of range: %d", sequence)
		}
		if seen[sequence] {
			t.Fatalf("duplicate heartbeat sequence %d", sequence)
		}
		seen[sequence] = true
	}
	if len(seen) != heartbeatWorkers*perWorker {
		t.Fatalf("heartbeat sequences = %d, want %d", len(seen), heartbeatWorkers*perWorker)
	}
}

func assertPayloads(t *testing.T, entries []OutboxEntry, want ...events.Event) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("entries = %d, want %d", len(entries), len(want))
	}
	for i := range want {
		payload, err := json.Marshal(want[i])
		if err != nil {
			t.Fatal(err)
		}
		if entries[i].EventID != want[i].EventID {
			t.Fatalf("entry %d event id = %q, want %q", i, entries[i].EventID, want[i].EventID)
		}
		if !bytes.Equal(entries[i].Payload, payload) {
			t.Fatalf("entry %d payload does not match original event", i)
		}
	}
}
