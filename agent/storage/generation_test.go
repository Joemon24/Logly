package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"agent/events"
)

func TestGenerationLifecycleAndRestart(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	if _, found, err := store.ActiveGeneration(ctx, "auth-log"); err != nil || found {
		t.Fatalf("active generation before first use: found=%v err=%v", found, err)
	}

	initial, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "", "gen-1")
	if err != nil {
		t.Fatalf("initial transition: %v", err)
	}
	if initial.Generation != "gen-1" || initial.Sequence != 1 {
		t.Fatalf("initial generation = %+v", initial)
	}

	// Repeating the same generation is a no-op: a process restart must not
	// rotate the source.
	repeated, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "gen-1", "gen-1")
	if err != nil {
		t.Fatalf("repeated transition: %v", err)
	}
	if repeated.Generation != "gen-1" || repeated.Sequence != 1 {
		t.Fatalf("repeated transition = %+v", repeated)
	}

	store = reopen(t, store, cfg)
	restarted, found, err := store.ActiveGeneration(ctx, "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation after restart: found=%v err=%v", found, err)
	}
	if restarted.Generation != "gen-1" || restarted.Sequence != 1 {
		t.Fatalf("restart changed the active generation: %+v", restarted)
	}

	// File rotation is only accepted after an explicit transition.
	rotated := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-2", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{rotated}); !errors.Is(err, ErrSourceGenerationMismatch) {
		t.Fatalf("implicit rotation = %v, want ErrSourceGenerationMismatch", err)
	}

	next, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "gen-1", "gen-2")
	if err != nil {
		t.Fatalf("rotate to gen-2: %v", err)
	}
	if next.Generation != "gen-2" || next.Sequence != 2 {
		t.Fatalf("rotated generation = %+v", next)
	}
	if err := store.Append(ctx, []events.Event{rotated}); err != nil {
		t.Fatalf("append to the active generation: %v", err)
	}

	// A retired generation can never become active again.
	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "gen-2", "gen-1"); !errors.Is(err, ErrGenerationReuse) {
		t.Fatalf("rollback to gen-1 = %v, want ErrGenerationReuse", err)
	}
	// A stale expectation cannot skip a generation.
	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "gen-1", "gen-3"); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("stale expected generation = %v, want ErrGenerationConflict", err)
	}
	// The source type is part of the source identity and cannot change.
	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeAudit, "gen-2", "gen-3"); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("source type change = %v, want ErrGenerationConflict", err)
	}

	// Events from a retired generation are rejected even at a forward position.
	retired := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 5, int64Ptr(500))
	if err := store.Append(ctx, []events.Event{retired}); !errors.Is(err, ErrSourceGenerationMismatch) {
		t.Fatalf("retired generation append = %v, want ErrSourceGenerationMismatch", err)
	}

	active, found, err := store.ActiveGeneration(ctx, "auth-log")
	if err != nil || !found {
		t.Fatalf("final active generation: found=%v err=%v", found, err)
	}
	if active.Generation != "gen-2" || active.Sequence != 2 {
		t.Fatalf("final active generation = %+v", active)
	}
}

func TestAppendBindsFirstUseGeneration(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	first := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{first}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	active, found, err := store.ActiveGeneration(ctx, "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation after first append: found=%v err=%v", found, err)
	}
	if active.Generation != "gen-1" || active.Sequence != 1 {
		t.Fatalf("first-use binding = %+v", active)
	}

	second := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 2, int64Ptr(200))
	if err := store.Append(ctx, []events.Event{second}); err != nil {
		t.Fatalf("same-generation append: %v", err)
	}

	store = reopen(t, store, cfg)
	afterRestart, found, err := store.ActiveGeneration(ctx, "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation after restart: found=%v err=%v", found, err)
	}
	if afterRestart.Generation != "gen-1" || afterRestart.Sequence != 1 {
		t.Fatalf("restart rebound the generation: %+v", afterRestart)
	}

	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "", "gen-9"); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("empty expectation for a bound source = %v, want ErrGenerationConflict", err)
	}
}

func TestAppendRollbackRemovesFirstUseGenerationBinding(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	first := testEvent(t, store, "source-a", events.SourceTypeFile, "gen-a", 1, int64Ptr(100))
	conflicting := testEvent(t, store, "source-b", events.SourceTypeFile, "gen-b", 1, int64Ptr(100))
	samePosition := withRaw(t, conflicting, "a different record at the same position\n")

	err := store.Append(ctx, []events.Event{first, conflicting, samePosition})
	if !errors.Is(err, ErrStalePosition) {
		t.Fatalf("batch error = %v, want ErrStalePosition", err)
	}

	for _, sourceID := range []string{"source-a", "source-b"} {
		if _, found, err := store.ActiveGeneration(ctx, sourceID); err != nil || found {
			t.Fatalf("generation binding for %q survived rollback: found=%v err=%v", sourceID, found, err)
		}
	}
	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rolled-back batch left %d pending events", len(entries))
	}
}

func TestConcurrentFirstUseGenerationBinding(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	const writers = 8
	batches := make([][]events.Event, writers)
	for writer := 0; writer < writers; writer++ {
		sourceID := fmt.Sprintf("concurrent-source-%d", writer)
		batches[writer] = []events.Event{
			testEvent(t, store, sourceID, events.SourceTypeFile, "gen-1", 1, int64Ptr(100)),
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
		t.Fatalf("concurrent first-use append: %v", err)
	}

	for writer := 0; writer < writers; writer++ {
		sourceID := fmt.Sprintf("concurrent-source-%d", writer)
		active, found, err := store.ActiveGeneration(ctx, sourceID)
		if err != nil || !found {
			t.Fatalf("active generation for %q: found=%v err=%v", sourceID, found, err)
		}
		if active.Generation != "gen-1" || active.Sequence != 1 {
			t.Fatalf("active generation for %q = %+v", sourceID, active)
		}
	}
}

func TestConcurrentTransitionAllowsOneWinner(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	if _, err := store.TransitionSourceGeneration(ctx, "auth-log", events.SourceTypeFile, "", "gen-1"); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.TransitionSourceGeneration(ctx,
				"auth-log", events.SourceTypeFile, "gen-1", "gen-2"); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("concurrent transitions succeeded %d times, want exactly 1", successes)
	}
	active, found, err := store.ActiveGeneration(ctx, "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation: found=%v err=%v", found, err)
	}
	if active.Generation != "gen-2" || active.Sequence != 2 {
		t.Fatalf("active generation after concurrent transitions = %+v", active)
	}
}
