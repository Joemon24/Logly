package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent/events"
	"agent/storage"
)

var testNow = time.Date(2026, 11, 9, 10, 15, 30, 0, time.UTC)

func newTestStore(t *testing.T, capacity int) *storage.Store {
	t.Helper()
	return newTestStoreAt(t, filepath.Join(t.TempDir(), "outbox.db"), capacity)
}

func newTestStoreAt(t *testing.T, path string, capacity int) *storage.Store {
	t.Helper()
	store, err := storage.Open(storage.Config{
		Path:         path,
		AgentID:      "linux-01",
		AgentType:    "linux",
		AgentVersion: "2.0.0",
		Capacity:     capacity,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func newTestCollector(t *testing.T, store Store, path string) *Collector {
	t.Helper()
	collector, err := New(Config{
		Store:    store,
		Path:     path,
		SourceID: "auth-log",
		Host:     events.Host{Hostname: "web-01"},
		Now:      func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	return collector
}

func newTestCollectorLimit(t *testing.T, store Store, path string, maxRecords int) *Collector {
	t.Helper()
	collector, err := New(Config{
		Store:             store,
		Path:              path,
		SourceID:          "auth-log",
		MaxRecordsPerPoll: maxRecords,
		Host:              events.Host{Hostname: "web-01"},
		Now:               func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	return collector
}

func irrelevantLines(count int) string {
	var builder strings.Builder
	for i := 0; i < count; i++ {
		fmt.Fprintf(&builder, "Nov  9 10:15:30 web-01 systemd[1]: unrelated record %d\n", i)
	}
	return builder.String()
}

func writeLog(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func appendLog(t *testing.T, path, content string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		t.Fatalf("append %s: %v", path, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

func pendingEvents(t *testing.T, store *storage.Store) []events.Event {
	t.Helper()
	entries, err := store.Pending(context.Background(), storage.MaxPendingLimit)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	result := make([]events.Event, 0, len(entries))
	for _, entry := range entries {
		var event events.Event
		if err := json.Unmarshal(entry.Payload, &event); err != nil {
			t.Fatalf("decode pending event: %v", err)
		}
		result = append(result, event)
	}
	return result
}

func poll(t *testing.T, collector *Collector) (int, error) {
	t.Helper()
	return collector.Poll(context.Background())
}

const (
	failedIPv4 = "Nov  9 10:15:30 web-01 sshd[123]: Failed password for alice from 203.0.113.9 port 50000 ssh2\n"
	acceptedV6 = "Nov  9 10:15:31 web-01 sshd[124]: Accepted password for bob from 2001:db8::1 port 50001 ssh2\n"
	invalidUse = "Nov  9 10:15:32 web-01 sshd[125]: Invalid user admin from 198.51.100.7 port 50002\n"
)

func TestNewDefaultsAndValidation(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing store error = %v, want ErrInvalidConfig", err)
	}
	collector, err := New(Config{Store: newFakeStore()})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if collector.path != DefaultPath {
		t.Fatalf("default path = %q, want %q", collector.path, DefaultPath)
	}
	if collector.sourceID != DefaultSourceID || collector.pollPeriod != DefaultPollInterval {
		t.Fatalf("defaults = %q, %v", collector.sourceID, collector.pollPeriod)
	}
	if collector.maxRecords != DefaultMaxRecordsPerPoll {
		t.Fatalf("default max records = %d", collector.maxRecords)
	}
}

func TestPollPersistsParsedSSHEvents(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4+acceptedV6+invalidUse)
	collector := newTestCollector(t, store, path)

	count, err := poll(t, collector)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if count != 3 {
		t.Fatalf("poll persisted %d events, want 3", count)
	}

	persisted := pendingEvents(t, store)
	if len(persisted) != 3 {
		t.Fatalf("pending events = %d, want 3", len(persisted))
	}

	checks := []struct {
		action  string
		outcome string
		user    string
		ip      string
		offset  int64
	}{
		{"ssh_login", "failure", "alice", "203.0.113.9", 0},
		{"ssh_login", "success", "bob", "2001:db8::1", int64(len(failedIPv4))},
		{"ssh_invalid_user", "failure", "admin", "198.51.100.7", int64(len(failedIPv4) + len(acceptedV6))},
	}
	for i, check := range checks {
		event := persisted[i]
		if event.Event.Category != events.CategoryAuthentication {
			t.Errorf("event %d category = %q", i, event.Event.Category)
		}
		if event.Event.Action != check.action || event.Event.Outcome != check.outcome {
			t.Errorf("event %d action/outcome = %q/%q", i, event.Event.Action, event.Event.Outcome)
		}
		if event.Target == nil || event.Target.Name != check.user {
			t.Errorf("event %d target = %+v", i, event.Target)
		}
		if event.Source == nil || event.Source.IP != check.ip {
			t.Errorf("event %d source = %+v", i, event.Source)
		}
		if event.Collection.Generation == "" {
			t.Errorf("event %d has an empty generation", i)
		}
		if event.Collection.Position.RecordNumber != uint64(i+1) {
			t.Errorf("event %d record = %d", i, event.Collection.Position.RecordNumber)
		}
		if event.Collection.Position.ByteOffset == nil || *event.Collection.Position.ByteOffset != check.offset {
			t.Errorf("event %d offset = %v, want %d", i, event.Collection.Position.ByteOffset, check.offset)
		}
		if event.Host.Hostname != "web-01" {
			t.Errorf("event %d hostname = %q", i, event.Host.Hostname)
		}
		computed, err := event.ComputeEventID()
		if err != nil || computed != event.EventID {
			t.Errorf("event %d id mismatch: %v", i, err)
		}
	}

	generation := persisted[0].Collection.Generation
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log", generation)
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%v err=%v", found, err)
	}
	if checkpoint.RecordNumber != 3 {
		t.Fatalf("checkpoint record = %d, want 3", checkpoint.RecordNumber)
	}
	if checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != int64(len(failedIPv4)+len(acceptedV6)) {
		t.Fatalf("checkpoint offset = %v", checkpoint.ByteOffset)
	}
	if checkpoint.SourceType != events.SourceTypeFile {
		t.Fatalf("checkpoint source type = %q", checkpoint.SourceType)
	}

	if again, err := poll(t, collector); err != nil || again != 0 {
		t.Fatalf("second poll = %d, %v; want 0, nil", again, err)
	}
	if len(pendingEvents(t, store)) != 3 {
		t.Fatal("second poll duplicated events")
	}
}

func TestMissingEmptyAndPartialFiles(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("missing file poll = %d, %v", count, err)
	}

	writeLog(t, path, "")
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("empty file poll = %d, %v", count, err)
	}

	partial := "Nov  9 10:15:30 web-01 sshd[123]: Failed password for alice from 203.0.113.9 port 50000 ssh2"
	writeLog(t, path, partial)
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("partial line poll = %d, %v; want 0, nil", count, err)
	}

	appendLog(t, path, "\n")
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("completed line poll = %d, %v; want 1, nil", count, err)
	}
	persisted := pendingEvents(t, store)
	if persisted[0].Raw.Data != partial+"\n" {
		t.Fatalf("raw data = %q", persisted[0].Raw.Data)
	}
}

func TestRestartResumesCheckpointWithoutDuplicates(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "outbox.db")
	store := newTestStoreAt(t, storePath, 0)
	path := filepath.Join(dir, "auth.log")
	writeLog(t, path, failedIPv4+acceptedV6)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 2 {
		t.Fatalf("initial poll = %d, %v", count, err)
	}
	before := pendingEvents(t, store)
	generation := before[0].Collection.Generation

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store = newTestStoreAt(t, storePath, 0)
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("restart poll = %d, %v; want 0, nil", count, err)
	}
	if len(pendingEvents(t, store)) != 2 {
		t.Fatal("restart duplicated checkpointed events")
	}

	third := "Nov  9 10:15:33 web-01 sshd[126]: Accepted publickey for carol from 203.0.113.10 port 50003 ssh2\n"
	appendLog(t, path, third)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("post-restart poll = %d, %v; want 1, nil", count, err)
	}

	after := pendingEvents(t, store)
	if len(after) != 3 {
		t.Fatalf("pending after restart = %d, want 3", len(after))
	}
	for i := 0; i < 2; i++ {
		if after[i].EventID != before[i].EventID {
			t.Fatalf("event %d changed after restart: %s != %s", i, after[i].EventID, before[i].EventID)
		}
	}
	if after[2].Collection.Generation != generation {
		t.Fatalf("generation changed across restart: %q != %q",
			after[2].Collection.Generation, generation)
	}
	if after[2].Collection.Position.RecordNumber != 3 {
		t.Fatalf("post-restart record = %d, want 3", after[2].Collection.Position.RecordNumber)
	}
	if after[2].Collection.Position.ByteOffset == nil ||
		*after[2].Collection.Position.ByteOffset != int64(len(failedIPv4)+len(acceptedV6)) {
		t.Fatalf("post-restart offset = %v", after[2].Collection.Position.ByteOffset)
	}
}

func TestIdenticalLinesHaveDistinctPositions(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4+failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 2 {
		t.Fatalf("poll = %d, %v", count, err)
	}
	persisted := pendingEvents(t, store)
	if persisted[0].EventID == persisted[1].EventID {
		t.Fatal("identical lines at different positions produced the same event ID")
	}
	if persisted[0].Collection.Position.RecordNumber != 1 ||
		persisted[1].Collection.Position.RecordNumber != 2 {
		t.Fatal("identical lines did not get consecutive record numbers")
	}
}

func TestRotationDrainsOldFileAndStartsNewGeneration(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "outbox.db")
	store := newTestStoreAt(t, storePath, 0)
	path := filepath.Join(dir, "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v", count, err)
	}
	oldGeneration := pendingEvents(t, store)[0].Collection.Generation

	rotated := path + ".1"
	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	writeLog(t, path, acceptedV6)
	third := "Nov  9 10:15:34 web-01 sshd[127]: Failed password for dave from 203.0.113.11 port 50004 ssh2\n"
	appendLog(t, rotated, third)

	if count, err := poll(t, collector); err != nil || count != 2 {
		t.Fatalf("rotation poll = %d, %v; want 2, nil", count, err)
	}

	persisted := pendingEvents(t, store)
	if len(persisted) != 3 {
		t.Fatalf("pending events = %d, want 3", len(persisted))
	}
	if persisted[1].Collection.Generation != oldGeneration {
		t.Fatal("unread rotated record did not stay in the old generation")
	}
	if persisted[1].Collection.Position.RecordNumber != 2 {
		t.Fatalf("rotated record number = %d, want 2", persisted[1].Collection.Position.RecordNumber)
	}
	if persisted[2].Collection.Generation == oldGeneration {
		t.Fatal("new file reused the retired generation")
	}
	if persisted[2].Collection.Position.RecordNumber != 1 {
		t.Fatalf("new generation record number = %d, want 1", persisted[2].Collection.Position.RecordNumber)
	}

	active, found, err := store.ActiveGeneration(context.Background(), "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation: found=%v err=%v", found, err)
	}
	if active.Generation != persisted[2].Collection.Generation || active.Sequence != 2 {
		t.Fatalf("active generation = %+v", active)
	}
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("poll after rotation = %d, %v", count, err)
	}

	// A restart must resume the post-rotation generation, not the retired one.
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store = newTestStoreAt(t, storePath, 0)
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("poll after rotation restart = %d, %v", count, err)
	}
	appendLog(t, path, "Nov  9 10:15:35 web-01 sshd[128]: Accepted password for erin from 203.0.113.12 port 50005 ssh2\n")
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("post-rotation restart poll = %d, %v", count, err)
	}
	all := pendingEvents(t, store)
	latest := all[len(all)-1]
	if latest.Collection.Generation != active.Generation || latest.Collection.Position.RecordNumber != 2 {
		t.Fatalf("post-rotation restart event = gen %q record %d",
			latest.Collection.Generation, latest.Collection.Position.RecordNumber)
	}
}

func TestTruncationStartsNewGeneration(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v", count, err)
	}
	oldGeneration := pendingEvents(t, store)[0].Collection.Generation

	if err := os.Truncate(path, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("truncation poll = %d, %v; want 0, nil", count, err)
	}

	active, found, err := store.ActiveGeneration(context.Background(), "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation: found=%v err=%v", found, err)
	}
	if active.Generation == oldGeneration || active.Sequence != 2 {
		t.Fatalf("truncation did not create a new generation: %+v", active)
	}
	if !strings.HasPrefix(active.Generation, oldGeneration+"-t") {
		t.Fatalf("truncation generation %q is not based on %q", active.Generation, oldGeneration)
	}

	writeLog(t, path, acceptedV6)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("post-truncation poll = %d, %v", count, err)
	}
	persisted := pendingEvents(t, store)
	latest := persisted[len(persisted)-1]
	if latest.Collection.Generation != active.Generation {
		t.Fatalf("post-truncation generation = %q, want %q",
			latest.Collection.Generation, active.Generation)
	}
	if latest.Collection.Position.RecordNumber != 1 {
		t.Fatalf("post-truncation record = %d, want 1", latest.Collection.Position.RecordNumber)
	}
	if latest.Collection.Position.ByteOffset == nil || *latest.Collection.Position.ByteOffset != 0 {
		t.Fatalf("post-truncation offset = %v, want 0", latest.Collection.Position.ByteOffset)
	}
}

func TestOutboxCapacityBackpressureAndRetry(t *testing.T) {
	store := newTestStore(t, 1)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4+acceptedV6)
	collector := newTestCollector(t, store, path)

	count, err := poll(t, collector)
	if count != 1 || !errors.Is(err, ErrBackpressure) || !errors.Is(err, storage.ErrCapacityReached) {
		t.Fatalf("capacity poll = %d, %v; want 1, ErrBackpressure", count, err)
	}
	if len(pendingEvents(t, store)) != 1 {
		t.Fatal("first event was not persisted")
	}

	persisted := pendingEvents(t, store)
	if _, err := store.Ack(context.Background(), []string{persisted[0].EventID}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("retry poll = %d, %v; want 1, nil (pending flush)", count, err)
	}

	remaining := pendingEvents(t, store)
	if len(remaining) != 1 {
		t.Fatalf("pending after retry = %d, want 1", len(remaining))
	}
	if remaining[0].Collection.Position.RecordNumber != 2 {
		t.Fatalf("retried record = %d, want 2", remaining[0].Collection.Position.RecordNumber)
	}
	generation := remaining[0].Collection.Generation
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log", generation)
	if err != nil || !found || checkpoint.RecordNumber != 2 {
		t.Fatalf("checkpoint after retry: found=%v record=%d err=%v", found, checkpoint.RecordNumber, err)
	}
}

func TestMalformedSSHRecordPersistsEvidenceAndResumesAfterRestart(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	malformed := "Nov  9 10:16:00 web-01 sshd[200]: Failed password for invalid user bad\xff from not-an-ip port 22 ssh2\n"
	writeLog(t, path, irrelevantLines(1)+malformed+acceptedV6)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); count != 2 || err != nil {
		t.Fatalf("poll = %d, %v; want two persisted events", count, err)
	}
	rows := pendingEvents(t, store)
	unparsed := rows[0]
	if unparsed.Event.Action != "ssh_unparsed" || unparsed.Event.Outcome != "unknown" ||
		unparsed.Source != nil || unparsed.Target != nil ||
		unparsed.Raw.Data != strings.ReplaceAll(malformed, "\xff", "?") ||
		unparsed.Collection.Position.RecordNumber != 2 {
		t.Fatalf("unexpected unparsed evidence: %+v", unparsed)
	}
	if rows[1].Event.Outcome != "success" || rows[1].Collection.Position.RecordNumber != 3 {
		t.Fatalf("collection did not reach the next login: %+v", rows[1])
	}
	if err := collector.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newTestCollector(t, store, path)
	defer restarted.Close()
	if count, err := poll(t, restarted); count != 0 || err != nil {
		t.Fatalf("restart poll = %d, %v; want no duplicate events", count, err)
	}
	if len(pendingEvents(t, store)) != 2 {
		t.Fatal("restart duplicated the unparsed evidence or the valid login")
	}
}

func TestOversizedRecordStopsWithoutAdvancing(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v", count, err)
	}

	oversized := strings.Repeat("A", events.MaxRawEvidenceBytes+1) + "\n"
	appendLog(t, path, oversized)
	count, err := poll(t, collector)
	if count != 0 || !errors.Is(err, ErrOversizedRecord) || !errors.Is(err, ErrRejectedRecord) {
		t.Fatalf("oversized poll = %d, %v; want 0, ErrOversizedRecord", count, err)
	}

	persisted := pendingEvents(t, store)
	if len(persisted) != 1 {
		t.Fatalf("oversized record changed pending events: %d", len(persisted))
	}
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log", persisted[0].Collection.Generation)
	if err != nil || !found || checkpoint.RecordNumber != 1 {
		t.Fatalf("checkpoint after oversized record: found=%v record=%d err=%v",
			found, checkpoint.RecordNumber, err)
	}
}

func TestLongRecordWithinLimitIsAccepted(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	prefix := "Nov  9 10:15:30 web-01 sshd[123]: Failed publickey for alice from 203.0.113.9 port 50000 ssh2: RSA SHA256:"
	padding := strings.Repeat("x", events.MaxRawEvidenceBytes-len(prefix)-1)
	line := prefix + padding + "\n"
	if len(line) != events.MaxRawEvidenceBytes {
		t.Fatalf("test line is %d bytes, want %d", len(line), events.MaxRawEvidenceBytes)
	}
	writeLog(t, path, line)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("long record poll = %d, %v", count, err)
	}
	persisted := pendingEvents(t, store)
	if len(persisted[0].Raw.Data) != events.MaxRawEvidenceBytes {
		t.Fatalf("stored raw length = %d", len(persisted[0].Raw.Data))
	}
}

func TestSourceIOErrorIsRetryableAfterRecovery(t *testing.T) {
	store := newTestStore(t, 0)
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.log")
	if err := os.Symlink("auth.log", path); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	collector := newTestCollector(t, store, path)

	if _, err := poll(t, collector); !errors.Is(err, ErrSourceIO) {
		t.Fatalf("symlink loop poll error = %v, want ErrSourceIO", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove symlink: %v", err)
	}
	writeLog(t, path, failedIPv4)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("recovered poll = %d, %v", count, err)
	}
}

func TestLongIrrelevantStretchUsesPhysicalRecordNumbers(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	const ignored = 300
	ignoredText := irrelevantLines(ignored)
	writeLog(t, path, ignoredText+failedIPv4)
	collector := newTestCollectorLimit(t, store, path, 128)

	total := 0
	for i := 0; i < 3; i++ {
		count, err := poll(t, collector)
		if err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		total += count
	}
	if total != 1 {
		t.Fatalf("persisted %d events, want 1", total)
	}
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("final poll = %d, %v; want 0, nil", count, err)
	}

	persisted := pendingEvents(t, store)
	if len(persisted) != 1 {
		t.Fatalf("pending events = %d, want 1", len(persisted))
	}
	event := persisted[0]
	wantRecord := uint64(ignored + 1)
	if event.Collection.Position.RecordNumber != wantRecord || event.Sequence != wantRecord {
		t.Fatalf("event record/sequence = %d/%d, want %d",
			event.Collection.Position.RecordNumber, event.Sequence, wantRecord)
	}
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log",
		event.Collection.Generation)
	if err != nil || !found {
		t.Fatalf("checkpoint: found=%v err=%v", found, err)
	}
	if checkpoint.RecordNumber != wantRecord {
		t.Fatalf("checkpoint record = %d, want %d", checkpoint.RecordNumber, wantRecord)
	}
	if checkpoint.ByteOffset == nil || *checkpoint.ByteOffset != int64(len(ignoredText)) {
		t.Fatalf("checkpoint offset = %v, want %d", checkpoint.ByteOffset, len(ignoredText))
	}
}

func TestRestartAfterIgnoredRecordsDoesNotRescan(t *testing.T) {
	store := newFakeStore()
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4+irrelevantLines(3))
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v; want 1, nil", count, err)
	}
	if store.advances != 3 {
		t.Fatalf("checkpoint-only advances = %d, want 3", store.advances)
	}
	advances := store.advances
	appended := len(store.appended)

	// Fresh collector, same durable fake store: the last ignored record is
	// skipped from the checkpoint and is not advanced again.
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("restart poll = %d, %v; want 0, nil", count, err)
	}
	if store.advances != advances {
		t.Fatalf("restart rescanned ignored records: advances %d -> %d", advances, store.advances)
	}
	if len(store.appended) != appended {
		t.Fatal("restart duplicated an event")
	}

	appendLog(t, path, acceptedV6)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("post-restart poll = %d, %v; want 1, nil", count, err)
	}
	latest := store.appended[len(store.appended)-1]
	if latest.Collection.Position.RecordNumber != 5 {
		t.Fatalf("post-restart record = %d, want 5", latest.Collection.Position.RecordNumber)
	}
}

func TestInterleavedRelevantAndIgnoredRecords(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "outbox.db")
	store := newTestStoreAt(t, storePath, 0)
	path := filepath.Join(dir, "auth.log")
	writeLog(t, path,
		irrelevantLines(1)+failedIPv4+irrelevantLines(2)+acceptedV6+irrelevantLines(1))
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 2 {
		t.Fatalf("poll = %d, %v; want 2, nil", count, err)
	}
	persisted := pendingEvents(t, store)
	if len(persisted) != 2 {
		t.Fatalf("pending events = %d, want 2", len(persisted))
	}
	if persisted[0].Collection.Position.RecordNumber != 2 ||
		persisted[1].Collection.Position.RecordNumber != 5 {
		t.Fatalf("event records = %d/%d, want 2/5",
			persisted[0].Collection.Position.RecordNumber,
			persisted[1].Collection.Position.RecordNumber)
	}
	generation := persisted[0].Collection.Generation
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log", generation)
	if err != nil || !found || checkpoint.RecordNumber != 6 {
		t.Fatalf("checkpoint = %+v found=%v err=%v, want record 6", checkpoint, found, err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store = newTestStoreAt(t, storePath, 0)
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 0 {
		t.Fatalf("restart poll = %d, %v; want 0, nil", count, err)
	}

	appendLog(t, path, invalidUse)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("post-restart poll = %d, %v; want 1, nil", count, err)
	}
	all := pendingEvents(t, store)
	if len(all) != 3 {
		t.Fatalf("pending events = %d, want 3", len(all))
	}
	latest := all[2]
	if latest.Collection.Generation != generation ||
		latest.Collection.Position.RecordNumber != 7 {
		t.Fatalf("post-restart event = gen %q record %d, want gen %q record 7",
			latest.Collection.Generation, latest.Collection.Position.RecordNumber, generation)
	}
}

func TestCrashBeforeCheckpointAdvanceReplaysIgnoredRecord(t *testing.T) {
	store := newFakeStore()
	store.advanceErr = errors.New("injected advance failure")
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, irrelevantLines(1)+failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); count != 0 || !errors.Is(err, ErrStorage) {
		t.Fatalf("failed poll = %d, %v; want 0, ErrStorage", count, err)
	}
	if store.advances != 0 {
		t.Fatalf("failed advance moved the checkpoint: %d", store.advances)
	}
	if len(store.checkpoints) != 0 {
		t.Fatal("failed advance left a checkpoint")
	}

	// Simulate a process restart after a crash before the checkpoint commit.
	store.advanceErr = nil
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("replay poll = %d, %v; want 1, nil", count, err)
	}
	if store.advances != 1 {
		t.Fatalf("replay advances = %d, want 1", store.advances)
	}
	if len(store.appended) != 1 {
		t.Fatalf("replay persisted %d events, want 1", len(store.appended))
	}
	event := store.appended[0]
	if event.Collection.Position.RecordNumber != 2 {
		t.Fatalf("replay record = %d, want 2", event.Collection.Position.RecordNumber)
	}
	checkpoint := store.checkpoints["auth-log/"+event.Collection.Generation]
	if checkpoint.RecordNumber != 2 {
		t.Fatalf("checkpoint record = %d, want 2", checkpoint.RecordNumber)
	}
}

func TestCrashAfterCheckpointAdvanceSkipsOnRestart(t *testing.T) {
	store := newFakeStore()
	store.advanceAfterCommitErr = errors.New("simulated crash after commit")
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, irrelevantLines(1)+failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); count != 0 || !errors.Is(err, ErrStorage) {
		t.Fatalf("failed poll = %d, %v; want 0, ErrStorage", count, err)
	}
	if store.advances != 1 {
		t.Fatalf("committed advances = %d, want 1", store.advances)
	}
	checkpoint := store.checkpoints["auth-log/"+store.active.Generation]
	if checkpoint.RecordNumber != 1 {
		t.Fatalf("checkpoint record = %d, want 1", checkpoint.RecordNumber)
	}

	// The restart sees the committed checkpoint, skips that record once, and
	// proceeds to the relevant record without re-advancing the ignored one.
	store.advanceAfterCommitErr = nil
	collector = newTestCollector(t, store, path)
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("restart poll = %d, %v; want 1, nil", count, err)
	}
	if store.advances != 1 {
		t.Fatalf("restart re-advanced the checkpointed record: %d", store.advances)
	}
	if len(store.appended) != 1 {
		t.Fatalf("restart persisted %d events, want 1", len(store.appended))
	}
	if store.appended[0].Collection.Position.RecordNumber != 2 {
		t.Fatalf("restart record = %d, want 2",
			store.appended[0].Collection.Position.RecordNumber)
	}
}

func TestOfflineRotationRecoversRotatedFile(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "outbox.db")
	store := newTestStoreAt(t, storePath, 0)
	path := filepath.Join(dir, "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v; want 1, nil", count, err)
	}
	oldGeneration := pendingEvents(t, store)[0].Collection.Generation

	// Leave unread records in the old file and rotate while stopped.
	appendLog(t, path, irrelevantLines(2)+acceptedV6)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	writeLog(t, path, invalidUse)

	store = newTestStoreAt(t, storePath, 0)
	collector = newTestCollector(t, store, path)
	count, err := poll(t, collector)
	if err != nil {
		t.Fatalf("recovery poll: %v", err)
	}
	if count != 2 {
		t.Fatalf("recovery poll persisted %d events, want 2", count)
	}

	all := pendingEvents(t, store)
	if len(all) != 3 {
		t.Fatalf("pending events = %d, want 3", len(all))
	}
	recovered := all[1]
	if recovered.Collection.Generation != oldGeneration {
		t.Fatalf("recovered event generation = %q, want %q",
			recovered.Collection.Generation, oldGeneration)
	}
	if recovered.Collection.Position.RecordNumber != 4 {
		t.Fatalf("recovered record = %d, want 4",
			recovered.Collection.Position.RecordNumber)
	}
	replacement := all[2]
	if replacement.Collection.Generation == oldGeneration {
		t.Fatal("replacement event reused the retired generation")
	}
	if replacement.Collection.Position.RecordNumber != 1 {
		t.Fatalf("replacement record = %d, want 1",
			replacement.Collection.Position.RecordNumber)
	}
}

func TestOfflineRotationWithoutRecoverableFileReportsCoverageGap(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "outbox.db")
	store := newTestStoreAt(t, storePath, 0)
	path := filepath.Join(dir, "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v; want 1, nil", count, err)
	}
	oldGeneration := pendingEvents(t, store)[0].Collection.Generation

	appendLog(t, path, acceptedV6)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	// Move the old inode outside the searched plain-rotation names.
	if err := os.Rename(path, path+".10"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	writeLog(t, path, invalidUse)

	store = newTestStoreAt(t, storePath, 0)
	collector = newTestCollector(t, store, path)
	count, err := poll(t, collector)
	if count != 0 || !errors.Is(err, ErrCoverageGap) {
		t.Fatalf("gap poll = %d, %v; want 0, ErrCoverageGap", count, err)
	}
	var gap *CoverageGapError
	if !errors.As(err, &gap) {
		t.Fatalf("error type = %T, want *CoverageGapError", err)
	}
	if gap.Generation != oldGeneration || gap.RecordNumber != 1 {
		t.Fatalf("gap = %+v, want generation %q record 1", gap, oldGeneration)
	}

	active, found, err := store.ActiveGeneration(context.Background(), "auth-log")
	if err != nil || !found {
		t.Fatalf("active generation: found=%v err=%v", found, err)
	}
	if active.Generation == oldGeneration {
		t.Fatal("coverage gap did not activate the replacement generation")
	}

	// The replacement generation is already bound, so the next poll proceeds.
	count, err = poll(t, collector)
	if err != nil || count != 1 {
		t.Fatalf("post-gap poll = %d, %v; want 1, nil", count, err)
	}
	all := pendingEvents(t, store)
	latest := all[len(all)-1]
	if latest.Collection.Generation != active.Generation ||
		latest.Collection.Position.RecordNumber != 1 {
		t.Fatalf("post-gap event = gen %q record %d",
			latest.Collection.Generation, latest.Collection.Position.RecordNumber)
	}
}

func TestRepeatedTruncationCreatesDistinctGenerations(t *testing.T) {
	store := newTestStore(t, 0)
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("initial poll = %d, %v; want 1, nil", count, err)
	}
	generations := map[string]bool{
		pendingEvents(t, store)[0].Collection.Generation: true,
	}

	for want := uint64(2); want <= 3; want++ {
		if err := os.Truncate(path, 0); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		if count, err := poll(t, collector); err != nil || count != 0 {
			t.Fatalf("truncation %d poll = %d, %v; want 0, nil", want, count, err)
		}
		appendLog(t, path, acceptedV6)
		if count, err := poll(t, collector); err != nil || count != 1 {
			t.Fatalf("post-truncation %d poll = %d, %v; want 1, nil", want, count, err)
		}
		active, found, err := store.ActiveGeneration(context.Background(), "auth-log")
		if err != nil || !found {
			t.Fatalf("active generation: found=%v err=%v", found, err)
		}
		if active.Sequence != want {
			t.Fatalf("truncation sequence = %d, want %d", active.Sequence, want)
		}
		all := pendingEvents(t, store)
		latest := all[len(all)-1]
		if latest.Collection.Generation != active.Generation ||
			latest.Collection.Position.RecordNumber != 1 {
			t.Fatalf("truncation %d event = gen %q record %d",
				want, latest.Collection.Generation, latest.Collection.Position.RecordNumber)
		}
		if generations[active.Generation] {
			t.Fatalf("truncation reused generation %q", active.Generation)
		}
		generations[active.Generation] = true
	}
}

func TestCapacityBackpressureWithInterleavedIgnoredRecords(t *testing.T) {
	store := newTestStore(t, 1)
	path := filepath.Join(t.TempDir(), "auth.log")
	fourth := "Nov  9 10:15:33 web-01 sshd[126]: Accepted publickey for carol from 203.0.113.10 port 50003 ssh2\n"
	writeLog(t, path, failedIPv4+irrelevantLines(1)+acceptedV6+fourth)
	collector := newTestCollector(t, store, path)

	seenIDs := make(map[string]bool)
	seed := func(event events.Event) {
		t.Helper()
		if seenIDs[event.EventID] {
			t.Fatalf("event %s was emitted twice", event.EventID)
		}
		seenIDs[event.EventID] = true
	}

	count, err := poll(t, collector)
	if count != 1 || !errors.Is(err, ErrBackpressure) {
		t.Fatalf("first poll = %d, %v; want 1, ErrBackpressure", count, err)
	}
	persisted := pendingEvents(t, store)
	if len(persisted) != 1 || persisted[0].Collection.Position.RecordNumber != 1 {
		t.Fatalf("first event = %+v", persisted)
	}
	seed(persisted[0])
	generation := persisted[0].Collection.Generation
	checkpoint, found, err := store.Checkpoint(context.Background(), "auth-log", generation)
	if err != nil || !found || checkpoint.RecordNumber != 2 {
		t.Fatalf("checkpoint after backpressure = %+v found=%v err=%v, want record 2",
			checkpoint, found, err)
	}

	if _, err := store.Ack(context.Background(), []string{persisted[0].EventID}); err != nil {
		t.Fatalf("ack first: %v", err)
	}
	count, err = poll(t, collector)
	if count != 1 || !errors.Is(err, ErrBackpressure) {
		t.Fatalf("second poll = %d, %v; want 1, ErrBackpressure", count, err)
	}
	persisted = pendingEvents(t, store)
	if len(persisted) != 1 || persisted[0].Collection.Position.RecordNumber != 3 {
		t.Fatalf("second event = %+v", persisted)
	}
	seed(persisted[0])

	if _, err := store.Ack(context.Background(), []string{persisted[0].EventID}); err != nil {
		t.Fatalf("ack second: %v", err)
	}
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("third poll = %d, %v; want 1, nil", count, err)
	}
	all := pendingEvents(t, store)
	if len(all) != 1 || all[0].Collection.Position.RecordNumber != 4 {
		t.Fatalf("third event = %+v, want record 4", all)
	}
	seed(all[0])
	if len(seenIDs) != 3 {
		t.Fatalf("distinct security events = %d, want 3", len(seenIDs))
	}
}

// fakeStore injects storage failures while preserving the collector-visible
// checkpoint contract.
type fakeStore struct {
	identity    storage.Identity
	active      storage.SourceGeneration
	hasActive   bool
	transitions int
	checkpoints map[string]storage.Checkpoint
	appendErr   error
	attempts    []events.Event
	appended    []events.Event

	advanceErr            error
	advanceAfterCommitErr error
	advanceAttempts       int
	advances              int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		identity: storage.Identity{
			AgentID: "linux-01", AgentType: "linux",
			AgentVersion: "2.0.0", Generation: "agent-gen",
		},
		checkpoints: make(map[string]storage.Checkpoint),
	}
}

func (f *fakeStore) Identity() storage.Identity { return f.identity }

func (f *fakeStore) ActiveGeneration(context.Context, string) (storage.SourceGeneration, bool, error) {
	return f.active, f.hasActive, nil
}

func (f *fakeStore) Checkpoint(_ context.Context, sourceID, generation string) (storage.Checkpoint, bool, error) {
	checkpoint, found := f.checkpoints[sourceID+"/"+generation]
	return checkpoint, found, nil
}

func (f *fakeStore) TransitionSourceGeneration(
	_ context.Context, sourceID string, sourceType events.SourceType,
	expected, next string,
) (storage.SourceGeneration, error) {
	if f.hasActive {
		if expected != f.active.Generation {
			return storage.SourceGeneration{}, fmt.Errorf("%w: %s != %s",
				storage.ErrGenerationConflict, expected, f.active.Generation)
		}
	} else if expected != "" {
		return storage.SourceGeneration{}, fmt.Errorf("%w: no active generation", storage.ErrGenerationConflict)
	}
	f.transitions++
	f.active = storage.SourceGeneration{
		SourceID:   sourceID,
		SourceType: sourceType,
		Generation: next,
		Sequence:   uint64(f.transitions),
	}
	f.hasActive = true
	return f.active, nil
}

func (f *fakeStore) AdvanceCheckpoint(
	_ context.Context, sourceID string, sourceType events.SourceType,
	generation string, position events.Position,
) error {
	f.advanceAttempts++
	if f.advanceErr != nil {
		return f.advanceErr
	}
	key := sourceID + "/" + generation
	current, found := f.checkpoints[key]
	if found {
		if position.ByteOffset == nil || current.ByteOffset == nil ||
			*position.ByteOffset <= *current.ByteOffset ||
			position.RecordNumber <= current.RecordNumber {
			return fmt.Errorf("%w: stale checkpoint advance", storage.ErrCheckpointConflict)
		}
	}
	f.advances++
	f.checkpoints[key] = storage.Checkpoint{
		SourceID:     sourceID,
		SourceType:   sourceType,
		Generation:   generation,
		ByteOffset:   position.ByteOffset,
		RecordNumber: position.RecordNumber,
	}
	// A simulated crash after the durable commit still reports an error so the
	// caller does not update its in-memory position.
	return f.advanceAfterCommitErr
}

func (f *fakeStore) Append(_ context.Context, batch []events.Event) error {
	f.attempts = append(f.attempts, batch...)
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appended = append(f.appended, batch...)
	for _, event := range batch {
		position := event.Collection.Position
		f.checkpoints[event.Collection.SourceID+"/"+event.Collection.Generation] = storage.Checkpoint{
			SourceID:     event.Collection.SourceID,
			SourceType:   event.Collection.SourceType,
			Generation:   event.Collection.Generation,
			ByteOffset:   position.ByteOffset,
			RecordNumber: position.RecordNumber,
		}
	}
	return nil
}

// TestManualAuthLog is a gated smoke test for a real Linux auth log. It is
// skipped unless LOGLY_AUTH_LOG is set. Set LOGLY_MANUAL_DB to inspect the
// temporary outbox after the run; otherwise the database lives under t.TempDir.
func TestManualAuthLog(t *testing.T) {
	path := os.Getenv("LOGLY_AUTH_LOG")
	if path == "" {
		t.Skip("set LOGLY_AUTH_LOG to a real authentication log")
	}
	dbPath := os.Getenv("LOGLY_MANUAL_DB")
	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), "outbox.db")
	} else if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("create manual db dir: %v", err)
	}

	store := newTestStoreAt(t, dbPath, 0)
	collector, err := New(Config{
		Store:             store,
		Path:              path,
		SourceID:          "auth-log",
		MaxRecordsPerPoll: 50,
	})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	count, pollErr := collector.Poll(context.Background())
	if pollErr != nil && !errors.Is(pollErr, ErrRejectedRecord) {
		t.Fatalf("poll %s: %v", path, pollErr)
	}
	if pollErr != nil {
		var rejection *RejectedRecordError
		errors.As(pollErr, &rejection)
		t.Logf("rejection (checkpoint unchanged): %v", rejection)
	}

	active, found, err := store.ActiveGeneration(context.Background(), "auth-log")
	if err != nil {
		t.Fatalf("active generation: %v", err)
	}
	pending, err := store.Pending(context.Background(), storage.MaxPendingLimit)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	t.Logf("database=%s persisted=%d pending=%d active=%v found=%v",
		dbPath, count, len(pending), active.Generation, found)
}

func TestStorageFailureKeepsPendingEventAndCheckpoint(t *testing.T) {
	store := newFakeStore()
	store.appendErr = errors.New("injected storage failure")
	path := filepath.Join(t.TempDir(), "auth.log")
	writeLog(t, path, failedIPv4)
	collector := newTestCollector(t, store, path)

	count, err := poll(t, collector)
	if count != 0 || !errors.Is(err, ErrStorage) {
		t.Fatalf("failed poll = %d, %v; want 0, ErrStorage", count, err)
	}
	if len(store.attempts) != 1 {
		t.Fatalf("append attempts = %d, want 1", len(store.attempts))
	}
	if len(store.appended) != 0 {
		t.Fatal("storage failure still recorded an event")
	}
	if len(store.checkpoints) != 0 {
		t.Fatal("storage failure advanced a checkpoint")
	}
	firstAttempt := store.attempts[0]

	store.appendErr = nil
	if count, err := poll(t, collector); err != nil || count != 1 {
		t.Fatalf("retry poll = %d, %v; want 1, nil", count, err)
	}
	if len(store.appended) != 1 {
		t.Fatalf("append attempts = %d, appended = %d", len(store.attempts), len(store.appended))
	}
	retried := store.appended[0]
	if retried.EventID != firstAttempt.EventID {
		t.Fatalf("retry changed event ID: %s != %s", retried.EventID, firstAttempt.EventID)
	}
	if !retried.IngestTimestamp.Equal(firstAttempt.IngestTimestamp) {
		t.Fatal("retry changed the event payload timestamp")
	}
	if retried.Collection.Position.RecordNumber != 1 {
		t.Fatalf("retry record = %d, want 1", retried.Collection.Position.RecordNumber)
	}
	if len(store.attempts) != 2 {
		t.Fatalf("append attempts after retry = %d, want 2", len(store.attempts))
	}
}

func TestRecreatedDatabaseStartsNewAgentGenerationAndRecollectsCurrentFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.log")
	db := filepath.Join(dir, "outbox.db")
	writeLog(t, path, failedIPv4)
	store := newTestStoreAt(t, db, 0)
	first := newTestCollector(t, store, path)
	if n, err := poll(t, first); n != 1 || err != nil {
		t.Fatalf("first collection = %d, %v", n, err)
	}
	original := pendingEvents(t, store)[0]
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(db); err != nil {
		t.Fatal(err)
	}

	store = newTestStoreAt(t, db, 0)
	second := newTestCollector(t, store, path)
	t.Cleanup(func() { _ = second.Close() })
	if n, err := poll(t, second); n != 1 || err != nil {
		t.Fatalf("collection with recreated DB = %d, %v", n, err)
	}
	recollected := pendingEvents(t, store)[0]
	if original.Agent.Generation == recollected.Agent.Generation || original.EventID == recollected.EventID {
		t.Fatal("database recreation reused the previous agent generation or event ID")
	}
	if original.Collection.Generation != recollected.Collection.Generation ||
		recollected.Collection.Position.RecordNumber != 1 ||
		recollected.Collection.Position.ByteOffset == nil || *recollected.Collection.Position.ByteOffset != 0 {
		t.Fatalf("same inode did not start from the beginning: %+v", recollected.Collection)
	}
}
