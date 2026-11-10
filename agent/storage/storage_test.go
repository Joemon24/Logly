package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"agent/events"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Path:         filepath.Join(t.TempDir(), "outbox.db"),
		AgentID:      "linux-01",
		AgentType:    "linux",
		AgentVersion: "2.0.0",
		Capacity:     0,
	}
}

func openTestStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func reopen(t *testing.T, store *Store, cfg Config) *Store {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return openTestStore(t, cfg)
}

func int64Ptr(value int64) *int64 {
	return &value
}

func testEvent(t *testing.T, store *Store, sourceID string, sourceType events.SourceType, generation string, recordNumber uint64, byteOffset *int64) events.Event {
	t.Helper()

	identity := store.Identity()
	base := time.Date(2026, 11, 9, 10, 15, 30, 0, time.UTC)
	timestamp := base.Add(time.Duration(recordNumber) * time.Second)

	path := ""
	switch sourceType {
	case events.SourceTypeFile, events.SourceTypeAudit:
		path = "/var/log/" + sourceID
	}

	event := events.Event{
		SchemaVersion:   events.SchemaVersionV2,
		Timestamp:       timestamp,
		IngestTimestamp: timestamp.Add(10 * time.Millisecond),
		TimestampSource: events.TimestampSourceLog,
		Sequence:        recordNumber,
		Agent: events.Agent{
			AgentID:      identity.AgentID,
			AgentType:    identity.AgentType,
			AgentVersion: identity.AgentVersion,
			Generation:   identity.Generation,
		},
		Host: events.Host{
			Hostname:  "web-01",
			OS:        "ubuntu",
			OSVersion: "24.04",
			Kernel:    "6.8.0-45-generic",
		},
		Event: events.EventMeta{
			Category: events.CategoryAuthentication,
			Action:   "ssh_login",
			Outcome:  "failure",
		},
		Source: &events.Entity{Type: events.EntityIP, IP: "203.0.113.9"},
		Target: &events.Entity{Type: events.EntityUser, ID: "1000", Name: "alice"},
		Raw: events.Raw{
			Format: events.RawFormatText,
			Data:   fmt.Sprintf("%s %s record %d\n", sourceID, generation, recordNumber),
		},
		Collection: events.Collection{
			SourceID:   sourceID,
			SourceType: sourceType,
			Path:       path,
			Generation: generation,
			Position: events.Position{
				ByteOffset:   byteOffset,
				RecordNumber: recordNumber,
			},
		},
	}

	id, err := event.ComputeEventID()
	if err != nil {
		t.Fatalf("compute event id: %v", err)
	}
	event.EventID = id
	return event
}

func withRaw(t *testing.T, event events.Event, raw string) events.Event {
	t.Helper()
	event.Raw.Data = raw
	id, err := event.ComputeEventID()
	if err != nil {
		t.Fatalf("compute event id: %v", err)
	}
	event.EventID = id
	return event
}

func TestOpenPersistsIdentityPermissionsAndSchema(t *testing.T) {
	cfg := testConfig(t)
	cfg.Path = filepath.Join(t.TempDir(), "nested", "outbox.db")

	store := openTestStore(t, cfg)
	identity := store.Identity()
	if identity.AgentID != cfg.AgentID || identity.AgentType != cfg.AgentType {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if !strings.HasPrefix(identity.Generation, "agent-gen-") {
		t.Fatalf("generation = %q", identity.Generation)
	}

	assertMode(t, cfg.Path, 0o600)
	assertMode(t, filepath.Dir(cfg.Path), 0o700)

	store = reopen(t, store, cfg)
	if store.Identity() != identity {
		t.Fatalf("identity changed across reopen: %+v != %+v", store.Identity(), identity)
	}

	cfg.AgentVersion = "2.1.0"
	store = reopen(t, store, cfg)
	if store.Identity().Generation != identity.Generation {
		t.Fatal("generation changed after agent version update")
	}
	if store.Identity().AgentVersion != "2.1.0" {
		t.Fatalf("agent version = %q", store.Identity().AgentVersion)
	}

	cfg.AgentID = "linux-02"
	if _, err := Open(cfg); !errors.Is(err, ErrAgentIdentityMismatch) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
}

func TestDurabilityPragmasAndBoundedPool(t *testing.T) {
	store := openTestStore(t, testConfig(t))

	if got := store.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}

	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var synchronous int
	if err := store.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 2 {
		t.Fatalf("synchronous = %d, want FULL (2)", synchronous)
	}

	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatal(err)
	}
	if busyTimeout != busyTimeoutMillis {
		t.Fatalf("busy_timeout = %d, want %d", busyTimeout, busyTimeoutMillis)
	}
}

func TestPendingRequiresPositiveBoundedLimit(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	ctx := context.Background()

	for _, limit := range []int{-1, 0, MaxPendingLimit + 1} {
		if _, err := store.Pending(ctx, limit); !errors.Is(err, ErrInvalidLimit) {
			t.Fatalf("Pending(%d) error = %v, want ErrInvalidLimit", limit, err)
		}
	}

	event := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{event}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Pending(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertPayloads(t, entries, event)
}

func TestSchemaVersioningAndGenerationMigration(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != storageSchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, storageSchemaVersion)
	}

	first := testEvent(t, store, "migrate-source", events.SourceTypeFile, "gen-1", 1, int64Ptr(100))
	if err := store.Append(ctx, []events.Event{first}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionSourceGeneration(ctx,
		"migrate-source", events.SourceTypeFile, "gen-1", "gen-2"); err != nil {
		t.Fatal(err)
	}
	second := testEvent(t, store, "migrate-source", events.SourceTypeFile, "gen-2", 1, int64Ptr(0))
	if err := store.Append(ctx, []events.Event{second}); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DELETE FROM source_generations`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 0`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	store = openTestStore(t, cfg)
	active, ok, err := store.ActiveGeneration(ctx, "migrate-source")
	if err != nil || !ok {
		t.Fatalf("active generation after migration: ok=%v err=%v", ok, err)
	}
	if active.Generation != "gen-2" || active.Sequence != 2 {
		t.Fatalf("active generation = %+v, want gen-2 sequence 2", active)
	}
	if _, err := store.TransitionSourceGeneration(ctx,
		"migrate-source", events.SourceTypeFile, "gen-2", "gen-1"); !errors.Is(err, ErrGenerationReuse) {
		t.Fatalf("expected ErrGenerationReuse after migration, got %v", err)
	}

	entries, err := store.Pending(ctx, MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("pending after migration = %d, want 2", len(entries))
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = sql.Open("sqlite", cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfg); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("expected ErrUnsupportedSchema, got %v", err)
	}
}

func TestAppendRejectsForeignAgentIdentity(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)

	event := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(0))
	event.Agent.AgentID = "other-agent"
	id, err := event.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	event.EventID = id

	if err := store.Append(context.Background(), []events.Event{event}); !errors.Is(err, ErrAgentIdentityMismatch) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
}

func TestHeartbeatSequencePersists(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	ctx := context.Background()

	for want := uint64(1); want <= 3; want++ {
		got, err := store.NextHeartbeatSequence(ctx)
		if err != nil {
			t.Fatalf("heartbeat sequence: %v", err)
		}
		if got != want {
			t.Fatalf("heartbeat sequence = %d, want %d", got, want)
		}
	}

	store = reopen(t, store, cfg)
	got, err := store.NextHeartbeatSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("heartbeat sequence after reopen = %d, want 4", got)
	}
}

func TestMapErrorMapsDiskFull(t *testing.T) {
	err := mapError(&os.PathError{Op: "write", Path: "outbox.db", Err: syscall.ENOSPC})
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}
	if mapError(nil) != nil {
		t.Fatal("nil error mapped to non-nil")
	}
}

func TestOpenRejectsCorruptDatabase(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(cfg.Path + suffix)
	}
	if err := os.WriteFile(cfg.Path, []byte("this is not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Open(cfg)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCorruptStoredPayloadIsRejectedOnRead(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	event := testEvent(t, store, "auth-log", events.SourceTypeFile, "gen-1", 1, int64Ptr(0))
	if err := store.Append(context.Background(), []events.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE outbox SET payload = ? WHERE event_id = ?`, []byte(`{"broken":`), event.EventID); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	store = openTestStore(t, cfg)
	if _, err := store.Pending(context.Background(), MaxPendingLimit); !errors.Is(err, ErrCorruptPayload) {
		t.Fatalf("expected ErrCorruptPayload, got %v", err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}
