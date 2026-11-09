// Package storage provides a durable SQLite-backed outbox and source
// checkpoints for schema-v2 agent events.
//
// The package is not wired into the runtime pipeline yet. It stores finalized
// event payloads exactly as received; replay returns those bytes unchanged.
// A future dispatcher must acknowledge an event only after Kafka confirms
// delivery.
package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// DefaultCapacity is used when Config.Capacity is zero.
const DefaultCapacity = 10_000

const (
	dbFileMode        = 0o600
	dbDirMode         = 0o700
	busyTimeoutMillis = 5_000
	openTimeout       = 30 * time.Second
	agentMetaRow      = 1
)

// Errors returned by this package. Use errors.Is to test them.
var (
	ErrDiskFull              = errors.New("storage: disk full")
	ErrCorrupt               = errors.New("storage: database is corrupt")
	ErrCorruptPayload        = errors.New("storage: stored event payload is corrupt")
	ErrEventIDConflict       = errors.New("storage: event_id conflict")
	ErrCheckpointConflict    = errors.New("storage: checkpoint conflict")
	ErrCapacityReached       = errors.New("storage: outbox capacity reached")
	ErrAgentIdentityMismatch = errors.New("storage: agent identity mismatch")
	ErrOversizedEvent        = errors.New("storage: event exceeds the raw evidence size limit")
	ErrInvalidEvent          = errors.New("storage: invalid event")
	ErrInvalidConfig         = errors.New("storage: invalid config")
)

// Config controls opening a Store.
type Config struct {
	// Path is the SQLite database file. Its parent directory is created with
	// 0700 permissions if missing.
	Path string

	// AgentID and AgentType identify this agent installation. AgentID must stay
	// the same across restarts; it is checked against the identity already in
	// the database. AgentType and AgentVersion are refreshed on every open.
	AgentID      string
	AgentType    string
	AgentVersion string

	// Capacity is the maximum number of pending (unacknowledged) events.
	// Zero means DefaultCapacity. Negative values are rejected.
	Capacity int
}

// Identity is the persistent agent identity stored in the database.
type Identity struct {
	AgentID      string
	AgentType    string
	AgentVersion string
	// Generation is generated once and survives restarts. It changes only when
	// the database is deleted or deliberately reset.
	Generation string
}

// Store is a SQLite-backed outbox. It is safe for concurrent use by multiple
// goroutines; the underlying pool is limited to one connection so writes are
// serialized.
type Store struct {
	db       *sql.DB
	path     string
	identity Identity
	capacity int
}

// Open opens or creates a Store and initializes its schema and agent identity.
func Open(cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.AgentID) == "" {
		return nil, fmt.Errorf("%w: agent_id is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.AgentType) == "" {
		return nil, fmt.Errorf("%w: agent_type is required", ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.AgentVersion) == "" {
		return nil, fmt.Errorf("%w: agent_version is required", ErrInvalidConfig)
	}
	if cfg.Capacity < 0 {
		return nil, fmt.Errorf("%w: capacity must not be negative", ErrInvalidConfig)
	}

	capacity := cfg.Capacity
	if capacity == 0 {
		capacity = DefaultCapacity
	}

	path, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("storage: resolve database path: %w", err)
	}
	if err := prepareDatabaseFile(path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dataSourceName(path))
	if err != nil {
		return nil, mapError(err)
	}
	// A single connection keeps write transactions serialized and makes the
	// per-connection pragmas (busy timeout, synchronous) predictable.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	s := &Store{db: db, path: path, capacity: capacity}
	if err := s.initialize(cfg); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database. Pending events remain on disk for replay.
func (s *Store) Close() error {
	return s.db.Close()
}

// Identity returns the persistent identity loaded when the store was opened.
func (s *Store) Identity() Identity {
	return s.identity
}

func (s *Store) initialize(cfg Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()

	if err := s.db.PingContext(ctx); err != nil {
		return mapError(err)
	}
	if err := s.checkIntegrity(ctx); err != nil {
		return err
	}
	if err := s.createSchema(ctx); err != nil {
		return err
	}
	if err := s.initIdentity(ctx, cfg); err != nil {
		return err
	}
	// WAL and shared-memory files are created on first use. Tighten them when
	// they already exist; SQLite normally inherits the main file's mode.
	return secureSidecars(s.path)
}

func (s *Store) checkIntegrity(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return mapError(err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		found = true
		var result string
		if err := rows.Scan(&result); err != nil {
			return mapError(err)
		}
		if result != "ok" {
			return fmt.Errorf("%w: %s", ErrCorrupt, result)
		}
	}
	if err := rows.Err(); err != nil {
		return mapError(err)
	}
	if !found {
		return fmt.Errorf("%w: quick_check returned no result", ErrCorrupt)
	}
	return nil
}

func (s *Store) createSchema(ctx context.Context) error {
	for _, statement := range schemaStatements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return mapError(err)
		}
	}
	return nil
}

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS agent_metadata (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		agent_id TEXT NOT NULL,
		agent_type TEXT NOT NULL,
		agent_version TEXT NOT NULL,
		generation TEXT NOT NULL,
		heartbeat_sequence INTEGER NOT NULL DEFAULT 0 CHECK (heartbeat_sequence >= 0),
		created_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS source_checkpoints (
		source_id TEXT NOT NULL,
		generation TEXT NOT NULL,
		source_type TEXT NOT NULL,
		byte_offset INTEGER,
		record_number INTEGER NOT NULL CHECK (record_number > 0),
		updated_at TEXT NOT NULL,
		PRIMARY KEY (source_id, generation)
	)`,
	`CREATE TABLE IF NOT EXISTS outbox (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		event_id TEXT NOT NULL UNIQUE,
		source_id TEXT NOT NULL,
		source_generation TEXT NOT NULL,
		record_number INTEGER NOT NULL,
		byte_offset INTEGER,
		payload BLOB NOT NULL,
		stored_at TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS outbox_source_position
		ON outbox (source_id, source_generation, record_number)`,
}

func (s *Store) initIdentity(ctx context.Context, cfg Config) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mapError(err)
	}
	defer tx.Rollback()

	var id Identity
	err = tx.QueryRowContext(ctx,
		`SELECT agent_id, agent_type, agent_version, generation
		 FROM agent_metadata WHERE id = ?`, agentMetaRow).
		Scan(&id.AgentID, &id.AgentType, &id.AgentVersion, &id.Generation)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		id.AgentID = cfg.AgentID
		id.AgentType = cfg.AgentType
		id.AgentVersion = cfg.AgentVersion
		id.Generation, err = newGeneration()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO agent_metadata
			 (id, agent_id, agent_type, agent_version, generation, heartbeat_sequence, created_at)
			 VALUES (?, ?, ?, ?, ?, 0, ?)`,
			agentMetaRow, id.AgentID, id.AgentType, id.AgentVersion, id.Generation,
			time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return mapError(err)
		}
	case err != nil:
		return mapError(err)
	default:
		if id.AgentID != cfg.AgentID {
			return fmt.Errorf("%w: database agent_id %q does not match config %q",
				ErrAgentIdentityMismatch, id.AgentID, cfg.AgentID)
		}
		if id.AgentType != cfg.AgentType || id.AgentVersion != cfg.AgentVersion {
			_, err = tx.ExecContext(ctx,
				`UPDATE agent_metadata SET agent_type = ?, agent_version = ? WHERE id = ?`,
				cfg.AgentType, cfg.AgentVersion, agentMetaRow)
			if err != nil {
				return mapError(err)
			}
			id.AgentType = cfg.AgentType
			id.AgentVersion = cfg.AgentVersion
		}
	}

	if err := tx.Commit(); err != nil {
		return mapError(err)
	}
	s.identity = id
	return nil
}

func newGeneration() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("storage: generate agent generation: %w", err)
	}
	return "agent-gen-" + hex.EncodeToString(b[:]), nil
}

func prepareDatabaseFile(path string) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, dbDirMode); err != nil {
			return fmt.Errorf("storage: create database directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, dbFileMode)
	if err != nil {
		return fmt.Errorf("storage: create database file: %w", mapError(err))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("storage: close database file: %w", err)
	}
	// OpenFile does not change the mode of an existing file.
	if err := os.Chmod(path, dbFileMode); err != nil {
		return fmt.Errorf("storage: set database permissions: %w", err)
	}
	return nil
}

func secureSidecars(path string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Chmod(path+suffix, dbFileMode); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("storage: set %s permissions: %w", suffix, err)
		}
	}
	return nil
}

func dataSourceName(path string) string {
	values := url.Values{}
	values.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMillis))
	values.Add("_pragma", "journal_mode(WAL)")
	values.Add("_pragma", "synchronous(FULL)")
	values.Add("_pragma", "foreign_keys(ON)")
	values.Set("_txlock", "immediate")
	return (&url.URL{Scheme: "file", Path: path, RawQuery: values.Encode()}).String()
}

// mapError converts SQLite result codes and OS errors into package sentinels.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: %v", ErrDiskFull, err)
	}

	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		// Extended result codes share the low byte with the primary code.
		switch sqliteErr.Code() & 0xff {
		case sqlite3.SQLITE_FULL:
			return fmt.Errorf("%w: %v", ErrDiskFull, err)
		case sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB:
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	return err
}

// rowQueryer is implemented by both *sql.DB and *sql.Tx.
type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
