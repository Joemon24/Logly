// Package events defines the versioned Logly agent security event schema.
//
// Schema version 2 is additive: the existing v1 runtime types remain in
// agent/types and agent/sender. Nothing in this package is wired into the
// pipeline yet.
package events

import (
	"fmt"
	"time"
)

// SchemaVersionV2 is the schema version emitted by this model.
const SchemaVersionV2 = 2

// MaxRawEvidenceBytes is the maximum accepted size of Raw.Data.
// Records larger than this must be rejected by the collector (see docs).
const MaxRawEvidenceBytes = 64 * 1024

// Category identifies the security event family.
type Category string

const (
	CategoryAuthentication Category = "authentication"
	CategorySudo           Category = "sudo"
	CategoryAccount        Category = "account"
	CategoryProcess        Category = "process"
	CategoryFileIntegrity  Category = "file_integrity"
	CategoryHeartbeat      Category = "heartbeat"
)

// Valid reports whether c is a supported category.
func (c Category) Valid() bool {
	switch c {
	case CategoryAuthentication, CategorySudo, CategoryAccount,
		CategoryProcess, CategoryFileIntegrity, CategoryHeartbeat:
		return true
	}
	return false
}

// TimestampSource explains where the event timestamp came from.
type TimestampSource string

const (
	// TimestampSourceLog means the timestamp was parsed from the source record.
	TimestampSourceLog TimestampSource = "log"
	// TimestampSourceIngest means the source had no usable timestamp and the
	// agent used its ingestion clock.
	TimestampSourceIngest TimestampSource = "ingest"
)

// Valid reports whether s is a supported timestamp source.
func (s TimestampSource) Valid() bool {
	return s == TimestampSourceLog || s == TimestampSourceIngest
}

// SourceType identifies the collector that produced a record.
type SourceType string

const (
	SourceTypeFile     SourceType = "file"
	SourceTypeJournald SourceType = "journald"
	SourceTypeAudit    SourceType = "audit"
	SourceTypeAgent    SourceType = "agent"
)

// Valid reports whether s is a supported collection source type.
func (s SourceType) Valid() bool {
	switch s {
	case SourceTypeFile, SourceTypeJournald, SourceTypeAudit, SourceTypeAgent:
		return true
	}
	return false
}

// EntityType identifies what a source or target entity describes.
type EntityType string

const (
	EntityUser    EntityType = "user"
	EntityIP      EntityType = "ip"
	EntityHost    EntityType = "host"
	EntityProcess EntityType = "process"
	EntityFile    EntityType = "file"
	EntitySession EntityType = "session"
)

// Valid reports whether t is a supported entity type.
func (t EntityType) Valid() bool {
	switch t {
	case EntityUser, EntityIP, EntityHost, EntityProcess, EntityFile, EntitySession:
		return true
	}
	return false
}

// RawFormat identifies the encoding of the raw evidence.
type RawFormat string

const (
	RawFormatText  RawFormat = "text"
	RawFormatJSON  RawFormat = "json"
	RawFormatAudit RawFormat = "audit"
)

// Valid reports whether f is a supported raw format.
func (f RawFormat) Valid() bool {
	return f == RawFormatText || f == RawFormatJSON || f == RawFormatAudit
}

// Event is one schema-v2 security event.
type Event struct {
	SchemaVersion   int             `json:"schema_version"`
	EventID         string          `json:"event_id"`
	Timestamp       time.Time       `json:"timestamp"`
	IngestTimestamp time.Time       `json:"ingest_timestamp"`
	TimestampSource TimestampSource `json:"timestamp_source"`
	Sequence        uint64          `json:"sequence"`

	Agent      Agent      `json:"agent"`
	Host       Host       `json:"host"`
	Event      EventMeta  `json:"event"`
	Source     *Entity    `json:"source,omitempty"`
	Target     *Entity    `json:"target,omitempty"`
	Raw        Raw        `json:"raw_event"`
	Collection Collection `json:"collection"`
}

// Agent is the persistent identity of the collecting agent.
type Agent struct {
	AgentID      string `json:"agent_id"`
	AgentType    string `json:"agent_type"`
	AgentVersion string `json:"agent_version"`
	// Generation is a persistent installation identity. It must survive agent
	// restarts and only change when the agent identity is deliberately reset.
	Generation string `json:"generation"`
}

// Host describes the host the agent is running on.
type Host struct {
	Hostname  string `json:"hostname,omitempty"`
	OS        string `json:"os,omitempty"`
	OSVersion string `json:"os_version,omitempty"`
	Kernel    string `json:"kernel,omitempty"`
}

// EventMeta describes the security action.
type EventMeta struct {
	Category Category `json:"category"`
	Action   string   `json:"action"`
	Outcome  string   `json:"outcome"`
}

// Entity identifies a source or target actor.
type Entity struct {
	Type EntityType `json:"entity_type"`
	ID   string     `json:"id,omitempty"`
	Name string     `json:"name,omitempty"`
	IP   string     `json:"ip,omitempty"`
	Path string     `json:"path,omitempty"`
	PID  *int       `json:"pid,omitempty"`
}

// Raw carries the evidence the event was derived from.
type Raw struct {
	Format RawFormat `json:"format"`
	Data   string    `json:"data"`
}

// Collection describes where and how the record was collected.
type Collection struct {
	SourceID   string     `json:"source_id"`
	SourceType SourceType `json:"source_type"`
	Path       string     `json:"path,omitempty"`
	// Generation is a persistent identity for this source incarnation. It must
	// survive agent restarts and change when the source is replaced.
	Generation string   `json:"generation"`
	Position   Position `json:"position"`
}

// Position is the record position within a source generation.
type Position struct {
	// ByteOffset is the byte offset of the record start. It is omitted for
	// agent-generated events such as heartbeats and may be omitted for sources
	// without byte positions (for example journald).
	ByteOffset *int64 `json:"byte_offset,omitempty"`
	// RecordNumber is a 1-based record ordinal within the source generation.
	// For agent-generated heartbeats it is a logical occurrence number.
	RecordNumber uint64 `json:"record_number"`
}

// Validate checks the structural contract and recomputes event_id.
//
// The ID check is an internal consistency check, not cryptographic
// authentication: a party that can rewrite the event can also recompute the ID.
// Authenticity must come from authenticated transport or a separate signature.
func (e Event) Validate() error {
	if e.SchemaVersion != SchemaVersionV2 {
		return fmt.Errorf("schema_version must be %d, got %d", SchemaVersionV2, e.SchemaVersion)
	}
	if !IsValidEventID(e.EventID) {
		return fmt.Errorf("invalid event_id %q", e.EventID)
	}
	if err := validateTimestamp("timestamp", e.Timestamp); err != nil {
		return err
	}
	if err := validateTimestamp("ingest_timestamp", e.IngestTimestamp); err != nil {
		return err
	}
	if !e.TimestampSource.Valid() {
		return fmt.Errorf("invalid timestamp_source %q", e.TimestampSource)
	}
	if e.Agent.AgentID == "" {
		return fmt.Errorf("agent.agent_id is required")
	}
	if e.Agent.AgentType == "" {
		return fmt.Errorf("agent.agent_type is required")
	}
	if e.Agent.AgentVersion == "" {
		return fmt.Errorf("agent.agent_version is required")
	}
	if e.Agent.Generation == "" {
		return fmt.Errorf("agent.generation is required")
	}
	if !e.Event.Category.Valid() {
		return fmt.Errorf("invalid event.category %q", e.Event.Category)
	}
	if e.Event.Action == "" {
		return fmt.Errorf("event.action is required")
	}
	if e.Event.Outcome == "" {
		return fmt.Errorf("event.outcome is required")
	}
	if e.Source != nil {
		if err := e.Source.validate("source"); err != nil {
			return err
		}
	}
	if e.Target != nil {
		if err := e.Target.validate("target"); err != nil {
			return err
		}
	}
	if !e.Raw.Format.Valid() {
		return fmt.Errorf("invalid raw.format %q", e.Raw.Format)
	}
	if len(e.Raw.Data) > MaxRawEvidenceBytes {
		return fmt.Errorf("raw_event.data exceeds %d bytes", MaxRawEvidenceBytes)
	}
	if e.Collection.SourceID == "" {
		return fmt.Errorf("collection.source_id is required")
	}
	if !e.Collection.SourceType.Valid() {
		return fmt.Errorf("invalid collection.source_type %q", e.Collection.SourceType)
	}
	if e.Collection.Generation == "" {
		return fmt.Errorf("collection.generation is required")
	}
	if e.Collection.Position.RecordNumber == 0 {
		return fmt.Errorf("collection.position.record_number must be 1 or greater")
	}
	if e.Collection.Position.ByteOffset != nil && *e.Collection.Position.ByteOffset < 0 {
		return fmt.Errorf("collection.position.byte_offset must not be negative")
	}
	if err := e.validateHeartbeat(); err != nil {
		return err
	}

	computed, err := e.ComputeEventID()
	if err != nil {
		return fmt.Errorf("cannot derive event_id: %w", err)
	}
	if computed != e.EventID {
		return fmt.Errorf("event_id mismatch: have %q, computed %q", e.EventID, computed)
	}
	return nil
}

// validateHeartbeat enforces explicit provenance for agent-generated
// heartbeats. They are not file records, so they must not carry a byte offset.
func (e Event) validateHeartbeat() error {
	if e.Event.Category != CategoryHeartbeat {
		return nil
	}
	if e.Collection.SourceType != SourceTypeAgent {
		return fmt.Errorf("heartbeat requires collection.source_type %q", SourceTypeAgent)
	}
	if e.TimestampSource != TimestampSourceIngest {
		return fmt.Errorf("heartbeat requires timestamp_source %q", TimestampSourceIngest)
	}
	if e.Collection.Position.ByteOffset != nil {
		return fmt.Errorf("heartbeat must not set collection.position.byte_offset")
	}
	if e.Collection.Path != "" {
		return fmt.Errorf("heartbeat must not set collection.path")
	}
	if e.Source != nil || e.Target != nil {
		return fmt.Errorf("heartbeat must not set source or target")
	}
	return nil
}

func validateTimestamp(name string, t time.Time) error {
	if t.IsZero() {
		return fmt.Errorf("%s is required", name)
	}
	if _, offset := t.Zone(); offset != 0 {
		return fmt.Errorf("%s must be UTC", name)
	}
	return nil
}

func (e Entity) validate(name string) error {
	if !e.Type.Valid() {
		return fmt.Errorf("invalid %s.type %q", name, e.Type)
	}
	if e.PID != nil && *e.PID < 0 {
		return fmt.Errorf("%s.pid must not be negative", name)
	}
	return nil
}
