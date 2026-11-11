package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const eventIDPrefix = "evt2_"

// IDInput contains the stable coordinates used to derive an event ID.
//
// AgentID and AgentGeneration identify the persistent agent. SourceID,
// SourceType and SourceGeneration identify one source incarnation. Position is
// the record position within that generation. RawData is the exact evidence
// bytes read from the source.
type IDInput struct {
	AgentID          string
	AgentGeneration  string
	SourceID         string
	SourceType       SourceType
	SourceGeneration string
	Position         Position
	RawData          string
}

type canonicalID struct {
	AgentID          string     `json:"agent_id"`
	AgentGeneration  string     `json:"agent_generation"`
	SourceID         string     `json:"source_id"`
	SourceType       SourceType `json:"source_type"`
	SourceGeneration string     `json:"source_generation"`
	ByteOffset       *int64     `json:"byte_offset,omitempty"`
	RecordNumber     uint64     `json:"record_number"`
	RawSHA256        string     `json:"raw_sha256"`
}

// GenerateEventID derives a deterministic, versioned event ID from the
// persistent agent/source identities and the record position.
//
// The ID is "evt2_" followed by the lowercase hex SHA-256 of a canonical JSON
// document containing the input fields and the SHA-256 of RawData.
//
// Identical repeated lines are distinguished by position: the same bytes at
// two different record positions produce different IDs. Re-reading the same
// record at the same position produces the same ID, which makes retries and
// replay deduplicable. If bytes at the same position change (for example an
// in-place rewrite), the raw hash changes and the ID changes.
func GenerateEventID(in IDInput) (string, error) {
	if in.AgentID == "" {
		return "", fmt.Errorf("agent_id is required")
	}
	if in.AgentGeneration == "" {
		return "", fmt.Errorf("agent_generation is required")
	}
	if in.SourceID == "" {
		return "", fmt.Errorf("source_id is required")
	}
	if !in.SourceType.Valid() {
		return "", fmt.Errorf("invalid source_type %q", in.SourceType)
	}
	if in.SourceGeneration == "" {
		return "", fmt.Errorf("source_generation is required")
	}
	if in.Position.RecordNumber == 0 {
		return "", fmt.Errorf("record_number must be 1 or greater")
	}
	if in.Position.ByteOffset != nil && *in.Position.ByteOffset < 0 {
		return "", fmt.Errorf("byte_offset must not be negative")
	}
	if len(in.RawData) > MaxRawEvidenceBytes {
		return "", fmt.Errorf("raw data exceeds %d bytes", MaxRawEvidenceBytes)
	}

	rawSum := sha256.Sum256([]byte(in.RawData))
	canonical, err := json.Marshal(canonicalID{
		AgentID:          in.AgentID,
		AgentGeneration:  in.AgentGeneration,
		SourceID:         in.SourceID,
		SourceType:       in.SourceType,
		SourceGeneration: in.SourceGeneration,
		ByteOffset:       in.Position.ByteOffset,
		RecordNumber:     in.Position.RecordNumber,
		RawSHA256:        hex.EncodeToString(rawSum[:]),
	})
	if err != nil {
		return "", fmt.Errorf("encode canonical event id input: %w", err)
	}

	sum := sha256.Sum256(canonical)
	return eventIDPrefix + hex.EncodeToString(sum[:]), nil
}

// ComputeEventID derives the event ID for this event from its current fields.
// It does not read or write EventID.
func (e Event) ComputeEventID() (string, error) {
	return GenerateEventID(IDInput{
		AgentID:          e.Agent.AgentID,
		AgentGeneration:  e.Agent.Generation,
		SourceID:         e.Collection.SourceID,
		SourceType:       e.Collection.SourceType,
		SourceGeneration: e.Collection.Generation,
		Position:         e.Collection.Position,
		RawData:          e.Raw.Data,
	})
}

// IsValidEventID reports whether id is exactly "evt2_" followed by 64
// lowercase hexadecimal characters.
func IsValidEventID(id string) bool {
	if !strings.HasPrefix(id, eventIDPrefix) {
		return false
	}
	encoded := strings.TrimPrefix(id, eventIDPrefix)
	if len(encoded) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(encoded); i++ {
		c := encoded[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
