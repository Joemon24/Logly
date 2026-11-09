package events

import (
	"strings"
	"testing"
	"time"
)

func testIDInput() IDInput {
	return IDInput{
		AgentID:          "linux-01",
		AgentGeneration:  "agent-gen-0001",
		SourceID:         "auth-log",
		SourceType:       SourceTypeFile,
		SourceGeneration: "source-gen-0001",
		Position:         Position{ByteOffset: int64Ptr(1024), RecordNumber: 7},
		RawData:          "Failed password for alice from 203.0.113.9 port 50000 ssh2\n",
	}
}

func TestGenerateEventIDIsDeterministic(t *testing.T) {
	in := testIDInput()
	first, err := GenerateEventID(in)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		got, err := GenerateEventID(in)
		if err != nil {
			t.Fatal(err)
		}
		if got != first {
			t.Fatalf("iteration %d: id changed: %q != %q", i, got, first)
		}
	}
	if !IsValidEventID(first) {
		t.Fatalf("generated id is invalid: %q", first)
	}
}

func TestGenerateEventIDDistinguishesIdenticalRepeatedLines(t *testing.T) {
	in := testIDInput()
	first, err := GenerateEventID(in)
	if err != nil {
		t.Fatal(err)
	}

	in2 := testIDInput()
	in2.Position = Position{ByteOffset: int64Ptr(2048), RecordNumber: 8}
	second, err := GenerateEventID(in2)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("identical repeated lines at different positions must have different ids")
	}
}

func TestGenerateEventIDDistinguishesMissingAndZeroByteOffset(t *testing.T) {
	noOffset := testIDInput()
	noOffset.Position.ByteOffset = nil
	missing, err := GenerateEventID(noOffset)
	if err != nil {
		t.Fatal(err)
	}

	zeroOffset := testIDInput()
	zeroOffset.Position.ByteOffset = int64Ptr(0)
	zero, err := GenerateEventID(zeroOffset)
	if err != nil {
		t.Fatal(err)
	}
	if missing == zero {
		t.Fatal("omitted byte_offset must not collide with an explicit zero offset")
	}
}

func TestGenerateEventIDRejectsOversizedRawData(t *testing.T) {
	in := testIDInput()
	in.RawData = strings.Repeat("x", MaxRawEvidenceBytes+1)
	if _, err := GenerateEventID(in); err == nil {
		t.Fatal("oversized raw data accepted")
	}

	in.RawData = strings.Repeat("x", MaxRawEvidenceBytes)
	if _, err := GenerateEventID(in); err != nil {
		t.Fatalf("maximum-size raw data rejected: %v", err)
	}
}

func TestGenerateEventIDIsStableForTheSameRecord(t *testing.T) {
	in := testIDInput()
	a, err := GenerateEventID(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateEventID(in)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("same record position must produce the same id")
	}
}

func TestGenerateEventIDChangesWhenRecordCoordinatesChange(t *testing.T) {
	base, err := GenerateEventID(testIDInput())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*IDInput)
	}{
		{"agent id", func(in *IDInput) { in.AgentID = "linux-02" }},
		{"agent generation", func(in *IDInput) { in.AgentGeneration = "agent-gen-0002" }},
		{"source id", func(in *IDInput) { in.SourceID = "sudo-log" }},
		{"source type", func(in *IDInput) { in.SourceType = SourceTypeJournald }},
		{"source generation", func(in *IDInput) { in.SourceGeneration = "source-gen-0002" }},
		{"record number", func(in *IDInput) { in.Position.RecordNumber = 8 }},
		{"byte offset", func(in *IDInput) { in.Position.ByteOffset = int64Ptr(2048) }},
		{"raw data", func(in *IDInput) { in.RawData = "a different line\n" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := testIDInput()
			tt.mutate(&in)
			got, err := GenerateEventID(in)
			if err != nil {
				t.Fatal(err)
			}
			if got == base {
				t.Fatalf("id did not change after %s", tt.name)
			}
		})
	}
}

func TestGenerateEventIDRequiresStableCoordinates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*IDInput)
	}{
		{"agent id", func(in *IDInput) { in.AgentID = "" }},
		{"agent generation", func(in *IDInput) { in.AgentGeneration = "" }},
		{"source id", func(in *IDInput) { in.SourceID = "" }},
		{"source type", func(in *IDInput) { in.SourceType = "socket" }},
		{"source generation", func(in *IDInput) { in.SourceGeneration = "" }},
		{"record number", func(in *IDInput) { in.Position.RecordNumber = 0 }},
		{"byte offset", func(in *IDInput) { in.Position.ByteOffset = int64Ptr(-1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := testIDInput()
			tt.mutate(&in)
			if _, err := GenerateEventID(in); err == nil {
				t.Fatal("invalid id input accepted")
			}
		})
	}
}

func TestComputeEventIDMatchesGenerateEventID(t *testing.T) {
	e := testEvent(t)
	computed, err := e.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	direct, err := GenerateEventID(IDInput{
		AgentID:          e.Agent.AgentID,
		AgentGeneration:  e.Agent.Generation,
		SourceID:         e.Collection.SourceID,
		SourceType:       e.Collection.SourceType,
		SourceGeneration: e.Collection.Generation,
		Position:         e.Collection.Position,
		RawData:          e.Raw.Data,
	})
	if err != nil {
		t.Fatal(err)
	}
	if computed != direct || e.EventID != direct {
		t.Fatalf("id mismatch: computed=%q direct=%q event=%q", computed, direct, e.EventID)
	}
}

func TestComputeEventIDIgnoresMutableEventFields(t *testing.T) {
	e := testEvent(t)
	want := e.EventID

	e.Timestamp = e.Timestamp.Add(24 * time.Hour)
	e.IngestTimestamp = e.IngestTimestamp.Add(48 * time.Hour)
	e.Sequence = 999
	e.Agent.AgentVersion = "9.9.9"
	e.Host.Hostname = "other-host"
	e.Event.Action = "different_action"
	e.Event.Outcome = "different_outcome"

	got, err := e.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("id must depend only on identity, position and raw evidence: %q != %q", got, want)
	}
}

func TestIsValidEventID(t *testing.T) {
	valid, err := GenerateEventID(testIDInput())
	if err != nil {
		t.Fatal(err)
	}
	if !IsValidEventID(valid) {
		t.Fatalf("generated id rejected: %q", valid)
	}

	invalid := []string{
		"",
		"heartbeat",
		"evt1_" + strings.Repeat("a", 64),
		"evt2_" + strings.Repeat("a", 63),
		"evt2_" + strings.Repeat("z", 64),
		"evt2_" + strings.Repeat("A", 64),
		"evt2_" + strings.Repeat("a", 65),
	}
	for _, id := range invalid {
		if IsValidEventID(id) {
			t.Fatalf("invalid id accepted: %q", id)
		}
	}
}
