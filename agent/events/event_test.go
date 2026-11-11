package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testEvent(t *testing.T) Event {
	t.Helper()

	e := Event{
		SchemaVersion:   SchemaVersionV2,
		Timestamp:       time.Date(2026, 11, 9, 10, 15, 30, 123456789, time.UTC),
		IngestTimestamp: time.Date(2026, 11, 9, 10, 15, 31, 987654321, time.UTC),
		TimestampSource: TimestampSourceLog,
		Sequence:        42,
		Agent: Agent{
			AgentID:      "linux-01",
			AgentType:    "linux",
			AgentVersion: "2.0.0",
			Generation:   "agent-gen-0001",
		},
		Host: Host{
			Hostname:  "web-01",
			OS:        "ubuntu",
			OSVersion: "24.04",
			Kernel:    "6.8.0-45-generic",
		},
		Event: EventMeta{
			Category: CategoryAuthentication,
			Action:   "ssh_login",
			Outcome:  "failure",
		},
		Source: &Entity{Type: EntityIP, IP: "203.0.113.9"},
		Target: &Entity{Type: EntityUser, ID: "1000", Name: "alice"},
		Raw: Raw{
			Format: RawFormatText,
			Data:   "Failed password for alice from 203.0.113.9 port 50000 ssh2\n",
		},
		Collection: Collection{
			SourceID:   "auth-log",
			SourceType: SourceTypeFile,
			Path:       "/var/log/auth.log",
			Generation: "source-gen-0001",
			Position:   Position{ByteOffset: int64Ptr(1024), RecordNumber: 7},
		},
	}

	id, err := e.ComputeEventID()
	if err != nil {
		t.Fatalf("compute id: %v", err)
	}
	e.EventID = id
	return e
}

func int64Ptr(v int64) *int64 { return &v }

func TestEventValidateAcceptsRepresentativeEvent(t *testing.T) {
	if err := testEvent(t).Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestEventValidateRejectsIDMismatch(t *testing.T) {
	e := testEvent(t)
	e.EventID = "evt2_" + strings.Repeat("0", 64)
	err := e.Validate()
	if err == nil || !strings.Contains(err.Error(), "event_id mismatch") {
		t.Fatalf("expected mismatch error, got %v", err)
	}
}

func TestEventValidateRejectsOversizedRawEvidence(t *testing.T) {
	e := testEvent(t)
	e.Raw.Data = strings.Repeat("x", MaxRawEvidenceBytes+1)
	err := e.Validate()
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size error, got %v", err)
	}
}

func TestEventValidateAcceptsMaximumRawEvidence(t *testing.T) {
	e := testEvent(t)
	e.Raw.Data = strings.Repeat("x", MaxRawEvidenceBytes)
	id, err := e.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	e.EventID = id
	if err := e.Validate(); err != nil {
		t.Fatalf("maximum-size evidence rejected: %v", err)
	}
}

func testHeartbeat(t *testing.T) Event {
	t.Helper()

	e := testEvent(t)
	e.TimestampSource = TimestampSourceIngest
	e.Event = EventMeta{Category: CategoryHeartbeat, Action: "agent_alive", Outcome: "success"}
	e.Source = nil
	e.Target = nil
	e.Raw = Raw{Format: RawFormatJSON, Data: `{"uptime_seconds":3600}`}
	e.Collection = Collection{
		SourceID:   "agent-heartbeat",
		SourceType: SourceTypeAgent,
		Generation: "agent-gen-heartbeat",
		Position:   Position{RecordNumber: uint64(e.Timestamp.UnixNano())},
	}

	id, err := e.ComputeEventID()
	if err != nil {
		t.Fatalf("compute heartbeat id: %v", err)
	}
	e.EventID = id
	return e
}

func TestHeartbeatProvenanceAndStableID(t *testing.T) {
	e := testHeartbeat(t)
	if err := e.Validate(); err != nil {
		t.Fatalf("valid heartbeat rejected: %v", err)
	}
	again, err := e.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	if again != e.EventID {
		t.Fatalf("heartbeat id is not stable: %q != %q", again, e.EventID)
	}

	next := e
	next.Timestamp = next.Timestamp.Add(10 * time.Second)
	next.Collection.Position.RecordNumber = uint64(next.Timestamp.UnixNano())
	nextID, err := next.ComputeEventID()
	if err != nil {
		t.Fatal(err)
	}
	if nextID == e.EventID {
		t.Fatal("a later heartbeat occurrence must have a different id")
	}
}

func TestHeartbeatRejectsFileProvenance(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
	}{
		{"file source type", func(e *Event) { e.Collection.SourceType = SourceTypeFile }},
		{"log timestamp source", func(e *Event) { e.TimestampSource = TimestampSourceLog }},
		{"byte offset", func(e *Event) { e.Collection.Position.ByteOffset = int64Ptr(0) }},
		{"path", func(e *Event) { e.Collection.Path = "/var/log/auth.log" }},
		{"source entity", func(e *Event) { e.Source = &Entity{Type: EntityUser, Name: "alice"} }},
		{"target entity", func(e *Event) { e.Target = &Entity{Type: EntityUser, Name: "alice"} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := testHeartbeat(t)
			tt.mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("invalid heartbeat provenance accepted")
			}
		})
	}
}

func TestEventValidateRejectsInvalidFields(t *testing.T) {
	nonUTC := time.Date(2026, 11, 9, 10, 15, 30, 0, time.FixedZone("CET", 3600))

	tests := []struct {
		name   string
		mutate func(*Event)
	}{
		{"schema version", func(e *Event) { e.SchemaVersion = 1 }},
		{"event id", func(e *Event) { e.EventID = "heartbeat" }},
		{"uppercase event id", func(e *Event) { e.EventID = "evt2_" + strings.Repeat("A", 64) }},
		{"zero event timestamp", func(e *Event) { e.Timestamp = time.Time{} }},
		{"zero ingest timestamp", func(e *Event) { e.IngestTimestamp = time.Time{} }},
		{"non UTC event timestamp", func(e *Event) { e.Timestamp = nonUTC }},
		{"non UTC ingest timestamp", func(e *Event) { e.IngestTimestamp = nonUTC }},
		{"timestamp source", func(e *Event) { e.TimestampSource = "wallclock" }},
		{"agent id", func(e *Event) { e.Agent.AgentID = "" }},
		{"agent type", func(e *Event) { e.Agent.AgentType = "" }},
		{"agent version", func(e *Event) { e.Agent.AgentVersion = "" }},
		{"agent generation", func(e *Event) { e.Agent.Generation = "" }},
		{"category", func(e *Event) { e.Event.Category = "malware" }},
		{"action", func(e *Event) { e.Event.Action = "" }},
		{"outcome", func(e *Event) { e.Event.Outcome = "" }},
		{"source type", func(e *Event) { e.Source.Type = "socket" }},
		{"target type", func(e *Event) { e.Target.Type = "socket" }},
		{"negative pid", func(e *Event) { pid := -1; e.Target.PID = &pid }},
		{"raw format", func(e *Event) { e.Raw.Format = "binary" }},
		{"source id", func(e *Event) { e.Collection.SourceID = "" }},
		{"collection source type", func(e *Event) { e.Collection.SourceType = "socket" }},
		{"source generation", func(e *Event) { e.Collection.Generation = "" }},
		{"record number", func(e *Event) { e.Collection.Position.RecordNumber = 0 }},
		{"byte offset", func(e *Event) { e.Collection.Position.ByteOffset = int64Ptr(-1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := testEvent(t)
			tt.mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
}

func TestCategoriesAreSupported(t *testing.T) {
	categories := []Category{
		CategoryAuthentication,
		CategorySudo,
		CategoryAccount,
		CategoryProcess,
		CategoryFileIntegrity,
		CategoryHeartbeat,
	}
	for _, c := range categories {
		if !c.Valid() {
			t.Fatalf("category %q is not valid", c)
		}
	}
	if Category("malware").Valid() {
		t.Fatal("unknown category accepted")
	}
}

func TestTimestampsUseRFC3339Nano(t *testing.T) {
	e := testEvent(t)
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"timestamp":"2026-11-09T10:15:30.123456789Z"`) {
		t.Fatalf("timestamp is not RFC3339Nano UTC: %s", data)
	}
	if !strings.Contains(string(data), `"ingest_timestamp":"2026-11-09T10:15:31.987654321Z"`) {
		t.Fatalf("ingest timestamp is not RFC3339Nano UTC: %s", data)
	}
}

func TestOptionalFieldsAreOmitted(t *testing.T) {
	e := testEvent(t)
	e.Source = nil
	e.Target = nil
	e.Host = Host{Hostname: "web-01"}
	e.Collection.Path = ""

	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["source"]; ok {
		t.Fatal("nil source was not omitted")
	}
	if _, ok := decoded["target"]; ok {
		t.Fatal("nil target was not omitted")
	}
	host := decoded["host"].(map[string]any)
	for _, key := range []string{"os", "os_version", "kernel"} {
		if _, ok := host[key]; ok {
			t.Fatalf("empty host.%s was not omitted", key)
		}
	}
	collection := decoded["collection"].(map[string]any)
	if _, ok := collection["path"]; ok {
		t.Fatal("empty collection.path was not omitted")
	}
}

func TestEventJSONRoundTripPreservesFields(t *testing.T) {
	e := testEvent(t)
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped event invalid: %v", err)
	}
	if got.EventID != e.EventID {
		t.Fatalf("event id changed: %q != %q", got.EventID, e.EventID)
	}
	if !got.Timestamp.Equal(e.Timestamp) || !got.IngestTimestamp.Equal(e.IngestTimestamp) {
		t.Fatal("timestamps changed")
	}
	if got.Source == nil || got.Target == nil {
		t.Fatal("entities lost")
	}
}
