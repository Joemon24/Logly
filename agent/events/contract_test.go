package events

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var fixtureCategories = map[string]Category{
	"authentication.json": CategoryAuthentication,
	"sudo.json":           CategorySudo,
	"account.json":        CategoryAccount,
	"process.json":        CategoryProcess,
	"file_integrity.json": CategoryFileIntegrity,
	"heartbeat.json":      CategoryHeartbeat,
}

func TestFixturesMatchSchemaAndIDs(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(fixtureCategories) {
		t.Fatalf("expected %d fixtures, found %d", len(fixtureCategories), len(entries))
	}

	for name, wantCategory := range fixtureCategories {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}

			var event Event
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&event); err != nil {
				t.Fatalf("decode: %v", err)
			}
			var extra any
			if err := dec.Decode(&extra); err != io.EOF {
				t.Fatalf("trailing JSON data: %v", err)
			}

			if event.SchemaVersion != SchemaVersionV2 {
				t.Fatalf("schema_version = %d", event.SchemaVersion)
			}
			if err := event.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			if event.Event.Category != wantCategory {
				t.Fatalf("category = %q, want %q", event.Event.Category, wantCategory)
			}
			if !IsValidEventID(event.EventID) {
				t.Fatalf("invalid event_id %q", event.EventID)
			}
			computed, err := event.ComputeEventID()
			if err != nil {
				t.Fatalf("compute id: %v", err)
			}
			if computed != event.EventID {
				t.Fatalf("event_id does not match derivation: fixture=%q computed=%q", event.EventID, computed)
			}

			var doc map[string]json.RawMessage
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{
				"schema_version", "event_id", "timestamp", "ingest_timestamp",
				"timestamp_source", "sequence", "agent", "host", "event", "raw_event", "collection",
			} {
				if _, ok := doc[key]; !ok {
					t.Fatalf("missing required key %q", key)
				}
			}
			for _, key := range []string{"timestamp", "ingest_timestamp"} {
				var value string
				if err := json.Unmarshal(doc[key], &value); err != nil {
					t.Fatalf("%s: %v", key, err)
				}
				parsed, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					t.Fatalf("%s is not RFC3339Nano: %v", key, err)
				}
				if parsed.Format(time.RFC3339Nano) != value || !strings.HasSuffix(value, "Z") {
					t.Fatalf("%s is not canonical UTC RFC3339Nano: %q", key, value)
				}
			}
		})
	}
}

func TestFixturesRoundTrip(t *testing.T) {
	for name := range fixtureCategories {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			var event Event
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, data, encoded)
		})
	}
}

func TestFixturesUsePythonCompatibleNames(t *testing.T) {
	for name := range fixtureCategories {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			if _, ok := doc["raw_event"]; !ok {
				t.Fatal("missing raw_event")
			}
			if _, ok := doc["raw"]; ok {
				t.Fatal("legacy raw key must not be emitted")
			}

			for _, key := range []string{"source", "target"} {
				raw, ok := doc[key]
				if !ok {
					continue
				}
				var entity map[string]json.RawMessage
				if err := json.Unmarshal(raw, &entity); err != nil {
					t.Fatal(err)
				}
				if _, ok := entity["entity_type"]; !ok {
					t.Fatalf("%s missing entity_type", key)
				}
				if _, ok := entity["type"]; ok {
					t.Fatalf("%s must not use legacy type key", key)
				}
			}
		})
	}
}

func TestHeartbeatFixtureOmitsEntities(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "heartbeat.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["source"]; ok {
		t.Fatal("heartbeat fixture should omit source")
	}
	if _, ok := doc["target"]; ok {
		t.Fatal("heartbeat fixture should omit target")
	}

	var collection struct {
		SourceType string `json:"source_type"`
		Generation string `json:"generation"`
		Path       string `json:"path"`
		Position   struct {
			ByteOffset   *int64 `json:"byte_offset"`
			RecordNumber uint64 `json:"record_number"`
		} `json:"position"`
	}
	if err := json.Unmarshal(doc["collection"], &collection); err != nil {
		t.Fatal(err)
	}
	if collection.SourceType != string(SourceTypeAgent) {
		t.Fatalf("heartbeat source_type = %q", collection.SourceType)
	}
	if collection.Generation == "" || collection.Position.RecordNumber == 0 {
		t.Fatal("heartbeat provenance is incomplete")
	}
	if collection.Path != "" {
		t.Fatal("heartbeat must not have a path")
	}
	if collection.Position.ByteOffset != nil {
		t.Fatal("heartbeat must omit byte_offset")
	}

	var timestampSource string
	if err := json.Unmarshal(doc["timestamp_source"], &timestampSource); err != nil {
		t.Fatal(err)
	}
	if timestampSource != string(TimestampSourceIngest) {
		t.Fatalf("heartbeat timestamp_source = %q", timestampSource)
	}
}

func assertJSONEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var wantValue, gotValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantValue, gotValue) {
		t.Fatalf("JSON mismatch\nwant: %s\ngot:  %s", want, got)
	}
}
