package runtimev2

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/dispatcher"
	"agent/events"
	"agent/storage"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(t *testing.T, logPath, dbPath string) Config {
	t.Helper()
	return Config{
		LogPath:         logPath,
		SourceID:        "auth-log",
		DBPath:          dbPath,
		AgentID:         "linux-01",
		AgentType:       "linux",
		AgentVersion:    "2.0.0",
		KafkaBrokers:    []string{"127.0.0.1:19092"},
		KafkaTopic:      "ids.events.v2",
		PollInterval:    10 * time.Millisecond,
		PublishTimeout:  time.Second,
		AckTimeout:      time.Second,
		ShutdownTimeout: 5 * time.Second,
		StatsInterval:   time.Hour,
		Hostname:        "web-01",
	}
}

func copyFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type fakeProducer struct {
	mu          sync.Mutex
	messages    []dispatcher.Message
	failPublish bool
	attempts    int
}

func (p *fakeProducer) Publish(_ context.Context, messages []dispatcher.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.failPublish {
		return errors.New("broker unavailable")
	}
	for _, message := range messages {
		p.messages = append(p.messages, dispatcher.Message{
			Key:   append([]byte(nil), message.Key...),
			Value: append([]byte(nil), message.Value...),
		})
	}
	return nil
}

func (p *fakeProducer) Close() error { return nil }

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func (p *fakeProducer) attemptCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

func (p *fakeProducer) delivered() []dispatcher.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]dispatcher.Message(nil), p.messages...)
}

func (p *fakeProducer) allowDelivery() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failPublish = false
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func runRuntime(t *testing.T, rt *Runtime) (context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	return cancel, done
}

func stopRuntime(t *testing.T, cancel context.CancelFunc, done chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRunCollectsDispatchesAndPersistsCheckpoint(t *testing.T) {
	logPath := copyFixture(t, "auth.txt")
	dbPath := filepath.Join(t.TempDir(), "outbox.db")
	cfg := testConfig(t, logPath, dbPath)

	producer := &fakeProducer{}
	rt, err := New(cfg, testLogger(), producer)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	cancel, done := runRuntime(t, rt)
	waitFor(t, "two delivered events", func() bool { return producer.count() == 2 })
	stopRuntime(t, cancel, done)
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	messages := producer.delivered()
	if len(messages) != 2 {
		t.Fatalf("delivered = %d, want 2", len(messages))
	}
	for i, message := range messages {
		var event events.Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			t.Fatalf("message %d is not schema-v2 JSON: %v", i, err)
		}
		if event.SchemaVersion != events.SchemaVersionV2 {
			t.Fatalf("message %d schema = %d", i, event.SchemaVersion)
		}
		if event.Event.Category != events.CategoryAuthentication {
			t.Fatalf("message %d category = %q", i, event.Event.Category)
		}
		if string(message.Key) != "linux-01/auth-log" {
			t.Fatalf("message %d key = %q", i, message.Key)
		}
		if event.Collection.SourceID != "auth-log" || event.Raw.Data == "" {
			t.Fatalf("message %d lost collection provenance: %+v", i, event.Collection)
		}
	}

	var failure, success bool
	for _, message := range messages {
		var event events.Event
		_ = json.Unmarshal(message.Value, &event)
		switch event.Event.Outcome {
		case "failure":
			failure = true
		case "success":
			success = true
		}
	}
	if !failure || !success {
		t.Fatalf("failure=%v success=%v, want both", failure, success)
	}

	reopened, err := storage.Open(cfg.toStorageConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending, err := reopened.Pending(context.Background(), storage.MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after a clean shutdown = %d, want 0", len(pending))
	}
}

func TestRestartDeliversPendingEvents(t *testing.T) {
	logPath := copyFixture(t, "auth.txt")
	dbPath := filepath.Join(t.TempDir(), "outbox.db")
	cfg := testConfig(t, logPath, dbPath)

	failing := &fakeProducer{failPublish: true}
	first, err := New(cfg, testLogger(), failing)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runRuntime(t, first)
	waitFor(t, "pending events to be retained", func() bool {
		return failing.attemptCount() > 0 && first.Stats().Pending == 2
	})
	stopRuntime(t, cancel, done)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	producer := &fakeProducer{}
	second, err := New(cfg, testLogger(), producer)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	cancel, done = runRuntime(t, second)
	waitFor(t, "replayed events", func() bool { return producer.count() == 2 })
	stopRuntime(t, cancel, done)

	for i, message := range producer.delivered() {
		var event events.Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			t.Fatalf("replayed message %d is invalid: %v", i, err)
		}
		if event.EventID == "" {
			t.Fatalf("replayed message %d lost its event_id", i)
		}
	}
	if stats := second.Stats(); stats.Acknowledged != 2 {
		t.Fatalf("acknowledged = %d, want 2", stats.Acknowledged)
	}
}

func TestRunStopsOnRejectedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.log")
	line := strings.Repeat("x", events.MaxRawEvidenceBytes+1) + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, path, filepath.Join(t.TempDir(), "outbox.db"))

	rt, err := New(cfg, testLogger(), &fakeProducer{})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = rt.Run(ctx)
	if err == nil {
		t.Fatal("Run() = nil, want a rejected-record error")
	}
	if !strings.Contains(err.Error(), "rejected_record") {
		t.Fatalf("Run() = %v, want an actionable rejected_record reason", err)
	}
}

func TestConfigValidation(t *testing.T) {
	valid := testConfig(t, "/tmp/auth.log", "/tmp/outbox.db")
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing db", func(c *Config) { c.DBPath = "" }},
		{"missing agent id", func(c *Config) { c.AgentID = "" }},
		{"missing agent type", func(c *Config) { c.AgentType = "" }},
		{"missing topic", func(c *Config) { c.KafkaTopic = "" }},
		{"missing brokers", func(c *Config) { c.KafkaBrokers = nil }},
		{"blank broker", func(c *Config) { c.KafkaBrokers = []string{" "} }},
		{"missing log path", func(c *Config) { c.LogPath = "" }},
		{"missing source id", func(c *Config) { c.SourceID = "" }},
		{"negative capacity", func(c *Config) { c.Capacity = -1 }},
		{"negative timeout", func(c *Config) { c.PublishTimeout = -time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestLoadConfigFromEnvironment(t *testing.T) {
	t.Setenv("LOGLY_V2_LOG_PATH", "/tmp/auth.log")
	t.Setenv("LOGLY_V2_SOURCE_ID", "auth-log")
	t.Setenv("LOGLY_V2_DB_PATH", "/tmp/outbox.db")
	t.Setenv("LOGLY_V2_AGENT_ID", "linux-01")
	t.Setenv("LOGLY_V2_AGENT_TYPE", "linux")
	t.Setenv("LOGLY_V2_KAFKA_BROKERS", "localhost:19092, localhost:19093")
	t.Setenv("LOGLY_V2_KAFKA_TOPIC", "ids.events.v2")
	t.Setenv("LOGLY_V2_CAPACITY", "50")
	t.Setenv("LOGLY_V2_POLL_INTERVAL", "2s")
	t.Setenv("LOGLY_V2_SHUTDOWN_TIMEOUT", "15s")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Capacity != 50 || cfg.PollInterval != 2*time.Second ||
		cfg.ShutdownTimeout != 15*time.Second {
		t.Fatalf("parsed config = %+v", cfg)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "localhost:19093" {
		t.Fatalf("brokers = %v", cfg.KafkaBrokers)
	}

	t.Setenv("LOGLY_V2_KAFKA_BROKERS", "")
	if _, err := LoadConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("LoadConfig without brokers = %v, want ErrInvalidConfig", err)
	}
	t.Setenv("LOGLY_V2_KAFKA_BROKERS", "localhost:19092")
	t.Setenv("LOGLY_V2_CAPACITY", "not-a-number")
	if _, err := LoadConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("LoadConfig with bad capacity = %v, want ErrInvalidConfig", err)
	}
}

func TestDescribeClassifiesErrors(t *testing.T) {
	tests := []struct {
		err    error
		reason string
	}{
		{storage.ErrCorruptPayload, "corrupt_storage"},
		{dispatcher.ErrPublish, "kafka_unavailable"},
		{dispatcher.ErrAck, "outbox_ack_failure"},
		{errors.New("other"), "component_error"},
	}
	for _, test := range tests {
		reason, hint := describe(test.err)
		if reason != test.reason || hint == "" {
			t.Fatalf("describe(%v) = %q, %q", test.err, reason, hint)
		}
	}
}
