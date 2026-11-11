package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent/events"
	"agent/storage"
)

func testConfig(t *testing.T) storage.Config {
	t.Helper()
	return storage.Config{
		Path:         filepath.Join(t.TempDir(), "outbox.db"),
		AgentID:      "linux-01",
		AgentType:    "linux",
		AgentVersion: "2.0.0",
		Capacity:     0,
	}
}

func openTestStore(t *testing.T, cfg storage.Config) *storage.Store {
	t.Helper()
	store, err := storage.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func reopenStore(t *testing.T, store *storage.Store, cfg storage.Config) *storage.Store {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return openTestStore(t, cfg)
}

func dispatcherEvent(t *testing.T, store *storage.Store, sourceID, generation string, record uint64) events.Event {
	t.Helper()
	identity := store.Identity()
	offset := int64(record * 100)
	timestamp := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC).
		Add(time.Duration(record) * time.Second)

	event := events.Event{
		SchemaVersion:   events.SchemaVersionV2,
		Timestamp:       timestamp,
		IngestTimestamp: timestamp.Add(10 * time.Millisecond),
		TimestampSource: events.TimestampSourceLog,
		Sequence:        record,
		Agent: events.Agent{
			AgentID:      identity.AgentID,
			AgentType:    identity.AgentType,
			AgentVersion: identity.AgentVersion,
			Generation:   identity.Generation,
		},
		Host: events.Host{Hostname: "web-01"},
		Event: events.EventMeta{
			Category: events.CategoryAuthentication,
			Action:   "ssh_login",
			Outcome:  "failure",
		},
		Source: &events.Entity{Type: events.EntityIP, IP: "203.0.113.9"},
		Raw: events.Raw{
			Format: events.RawFormatText,
			Data:   fmt.Sprintf("%s %s record %d\n", sourceID, generation, record),
		},
		Collection: events.Collection{
			SourceID:   sourceID,
			SourceType: events.SourceTypeFile,
			Path:       "/var/log/" + sourceID,
			Generation: generation,
			Position: events.Position{
				ByteOffset:   &offset,
				RecordNumber: record,
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

func appendEvents(t *testing.T, store *storage.Store, eventsToStore ...events.Event) {
	t.Helper()
	if err := store.Append(context.Background(), eventsToStore); err != nil {
		t.Fatalf("append events: %v", err)
	}
}

type fakeProducer struct {
	mu     sync.Mutex
	calls  [][]Message
	closed bool
	fn     func(context.Context, []Message) error
}

func (p *fakeProducer) Publish(ctx context.Context, messages []Message) error {
	cloned := make([]Message, len(messages))
	for i, message := range messages {
		cloned[i] = Message{
			Key:   append([]byte(nil), message.Key...),
			Value: append([]byte(nil), message.Value...),
		}
	}
	p.mu.Lock()
	p.calls = append(p.calls, cloned)
	fn := p.fn
	p.mu.Unlock()
	if fn != nil {
		return fn(ctx, cloned)
	}
	return nil
}

func (p *fakeProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *fakeProducer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *fakeProducer) call(index int) []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[index]
}

type fakeStore struct {
	mu           sync.Mutex
	entries      []storage.OutboxEntry
	ackErr       error
	pendingCalls int
	ackCalls     int
}

func (s *fakeStore) Pending(_ context.Context, limit int) ([]storage.OutboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingCalls++
	if limit > len(s.entries) {
		limit = len(s.entries)
	}
	out := make([]storage.OutboxEntry, limit)
	copy(out, s.entries[:limit])
	return out, nil
}

func (s *fakeStore) Ack(_ context.Context, eventIDs []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	if s.ackErr != nil {
		return 0, s.ackErr
	}
	removed := 0
	for _, eventID := range eventIDs {
		for i, entry := range s.entries {
			if entry.EventID == eventID {
				s.entries = append(s.entries[:i], s.entries[i+1:]...)
				removed++
				break
			}
		}
	}
	return removed, nil
}

func (s *fakeStore) OutboxStats(context.Context) (storage.OutboxStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return storage.OutboxStats{Pending: len(s.entries)}, nil
}

func (s *fakeStore) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *fakeStore) pendingCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingCalls
}

func (s *fakeStore) ackCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackCalls
}

func newTestDispatcher(t *testing.T, store Store, producer Producer) *Dispatcher {
	t.Helper()
	d, err := New(Config{
		Store:          store,
		Producer:       producer,
		BatchSize:      10,
		PollInterval:   time.Millisecond,
		PublishTimeout: time.Second,
		AckTimeout:     time.Second,
		RetryBase:      time.Millisecond,
		RetryMax:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	t.Cleanup(func() {
		_ = d.Close()
	})
	return d
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	producer := &fakeProducer{}
	store := &fakeStore{}
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "missing store", cfg: Config{Producer: producer}},
		{name: "missing producer", cfg: Config{Store: store}},
		{name: "batch too large", cfg: Config{
			Store: store, Producer: producer, BatchSize: storage.MaxPendingLimit + 1}},
		{name: "negative batch", cfg: Config{Store: store, Producer: producer, BatchSize: -1}},
		{name: "negative poll", cfg: Config{Store: store, Producer: producer, PollInterval: -time.Second}},
		{name: "negative publish timeout", cfg: Config{
			Store: store, Producer: producer, PublishTimeout: -time.Second}},
		{name: "negative ack timeout", cfg: Config{
			Store: store, Producer: producer, AckTimeout: -time.Second}},
		{name: "retry max below base", cfg: Config{
			Store: store, Producer: producer, RetryBase: time.Second, RetryMax: time.Millisecond}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestDispatchPublishesAndAcknowledges(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	first := dispatcherEvent(t, store, "auth-log", "gen-1", 1)
	second := dispatcherEvent(t, store, "auth-log", "gen-1", 2)
	appendEvents(t, store, first, second)

	producer := &fakeProducer{}
	d := newTestDispatcher(t, store, producer)

	acked, err := d.DispatchOnce(context.Background())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if acked != 2 {
		t.Fatalf("acked = %d, want 2", acked)
	}
	if producer.callCount() != 1 || len(producer.call(0)) != 2 {
		t.Fatalf("producer calls = %d, batch = %d", producer.callCount(), len(producer.call(0)))
	}
	if entries, err := store.Pending(context.Background(), storage.MaxPendingLimit); err != nil || len(entries) != 0 {
		t.Fatalf("pending after dispatch = %d, err %v", len(entries), err)
	}

	stats := d.Stats()
	if stats.Delivered != 2 || stats.Acknowledged != 2 || stats.Failures != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if !stats.PendingKnown || stats.Pending != 0 || stats.OldestPendingAge != 0 {
		t.Fatalf("backlog stats = %+v", stats)
	}
}

func TestPayloadIdentityAndEventIDKey(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	event := dispatcherEvent(t, store, "auth-log", "gen-1", 1)
	appendEvents(t, store, event)

	stored, err := store.Pending(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	producer := &fakeProducer{}
	d := newTestDispatcher(t, store, producer)
	if _, err := d.DispatchOnce(context.Background()); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	message := producer.call(0)[0]
	if !bytes.Equal(message.Value, stored[0].Payload) {
		t.Fatal("kafka value is not the exact stored payload")
	}
	if string(message.Key) != stored[0].EventID {
		t.Fatalf("kafka key = %q, want %q", message.Key, stored[0].EventID)
	}

	var decoded events.Event
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatalf("payload is not the stored JSON: %v", err)
	}
	if decoded.EventID != event.EventID {
		t.Fatalf("payload event_id = %q, want %q", decoded.EventID, event.EventID)
	}
}

func TestBatchOrderingAndBounds(t *testing.T) {
	store := openTestStore(t, testConfig(t))
	var all []events.Event
	for i := uint64(1); i <= 5; i++ {
		sourceID := "source-a"
		if i%2 == 0 {
			sourceID = "source-b"
		}
		all = append(all, dispatcherEvent(t, store, sourceID, "gen-1", i))
	}
	appendEvents(t, store, all...)

	producer := &fakeProducer{}
	d, err := New(Config{
		Store:          store,
		Producer:       producer,
		BatchSize:      2,
		PollInterval:   time.Millisecond,
		PublishTimeout: time.Second,
		AckTimeout:     time.Second,
		RetryBase:      time.Millisecond,
		RetryMax:       5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var delivered []string
	for i := 0; i < 3; i++ {
		acked, err := d.DispatchOnce(context.Background())
		if err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
		want := 2
		if i == 2 {
			want = 1
		}
		if acked != want {
			t.Fatalf("dispatch %d acked = %d, want %d", i, acked, want)
		}
		for _, message := range producer.call(i) {
			delivered = append(delivered, string(message.Key))
		}
	}
	if producer.callCount() != 3 {
		t.Fatalf("producer calls = %d, want 3", producer.callCount())
	}
	for i, event := range all {
		if delivered[i] != event.EventID {
			t.Fatalf("delivery order %d = %q, want %q", i, delivered[i], event.EventID)
		}
	}
	if stats := d.Stats(); stats.Batches != 3 || stats.Delivered != 5 || stats.Acknowledged != 5 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestOnlyOnePublishInFlight(t *testing.T) {
	store := &fakeStore{entries: []storage.OutboxEntry{
		{EventID: "evt2_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", Payload: []byte(`{"a":1}`)},
		{EventID: "evt2_1111111111111111111111111111111111111111111111111111111111111111", Payload: []byte(`{"b":2}`)},
		{EventID: "evt2_2222222222222222222222222222222222222222222222222222222222222222", Payload: []byte(`{"c":3}`)},
	}}
	var active atomic.Int32
	var maximum atomic.Int32
	producer := &fakeProducer{fn: func(context.Context, []Message) error {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	}}
	d := newTestDispatcher(t, store, producer)
	d.batchSize = 1

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	waitFor(t, func() bool { return store.pendingCount() == 0 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}

	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent publishes = %d, want 1", maximum.Load())
	}
	if producer.callCount() != 3 {
		t.Fatalf("publish attempts = %d, want 3", producer.callCount())
	}
}

func TestBrokerUnavailableRetainsEventsAndRetries(t *testing.T) {
	store := &fakeStore{entries: []storage.OutboxEntry{
		{EventID: "evt2_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Payload: []byte(`{"one":1}`)},
	}}
	producer := &fakeProducer{fn: func(context.Context, []Message) error {
		return errors.New("connection refused")
	}}
	d := newTestDispatcher(t, store, producer)

	acked, err := d.DispatchOnce(context.Background())
	if acked != 0 || !errors.Is(err, ErrPublish) {
		t.Fatalf("dispatch = %d, %v; want 0, ErrPublish", acked, err)
	}
	if store.pendingCount() != 1 || store.ackCallCount() != 0 {
		t.Fatalf("pending = %d, acks = %d; event was not retained", store.pendingCount(), store.ackCallCount())
	}
	stats := d.Stats()
	if stats.PublishFailures != 1 || stats.Delivered != 0 || stats.Failures != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if !stats.PendingKnown || stats.Pending != 1 {
		t.Fatalf("backlog stats = %+v", stats)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	waitFor(t, func() bool { return d.Stats().Retries >= 2 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if store.pendingCount() != 1 || store.ackCallCount() != 0 {
		t.Fatalf("pending = %d, acks = %d after retries", store.pendingCount(), store.ackCallCount())
	}
	if producer.callCount() < 3 {
		t.Fatalf("publish attempts = %d, want at least 3", producer.callCount())
	}
}

func TestProducerTimeoutRetainsEvents(t *testing.T) {
	store := &fakeStore{entries: []storage.OutboxEntry{
		{EventID: "evt2_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Payload: []byte(`{"two":2}`)},
	}}
	producer := &fakeProducer{fn: func(ctx context.Context, _ []Message) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	d, err := New(Config{
		Store:          store,
		Producer:       producer,
		PublishTimeout: 50 * time.Millisecond,
		AckTimeout:     time.Second,
		RetryBase:      time.Millisecond,
		RetryMax:       time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	acked, err := d.DispatchOnce(context.Background())
	if acked != 0 || !errors.Is(err, ErrPublish) {
		t.Fatalf("dispatch = %d, %v; want 0, ErrPublish", acked, err)
	}
	if store.pendingCount() != 1 || store.ackCallCount() != 0 {
		t.Fatalf("pending = %d, acks = %d; event was not retained", store.pendingCount(), store.ackCallCount())
	}
	if stats := d.Stats(); stats.PublishFailures != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestAckFailureAfterDeliveryRepublishes(t *testing.T) {
	payload := []byte(`{"three":3}`)
	store := &fakeStore{
		entries: []storage.OutboxEntry{{EventID: "evt2_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Payload: payload}},
		ackErr:  errors.New("database is locked"),
	}
	producer := &fakeProducer{}
	d := newTestDispatcher(t, store, producer)

	acked, err := d.DispatchOnce(context.Background())
	if acked != 0 || !errors.Is(err, ErrAck) {
		t.Fatalf("dispatch = %d, %v; want 0, ErrAck", acked, err)
	}
	if producer.callCount() != 1 {
		t.Fatalf("publish attempts = %d, want 1", producer.callCount())
	}
	if store.pendingCount() != 1 {
		t.Fatal("event was removed despite acknowledgement failure")
	}
	stats := d.Stats()
	if stats.Delivered != 1 || stats.AckFailures != 1 || stats.Acknowledged != 0 {
		t.Fatalf("stats = %+v", stats)
	}

	// The retry republishes the same bytes; the consumer will deduplicate on
	// event_id. This is the documented at-least-once window.
	store.mu.Lock()
	store.ackErr = nil
	store.mu.Unlock()
	acked, err = d.DispatchOnce(context.Background())
	if err != nil || acked != 1 {
		t.Fatalf("retry dispatch = %d, %v; want 1, nil", acked, err)
	}
	if producer.callCount() != 2 {
		t.Fatalf("publish attempts = %d, want 2", producer.callCount())
	}
	if !bytes.Equal(producer.call(0)[0].Value, producer.call(1)[0].Value) {
		t.Fatal("retry changed the payload bytes")
	}
	if string(producer.call(0)[0].Key) != string(producer.call(1)[0].Key) {
		t.Fatal("retry changed the event_id key")
	}
	if stats := d.Stats(); stats.Delivered != 2 || stats.Acknowledged != 1 {
		t.Fatalf("stats after retry = %+v", stats)
	}
}

func TestGracefulShutdownCompletesInFlightBatch(t *testing.T) {
	store := &fakeStore{entries: []storage.OutboxEntry{
		{EventID: "evt2_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Payload: []byte(`{"four":4}`)},
		{EventID: "evt2_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Payload: []byte(`{"five":5}`)},
	}}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	producer := &fakeProducer{fn: func(ctx context.Context, _ []Message) error {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	d := newTestDispatcher(t, store, producer)
	// One row per batch makes it observable that shutdown does not read again.
	d.batchSize = 1

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("publish did not start")
	}
	cancel()
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish in-flight delivery")
	}

	if store.ackCallCount() != 1 || store.pendingCount() != 1 {
		t.Fatalf("acks = %d, pending = %d; want one in-flight batch acknowledged",
			store.ackCallCount(), store.pendingCount())
	}
	if store.pendingCallCount() != 1 {
		t.Fatalf("Pending calls = %d, want 1; shutdown accepted new work",
			store.pendingCallCount())
	}
}

func TestCancelledIdleRunStopsWithoutPublishing(t *testing.T) {
	store := &fakeStore{}
	producer := &fakeProducer{}
	d := newTestDispatcher(t, store, producer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
	if producer.callCount() != 0 || store.pendingCallCount() != 0 {
		t.Fatalf("cancelled Run published %d batches and read %d times",
			producer.callCount(), store.pendingCallCount())
	}
}

func TestRestartWithPendingEvents(t *testing.T) {
	cfg := testConfig(t)
	store := openTestStore(t, cfg)
	first := dispatcherEvent(t, store, "auth-log", "gen-1", 1)
	second := dispatcherEvent(t, store, "auth-log", "gen-1", 2)
	appendEvents(t, store, first, second)

	failing := &fakeProducer{fn: func(context.Context, []Message) error {
		return errors.New("broker down")
	}}
	d := newTestDispatcher(t, store, failing)
	if _, err := d.DispatchOnce(context.Background()); !errors.Is(err, ErrPublish) {
		t.Fatalf("dispatch = %v, want ErrPublish", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := store.Pending(context.Background(), storage.MaxPendingLimit)
	if err != nil {
		t.Fatal(err)
	}
	store = reopenStore(t, store, cfg)

	producer := &fakeProducer{}
	restarted := newTestDispatcher(t, store, producer)
	acked, err := restarted.DispatchOnce(context.Background())
	if err != nil || acked != 2 {
		t.Fatalf("restart dispatch = %d, %v; want 2, nil", acked, err)
	}
	if producer.callCount() != 1 || len(producer.call(0)) != 2 {
		t.Fatalf("producer calls = %d, batch = %d", producer.callCount(), len(producer.call(0)))
	}
	for i, message := range producer.call(0) {
		if !bytes.Equal(message.Value, before[i].Payload) {
			t.Fatalf("restart changed payload %d", i)
		}
		if string(message.Key) != before[i].EventID {
			t.Fatalf("restart changed event_id key %d", i)
		}
	}
	if entries, err := store.Pending(context.Background(), storage.MaxPendingLimit); err != nil || len(entries) != 0 {
		t.Fatalf("pending after restart dispatch = %d, err %v", len(entries), err)
	}
}

func TestCloseClosesProducer(t *testing.T) {
	producer := &fakeProducer{}
	d := newTestDispatcher(t, &fakeStore{}, producer)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	producer.mu.Lock()
	closed := producer.closed
	producer.mu.Unlock()
	if !closed {
		t.Fatal("Close did not close the producer")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}
