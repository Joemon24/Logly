// Package dispatcher delivers schema-v2 events from the durable SQLite outbox
// to Kafka.
//
// Delivery is at-least-once. An event is removed from the outbox only after the
// producer reports a broker acknowledgement. If the process crashes, or a
// publish succeeds but the acknowledgement write fails, the event is replayed
// and may appear in Kafka more than once. Consumers must deduplicate on
// event_id.
//
// The dispatcher reads one bounded batch at a time and never has more than one
// publish in flight. That keeps per-source ordering within the process simple:
// batches are read in outbox sequence order and published in that order.
package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"agent/storage"
)

const (
	// DefaultBatchSize is the maximum number of events read per Pending call.
	DefaultBatchSize = 100
	// DefaultPollInterval is how long Run waits when the outbox is empty.
	DefaultPollInterval = time.Second
	// DefaultPublishTimeout bounds one producer call, including broker acks.
	DefaultPublishTimeout = 10 * time.Second
	// DefaultAckTimeout bounds one SQLite acknowledgement write.
	DefaultAckTimeout = 5 * time.Second
	// DefaultRetryBase is the first backoff delay after a failed batch.
	DefaultRetryBase = 250 * time.Millisecond
	// DefaultRetryMax caps the exponential backoff delay.
	DefaultRetryMax = 30 * time.Second
)

// Errors returned by this package. Use errors.Is to test them.
var (
	ErrInvalidConfig = errors.New("dispatcher: invalid config")
	ErrStoreRead     = errors.New("dispatcher: outbox read failed")
	ErrPublish       = errors.New("dispatcher: kafka publish failed")
	ErrAck           = errors.New("dispatcher: outbox acknowledgement failed")
)

// Store is the read/ack surface the dispatcher needs from agent/storage.
type Store interface {
	Pending(ctx context.Context, limit int) ([]storage.OutboxEntry, error)
	Ack(ctx context.Context, eventIDs []string) (int, error)
	OutboxStats(ctx context.Context) (storage.OutboxStats, error)
}

// Message is one Kafka record. Value is the stored outbox payload unchanged.
type Message struct {
	Key   []byte
	Value []byte
}

// DeliveryKey is the Kafka message key for an outbox entry: the stable
// agent/source identity, so all events from one source share a partition and
// keep their relative order. event_id stays in the payload for deduplication.
func DeliveryKey(entry storage.OutboxEntry) []byte {
	return []byte(entry.AgentID + "/" + entry.SourceID)
}

// Producer publishes a batch and must not report success before the broker has
// acknowledged every message it accepted. A non-nil error means delivery is
// unknown and the caller will retain the events for retry.
type Producer interface {
	Publish(ctx context.Context, messages []Message) error
	Close() error
}

// Config controls a Dispatcher. Store and Producer are required; everything
// else has a documented default.
type Config struct {
	Store    Store
	Producer Producer

	BatchSize      int
	PollInterval   time.Duration
	PublishTimeout time.Duration
	AckTimeout     time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
}

// Stats is an operational snapshot. Delivered counts broker-acknowledged
// publishes, not unique events: an acknowledgement failure can cause the same
// event to be delivered again.
type Stats struct {
	Batches         uint64
	Delivered       uint64
	Acknowledged    uint64
	PublishFailures uint64
	AckFailures     uint64
	Retries         uint64
	Failures        uint64

	Pending          int
	PendingKnown     bool
	OldestPendingAge time.Duration
	LastError        string
}

// Dispatcher owns one sequential delivery loop. It does not start background
// goroutines; callers drive it with Run or DispatchOnce.
type Dispatcher struct {
	store    Store
	producer Producer

	batchSize      int
	pollInterval   time.Duration
	publishTimeout time.Duration
	ackTimeout     time.Duration
	retryBase      time.Duration
	retryMax       time.Duration

	mu      sync.Mutex
	stats   Stats
	backoff time.Duration

	closeOnce sync.Once
	closeErr  error
}

// New validates cfg and returns a dispatcher. Invalid or missing Kafka
// configuration belongs to the producer constructor, not here.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidConfig)
	}
	if cfg.Producer == nil {
		return nil, fmt.Errorf("%w: producer is required", ErrInvalidConfig)
	}

	d := &Dispatcher{
		store:          cfg.Store,
		producer:       cfg.Producer,
		batchSize:      cfg.BatchSize,
		pollInterval:   cfg.PollInterval,
		publishTimeout: cfg.PublishTimeout,
		ackTimeout:     cfg.AckTimeout,
		retryBase:      cfg.RetryBase,
		retryMax:       cfg.RetryMax,
	}
	if d.batchSize == 0 {
		d.batchSize = DefaultBatchSize
	}
	if d.batchSize < 1 || d.batchSize > storage.MaxPendingLimit {
		return nil, fmt.Errorf("%w: batch size %d is outside 1..%d",
			ErrInvalidConfig, d.batchSize, storage.MaxPendingLimit)
	}
	if d.pollInterval == 0 {
		d.pollInterval = DefaultPollInterval
	}
	if d.publishTimeout == 0 {
		d.publishTimeout = DefaultPublishTimeout
	}
	if d.ackTimeout == 0 {
		d.ackTimeout = DefaultAckTimeout
	}
	if d.retryBase == 0 {
		d.retryBase = DefaultRetryBase
	}
	if d.retryMax == 0 {
		d.retryMax = DefaultRetryMax
	}
	switch {
	case d.pollInterval < 0:
		return nil, fmt.Errorf("%w: poll interval must be positive", ErrInvalidConfig)
	case d.publishTimeout < 0:
		return nil, fmt.Errorf("%w: publish timeout must be positive", ErrInvalidConfig)
	case d.ackTimeout < 0:
		return nil, fmt.Errorf("%w: ack timeout must be positive", ErrInvalidConfig)
	case d.retryBase < 0:
		return nil, fmt.Errorf("%w: retry base must be positive", ErrInvalidConfig)
	case d.retryMax < 0:
		return nil, fmt.Errorf("%w: retry max must be positive", ErrInvalidConfig)
	case d.retryMax < d.retryBase:
		return nil, fmt.Errorf("%w: retry max is smaller than retry base", ErrInvalidConfig)
	}
	return d, nil
}

// Run delivers batches until ctx is cancelled. When the context is cancelled it
// stops reading new batches, lets an already-started publish finish within
// PublishTimeout, then acknowledges it within AckTimeout. Unacknowledged events
// remain in SQLite for the next process.
//
// Transient publish, acknowledgement, and storage errors are retried with
// bounded exponential backoff and jitter. Corrupt or unsupported data is
// returned so it stays visible instead of being retried forever.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		acked, err := d.DispatchOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if fatal(err) {
				return err
			}
			d.countRetry()
			if !sleep(ctx, d.nextBackoff()) {
				return ctx.Err()
			}
			continue
		}

		if acked > 0 {
			d.resetBackoff()
			continue
		}
		if !sleep(ctx, d.pollInterval) {
			return ctx.Err()
		}
	}
}

// DispatchOnce reads and delivers at most one batch. It does not retry or
// sleep; Run owns that policy. The return value is the number of outbox rows
// acknowledged.
//
// Once a batch is read, publishing and acknowledgement use a detached context
// with their own timeouts. A shutdown signal therefore stops the next batch but
// lets the current one finish or time out; unacknowledged events stay durable.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	entries, err := d.store.Pending(ctx, d.batchSize)
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrStoreRead, err)
		d.recordStoreFailure(err)
		return 0, err
	}
	if len(entries) == 0 {
		d.observe(ctx)
		return 0, nil
	}

	messages := make([]Message, len(entries))
	ids := make([]string, len(entries))
	for i, entry := range entries {
		// Pass the stored bytes through unchanged: no unmarshal, remarshal, or
		// event reconstruction on the delivery path.
		messages[i] = Message{Key: DeliveryKey(entry), Value: entry.Payload}
		ids[i] = entry.EventID
	}

	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.publishTimeout)
	err = d.producer.Publish(publishCtx, messages)
	cancel()
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrPublish, err)
		d.recordPublishFailure(err)
		d.observe(ctx)
		return 0, err
	}
	d.recordPublished(len(entries))

	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.ackTimeout)
	acked, err := d.store.Ack(ackCtx, ids)
	cancel()
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrAck, err)
		d.recordAckFailure(err)
		d.observe(ctx)
		return 0, err
	}
	d.recordAcknowledged(acked)
	d.observe(ctx)
	return acked, nil
}

// Stats returns a consistent snapshot of counters and the last observed
// backlog. Pending and OldestPendingAge are updated after each batch and when
// the outbox is found empty.
func (d *Dispatcher) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// Close closes the producer. Unacknowledged events are already durable in
// SQLite. It is safe to call more than once.
func (d *Dispatcher) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.producer.Close()
	})
	return d.closeErr
}

func (d *Dispatcher) recordStoreFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Failures++
	d.stats.LastError = err.Error()
}

func (d *Dispatcher) recordPublishFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Batches++
	d.stats.PublishFailures++
	d.stats.Failures++
	d.stats.LastError = err.Error()
}

func (d *Dispatcher) recordPublished(count int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Batches++
	d.stats.Delivered += uint64(count)
	d.stats.LastError = ""
}

func (d *Dispatcher) recordAckFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.AckFailures++
	d.stats.Failures++
	d.stats.LastError = err.Error()
}

func (d *Dispatcher) recordAcknowledged(count int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Acknowledged += uint64(count)
	d.stats.LastError = ""
}

func (d *Dispatcher) observe(ctx context.Context) {
	statsCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.ackTimeout)
	defer cancel()
	snapshot, err := d.store.OutboxStats(statsCtx)

	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		// Backlog metrics are auxiliary; do not overwrite a delivery error.
		d.stats.PendingKnown = false
		return
	}
	d.stats.Pending = snapshot.Pending
	d.stats.PendingKnown = true
	if snapshot.OldestStoredAt.IsZero() {
		d.stats.OldestPendingAge = 0
		return
	}
	d.stats.OldestPendingAge = time.Since(snapshot.OldestStoredAt)
	if d.stats.OldestPendingAge < 0 {
		d.stats.OldestPendingAge = 0
	}
}

func (d *Dispatcher) countRetry() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.Retries++
}

func (d *Dispatcher) nextBackoff() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.backoff == 0 {
		d.backoff = d.retryBase
	} else {
		d.backoff *= 2
		if d.backoff > d.retryMax {
			d.backoff = d.retryMax
		}
	}
	delay := d.backoff
	half := delay / 2
	if half > 0 {
		delay = half + time.Duration(rand.Int64N(int64(half)+1))
	}
	return delay
}

func (d *Dispatcher) resetBackoff() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.backoff = 0
}

func sleep(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// fatal reports errors that will not heal by retrying. They surface the damage
// to the operator instead of spinning with backoff.
func fatal(err error) bool {
	return errors.Is(err, storage.ErrCorruptPayload) ||
		errors.Is(err, storage.ErrCorrupt) ||
		errors.Is(err, storage.ErrUnsupportedSchema) ||
		errors.Is(err, storage.ErrInvalidLimit)
}
