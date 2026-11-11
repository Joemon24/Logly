package runtimev2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"agent/collector"
	"agent/dispatcher"
	"agent/events"
	"agent/storage"
)

// Runtime owns the store, collector, and dispatcher for one agent process.
type Runtime struct {
	cfg        Config
	logger     *slog.Logger
	store      *storage.Store
	collector  *collector.Collector
	dispatcher *dispatcher.Dispatcher

	closeOnce sync.Once
	closeErr  error
}

// New validates cfg, opens the store, and builds the collector and dispatcher.
// producer is optional and exists for tests; when nil, a real Kafka producer is
// created from cfg.KafkaBrokers and cfg.KafkaTopic.
//
// Pending events found in the outbox are logged here and delivered after Run
// starts; opening never discards them.
func New(cfg Config, logger *slog.Logger, producer dispatcher.Producer) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	store, err := storage.Open(cfg.toStorageConfig())
	if err != nil {
		return nil, fmt.Errorf("open outbox %s: %w", cfg.DBPath, err)
	}

	hostname := cfg.Hostname
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	collectorCfg := collector.Config{
		Store:             store,
		Path:              cfg.LogPath,
		SourceID:          cfg.SourceID,
		PollInterval:      cfg.pollInterval(),
		MaxRecordsPerPoll: cfg.MaxRecordsPerPoll,
		Host:              events.Host{Hostname: hostname},
	}
	col, err := collector.New(collectorCfg)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("build collector: %w", err)
	}

	if producer == nil {
		producer, err = dispatcher.NewKafkaProducer(dispatcher.KafkaConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
		})
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("build kafka producer: %w", err)
		}
	}
	disp, err := dispatcher.New(dispatcher.Config{
		Store:          store,
		Producer:       producer,
		PollInterval:   cfg.pollInterval(),
		PublishTimeout: cfg.publishTimeout(),
		AckTimeout:     cfg.ackTimeout(),
	})
	if err != nil {
		_ = producer.Close()
		_ = store.Close()
		return nil, fmt.Errorf("build dispatcher: %w", err)
	}

	return &Runtime{
		cfg:        cfg,
		logger:     logger,
		store:      store,
		collector:  col,
		dispatcher: disp,
	}, nil
}

// Run starts one collector goroutine and one dispatcher goroutine. It returns
// when ctx is cancelled or a component fails. Cancelling ctx stops new
// collection and new dispatch work, lets the in-flight batch finish within the
// configured timeouts, and leaves unacknowledged events durable.
func (r *Runtime) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	identity := r.store.Identity()
	r.logger.Info("agent starting",
		"agent_id", identity.AgentID,
		"agent_type", identity.AgentType,
		"agent_version", identity.AgentVersion,
		"agent_generation", identity.Generation,
		"log_path", r.cfg.LogPath,
		"source_id", r.cfg.SourceID,
		"db_path", r.cfg.DBPath,
		"kafka_topic", r.cfg.KafkaTopic,
		"kafka_brokers", r.cfg.KafkaBrokers,
		"capacity", r.cfg.Capacity)

	if pending, err := r.store.OutboxStats(ctx); err != nil {
		r.logger.Warn("could not read pending backlog", "error", err)
	} else {
		r.logger.Info("outbox restored",
			"pending_events", pending.Pending,
			"oldest_pending_age", pendingAge(pending))
	}

	var wg sync.WaitGroup
	failures := make(chan error, 2)
	run := func(name string, fn func(context.Context) error) {
		defer wg.Done()
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			reason, hint := describe(err)
			r.logger.Error("component failed",
				"component", name, "reason", reason, "hint", hint, "error", err)
			failures <- fmt.Errorf("%s: %s: %w", name, reason, err)
		}
	}
	wg.Add(2)
	go run("collector", r.collector.Run)
	go run("dispatcher", r.dispatcher.Run)

	statsDone := make(chan struct{})
	go r.logStats(ctx, statsDone)

	var runErr error
	select {
	case <-ctx.Done():
		r.logger.Info("shutdown requested")
	case runErr = <-failures:
		cancel()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	timer := time.NewTimer(r.cfg.shutdownTimeout())
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
		r.logger.Error("shutdown timed out",
			"timeout", r.cfg.shutdownTimeout(),
			"hint", "unacknowledged events remain durable in the outbox and will be replayed")
		return fmt.Errorf("%w after %s", ErrShutdownTimeout, r.cfg.shutdownTimeout())
	}
	<-statsDone

	select {
	case err := <-failures:
		if runErr == nil {
			runErr = err
		}
	default:
	}
	if runErr != nil {
		return runErr
	}
	r.logger.Info("agent stopped cleanly")
	return nil
}

// Stats returns the current dispatcher counters.
func (r *Runtime) Stats() dispatcher.Stats {
	return r.dispatcher.Stats()
}

// Close releases the producer, the log descriptor, and the database in that
// order. Unacknowledged events remain in SQLite for the next process.
func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		dispErr := r.dispatcher.Close()
		colErr := r.collector.Close()
		storeErr := r.store.Close()
		r.closeErr = errors.Join(dispErr, colErr, storeErr)
	})
	return r.closeErr
}

func (r *Runtime) logStats(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.cfg.statsInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Log the final counters so a shutdown shows what was delivered
			// and what was left pending.
			r.logCounters("agent counters")
			return
		case <-ticker.C:
			r.logCounters("agent counters")
		}
	}
}

func (r *Runtime) logCounters(message string) {
	stats := r.dispatcher.Stats()
	r.logger.Info(message,
		"pending_events", stats.Pending,
		"pending_known", stats.PendingKnown,
		"oldest_pending_age", stats.OldestPendingAge,
		"batches", stats.Batches,
		"delivered", stats.Delivered,
		"acknowledged", stats.Acknowledged,
		"publish_failures", stats.PublishFailures,
		"ack_failures", stats.AckFailures,
		"retries", stats.Retries,
		"failures", stats.Failures,
		"last_error", stats.LastError)
}

func pendingAge(stats storage.OutboxStats) time.Duration {
	if stats.OldestStoredAt.IsZero() {
		return 0
	}
	age := time.Since(stats.OldestStoredAt)
	if age < 0 {
		return 0
	}
	return age
}

// describe turns a component error into a stable reason and an operator hint.
func describe(err error) (string, string) {
	switch {
	case errors.Is(err, collector.ErrOversizedRecord):
		return "rejected_record",
			"a log record exceeds the raw evidence limit; the checkpoint stopped before it so no data is skipped"
	case errors.Is(err, collector.ErrRejectedRecord):
		return "rejected_record",
			"a malformed sshd record stopped the checkpoint; fix or rotate the log, then restart"
	case errors.Is(err, collector.ErrCoverageGap):
		return "coverage_gap",
			"a previous source generation was rotated away before it was read; the gap is reported, not hidden"
	case errors.Is(err, collector.ErrUnsupportedPlatform):
		return "unsupported_platform", "file identity is unavailable on this platform"
	case errors.Is(err, collector.ErrStorage):
		return "storage_failure", "the outbox write failed; the checkpoint was not advanced"
	case errors.Is(err, dispatcher.ErrPublish):
		return "kafka_unavailable", "events stay in the outbox; check brokers, topic, and network, then restart"
	case errors.Is(err, dispatcher.ErrAck):
		return "outbox_ack_failure", "delivery succeeded but the outbox row could not be removed; duplicates are possible"
	case errors.Is(err, dispatcher.ErrStoreRead):
		return "outbox_read_failure", "the outbox could not be read; check the database file and disk"
	case errors.Is(err, storage.ErrCorruptPayload), errors.Is(err, storage.ErrCorrupt):
		return "corrupt_storage", "the database or a stored event is damaged; do not delete it, inspect before recovery"
	default:
		return "component_error", "inspect the logged error before restarting"
	}
}
