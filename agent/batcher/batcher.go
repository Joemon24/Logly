package batcher

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Config controls hybrid batching behavior
type Config struct {
	MaxEvents   int
	MaxInterval time.Duration
}

// Batcher buffers items and flushes on count OR time
type Batcher struct {
	cfg Config
}

// New creates a new Batcher
func New(cfg Config) *Batcher {
	return &Batcher{cfg: cfg}
}

// Run consumes items from in and emits batches to out
func (b *Batcher) Run(ctx context.Context, in <-chan any, out chan<- Batch) {
	go func() {
		var (
			buf       []any
			startTime time.Time
			timer     = time.NewTimer(b.cfg.MaxInterval)
		)
		defer timer.Stop()

		flush := func(now time.Time) {
			if len(buf) == 0 {
				return
			}
			out <- Batch{
				ID:        uuid.New().String(),
				StartTime: startTime,
				EndTime:   now,
				Count:     len(buf),
				Events:    buf,
			}
			buf = nil
			startTime = time.Time{}
			timer.Reset(b.cfg.MaxInterval)
		}

		for {
			select {
			case <-ctx.Done():
				flush(time.Now().UTC())
				return

			case <-timer.C:
				flush(time.Now().UTC())

			case item := <-in:
				if len(buf) == 0 {
					startTime = time.Now().UTC()
					// reset timer at first item
					if !timer.Stop() {
						select { case <-timer.C: default: }
					}
					timer.Reset(b.cfg.MaxInterval)
				}

				buf = append(buf, item)
				if len(buf) >= b.cfg.MaxEvents {
					flush(time.Now().UTC())
				}
			}
		}
	}()
}
