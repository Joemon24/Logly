// Package collector persists Linux security records and file checkpoints
// atomically in a durable outbox. Physical record numbers include ignored
// lines. Each collector instance follows one configured source.
package collector

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agent/events"
	"agent/storage"
)

const (
	// DefaultPath is the initial authentication log collected in Phase 3.
	DefaultPath = "/var/log/auth.log"
	// DefaultSourceID is the logical source ID for DefaultPath.
	DefaultSourceID = "auth-log"
	// DefaultPollInterval is how long Run waits between Poll calls.
	DefaultPollInterval = time.Second
	// DefaultMaxRecordsPerPoll bounds work per Poll call.
	DefaultMaxRecordsPerPoll = 256
)

// Errors returned by this package. Use errors.Is to test them.
var (
	ErrInvalidConfig       = errors.New("collector: invalid config")
	ErrSourceIO            = errors.New("collector: source read failed")
	ErrRejectedRecord      = errors.New("collector: record rejected")
	ErrOversizedRecord     = errors.New("collector: record exceeds the raw evidence size limit")
	ErrBackpressure        = errors.New("collector: outbox backpressure")
	ErrStorage             = errors.New("collector: storage operation failed")
	ErrCollectorState      = errors.New("collector: inconsistent collector state")
	ErrCoverageGap         = errors.New("collector: collection coverage gap")
	ErrUnsupportedPlatform = errors.New("collector: file identity is not supported on this platform")
)

var errRotatedTruncated = errors.New("collector: rotated file is shorter than its checkpoint")

// CoverageGapError reports that unread input from a previous source generation
// could not be recovered after an offline rotation. The collector has already
// activated the replacement generation; the error is returned once so the lost
// coverage is observable instead of silently resuming.
type CoverageGapError struct {
	SourceID     string
	Path         string
	Generation   string
	ByteOffset   *int64
	RecordNumber uint64
	Searched     []string
}

func (e *CoverageGapError) Error() string {
	return fmt.Sprintf(
		"collector: coverage gap for %s generation %s after offline rotation from %s; searched %s",
		e.Path, e.Generation, positionText(e.ByteOffset, e.RecordNumber),
		strings.Join(e.Searched, ", "))
}

func (e *CoverageGapError) Unwrap() error { return ErrCoverageGap }

func positionText(offset *int64, record uint64) string {
	if offset == nil {
		return fmt.Sprintf("record %d (no checkpoint)", record)
	}
	return fmt.Sprintf("byte %d record %d", *offset, record)
}

// Store is the subset of storage.Store the collector needs. Keeping it narrow
// makes the collector testable and leaves the future dispatcher independent.
type Store interface {
	Identity() storage.Identity
	ActiveGeneration(ctx context.Context, sourceID string) (storage.SourceGeneration, bool, error)
	Checkpoint(ctx context.Context, sourceID, generation string) (storage.Checkpoint, bool, error)
	AdvanceCheckpoint(
		ctx context.Context,
		sourceID string,
		sourceType events.SourceType,
		generation string,
		position events.Position,
	) error
	Append(ctx context.Context, batch []events.Event) error
	TransitionSourceGeneration(
		ctx context.Context,
		sourceID string,
		sourceType events.SourceType,
		expectedGeneration, nextGeneration string,
	) (storage.SourceGeneration, error)
}

// Config controls a Collector.
type Config struct {
	// Store is the durable outbox and checkpoint store. Required.
	Store Store

	// Path is the log file to tail. Empty selects DefaultPath.
	Path string

	// Audit selects keyed Linux audit records instead of auth log messages.
	Audit bool

	// SourceID is the stable logical source identity. Defaults to "auth-log".
	SourceID string

	// PollInterval is used by Run. Defaults to one second.
	PollInterval time.Duration

	// MaxRecordsPerPoll bounds how many complete records one Poll consumes.
	// Defaults to 256.
	MaxRecordsPerPoll int

	// Host is attached to every event. Hostname defaults to os.Hostname().
	Host events.Host

	// ReportSource reports state transitions for a missing/unreadable source.
	ReportSource func(path string, problem error)

	// Now supplies ingest timestamps and syslog year inference. Defaults to
	// time.Now. It exists for deterministic tests.
	Now func() time.Time
}

// Collector tails one authentication log file. It is not safe for concurrent
// use; drive it from a single goroutine with Poll or Run.
type Collector struct {
	store        Store
	path         string
	sourceID     string
	sourceType   events.SourceType
	pollPeriod   time.Duration
	maxRecords   int
	host         events.Host
	now          func() time.Time
	reportSource func(string, error)

	stateLoaded bool
	active      storage.SourceGeneration
	hasActive   bool

	file       *os.File
	identity   fileIdentity
	base       string
	generation string
	scanOffset int64
	record     uint64
	lastSize   int64
	skipLine   bool
	draining   bool

	// persisted counts events committed by this process; Poll derives its
	// return value from the difference so recovery drains are included.
	persisted int

	pending    *events.Event
	pendingEnd int64
}

// New validates the config and creates a collector. It does not touch the
// filesystem or the store; the first Poll opens the source and resumes the
// durable checkpoint.
func New(cfg Config) (*Collector, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidConfig)
	}
	pathConfig := cfg.Path
	if strings.TrimSpace(pathConfig) == "" {
		pathConfig = DefaultPath
	}
	path, err := filepath.Abs(pathConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve path: %v", ErrInvalidConfig, err)
	}

	sourceID := cfg.SourceID
	if sourceID == "" {
		sourceID = DefaultSourceID
	}
	sourceType := events.SourceTypeFile
	if cfg.Audit {
		sourceType = events.SourceTypeAudit
	}
	pollPeriod := cfg.PollInterval
	if pollPeriod <= 0 {
		pollPeriod = DefaultPollInterval
	}
	maxRecords := cfg.MaxRecordsPerPoll
	if maxRecords <= 0 {
		maxRecords = DefaultMaxRecordsPerPoll
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	host := cfg.Host
	if host.Hostname == "" {
		if hostname, err := os.Hostname(); err == nil {
			host.Hostname = hostname
		}
	}

	return &Collector{
		store:        cfg.Store,
		path:         path,
		sourceID:     sourceID,
		sourceType:   sourceType,
		pollPeriod:   pollPeriod,
		maxRecords:   maxRecords,
		host:         host,
		now:          now,
		reportSource: cfg.ReportSource,
	}, nil
}

// Poll reads and persists currently available complete records. It returns the
// number of events persisted by this call.
//
// A capacity failure returns ErrBackpressure (wrapping
// storage.ErrCapacityReached) and keeps the unpersisted event for the next
// call. A read failure returns ErrSourceIO and leaves the position unchanged.
// A malformed SSH record returns *RejectedRecordError and leaves the position
// unchanged. Other storage failures return ErrStorage and also leave the
// position unchanged.
func (c *Collector) Poll(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	start := c.persisted

	if c.pending != nil {
		if err := c.flushPending(ctx); err != nil {
			return c.persisted - start, err
		}
	}

	if err := c.sync(ctx); err != nil {
		return c.persisted - start, err
	}
	if c.file == nil {
		return c.persisted - start, nil
	}
	for {
		if c.draining {
			info, err := c.file.Stat()
			if err != nil {
				return c.persisted - start, fmt.Errorf("%w: stat %s: %v", ErrSourceIO, c.path, err)
			}
			if err := c.readFrom(ctx, info.Size(), true); err != nil {
				return c.persisted - start, err
			}
			if c.scanOffset < info.Size() {
				// maxRecords reached; continue draining next poll.
				return c.persisted - start, nil
			}
			if err := c.finishRotation(ctx); err != nil {
				return c.persisted - start, err
			}
			if c.file == nil {
				return c.persisted - start, nil
			}
			continue
		}

		err := c.readFrom(ctx, c.lastSize, false)
		return c.persisted - start, err
	}
}

// Run polls until ctx is cancelled. Source read failures and outbox
// backpressure are retried; rejected records and storage errors stop the loop
// so they stay observable. A missing file is treated as no work and retried
// until it appears.
func (c *Collector) Run(ctx context.Context) error {
	var lastIssue string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := c.Poll(ctx)
		if err != nil && !retryable(err) {
			return err
		}
		if c.reportSource != nil {
			var sourceIssue error
			if errors.Is(err, ErrSourceIO) {
				sourceIssue = err
			} else if c.file == nil {
				sourceIssue = fs.ErrNotExist
			}
			issue := ""
			if sourceIssue != nil {
				issue = sourceIssue.Error()
			}
			if issue != lastIssue {
				c.reportSource(c.path, sourceIssue)
				lastIssue = issue
			}
		}

		timer := time.NewTimer(c.pollPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryable(err error) bool {
	return errors.Is(err, ErrBackpressure) || errors.Is(err, ErrSourceIO)
}

// Close releases the open log descriptor. An unpersisted pending event is left
// for the next process to re-read from the durable checkpoint.
func (c *Collector) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

// sync opens the source or reacts to rotation/truncation. It never moves a
// position on its own except when starting a brand-new generation at zero.
func (c *Collector) sync(ctx context.Context) error {
	info, err := c.statPath()
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	identity, err := identify(info)
	if err != nil {
		return err
	}

	if c.file == nil {
		file, err := os.Open(c.path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("%w: open %s: %v", ErrSourceIO, c.path, err)
		}
		opened, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("%w: stat %s: %v", ErrSourceIO, c.path, err)
		}
		if !opened.Mode().IsRegular() {
			_ = file.Close()
			return fmt.Errorf("%w: %s is not a regular file", ErrSourceIO, c.path)
		}
		identity, err = identify(opened)
		if err != nil {
			_ = file.Close()
			return err
		}
		if err := c.bindFile(ctx, file, opened, identity, true); err != nil {
			if c.file == nil {
				_ = file.Close()
			}
			return err
		}
		return nil
	}

	if identity != c.identity {
		// Keep the open descriptor; reads still reach unread records in the
		// rotated file. Drain it before switching.
		c.draining = true
		return nil
	}
	if info.Size() < c.lastSize || info.Size() < c.scanOffset {
		if err := c.truncate(ctx); err != nil {
			// Leave lastSize unchanged so a retry still sees the shrink.
			return err
		}
		c.lastSize = info.Size()
		return nil
	}
	c.lastSize = info.Size()
	return nil
}

// bindFile resumes the active generation for this file identity, or performs
// an explicit rotation transition when the identity is new. recovery enables
// searching the plain rotated files for the previous inode after an offline
// rotation; it is enabled only when first opening the path.
func (c *Collector) bindFile(
	ctx context.Context,
	file *os.File,
	info os.FileInfo,
	identity fileIdentity,
	recovery bool,
) error {
	if !c.stateLoaded {
		active, found, err := c.store.ActiveGeneration(ctx, c.sourceID)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrStorage, err)
		}
		c.active = active
		c.hasActive = found
		c.stateLoaded = true
	}

	base := identity.generationBase()
	if c.hasActive && generationMatchesIdentity(c.active.Generation, base) {
		return c.resumeGeneration(ctx, file, info, identity, c.active.Generation, true)
	}

	old := c.active
	hadActive := c.hasActive
	if hadActive && recovery {
		rotated, rotatedInfo, rotatedIdentity, err := c.findRotated(ctx)
		if err != nil {
			return err
		}
		if rotated != nil {
			err := c.resumeGeneration(ctx, rotated, rotatedInfo, rotatedIdentity,
				old.Generation, false)
			if err == nil {
				_ = file.Close()
				c.draining = true
				return nil
			}
			_ = rotated.Close()
			c.file = nil
			if !errors.Is(err, errRotatedTruncated) {
				_ = file.Close()
				return err
			}
			// The old file shrank below its checkpoint. Re-reading it from
			// zero could duplicate already emitted records, so treat it as an
			// unrecoverable gap.
		}
	}

	if _, err := c.transition(ctx, base); err != nil {
		return err
	}
	if err := c.resumeGeneration(ctx, file, info, identity, base, true); err != nil {
		return err
	}
	if hadActive && recovery {
		return c.coverageGap(ctx, old.Generation)
	}
	return nil
}

// resumeGeneration binds a file to an already selected generation and restores
// its checkpoint. allowTruncate distinguishes the current path (where a shrink
// is a fresh truncation generation) from a recovered rotated file (where a
// shrink means unread data is gone).
func (c *Collector) resumeGeneration(
	ctx context.Context,
	file *os.File,
	info os.FileInfo,
	identity fileIdentity,
	generation string,
	allowTruncate bool,
) error {
	checkpoint, hasCheckpoint, err := c.store.Checkpoint(ctx, c.sourceID, generation)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStorage, err)
	}
	if hasCheckpoint && checkpoint.ByteOffset == nil {
		return fmt.Errorf("%w: checkpoint %s/%s has no byte offset",
			ErrCollectorState, c.sourceID, generation)
	}

	c.file = file
	c.identity = identity
	c.base = identity.generationBase()
	c.generation = generation
	c.lastSize = info.Size()
	c.skipLine = false
	c.draining = false

	if !hasCheckpoint {
		c.scanOffset = 0
		c.record = 0
		return nil
	}

	c.record = checkpoint.RecordNumber
	c.scanOffset = *checkpoint.ByteOffset
	if info.Size() <= c.scanOffset {
		if !allowTruncate {
			return errRotatedTruncated
		}
		return c.truncate(ctx)
	}
	// The checkpoint is the start of the last consumed record. Skip that
	// record once so a restart never re-processes it.
	c.skipLine = true
	return nil
}

// transition explicitly rotates to next and updates the cached active state.
func (c *Collector) transition(ctx context.Context, next string) (storage.SourceGeneration, error) {
	expected := ""
	if c.hasActive {
		expected = c.active.Generation
	}
	generation, err := c.store.TransitionSourceGeneration(
		ctx, c.sourceID, c.sourceType, expected, next)
	if err != nil {
		return storage.SourceGeneration{}, fmt.Errorf("%w: %w", ErrStorage, err)
	}
	c.active = generation
	c.hasActive = true
	c.stateLoaded = true
	return generation, nil
}

// truncate starts a new generation for the same file identity. Offsets are
// never reset within a generation, and retired generations are never reused.
func (c *Collector) truncate(ctx context.Context) error {
	if !c.hasActive {
		return fmt.Errorf("%w: truncation without an active generation", ErrCollectorState)
	}
	next := fmt.Sprintf("%s-t%d", c.base, c.active.Sequence+1)
	generation, err := c.transition(ctx, next)
	if err != nil {
		return err
	}
	c.generation = generation.Generation
	c.scanOffset = 0
	c.record = 0
	c.skipLine = false
	return nil
}

// findRotated looks for the previously active inode among path.1 .. path.9.
// Plain rotated files are the only candidates; compressed files cannot be read
// as text and a missing candidate is reported as a coverage gap.
func (c *Collector) findRotated(ctx context.Context) (*os.File, os.FileInfo, fileIdentity, error) {
	for _, candidate := range c.rotatedCandidates() {
		if err := ctx.Err(); err != nil {
			return nil, nil, fileIdentity{}, err
		}
		info, err := os.Stat(candidate)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		identity, err := identify(info)
		if err != nil ||
			!generationMatchesIdentity(c.active.Generation, identity.generationBase()) {
			continue
		}
		file, err := os.Open(candidate)
		if err != nil {
			continue
		}
		opened, err := file.Stat()
		if err != nil {
			_ = file.Close()
			continue
		}
		identity, err = identify(opened)
		if err != nil ||
			!generationMatchesIdentity(c.active.Generation, identity.generationBase()) {
			_ = file.Close()
			continue
		}
		return file, opened, identity, nil
	}
	return nil, nil, fileIdentity{}, nil
}

func (c *Collector) rotatedCandidates() []string {
	paths := make([]string, 0, 9)
	for i := 1; i <= 9; i++ {
		paths = append(paths, fmt.Sprintf("%s.%d", c.path, i))
	}
	return paths
}

// coverageGap reports unread input from oldGeneration that could not be found.
// It is returned once per startup after the new generation has been activated.
func (c *Collector) coverageGap(ctx context.Context, oldGeneration string) error {
	gap := &CoverageGapError{
		SourceID:   c.sourceID,
		Path:       c.path,
		Generation: oldGeneration,
		Searched:   c.rotatedCandidates(),
	}
	checkpoint, found, err := c.store.Checkpoint(ctx, c.sourceID, oldGeneration)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStorage, err)
	}
	if found {
		gap.ByteOffset = checkpoint.ByteOffset
		gap.RecordNumber = checkpoint.RecordNumber
	}
	return gap
}

// finishRotation closes the drained file and binds the replacement. If the
// path points back at the drained inode, collection continues in place.
func (c *Collector) finishRotation(ctx context.Context) error {
	oldIdentity := c.identity
	if c.file != nil {
		_ = c.file.Close()
		c.file = nil
	}
	c.draining = false

	info, err := c.statPath()
	if err != nil {
		return err
	}
	if info == nil {
		return nil
	}
	file, err := os.Open(c.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("%w: open %s: %v", ErrSourceIO, c.path, err)
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("%w: stat %s: %v", ErrSourceIO, c.path, err)
	}
	if !opened.Mode().IsRegular() {
		_ = file.Close()
		return fmt.Errorf("%w: %s is not a regular file", ErrSourceIO, c.path)
	}
	identity, err := identify(opened)
	if err != nil {
		_ = file.Close()
		return err
	}
	if identity == oldIdentity {
		c.file = file
		c.identity = identity
		c.lastSize = opened.Size()
		return nil
	}
	if err := c.bindFile(ctx, file, opened, identity, false); err != nil {
		if c.file == nil {
			_ = file.Close()
		}
		return err
	}
	return nil
}

func (c *Collector) flushPending(ctx context.Context) error {
	event := *c.pending
	if err := c.store.Append(ctx, []events.Event{event}); err != nil {
		return persistError(err)
	}
	c.record = event.Collection.Position.RecordNumber
	c.scanOffset = c.pendingEnd
	c.persisted++
	c.pending = nil
	return nil
}

func (c *Collector) statPath() (os.FileInfo, error) {
	info, err := os.Stat(c.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: stat %s: %v", ErrSourceIO, c.path, err)
	}
	return info, nil
}

func persistError(err error) error {
	if errors.Is(err, storage.ErrCapacityReached) {
		return fmt.Errorf("%w: %w", ErrBackpressure, err)
	}
	return fmt.Errorf("%w: %w", ErrStorage, err)
}
