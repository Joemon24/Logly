package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"agent/events"
)

// fileIdentity is the stable identity of one file incarnation. Device plus
// inode survives renames and distinguishes a replacement file.
type fileIdentity struct {
	dev uint64
	ino uint64
}

func identify(info os.FileInfo) (fileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("%w: %T", ErrUnsupportedPlatform, info.Sys())
	}
	return fileIdentity{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, nil
}

func (id fileIdentity) generationBase() string {
	return fmt.Sprintf("file-%x-%x", id.dev, id.ino)
}

// generationMatchesIdentity recognizes both the original generation for a file
// identity and its documented truncation successors.
func generationMatchesIdentity(generation, base string) bool {
	return generation == base || strings.HasPrefix(generation, base+"-t")
}

// readFrom consumes complete records from the current file up to size. When
// final is true, a trailing line without a newline is treated as complete;
// this is used only while draining a rotated file that will not grow again.
//
// Every complete line consumes the next physical record number. Ignored lines
// durably advance the checkpoint without inserting an outbox event; relevant
// lines insert the event and advance the checkpoint in the same transaction.
func (c *Collector) readFrom(ctx context.Context, size int64, final bool) error {
	if size <= c.scanOffset {
		return nil
	}

	reader := bufio.NewReaderSize(
		io.NewSectionReader(c.file, c.scanOffset, size-c.scanOffset),
		events.MaxRawEvidenceBytes+1)

	count := 0
	for count < c.maxRecords {
		if err := ctx.Err(); err != nil {
			return err
		}

		lineStart := c.scanOffset
		data, err := reader.ReadSlice('\n')
		complete := false
		switch {
		case err == nil:
			complete = true
		case errors.Is(err, bufio.ErrBufferFull):
			return c.reject(lineStart, ErrOversizedRecord,
				fmt.Sprintf("record exceeds %d bytes", events.MaxRawEvidenceBytes))
		case errors.Is(err, io.EOF):
			if !final || len(data) == 0 {
				return nil
			}
			complete = true
		default:
			return fmt.Errorf("%w: read %s at offset %d: %v",
				ErrSourceIO, c.path, lineStart, err)
		}

		if !complete {
			return nil
		}
		if len(data) > events.MaxRawEvidenceBytes {
			return c.reject(lineStart, ErrOversizedRecord,
				fmt.Sprintf("record exceeds %d bytes", events.MaxRawEvidenceBytes))
		}

		end := lineStart + int64(len(data))
		if c.skipLine {
			c.scanOffset = end
			c.skipLine = false
			continue
		}

		recordNumber := c.record + 1
		text := string(data)
		parse := parseSecurityLine
		if c.sourceType == events.SourceTypeAudit {
			parse = parseAuditLine
		}
		record, relevant, parseErr := parse(text)
		if !relevant {
			// Recognized as unrelated: advance the durable checkpoint so a
			// restart does not rescan it, but do not insert an outbox event.
			position := events.Position{
				ByteOffset:   &lineStart,
				RecordNumber: recordNumber,
			}
			if err := c.store.AdvanceCheckpoint(
				ctx, c.sourceID, c.sourceType, c.generation, position); err != nil {
				return persistError(err)
			}
			c.record = recordNumber
			c.scanOffset = end
			count++
			continue
		}
		if parseErr != nil {
			return c.reject(lineStart, ErrRejectedRecord, parseErr.Error())
		}

		event, err := c.buildEvent(record, text, lineStart, recordNumber)
		if err != nil {
			return c.reject(lineStart, ErrRejectedRecord, err.Error())
		}
		if err := c.store.Append(ctx, []events.Event{event}); err != nil {
			c.pending = &event
			c.pendingEnd = end
			return persistError(err)
		}

		c.persisted++
		c.record = recordNumber
		c.scanOffset = end
		count++
	}
	return nil
}

func (c *Collector) reject(offset int64, kind error, reason string) error {
	return &RejectedRecordError{
		SourceID:     c.sourceID,
		Path:         c.path,
		Generation:   c.generation,
		ByteOffset:   offset,
		RecordNumber: c.record + 1,
		Reason:       reason,
		Err:          kind,
	}
}

// RejectedRecordError identifies a record the collector refused to persist.
// The checkpoint stays at the previous persisted record until an operator
// resolves it.
type RejectedRecordError struct {
	SourceID     string
	Path         string
	Generation   string
	ByteOffset   int64
	RecordNumber uint64
	Reason       string
	Err          error
}

func (e *RejectedRecordError) Error() string {
	return fmt.Sprintf("collector: rejected record in %s generation %s at byte %d: %s",
		e.Path, e.Generation, e.ByteOffset, e.Reason)
}

func (e *RejectedRecordError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrRejectedRecord}
	}
	return []error{ErrRejectedRecord, e.Err}
}
