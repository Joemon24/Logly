package collector

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"agent/events"
)

// SSH record patterns cover the common OpenSSH authentication messages emitted
// by password, publickey, and keyboard-interactive methods. The syslog prefix,
// if any, is ignored: the patterns match anywhere in the line.
var (
	sshFailed = regexp.MustCompile(
		`(?i:failed) (?:password|publickey|keyboard-interactive/pam) for (?:invalid user |illegal user )?(\S+) from (\S+)(?: port \d+)?`)
	sshAccepted = regexp.MustCompile(
		`(?i:accepted) (?:password|publickey|keyboard-interactive/pam) for (\S+) from (\S+)(?: port \d+)?`)
	sshInvalidUser = regexp.MustCompile(
		`(?i:invalid) user (\S+) from (\S+)(?: port \d+)?`)
)

type authRecord struct {
	Action  string
	Outcome string
	User    string
	IP      string
}

// parseAuthLine extracts an SSH authentication record. relevant is true for
// lines that look like SSH authentication but cannot be parsed, so the caller
// can reject them instead of silently skipping them.
func parseAuthLine(line string) (authRecord, bool, error) {
	if match := sshFailed.FindStringSubmatch(line); match != nil {
		return authRecord{
			Action:  "ssh_login",
			Outcome: "failure",
			User:    match[1],
			IP:      match[2],
		}, true, nil
	}
	if match := sshAccepted.FindStringSubmatch(line); match != nil {
		return authRecord{
			Action:  "ssh_login",
			Outcome: "success",
			User:    match[1],
			IP:      match[2],
		}, true, nil
	}
	if match := sshInvalidUser.FindStringSubmatch(line); match != nil {
		return authRecord{
			Action:  "ssh_invalid_user",
			Outcome: "failure",
			User:    match[1],
			IP:      match[2],
		}, true, nil
	}
	if looksLikeSSHAuth(line) {
		return authRecord{}, true, fmt.Errorf("unrecognized sshd authentication record")
	}
	return authRecord{}, false, nil
}

func looksLikeSSHAuth(line string) bool {
	lower := strings.ToLower(line)
	if !strings.Contains(lower, "sshd") {
		return false
	}
	for _, marker := range []string{
		"failed password",
		"failed publickey",
		"failed keyboard-interactive",
		"accepted password",
		"accepted publickey",
		"accepted keyboard-interactive",
		"invalid user",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// parseAuthTimestamp understands RFC3339 prefixes and the classic syslog
// "Jan _2 15:04:05" prefix. A missing or unusable prefix falls back to ingest
// time. Syslog timestamps have no year, so the current year is inferred; a
// timestamp more than a day in the future is treated as last year.
func parseAuthTimestamp(line string, now time.Time) (time.Time, bool) {
	if fields := strings.Fields(line); len(fields) > 0 {
		if parsed, err := time.Parse(time.RFC3339Nano, fields[0]); err == nil {
			return parsed.UTC(), true
		}
	}
	if len(line) >= 15 {
		if parsed, err := time.Parse("Jan _2 15:04:05", line[:15]); err == nil {
			now = now.UTC()
			candidate := time.Date(now.Year(), parsed.Month(), parsed.Day(),
				parsed.Hour(), parsed.Minute(), parsed.Second(), 0, time.UTC)
			if candidate.After(now.Add(24 * time.Hour)) {
				candidate = candidate.AddDate(-1, 0, 0)
			}
			return candidate, true
		}
	}
	return time.Time{}, false
}

func (c *Collector) buildEvent(record authRecord, raw string, offset int64, recordNumber uint64) (events.Event, error) {
	now := c.now().UTC()
	timestamp, fromLog := parseAuthTimestamp(raw, now)
	timestampSource := events.TimestampSourceIngest
	if fromLog {
		timestampSource = events.TimestampSourceLog
	} else {
		timestamp = now
	}

	identity := c.store.Identity()
	event := events.Event{
		SchemaVersion:   events.SchemaVersionV2,
		Timestamp:       timestamp,
		IngestTimestamp: now,
		TimestampSource: timestampSource,
		Sequence:        recordNumber,
		Agent: events.Agent{
			AgentID:      identity.AgentID,
			AgentType:    identity.AgentType,
			AgentVersion: identity.AgentVersion,
			Generation:   identity.Generation,
		},
		Host: c.host,
		Event: events.EventMeta{
			Category: events.CategoryAuthentication,
			Action:   record.Action,
			Outcome:  record.Outcome,
		},
		Raw: events.Raw{
			Format: events.RawFormatText,
			Data:   raw,
		},
		Collection: events.Collection{
			SourceID:   c.sourceID,
			SourceType: events.SourceTypeFile,
			Path:       c.path,
			Generation: c.generation,
			Position: events.Position{
				ByteOffset:   &offset,
				RecordNumber: recordNumber,
			},
		},
	}
	if record.IP != "" {
		event.Source = &events.Entity{Type: events.EntityIP, IP: record.IP}
	}
	if record.User != "" {
		event.Target = &events.Entity{Type: events.EntityUser, Name: record.User}
	}

	id, err := event.ComputeEventID()
	if err != nil {
		return events.Event{}, fmt.Errorf("compute event id: %w", err)
	}
	event.EventID = id
	return event, nil
}
