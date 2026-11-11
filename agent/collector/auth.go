package collector

import (
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"agent/events"
)

// Match an OpenSSH message at the start of the record or after an sshd
// syslog envelope. Text embedded in a different message is not an auth record.
var (
	sshPrefix              = regexp.MustCompile(`^(?:(?:[A-Z][a-z]{2} +[0-9]{1,2} +[0-9:]{8}|[0-9]{4}-[0-9]{2}-[0-9]{2}T\S+) +\S+ +)?sshd(?:-session|-auth)?(?:\[[0-9]+\])?: +`)
	sshLogin               = regexp.MustCompile(`^(?i:(Failed|Accepted)) (?:password|publickey|keyboard-interactive/pam) for `)
	sshLoginEndpoint       = regexp.MustCompile(` from (\S+) port ([0-9]+) ssh2\b`)
	sshLoginTrailer        = regexp.MustCompile(`^(?:: .*)?(?: \[preauth\])?$`)
	sshInvalidUser         = regexp.MustCompile(`^(?i:Invalid) user `)
	sshInvalidUserEndpoint = regexp.MustCompile(` from (\S+) port ([0-9]+)(?: \[preauth\])?$`)
	sshAuthMarker          = regexp.MustCompile(`(?i)^(?:(?:failed|accepted) (?:password|publickey|keyboard-interactive)(?:[ /]|$)|invalid user(?: |$))`)
)

type authRecord struct {
	Category  events.Category
	Actor     string
	Evidence  string
	Target    *events.Entity
	Timestamp time.Time
	Action    string
	Outcome   string
	User      string
	IP        string
}

// parseAuthLine extracts an SSH authentication record. relevant is true for
// lines that look like SSH authentication but cannot be parsed, so the caller
// can preserve them as unparsed evidence.
func parseAuthLine(line string) (authRecord, bool, error) {
	message := strings.TrimRight(line, "\r\n")
	if prefix := sshPrefix.FindStringIndex(message); prefix != nil {
		message = message[prefix[1]:]
	}
	if !sshAuthMarker.MatchString(message) {
		return authRecord{}, false, nil
	}
	if !utf8.ValidString(line) {
		return authRecord{}, true, fmt.Errorf("invalid UTF-8 in sshd authentication record")
	}

	action, outcome := "ssh_invalid_user", "failure"
	var endpoint []int
	if prefix := sshInvalidUser.FindStringIndex(message); prefix != nil {
		message = message[prefix[1]:]
		endpoint = sshInvalidUserEndpoint.FindStringSubmatchIndex(message)
	} else if prefix := sshLogin.FindStringSubmatchIndex(message); prefix != nil {
		action = "ssh_login"
		if strings.EqualFold(message[prefix[2]:prefix[3]], "Accepted") {
			outcome = "success"
		}
		message = message[prefix[1]:]
		for _, qualifier := range []string{"invalid user ", "illegal user "} {
			if strings.HasPrefix(message, qualifier) {
				if outcome == "success" {
					return authRecord{}, true, fmt.Errorf("contradictory sshd authentication record")
				}
				message = strings.TrimPrefix(message, qualifier)
				break
			}
		}
		// Key details can contain arbitrary text. If both the username and
		// the trailer could account for a second endpoint, attribution is unsafe.
		matches := sshLoginEndpoint.FindAllStringSubmatchIndex(message, 2)
		if len(matches) > 1 {
			return authRecord{}, true, fmt.Errorf("ambiguous address in sshd authentication record")
		}
		if len(matches) == 1 && sshLoginTrailer.MatchString(message[matches[0][1]:]) {
			endpoint = matches[0]
		}
	}
	if endpoint == nil {
		return authRecord{}, true, fmt.Errorf("unrecognized sshd authentication record")
	}
	user, ip, port := message[:endpoint[0]], message[endpoint[2]:endpoint[3]], message[endpoint[4]:endpoint[5]]
	if strings.TrimSpace(user) == "" || strings.ContainsFunc(user, unicode.IsControl) {
		return authRecord{}, true, fmt.Errorf("invalid username in sshd authentication record")
	}
	if addr, err := netip.ParseAddr(ip); err != nil || addr.Zone() != "" {
		return authRecord{}, true, fmt.Errorf("invalid IP in sshd authentication record")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return authRecord{}, true, fmt.Errorf("invalid port in sshd authentication record")
	}
	return authRecord{Action: action, Outcome: outcome, User: user, IP: ip}, true, nil
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
			if candidate.Month() == parsed.Month() && candidate.Day() == parsed.Day() {
				return candidate, true
			}
		}
	}
	return time.Time{}, false
}

func (c *Collector) buildEvent(record authRecord, raw string, offset int64, recordNumber uint64) (events.Event, error) {
	now := c.now().UTC()
	timestamp, fromLog := parseAuthTimestamp(raw, now)
	if !record.Timestamp.IsZero() {
		timestamp, fromLog = record.Timestamp, true
	}
	category := record.Category
	evidence := record.Evidence
	if category == "" {
		category = events.CategoryAuthentication
		evidence = raw
	}
	format := events.RawFormatText
	if c.sourceType == events.SourceTypeAudit {
		format = events.RawFormatAudit
	}
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
			Category: category,
			Action:   record.Action,
			Outcome:  record.Outcome,
		},
		Raw: events.Raw{
			Format: format,
			Data:   evidence,
		},
		Collection: events.Collection{
			SourceID:   c.sourceID,
			SourceType: c.sourceType,
			Path:       c.path,
			Generation: c.generation,
			Position: events.Position{
				ByteOffset:   &offset,
				RecordNumber: recordNumber,
			},
		},
	}
	if record.Actor != "" {
		event.Source = &events.Entity{Type: events.EntityUser, Name: record.Actor}
	}
	event.Target = record.Target
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
