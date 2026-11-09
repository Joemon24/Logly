package parser

import (
	"regexp"
	"strings"
	"time"

	"agent/types"

	"github.com/google/uuid"
)

var ipRegex = regexp.MustCompile(`\b\d{1,3}(\.\d{1,3}){3}\b`)

func Parse(raw types.RawEvent, agent types.AgentInfo) types.NormalizedEvent {
	event := types.NormalizedEvent{
		EventID:         uuid.New().String(),
		Timestamp:       raw.Timestamp.UTC(),
		IngestTimestamp: time.Now().UTC(),
		Agent:           agent,
		EventCategory:   raw.Category,
		EventType:       "unknown",
		Action:          "detected",
		Outcome:         "unknown",
		Severity:        "info",
		SourceEntity:    map[string]string{},
		TargetEntity:    map[string]string{},
		HostContext:     map[string]string{},
		Raw:             raw,
	}

	line := strings.ToLower(raw.Line)

	/* ---------- AUTH LOGIC ---------- */

	if raw.Category == "auth" {

		event.EventType = "authentication"

		if strings.Contains(line, "failure") || strings.Contains(line, "failed") {
			event.Action = "login_failed"
			event.Outcome = "failure"
			event.Severity = "medium"
		}

		if strings.Contains(line, "success") {
			event.Action = "login_success"
			event.Outcome = "success"
			event.Severity = "info"
		}

		// Extract IP (if exists)
		ip := ipRegex.FindString(line)
		if ip != "" {
			event.SourceEntity["ip"] = ip
		}
	}

	return event
}
