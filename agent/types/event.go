package types

import "time"

type AgentInfo struct {
	AgentID      string
	AgentType    string
	AgentVersion string
}

type RawEvent struct {
	Timestamp time.Time
	Category  string
	Source    string
	Line      string
}

type NormalizedEvent struct {
	EventID         string
	Timestamp       time.Time
	IngestTimestamp time.Time

	Agent AgentInfo

	EventCategory string
	EventType     string
	Action        string
	Outcome       string
	Severity       string

	SourceEntity map[string]string
	TargetEntity map[string]string

	HostContext map[string]string

	Raw RawEvent
}
