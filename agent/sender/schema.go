package sender

// FINAL external schema — DO NOT CHANGE

type EventEnvelope struct {
	EventID         string `json:"event_id"`
	Timestamp       string `json:"timestamp"`
	IngestTimestamp string `json:"ingest_timestamp"`

	Agent AgentBlock `json:"agent"`
	Event EventBlock `json:"event"`
	Source Entity    `json:"source"`
	Target Entity    `json:"target"`

	Risk      RiskBlock      `json:"risk"`
	Integrity IntegrityBlock `json:"integrity"`

	RawEvent RawBlock `json:"raw_event"`

	// Domain-specific (on-prem)
	Host HostBlock `json:"host"`
}

type AgentBlock struct {
	AgentID      string  `json:"agent_id"`
	AgentType    string  `json:"agent_type"`
	AgentVersion string  `json:"agent_version"`
	TrustScore   float64 `json:"trust_score"`
}

type EventBlock struct {
	Category string `json:"category"`
	Type     string `json:"type"`
	Action   string `json:"action"`
	Outcome  string `json:"outcome"`
	Severity string `json:"severity"`
}

type Entity struct {
	EntityType string `json:"entity_type"`
	ID         string `json:"id,omitempty"`
	IP         string `json:"ip,omitempty"`
	Host       string `json:"host,omitempty"`
}

type RiskBlock struct {
	Score        float64 `json:"score"`
	Confidence   float64 `json:"confidence"`
	AnomalyScore float64 `json:"anomaly_score"`
}

type IntegrityBlock struct {
	Hash     string `json:"hash"`
	PrevHash string `json:"prev_hash,omitempty"`
	ChainID  string `json:"chain_id"`
}

type RawBlock struct {
	Format string `json:"format"`
	Data   any    `json:"data"`
}

type HostBlock struct {
	OS struct {
		Type    string `json:"type"`
		Version string `json:"version"`
	} `json:"os"`
}
