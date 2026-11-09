package batcher

import "time"

// Batch is an internal carrier (NOT the final schema)
type Batch struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	Count     int

	// Internal payload for next stages
	Events []any // we keep it generic for now
}
