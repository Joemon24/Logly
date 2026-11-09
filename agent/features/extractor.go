package features

import (
	"sync"
	"time"

	"agent/types"
)

// Extractor maintains rolling windows for host features
type Extractor struct {
	mu sync.Mutex

	// simple rolling state (Phase-1)
	failedLogins  []time.Time
	successLogins []time.Time
	userSeen      map[string]time.Time
}

func New() *Extractor {
	return &Extractor{
		userSeen: make(map[string]time.Time),
	}
}

// Update consumes ONE normalized event and updates host features
func (e *Extractor) Update(ev types.NormalizedEvent) HostFeatures {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()

	// Only auth category contributes in Phase-1
	if ev.EventCategory == "auth" {
		if ev.Outcome == "failure" {
			e.failedLogins = append(e.failedLogins, now)
		}
		if ev.Outcome == "success" {
			e.successLogins = append(e.successLogins, now)
		}
		// track user (best-effort)
		if u, ok := ev.SourceEntity["user"]; ok && u != "" {
			e.userSeen[u] = now
		}
	}

	// prune windows
	cut1m := now.Add(-1 * time.Minute)
	cut5m := now.Add(-5 * time.Minute)

	e.failedLogins = prune(e.failedLogins, cut1m)
	e.successLogins = prune(e.successLogins, cut1m)

	for u, ts := range e.userSeen {
		if ts.Before(cut5m) {
			delete(e.userSeen, u)
		}
	}

	return HostFeatures{
		FailedLogins1m:  len(e.failedLogins),
		SuccessLogins1m: len(e.successLogins),
		UniqueUsers5m:   len(e.userSeen),
		LastUpdated:     now,
	}
}

func prune(ts []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for ; i < len(ts); i++ {
		if ts[i].After(cutoff) {
			break
		}
	}
	return ts[i:]
}
