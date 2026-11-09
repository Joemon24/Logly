package features

import "time"

// HostFeatures are LOCAL, windowed signals
type HostFeatures struct {
	FailedLogins1m  int
	SuccessLogins1m int
	UniqueUsers5m   int
	LastUpdated     time.Time
}

