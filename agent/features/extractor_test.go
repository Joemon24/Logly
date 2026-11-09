package features

import (
	"agent/types"
	"testing"
)

func TestParserOutcomesAreCounted(t *testing.T) {
	e := New()
	result := e.Update(types.NormalizedEvent{EventCategory: "auth", Action: "login_failed", Outcome: "failure"})
	if result.FailedLogins1m != 1 {
		t.Fatal("failure not counted")
	}
	result = e.Update(types.NormalizedEvent{EventCategory: "auth", Action: "login_success", Outcome: "success"})
	if result.SuccessLogins1m != 1 {
		t.Fatal("success not counted")
	}
}
