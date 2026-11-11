package collector

import (
	"testing"
	"time"
)

func TestParseAuthLineRecognizesSSHRecords(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		action  string
		outcome string
		user    string
		ip      string
	}{
		{
			name:    "failed password ipv4",
			line:    "Nov  9 10:15:30 web-01 sshd[123]: Failed password for alice from 203.0.113.9 port 50000 ssh2\n",
			action:  "ssh_login",
			outcome: "failure",
			user:    "alice",
			ip:      "203.0.113.9",
		},
		{
			name:    "failed invalid user ipv6",
			line:    "Nov  9 10:15:30 web-01 sshd[123]: Failed password for invalid user admin from 2001:db8::5 port 50000 ssh2\n",
			action:  "ssh_login",
			outcome: "failure",
			user:    "admin",
			ip:      "2001:db8::5",
		},
		{
			name:    "failed publickey",
			line:    "Failed publickey for deploy from 192.0.2.10 port 22 ssh2\n",
			action:  "ssh_login",
			outcome: "failure",
			user:    "deploy",
			ip:      "192.0.2.10",
		},
		{
			name:    "accepted password ipv6",
			line:    "2026-11-09T10:15:31.123456+00:00 web-01 sshd[124]: Accepted password for bob from 2001:db8::1 port 50001 ssh2\n",
			action:  "ssh_login",
			outcome: "success",
			user:    "bob",
			ip:      "2001:db8::1",
		},
		{
			name:    "accepted publickey",
			line:    "Nov  9 10:15:31 web-01 sshd[124]: Accepted publickey for carol from 203.0.113.10 port 50001 ssh2: RSA SHA256:abc\n",
			action:  "ssh_login",
			outcome: "success",
			user:    "carol",
			ip:      "203.0.113.10",
		},
		{
			name:    "invalid user notice",
			line:    "Nov  9 10:15:32 web-01 sshd[125]: Invalid user admin from 198.51.100.7 port 50002\n",
			action:  "ssh_invalid_user",
			outcome: "failure",
			user:    "admin",
			ip:      "198.51.100.7",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, relevant, err := parseAuthLine(test.line)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !relevant {
				t.Fatal("line was not relevant")
			}
			if record.Action != test.action || record.Outcome != test.outcome ||
				record.User != test.user || record.IP != test.ip {
				t.Fatalf("record = %+v", record)
			}
		})
	}
}

func TestParseAuthLineIgnoresUnrelatedRecords(t *testing.T) {
	lines := []string{
		"Nov  9 10:15:30 web-01 systemd[1]: Started Daily apt upgrade.\n",
		"Nov  9 10:15:30 web-01 sshd[123]: Received disconnect from 203.0.113.9 port 50000:11: Bye Bye\n",
		"Nov  9 10:15:30 web-01 sudo: pam_unix(sudo:session): session opened for user root\n",
		"",
	}
	for _, line := range lines {
		record, relevant, err := parseAuthLine(line)
		if err != nil || relevant || record != (authRecord{}) {
			t.Fatalf("line %q = %+v, relevant=%v, err=%v", line, record, relevant, err)
		}
	}
}

func TestParseAuthLineRejectsMalformedSSHRecords(t *testing.T) {
	line := "Nov  9 10:16:00 web-01 sshd[200]: Failed password for from not-an-ip port ssh2\n"
	if _, relevant, err := parseAuthLine(line); !relevant || err == nil {
		t.Fatalf("malformed line: relevant=%v err=%v", relevant, err)
	}
}

func TestParseAuthTimestamp(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

	parsed, ok := parseAuthTimestamp(
		"2026-11-09T10:15:30.123456+00:00 web-01 sshd[1]: message\n", now)
	if !ok || !parsed.Equal(time.Date(2026, 11, 9, 10, 15, 30, 123456000, time.UTC)) {
		t.Fatalf("RFC3339 timestamp = %v, ok=%v", parsed, ok)
	}

	parsed, ok = parseAuthTimestamp("Nov  9 10:15:30 web-01 sshd[1]: message\n", now)
	if !ok || !parsed.Equal(time.Date(2026, 11, 9, 10, 15, 30, 0, time.UTC)) {
		t.Fatalf("syslog timestamp = %v, ok=%v", parsed, ok)
	}

	// A December timestamp observed in January belongs to the previous year.
	january := time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC)
	parsed, ok = parseAuthTimestamp("Dec 31 23:59:59 web-01 sshd[1]: message\n", january)
	if !ok || parsed.Year() != 2026 {
		t.Fatalf("year rollover timestamp = %v, ok=%v", parsed, ok)
	}

	if _, ok := parseAuthTimestamp("plain message without timestamp\n", now); ok {
		t.Fatal("line without a timestamp was treated as timestamped")
	}
}

func TestParseSecurityLineAdversarialSSHInput(t *testing.T) {
	tests := []struct {
		name, line, user string
		relevant, reject bool
	}{
		{"quoted message from another daemon", "Nov  9 10:15:30 node systemd[1]: sshd[4]: Accepted password for root from 192.0.2.7 port 22 ssh2", "", false, false},
		{"disconnect reason contains a login", "Nov  9 10:15:30 node sshd[4]: Received disconnect: Accepted password for root from 192.0.2.7 port 22 ssh2", "", false, false},
		{"disconnect reason contains a sudo record", "Nov  9 10:15:30 node sshd[4]: Received disconnect: sudo: alice : USER=root ; COMMAND=/bin/id", "", false, false},
		{"modern OpenSSH tag", "Nov  9 10:15:30 node sshd-session[4]: Accepted password for alice from 192.0.2.7 port 22 ssh2", "alice", true, false},
		{"malformed IP suffix", "sshd[4]: Failed password for root from 192.0.2.7junk port 22 ssh2", "", true, true},
		{"invalid IPv6", "sshd[4]: Failed password for root from 2001:::1 port 22 ssh2", "", true, true},
		{"malformed port", "sshd[4]: Failed password for root from 192.0.2.7 port ssh2", "", true, true},
		{"port out of range", "sshd[4]: Failed password for root from 192.0.2.7 port 65536 ssh2", "", true, true},
		{"unusual username", "sshd[4]: Failed password for équipe+ops@example.test from 2001:db8::1 port 22 ssh2", "équipe+ops@example.test", true, false},
		{"credential-shaped username is literal", "sshd[4]: Invalid user user=admin from 192.0.2.7 port 22", "user=admin", true, false},
		{"address delimiter in username", "sshd[4]: Failed publickey for invalid user root from 198.51.100.99 port 22 ssh2: x from 192.0.2.7 port 50000 ssh2", "", true, true},
		{"contradictory success", "sshd[4]: Accepted password for invalid user root from 192.0.2.7 port 22 ssh2", "", true, true},
		{"preauth suffix", "sshd[4]: Failed password for root from 192.0.2.7 port 22 ssh2 [preauth]", "root", true, false},
		{"invalid UTF-8 evidence", "sshd[4]: Failed password for bad\xff from 192.0.2.7 port 22 ssh2", "", true, true},
		{"control in username", "sshd[4]: Failed password for bad\x1bname from 192.0.2.7 port 22 ssh2", "", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record, relevant, err := parseSecurityLine(tc.line)
			if relevant != tc.relevant || (err != nil) != tc.reject || record.User != tc.user {
				t.Fatalf("parsed %+v, relevant=%v, err=%v", record, relevant, err)
			}
		})
	}
}

func TestParseAuthTimestampDoesNotNormalizeInvalidCalendarDate(t *testing.T) {
	now := time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC)
	for _, line := range []string{"Feb 29 10:15:30 node sshd[1]: message", "2025-02-29T10:15:30Z node sshd[1]: message"} {
		if got, ok := parseAuthTimestamp(line, now); ok {
			t.Fatalf("impossible timestamp %q became %v", line, got)
		}
	}
	leap := time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC)
	if got, ok := parseAuthTimestamp("Feb 29 10:15:30 node sshd[1]: message", leap); !ok || got.Day() != 29 {
		t.Fatalf("valid leap day = %v, ok=%v", got, ok)
	}
}
