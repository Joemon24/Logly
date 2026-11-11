package collector

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"agent/events"
)

func TestSecurityRecords(t *testing.T) {
	tests := []struct {
		name, line          string
		audit               bool
		category            events.Category
		outcome, user, path string
		rejected            bool
	}{
		{name: "sudo", line: "Nov  9 10:15:30 node sudo: alice : TTY=pts/0 ; USER=root ; COMMAND=/bin/echo command not allowed SECRET", category: events.CategorySudo, outcome: "success", user: "root"},
		{name: "sudo denied", line: "node sudo: alice : user NOT in sudoers ; USER=root ; COMMAND=/bin/id", category: events.CategorySudo, outcome: "failure", user: "root"},
		{name: "account created", line: "node useradd[9]: new user: name=bob, UID=1001, home=/home/bob", category: events.CategoryAccount, outcome: "success", user: "bob"},
		{name: "account changed", line: "node usermod[9]: add 'bob' to group 'wheel'", category: events.CategoryAccount, outcome: "success", user: "bob"},
		{name: "bad account", line: "node useradd[9]: new user: name=, UID=1001", rejected: true},
		{name: "unrelated", line: "node systemd[1]: audit log rotation"},
		{name: "exec", line: `type=SYSCALL msg=audit(1762673744.123:42): success=yes exe="/usr/bin/id" key="logly-exec"`, audit: true, category: events.CategoryProcess, outcome: "success", path: "/usr/bin/id"},
		{name: "file access denied", line: `type=SYSCALL msg=audit(1762673744.123:42): success=no key="logly-passwd"`, audit: true, category: events.CategoryFileIntegrity, outcome: "failure", path: "/etc/passwd"},
		{name: "unkeyed audit", line: `type=SYSCALL msg=audit(1762673744.123:42): success=yes key="other"`, audit: true},
		{name: "bad audit", line: `type=SYSCALL msg=audit(1762673744.123:42): key="logly-group"`, audit: true, rejected: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := parseSecurityLine
			if tt.audit {
				parse = parseAuditLine
			}
			record, relevant, err := parse(tt.line)
			if (err != nil) != tt.rejected || relevant != (tt.rejected || tt.category != "") {
				t.Fatalf("unexpected parse: relevant=%v error=%v", relevant, err)
			}
			if tt.category == "" {
				return
			}
			if record.Category != tt.category || record.Outcome != tt.outcome || record.User != tt.user ||
				(tt.path != "" && (record.Target == nil || record.Target.Path != tt.path)) {
				t.Fatalf("unexpected record: %+v", record)
			}
			if strings.Contains(record.Evidence, "SECRET") || strings.Contains(record.Evidence, "home=") {
				t.Fatal("stored sensitive arguments")
			}
		})
	}
}

func TestAuditCheckpointSurvivesCollectorRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	store := newTestStore(t, 5)
	ctx := context.Background()
	line := "type=SYSCALL msg=audit(1762673744.123:42): success=yes key=logly-exec\n"
	writeLog(t, path, "type=EXECVE private_arguments\n"+line)
	cfg := Config{Store: store, Path: path, SourceID: "linux-audit", Audit: true}
	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := first.Poll(ctx); err != nil || count != 1 {
		t.Fatalf("first poll: count=%d error=%v", count, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if count, err := second.Poll(ctx); err != nil || count != 0 {
		t.Fatalf("restart duplicated event: count=%d error=%v", count, err)
	}
	rows := pendingEvents(t, store)
	if len(rows) != 1 || rows[0].Collection.SourceType != events.SourceTypeAudit ||
		rows[0].Collection.Position.RecordNumber != 2 || rows[0].Timestamp.UnixMilli() != 1762673744123 {
		t.Fatalf("wrong audit provenance: %+v", rows)
	}
}
