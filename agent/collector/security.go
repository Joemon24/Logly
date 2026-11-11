package collector

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"agent/events"
)

var (
	sudoLine    = regexp.MustCompile(`(?:^|\s)sudo(?:\[\d+\])?:\s+(\S+)\s*:\s*(.*)`)
	sudoTarget  = regexp.MustCompile(`(?:^|;\s*)USER=([a-zA-Z0-9_.-]+)(?:\s*;|$)`)
	accountLine = regexp.MustCompile(`(?:^|\s)(useradd|usermod|userdel)\[\d+\]:\s+(.*)`)
	accountName = regexp.MustCompile(`(?:new user: name=|(?:user|name|add|delete) ')([a-zA-Z0-9_.-]+)(?:'|,|\s|$)`)
	auditLine   = regexp.MustCompile(`^type=SYSCALL msg=audit\((\d+\.\d+):\d+\): (.*)`)
	auditKey    = regexp.MustCompile(`(?:^| )key="?(logly-exec|logly-passwd|logly-group)"?(?: |$)`)
	auditResult = regexp.MustCompile(`(?:^| )success=(yes|no)(?: |$)`)
	auditExe    = regexp.MustCompile(`(?:^| )exe="(/[a-zA-Z0-9_./+-]+)"(?: |$)`)
)

func parseSecurityLine(line string) (authRecord, bool, error) {
	record, relevant, err := parseAuthLine(line)
	if err != nil {
		// readFrom has bounded this complete line. Keep a durable event with
		// no inferred actor or outcome so a parse failure cannot pin the file.
		return authRecord{
			Category: events.CategoryAuthentication,
			Action:   "ssh_unparsed", Outcome: "unknown",
			Evidence: strings.ToValidUTF8(line, "?"),
		}, true, nil
	}
	if relevant || sshPrefix.MatchString(line) {
		return record, relevant, nil
	}
	if match := sudoLine.FindStringSubmatch(line); match != nil {
		body := match[2]
		metadata, _, hasCommand := strings.Cut(body, " ; COMMAND=")
		target := sudoTarget.FindStringSubmatch(metadata)
		denied := strings.Contains(metadata, "NOT in sudoers") ||
			strings.Contains(metadata, "command not allowed") || strings.Contains(metadata, "incorrect password")
		if !denied && (!hasCommand || target == nil) {
			return authRecord{}, false, nil
		}
		record := authRecord{Category: events.CategorySudo, Action: "sudo_command",
			Outcome: "success", Actor: match[1]}
		if target != nil {
			record.User = target[1]
		}
		if denied {
			record.Outcome = "failure"
		}
		record.Evidence = "sudo command authorization: " + record.Outcome
		return record, true, nil
	}
	if match := accountLine.FindStringSubmatch(line); match != nil {
		command, body := match[1], match[2]
		action := ""
		switch {
		case command == "useradd" && strings.HasPrefix(body, "new user:"):
			action = "account_created"
		case command == "usermod" && (strings.HasPrefix(body, "change user ") || strings.HasPrefix(body, "add '") || strings.HasPrefix(body, "delete '")):
			action = "account_modified"
		case command == "userdel" && strings.HasPrefix(body, "delete user "):
			action = "account_deleted"
		default:
			return authRecord{}, false, nil
		}
		target := accountName.FindStringSubmatch(body)
		if target == nil {
			return authRecord{}, true, fmt.Errorf("unrecognized account change record")
		}
		return authRecord{Category: events.CategoryAccount, Action: action, Outcome: "success",
			User: target[1], Evidence: command + ": " + action}, true, nil
	}
	return authRecord{}, false, nil
}

func parseAuditLine(line string) (authRecord, bool, error) {
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "type=SYSCALL ") {
		return authRecord{}, false, nil
	}
	key := auditKey.FindStringSubmatch(line)
	if key == nil {
		return authRecord{}, false, nil
	}
	match := auditLine.FindStringSubmatch(line)
	result := auditResult.FindStringSubmatch(line)
	if match == nil || result == nil {
		return authRecord{}, true, fmt.Errorf("invalid keyed audit record")
	}
	timestamp, err := parseAuditTimestamp(match[1])
	if err != nil {
		return authRecord{}, true, fmt.Errorf("invalid audit timestamp")
	}
	record := authRecord{Category: events.CategoryProcess, Action: "process_exec",
		Outcome: "failure", Timestamp: timestamp, Evidence: "audit SYSCALL key=" + key[1] + " success=" + result[1]}
	if result[1] == "yes" {
		record.Outcome = "success"
	}
	switch key[1] {
	case "logly-exec":
		if executable := auditExe.FindStringSubmatch(match[2]); executable != nil {
			record.Target = &events.Entity{Type: events.EntityProcess, Path: executable[1]}
		}
	case "logly-passwd", "logly-group":
		path := "/etc/passwd"
		if key[1] == "logly-group" {
			path = "/etc/group"
		}
		record.Category = events.CategoryFileIntegrity
		record.Action = "watched_file_access"
		record.Target = &events.Entity{Type: events.EntityFile, Path: path}
	}
	return record, true, nil
}

func parseAuditTimestamp(value string) (time.Time, error) {
	seconds, fraction, _ := strings.Cut(value, ".")
	unix, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil || unix > 253402300799 || len(fraction) > 9 {
		return time.Time{}, fmt.Errorf("audit time out of range")
	}
	nanos, err := strconv.Atoi(fraction + strings.Repeat("0", 9-len(fraction)))
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(unix, int64(nanos)).UTC(), nil
}
