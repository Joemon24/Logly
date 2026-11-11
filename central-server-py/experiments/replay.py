"""Offline replay of the production SSH predicates on previously recorded rows."""
from api.incident_intelligence import (
    BRUTE_FORCE_WINDOW, SUSPICIOUS_WINDOW, detect_failure_attack,
    detect_suspicious_success, event_host, event_time, is_authentication_event,
)
from api.normalization import extract_ip, extract_outcome, extract_user

FEATURE_VERSION = "ssh-window-v1"
FEATURES = ("failures_5m", "distinct_users_5m", "distinct_hosts_5m", "successes_10m", "attempts_10m")


def replay(events):
    ordered = sorted(
        (row for row in events if event_time(row) and is_authentication_event(row)),
        key=event_time,
    )
    findings = []
    for index, row in enumerate(ordered):
        when, ip, user = event_time(row), extract_ip(row), extract_user(row)
        if ip == "unknown":
            continue
        history = ordered[:index + 1]
        if extract_outcome(row) == "failure":
            detection = detect_failure_attack(when, ip, history)
        else:
            detection = detect_suspicious_success(when, ip, user, row, history) if user != "unknown" else None
        if detection:
            findings.append({
                "rule": detection["attack_type"],
                "timestamp": row["timestamp"],
                "event_ids": list(dict.fromkeys(
                    item["event_id"] for item in detection["failure_events"] + detection["success_events"]
                )),
            })
    return findings


def features(events):
    rows = [row for row in events if event_time(row) and is_authentication_event(row)]
    if not rows:
        return [0] * len(FEATURES)
    last = max(rows, key=event_time)
    anchor, ip = event_time(last), extract_ip(last)
    recent = [row for row in rows if extract_ip(row) == ip
              and anchor - SUSPICIOUS_WINDOW <= event_time(row) <= anchor]
    failures = [row for row in recent if extract_outcome(row) == "failure"
                and event_time(row) >= anchor - BRUTE_FORCE_WINDOW]
    return [
        len(failures),
        len({extract_user(row) for row in failures} - {"unknown"}),
        len({event_host(row) for row in failures} - {"unknown"}),
        sum(extract_outcome(row) == "success" for row in recent),
        len(recent),
    ]
