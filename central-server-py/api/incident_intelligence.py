"""Evidence-based incident detection over stored schema-v2 authentication events.

Detection windows are queried from Elasticsearch, so the consumer keeps no
detection state. Incident IDs are deterministic and evidence event IDs are
unioned, so a Kafka replay can neither duplicate an incident nor shrink or
inflate its evidence.

Severity and risk are deterministic functions of the supporting evidence. No
confidence or accuracy value is produced.
"""
import hashlib
from datetime import datetime, timedelta, timezone

from elasticsearch import ConflictError, NotFoundError

from api.normalization import extract_ip, extract_outcome, extract_user

EVENT_INDEX = "ids-events"
INCIDENT_INDEX = "ids-incidents-v2"

BRUTE_FORCE = "ssh_brute_force"
PASSWORD_SPRAYING = "ssh_password_spraying"
SUSPICIOUS_SUCCESS = "suspicious_login_success"

MITRE = {
    BRUTE_FORCE: "T1110.001",
    PASSWORD_SPRAYING: "T1110.003",
    SUSPICIOUS_SUCCESS: "T1078",
}

BRUTE_FORCE_WINDOW = timedelta(minutes=5)
SUSPICIOUS_WINDOW = timedelta(minutes=10)
BRUTE_FORCE_MIN_FAILURES = 5
SPRAYING_MIN_USERS = 5
SUSPICIOUS_MIN_FAILURES = 3
QUERY_LIMIT = 500
MAX_UPSERT_ATTEMPTS = 3

AUTH_CATEGORIES = {"auth", "authentication"}


def event_time(event):
    """Return the event's UTC timestamp, preferring the log-time field."""
    for key in ("timestamp", "@timestamp"):
        value = event.get(key)
        if not isinstance(value, str):
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def utc_string(value):
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def event_host(event):
    agent = event.get("agent")
    if isinstance(agent, dict) and agent.get("agent_id"):
        return str(agent["agent_id"])
    return "unknown"


def is_authentication_event(event):
    metadata = event.get("event")
    if not isinstance(metadata, dict):
        return False
    if metadata.get("category") not in AUTH_CATEGORIES:
        return False
    return extract_outcome(event) in {"failure", "success"}


def severity_for(score):
    if score >= 80:
        return "critical"
    if score >= 60:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


def risk_for(attack_type, failure_count, distinct_users):
    """Deterministic score from observed evidence counts, capped at 100."""
    if attack_type == BRUTE_FORCE:
        score = 40 + 6 * max(0, failure_count - BRUTE_FORCE_MIN_FAILURES)
    elif attack_type == PASSWORD_SPRAYING:
        score = 50 + 8 * max(0, distinct_users - SPRAYING_MIN_USERS)
    elif attack_type == SUSPICIOUS_SUCCESS:
        score = 60 + 8 * max(0, failure_count - SUSPICIOUS_MIN_FAILURES)
    else:
        score = 0
    score = min(100, max(0, score))
    return score, severity_for(score)


def incident_id(attack_type, source_ip, anchor):
    window = SUSPICIOUS_WINDOW if attack_type == SUSPICIOUS_SUCCESS else BRUTE_FORCE_WINDOW
    seconds = int(window.total_seconds())
    bucket = int(anchor.timestamp()) // seconds * seconds
    material = f"{attack_type}|{source_ip}|{bucket}"
    return "inc_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def _detection(attack_type, anchor, source_ip, failures, successes, username=None):
    users = sorted({
        extract_user(item) for item in failures
        if extract_user(item) != "unknown"
    })
    hosts = sorted({
        event_host(item) for item in failures + successes
        if event_host(item) != "unknown"
    })
    return {
        "attack_type": attack_type,
        "source_ip": source_ip,
        "username": username,
        "usernames": users,
        "hosts": hosts,
        "failure_events": failures,
        "success_events": successes,
        "anchor": anchor,
    }


def _in_window(event, start, end):
    timestamp = event_time(event)
    return timestamp is not None and start <= timestamp <= end


def detect_failure_attack(anchor, source_ip, events):
    """Detect spraying first, then brute force, from failures for one IP."""
    start = anchor - BRUTE_FORCE_WINDOW
    failures = [
        event for event in events
        if extract_outcome(event) == "failure"
        and extract_ip(event) == source_ip
        and _in_window(event, start, anchor)
    ]
    if len(failures) < BRUTE_FORCE_MIN_FAILURES:
        return None
    distinct_users = {
        extract_user(event) for event in failures
        if extract_user(event) != "unknown"
    }
    attack_type = (
        PASSWORD_SPRAYING
        if len(distinct_users) >= SPRAYING_MIN_USERS
        else BRUTE_FORCE
    )
    return _detection(attack_type, anchor, source_ip, failures, [])


def detect_suspicious_success(anchor, source_ip, username, success_event, events):
    """A success after repeated failures for the same IP and user."""
    start = anchor - SUSPICIOUS_WINDOW
    failures = [
        event for event in events
        if extract_outcome(event) == "failure"
        and extract_ip(event) == source_ip
        and extract_user(event) == username
        and _in_window(event, start, anchor)
    ]
    if len(failures) < SUSPICIOUS_MIN_FAILURES:
        return None
    return _detection(
        SUSPICIOUS_SUCCESS, anchor, source_ip, failures, [success_event], username,
    )


def _search_failures(client, source_ip, start, end, username=None):
    filters = [
        {"term": {"src_ip.keyword": source_ip}},
        {"term": {"outcome.keyword": "failure"}},
        {"range": {"timestamp": {"gte": utc_string(start), "lte": utc_string(end)}}},
    ]
    if username is not None:
        filters.append({"term": {"username.keyword": username}})
    response = client.search(
        index=EVENT_INDEX,
        query={"bool": {"filter": filters}},
        size=QUERY_LIMIT,
        sort=[{"timestamp": "asc"}],
        allow_partial_search_results=False,
    )
    if response.get("timed_out") or response.get("_shards", {}).get("failed"):
        raise RuntimeError("incomplete Elasticsearch incident evidence search")
    return [
        hit.get("_source", {})
        for hit in response.get("hits", {}).get("hits", [])
    ]


def _event_ids(events):
    return sorted({
        event["event_id"] for event in events if event.get("event_id")
    })


def incident_from_detection(detection):
    failures = detection["failure_events"]
    successes = detection["success_events"]
    failure_ids = _event_ids(failures)
    success_ids = _event_ids(successes)
    users = detection["usernames"]
    attack_type = detection["attack_type"]
    score, severity = risk_for(attack_type, len(failure_ids), len(users))
    timestamps = [
        event_time(event)
        for event in failures + successes
        if event_time(event) is not None
    ]
    window = (
        SUSPICIOUS_WINDOW if attack_type == SUSPICIOUS_SUCCESS else BRUTE_FORCE_WINDOW
    )
    now = datetime.now(timezone.utc)
    username = detection.get("username")
    if username in (None, "unknown"):
        username = users[0] if len(users) == 1 else None
    return {
        "incident_id": incident_id(attack_type, detection["source_ip"], detection["anchor"]),
        "attack_type": attack_type,
        "status": "open",
        "source_ip": detection["source_ip"],
        "username": username,
        "usernames": users,
        "hosts": detection["hosts"],
        "severity": severity,
        "risk_score": score,
        "mitre": MITRE[attack_type],
        "failure_event_ids": failure_ids,
        "success_event_ids": success_ids,
        "event_ids": sorted(set(failure_ids + success_ids)),
        "first_seen": utc_string(min(timestamps)) if timestamps else None,
        "last_seen": utc_string(max(timestamps)) if timestamps else None,
        "window_start": utc_string(detection["anchor"] - window),
        "window_end": utc_string(detection["anchor"]),
        "evidence": {
            "failure_count": len(failure_ids),
            "success_count": len(success_ids),
            "distinct_usernames": len(users),
            "host_count": len(detection["hosts"]),
        },
        "detected_at": utc_string(now),
        "updated_at": utc_string(now),
    }


def merge_incident(existing, incoming):
    """Union evidence so replay or out-of-order processing never removes it."""
    if not existing:
        return incoming
    failure_ids = sorted(
        set(existing.get("failure_event_ids", []))
        | set(incoming.get("failure_event_ids", []))
    )
    success_ids = sorted(
        set(existing.get("success_event_ids", []))
        | set(incoming.get("success_event_ids", []))
    )
    users = sorted(
        set(existing.get("usernames", [])) | set(incoming.get("usernames", []))
    )
    hosts = sorted(
        set(existing.get("hosts", [])) | set(incoming.get("hosts", []))
    )
    score, severity = risk_for(incoming["attack_type"], len(failure_ids), len(users))
    timestamps = [
        parsed
        for parsed in (
            event_time({"timestamp": existing.get("first_seen")}),
            event_time({"timestamp": existing.get("last_seen")}),
            event_time({"timestamp": incoming.get("first_seen")}),
            event_time({"timestamp": incoming.get("last_seen")}),
        )
        if parsed is not None
    ]
    window_starts = [
        parsed
        for parsed in (
            event_time({"timestamp": existing.get("window_start")}),
            event_time({"timestamp": incoming.get("window_start")}),
        )
        if parsed is not None
    ]
    window_ends = [
        parsed
        for parsed in (
            event_time({"timestamp": existing.get("window_end")}),
            event_time({"timestamp": incoming.get("window_end")}),
        )
        if parsed is not None
    ]
    document = dict(incoming)
    document.update({
        "failure_event_ids": failure_ids,
        "success_event_ids": success_ids,
        "event_ids": sorted(set(failure_ids + success_ids)),
        "usernames": users,
        "hosts": hosts,
        "severity": severity,
        "risk_score": score,
        "username": incoming.get("username") or existing.get("username"),
        "first_seen": utc_string(min(timestamps)) if timestamps else None,
        "last_seen": utc_string(max(timestamps)) if timestamps else None,
        "window_start": utc_string(min(window_starts)) if window_starts else None,
        "window_end": utc_string(max(window_ends)) if window_ends else None,
        "detected_at": existing.get("detected_at"),
        "updated_at": utc_string(datetime.now(timezone.utc)),
        "evidence": {
            "failure_count": len(failure_ids),
            "success_count": len(success_ids),
            "distinct_usernames": len(users),
            "host_count": len(hosts),
        },
    })
    return document


def upsert_incident(client, incident, attempts=MAX_UPSERT_ATTEMPTS):
    """Create or merge one deterministic incident document."""
    identifier = incident["incident_id"]
    for _ in range(attempts):
        try:
            existing = client.get(index=INCIDENT_INDEX, id=identifier)
            document = merge_incident(existing.get("_source", {}), incident)
            options = {"index": INCIDENT_INDEX, "id": identifier, "document": document}
            if existing.get("_seq_no") is not None:
                options["if_seq_no"] = existing["_seq_no"]
                options["if_primary_term"] = existing["_primary_term"]
            client.index(**options)
            return document
        except NotFoundError:
            try:
                client.create(index=INCIDENT_INDEX, id=identifier, document=incident)
                return incident
            except ConflictError:
                continue
        except ConflictError:
            continue
    raise RuntimeError(f"incident write conflict: {identifier}")


def analyze_event(client, event):
    """Evaluate one authentication event and persist any detection.

    Returns the incident ID, or None when no threshold is met. The caller
    writes the event before calling this, so window queries include it.
    """
    if not is_authentication_event(event):
        return None
    anchor = event_time(event)
    source_ip = extract_ip(event)
    if anchor is None or source_ip == "unknown":
        return None

    outcome = extract_outcome(event)
    username = extract_user(event)
    if outcome == "success" and username == "unknown":
        return None

    if outcome == "failure":
        events = _search_failures(
            client, source_ip, anchor - BRUTE_FORCE_WINDOW, anchor,
        )
        if event.get("event_id") and event["event_id"] not in _event_ids(events):
            events.append(event)
        detection = detect_failure_attack(anchor, source_ip, events)
    else:
        events = _search_failures(
            client, source_ip, anchor - SUSPICIOUS_WINDOW, anchor,
            username=None if username == "unknown" else username,
        )
        detection = detect_suspicious_success(
            anchor, source_ip, username, event, events,
        )

    if detection is None:
        return None
    document = incident_from_detection(detection)
    upsert_incident(client, document)
    return document["incident_id"]
