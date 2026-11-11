"""Suppress repeated notifications without suppressing evidence."""


def alert_due(event, now, last_alert_time, cooldown=60):
    key = (event["agent"]["agent_id"], event["alert_type"], event["src_ip"])
    if event.get("schema_version") == 2:
        return key, False
    previous = last_alert_time.get(key)
    suppressed = previous is not None and (now - previous).total_seconds() < cooldown
    event["alert_suppressed"] = suppressed
    return key, event["severity"] in {"high", "critical"} and not suppressed
