"""Read-only investigator. The model chooses a topic; Python renders all claims."""
import json
import os
import re
from threading import BoundedSemaphore
from typing import Protocol
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler

from api.normalization import extract_ip, extract_outcome, extract_user

from api.incident_intelligence import (
    BRUTE_FORCE, PASSWORD_SPRAYING, SUSPICIOUS_SUCCESS, EVENT_INDEX, INCIDENT_INDEX, event_time,
    event_host, is_authentication_event,
    detect_failure_attack, detect_suspicious_success,
)

EVIDENCE_LIMIT = 50
TOPICS = ("summary", "source", "hosts", "related", "gaps", "next")
EVENT_ID = re.compile(r"evt2_[0-9a-f]{64}")
RULES = {
    BRUTE_FORCE: "At least five SSH failures from one source IP in five minutes.",
    PASSWORD_SPRAYING: "SSH failures from one IP across at least five known usernames in five minutes.",
    SUSPICIOUS_SUCCESS: "A successful SSH login after at least three failures for the same IP and username in ten minutes.",
}
MODEL_SLOTS = BoundedSemaphore(2)


class TopicProvider(Protocol):
    def topic(self, question: str) -> str: ...


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class OllamaTopicProvider:
    def __init__(self, model):
        self.model = model
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def topic(self, question):
        if not MODEL_SLOTS.acquire(blocking=False):
            raise ValueError("inference busy")
        try:
            payload = {
                "model": self.model, "stream": False, "think": False,
                "options": {"num_predict": 16, "num_ctx": 1024, "temperature": 0},
                "format": {"type": "string", "enum": list(TOPICS)},
                "system": (
                    "Classify an incident investigation question. Select summary for a general review; "
                    "source for IP addresses; hosts for affected machines; related for other incidents; "
                    "gaps for missing evidence or proof of compromise; next for recommended analyst actions."
                ),
                "prompt": question,
            }
            request = Request("http://127.0.0.1:11434/api/generate",
                              data=json.dumps(payload).encode(),
                              headers={"Content-Type": "application/json"})
            with self.opener.open(request, timeout=3) as response:
                body = json.loads(response.read(8192))
            return json.loads(body["response"])
        finally:
            MODEL_SLOTS.release()


def configured_provider():
    model = os.getenv("LOGLY_OLLAMA_MODEL", "").strip()
    return OllamaTopicProvider(model) if model else None


def choose_topic(question, provider):
    if not question:
        return "summary", "deterministic"
    if provider:
        try:
            topic = provider.topic(question)
            if topic in TOPICS:
                return topic, "ollama"
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            pass
    for topic, terms in (
        ("related", ("related", "other incidents")),
        ("gaps", ("missing", "unknown", "gaps", "compromise", "proven")),
        ("hosts", ("host", "machine")),
        ("source", ("source", "address", " ip")),
        ("next", ("next", "recommend", "should")),
    ):
        if any(term in question.lower() for term in terms):
            return topic, "deterministic"
    return "summary", "deterministic"


class InvestigationTools:
    """All lookup arguments come from the chosen incident and fixed query bounds."""
    def __init__(self, client):
        self.client = client

    def get_incident(self, incident_id):
        return self.client.get(index=INCIDENT_INDEX, id=incident_id)["_source"]

    def search_events(self, references):
        if not references:
            return {}
        from api.main import _search_or_empty
        response = _search_or_empty(
            self.client, index=EVENT_INDEX, query={"ids": {"values": references}},
            size=EVIDENCE_LIMIT, _source={"excludes": ["features"]},
        )
        return {hit["_id"]: hit["_source"] for hit in response.get("hits", {}).get("hits", [])
                if hit["_id"] in references}

    def get_host_history(self, host):
        from api.main import host_history
        result = host_history(host, limit=5, client=self.client)
        return {"host": host, "event_total": result["event_total"],
                "incident_total": result["incident_total"]}

    def get_related_incidents(self, source_ip, incident_id):
        from api.main import list_incidents
        result = list_incidents(limit=6, source_ip=source_ip, client=self.client)
        return [row["incident_id"] for row in result["incidents"]
                if row["incident_id"] != incident_id][:5]


def reproduce_rule(incident, records):
    attack = incident.get("attack_type")
    ip = incident.get("source_ip")
    anchor = event_time({"timestamp": incident.get("window_end")})
    if not anchor or not ip or attack not in RULES:
        return []
    events = [event for event in records.values() if is_authentication_event(event)]
    if attack == SUSPICIOUS_SUCCESS:
        result = None
        for event in events:
            when = event_time(event)
            if (when == anchor and extract_outcome(event) == "success"
                    and extract_ip(event) == ip and extract_user(event) != "unknown"):
                result = detect_suspicious_success(when, ip, extract_user(event), event, events)
                if result:
                    break
    else:
        result = detect_failure_attack(anchor, ip, events)
    if not result or result["attack_type"] != attack:
        return []
    supporting = {id(event) for event in result["failure_events"] + result["success_events"]}
    return [ref for ref, event in records.items() if id(event) in supporting]


def investigate(tools, incident_id, question, provider=None):
    incident = tools.get_incident(incident_id)
    topic, mode = choose_topic(question, provider)
    # Incident references are generated by the detection pipeline. Reject any
    # unexpected identifier before building an Elasticsearch query.
    references = list(dict.fromkeys(
        ref for ref in incident.get("event_ids", [])
        if isinstance(ref, str) and EVENT_ID.fullmatch(ref)
    ))
    bounded = references[:EVIDENCE_LIMIT]
    records = tools.search_events(bounded)
    failure_ids = [ref for ref in bounded if ref in records and ref in incident.get("failure_event_ids", [])]
    success_ids = [ref for ref in bounded if ref in records and ref in incident.get("success_event_ids", [])]
    rule_ids = reproduce_rule(incident, records)
    observations = []
    if rule_ids:
        observations.append({"text": "Retrieved events reproduce the stored rule classification.", "event_ids": rule_ids})
    if failure_ids:
        observations.append({"text": f"Retrieved {len(failure_ids)} referenced failure records.", "event_ids": failure_ids})
    if success_ids:
        observations.append({"text": f"Retrieved {len(success_ids)} referenced success records.", "event_ids": success_ids})
    source_ip = incident.get("source_ip")
    hosts = incident.get("hosts", [])[:3]
    if topic in {"summary", "source"} and source_ip:
        ids = [ref for ref, row in records.items() if extract_ip(row) == source_ip]
        if ids:
            observations.append({"text": f"Observed source IP {source_ip}.", "event_ids": ids})
    if topic in {"summary", "hosts"}:
        for host in hosts:
            ids = [ref for ref, row in records.items() if event_host(row) == host]
            if ids:
                observations.append({"text": f"Observed agent {host}.", "event_ids": ids})
    gaps = ["These records alone cannot establish attacker identity, account compromise or subsequent actions."]
    missing = len(bounded) - len(records)
    if missing:
        gaps.append(f"{missing} requested evidence references were unavailable.")
    if len(references) > len(bounded):
        gaps.append(f"Reviewed at most {EVIDENCE_LIMIT} of {len(references)} references.")
    if not rule_ids:
        gaps.append("The retrieved evidence did not independently reproduce the stored rule classification.")
    return {
        "incident_id": incident_id, "mode": mode, "topic": topic,
        "rule": RULES.get(incident.get("attack_type"), "No rule explanation is available."),
        "reproduced": bool(rule_ids), "observations": observations, "gaps": gaps,
        "recommendations": ["Review the original authentication records and confirm whether the account activity was expected.",
                            "Compare nearby host activity with the authentication timeline."],
        "related_incident_ids": tools.get_related_incidents(source_ip, incident_id) if topic == "related" and source_ip else [],
        "host_history": [tools.get_host_history(host) for host in hosts] if topic == "hosts" else [],
    }
