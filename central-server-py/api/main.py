"""Read-only FastAPI endpoints for events, hosts, and incidents.

Handlers take an optional Elasticsearch client so they can be tested without a
server. Filtering and aggregation happen in Elasticsearch; responses are
summaries, with full evidence available on the incident detail endpoint.
"""
from typing import Optional

from elasticsearch import ApiError, NotFoundError
from elastic_transport import TransportError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from api.incident_intelligence import EVENT_INDEX, INCIDENT_INDEX
from api.settings import elasticsearch_client
from api.investigator import InvestigationTools, investigate
from api.investigator_routing import configured_provider

es = elasticsearch_client()

app = FastAPI(title="Logly incident API")

MAX_RESULT_WINDOW = 10_000
MAX_TIMELINE_EVENTS = 500


@app.exception_handler(TransportError)
async def storage_transport_error(_request: Request, _error: TransportError):
    return JSONResponse(status_code=503, content={"detail": "telemetry storage unavailable"})


@app.exception_handler(ApiError)
async def storage_api_error(_request: Request, _error: ApiError):
    return JSONResponse(status_code=503, content={"detail": "telemetry storage query failed"})


EVENT_FIELDS = (
    "event_id", "storage_id", "timestamp", "@timestamp", "agent", "event",
    "source", "target", "collection", "username", "src_ip", "outcome",
    "severity", "alert_type", "mitre_technique", "risk",
)


def _client(client):
    return client if client is not None else es


def _search_or_empty(client, **kwargs):
    try:
        result = client.search(allow_partial_search_results=False, **kwargs)
        if result.get("timed_out") or result.get("_shards", {}).get("failed"):
            raise HTTPException(status_code=503, detail="incomplete telemetry search")
        return result
    except NotFoundError:
        return {}


def _count_or_zero(client, **kwargs):
    try:
        result = client.count(**kwargs)
        if result.get("_shards", {}).get("failed") or "count" not in result:
            raise HTTPException(status_code=503, detail="incomplete telemetry count")
        return result["count"]
    except NotFoundError:
        return 0


def _total(response):
    total = response.get("hits", {}).get("total", 0)
    return total.get("value", 0) if isinstance(total, dict) else total


def _buckets(response, name):
    buckets = response.get("aggregations", {}).get(name, {}).get("buckets", [])
    return {bucket["key"]: bucket["doc_count"] for bucket in buckets}


def event_summary(source):
    return {field: source[field] for field in EVENT_FIELDS if field in source}


def _event_query(agent_id=None, source_ip=None, outcome=None, username=None, since=None, category=None):
    filters = []
    if agent_id:
        filters.append({"term": {"agent.agent_id.keyword": agent_id}})
    if source_ip:
        filters.append({"term": {"src_ip.keyword": source_ip}})
    if outcome:
        filters.append({"term": {"outcome.keyword": outcome}})
    if username:
        filters.append({"term": {"username.keyword": username}})
    if category:
        filters.append({"term": {"event.category.keyword": category}})
    if since:
        filters.append({"range": {"timestamp": {"gte": since}}})
    return {"bool": {"filter": filters}} if filters else {"match_all": {}}


def list_events(limit=50, offset=0, agent_id=None, source_ip=None, outcome=None,
                username=None, since=None, client=None, category=None):
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    if offset + limit > MAX_RESULT_WINDOW:
        raise HTTPException(status_code=422, detail="offset and limit exceed the 10000-record search window")
    response = _search_or_empty(
        _client(client),
        index=EVENT_INDEX,
        query=_event_query(agent_id, source_ip, outcome, username, since, category),
        sort=[{"timestamp": {"order": "desc", "unmapped_type": "date"}}],
        from_=offset,
        size=limit,
        _source={"excludes": ["features", "raw_event"]},
    )
    events = [
        event_summary(hit.get("_source", {}))
        for hit in response.get("hits", {}).get("hits", [])
    ]
    return {
        "total": _total(response),
        "limit": limit,
        "offset": offset,
        "events": events,
    }


def _incident_query(severity=None, attack_type=None, status=None,
                    source_ip=None, host=None):
    filters = []
    if severity:
        filters.append({"term": {"severity.keyword": severity}})
    if attack_type:
        filters.append({"term": {"attack_type.keyword": attack_type}})
    if status:
        filters.append({"term": {"status.keyword": status}})
    if source_ip:
        filters.append({"term": {"source_ip.keyword": source_ip}})
    if host:
        filters.append({"term": {"hosts.keyword": host}})
    return {"bool": {"filter": filters}} if filters else {"match_all": {}}


def list_incidents(limit=50, offset=0, severity=None, attack_type=None,
                   status=None, source_ip=None, host=None, client=None):
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    if offset + limit > MAX_RESULT_WINDOW:
        raise HTTPException(status_code=422, detail="offset and limit exceed the 10000-record search window")
    response = _search_or_empty(
        _client(client),
        index=INCIDENT_INDEX,
        query=_incident_query(severity, attack_type, status, source_ip, host),
        sort=[{"last_seen": {"order": "desc", "unmapped_type": "date"}}],
        from_=offset,
        size=limit,
    )
    incidents = [
        hit.get("_source", {})
        for hit in response.get("hits", {}).get("hits", [])
    ]
    return {
        "total": _total(response),
        "limit": limit,
        "offset": offset,
        "incidents": incidents,
    }


def _timeline(client, event_ids):
    if not event_ids:
        return [], []
    response = _search_or_empty(
        client,
        index=EVENT_INDEX,
        query={"ids": {"values": list(event_ids)}},
        sort=[{"timestamp": {"order": "asc", "unmapped_type": "date"}}],
        size=len(event_ids),
    )
    hits = response.get("hits", {}).get("hits", [])
    timeline = [event_summary(hit.get("_source", {})) for hit in hits]
    found = {hit.get("_id") for hit in hits}
    return timeline, sorted(set(event_ids) - found)


def get_incident(incident_id, client=None):
    client = _client(client)
    try:
        response = client.get(index=INCIDENT_INDEX, id=incident_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail="incident not found")
    incident = response.get("_source", {})
    references = incident.get("event_ids", [])
    timeline, missing = _timeline(client, references[:MAX_TIMELINE_EVENTS])
    incident["omitted_event_count"] = max(0, len(references) - MAX_TIMELINE_EVENTS)
    incident["timeline"] = timeline
    incident["evidence_event_count"] = len(timeline)
    incident["missing_event_ids"] = missing
    return incident


def overview(client=None):
    client = _client(client)
    events = _count_or_zero(client, index=EVENT_INDEX)
    incidents = _count_or_zero(client, index=INCIDENT_INDEX)
    open_incidents = _count_or_zero(
        client,
        index=INCIDENT_INDEX,
        query={"term": {"status.keyword": "open"}},
    )
    incident_stats = _search_or_empty(
        client,
        index=INCIDENT_INDEX,
        size=0,
        aggs={
            "by_severity": {"terms": {"field": "severity.keyword", "size": 10}},
            "by_attack_type": {"terms": {"field": "attack_type.keyword", "size": 10}},
        },
    )
    host_stats = _search_or_empty(
        client,
        index=EVENT_INDEX,
        size=0,
        aggs={"hosts": {"cardinality": {"field": "agent.agent_id.keyword"}}},
    )
    return {
        "events": events,
        "incidents": incidents,
        "open_incidents": open_incidents,
        "hosts": host_stats.get("aggregations", {}).get("hosts", {}).get("value", 0),
        "by_severity": _buckets(incident_stats, "by_severity"),
        "by_attack_type": _buckets(incident_stats, "by_attack_type"),
    }


def list_hosts(limit=50, client=None):
    limit = max(1, min(int(limit), 500))
    response = _search_or_empty(
        _client(client),
        index=EVENT_INDEX,
        size=0,
        aggs={
            "hosts": {
                "terms": {"field": "agent.agent_id.keyword", "size": limit},
                "aggs": {"last_seen": {"max": {"field": "timestamp"}}},
            },
        },
    )
    hosts = []
    for bucket in response.get("aggregations", {}).get("hosts", {}).get("buckets", []):
        last_seen = bucket.get("last_seen", {})
        hosts.append({
            "host": bucket["key"],
            "events": bucket["doc_count"],
            "last_seen": last_seen.get("value_as_string", last_seen.get("value")),
        })
    return {"hosts": hosts}


def host_history(host, limit=50, client=None):
    client = _client(client)
    events = list_events(limit=limit, agent_id=host, client=client)
    incidents = list_incidents(limit=limit, host=host, client=client)
    return {
        "host": host,
        "event_total": events["total"],
        "events": events["events"],
        "incident_total": incidents["total"],
        "incidents": incidents["incidents"],
    }


@app.get("/api/overview")
def overview_endpoint():
    return overview()


@app.get("/api/events")
def events_endpoint(limit: int = 50, offset: int = Query(0, ge=0, lt=MAX_RESULT_WINDOW),
                    agent_id: Optional[str] = Query(None, max_length=256),
                    source_ip: Optional[str] = Query(None, max_length=64),
                    outcome: Optional[str] = Query(None, max_length=40),
                    username: Optional[str] = Query(None, max_length=256),
                    since: Optional[str] = Query(None, max_length=64), category: Optional[str] = Query(None, max_length=40)):
    return list_events(limit, offset, agent_id, source_ip, outcome, username, since, category=category)


@app.get("/api/incidents")
def incidents_endpoint(limit: int = 50, offset: int = Query(0, ge=0, lt=MAX_RESULT_WINDOW),
                       severity: Optional[str] = Query(None, max_length=32),
                       attack_type: Optional[str] = Query(None, max_length=128),
                       status: Optional[str] = Query(None, max_length=32),
                       source_ip: Optional[str] = Query(None, max_length=64),
                       host: Optional[str] = Query(None, max_length=256)):
    return list_incidents(
        limit, offset, severity, attack_type, status, source_ip, host,
    )


@app.get("/api/incidents/{incident_id}")
def incident_endpoint(incident_id: str):
    return get_incident(incident_id)


@app.get("/api/hosts")
def hosts_endpoint(limit: int = 50):
    return list_hosts(limit)


@app.get("/api/hosts/{host}")
def host_endpoint(host: str, limit: int = 50):
    return host_history(host, limit)


class InvestigationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(default="", max_length=400)


@app.post("/api/incidents/{incident_id}/investigate")
def investigate_endpoint(incident_id: str, request: InvestigationRequest):
    if not 1 <= len(incident_id) <= 160:
        raise HTTPException(status_code=422, detail="invalid incident reference")
    try:
        return investigate(InvestigationTools(es), incident_id, request.question.strip(), configured_provider())
    except NotFoundError as error:
        raise HTTPException(status_code=404, detail="incident not found") from error
