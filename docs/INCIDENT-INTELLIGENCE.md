# Incident intelligence and read API (Milestone D)

Status: **backend only, evidence-based, no ML or confidence scores.** The
existing v1 consumer path, v1 `ids-incidents` index, Go agent, and SOAR modules
are unchanged. v1 rule/SOAR alerts and incidents still run for v2 events; the
v2 incidents described here are additive and stored separately in
`ids-incidents-v2`.

Files:

- `central-server-py/api/incident_intelligence.py` — window detection,
  deterministic incident IDs, evidence merging, incident upsert.
- `central-server-py/api/main.py` — read-only FastAPI endpoints.
- `central-server-py/tests/test_incident_intelligence.py` — detection, benign,
  and replay tests.
- `central-server-py/tests/fixtures/incidents/` — deterministic scenario
  fixtures.
- `central-server-py/tests/test_api.py` — handler tests with canned
  Elasticsearch responses.

## Detection rules

Only schema-v2 authentication events with a known source IP and outcome are
evaluated. Unknown/absent IPs and non-authentication categories are skipped.
Windows end at the event's log `timestamp` and are queried from Elasticsearch;
the consumer keeps no detection state.

| Attack type | Evidence rule | Window | MITRE |
|---|---|---|---|
| `ssh_brute_force` | ≥ 5 failures from one source IP and fewer than 5 distinct usernames | 5 min | T1110.001 |
| `ssh_password_spraying` | ≥ 5 failures from one source IP against ≥ 5 distinct usernames | 5 min | T1110.003 |
| `suspicious_login_success` | a success for the same source IP and username after ≥ 3 failures | prior 10 min | T1078 |

Spraying takes precedence over brute force for the same evidence batch, so one
batch produces at most one incident. A success after failures is a separate
incident type, not a modification of the brute-force incident.

These are threshold observations from the stored evidence, not proof of
compromise. No attack classification is invented when the evidence is missing.

## Evidence and correlation

Every incident stores the supporting event IDs:

- `failure_event_ids` — failures in the detection window.
- `success_event_ids` — the success for a suspicious-login incident.
- `event_ids` — the sorted union, used for the timeline.

Hosts are `agent.agent_id` values from all supporting events. Because the
window is keyed by source IP rather than by agent, failures from the same IP
against several hosts land in the same incident (`hosts` has one entry per
host). Raw events are not copied into incidents; the detail endpoint resolves
the event IDs from `ids-events`.

## Deterministic IDs and replay

The incident ID is:

```
inc_ + sha256("<attack_type>|<source_ip>|<bucket_start_epoch>")[:32]
```

`bucket_start_epoch` is the event time floored to the detection window (300 s,
or 600 s for suspicious success). The same evidence at the same time produces
the same incident ID, so a Kafka replay updates one document instead of
creating another.

Evidence arrays are unioned and timestamp bounds are expanded on update.
Replaying an old event therefore cannot shrink an incident or remove evidence.
Updates use Elasticsearch `if_seq_no`/`if_primary_term` with a small retry loop
so a concurrent writer is re-read and merged rather than silently overwritten.

## Severity and risk

Risk is a deterministic linear function of the evidence count, capped at 100:

| Attack type | Score |
|---|---|
| brute force | `40 + 6 * max(0, failures - 5)` |
| spraying | `50 + 8 * max(0, distinct_users - 5)` |
| suspicious success | `60 + 8 * max(0, failures - 3)` |

Severity: `critical` ≥ 80, `high` ≥ 60, `medium` ≥ 40, otherwise `low`.
There is no confidence value, probability, or accuracy claim.

## Incident document

Stored in `ids-incidents-v2`:

```json
{
  "incident_id": "inc_05e8c6e202cfa68ade28196663105251",
  "attack_type": "ssh_brute_force",
  "status": "open",
  "source_ip": "198.18.104.9",
  "username": "alice",
  "usernames": ["alice"],
  "hosts": ["linux-verify-brute"],
  "severity": "medium",
  "risk_score": 40,
  "mitre": "T1110.001",
  "failure_event_ids": ["evt2_..."],
  "success_event_ids": [],
  "event_ids": ["evt2_..."],
  "first_seen": "2026-11-09T10:00:00Z",
  "last_seen": "2026-11-09T10:02:00Z",
  "window_start": "2026-11-09T09:57:00Z",
  "window_end": "2026-11-09T10:02:00Z",
  "evidence": {
    "failure_count": 5,
    "success_count": 0,
    "distinct_usernames": 1,
    "host_count": 1
  },
  "detected_at": "2026-11-11T04:00:00Z",
  "updated_at": "2026-11-11T04:00:00Z"
}
```

## API

Run:

```bash
cd central-server-py
ES_HOST=http://localhost:9200 python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

All endpoints are read-only and synchronous.

### Overview

```bash
curl -sS http://127.0.0.1:8000/api/overview
```

```json
{
  "events": 67,
  "incidents": 3,
  "open_incidents": 3,
  "hosts": 28,
  "by_severity": {"medium": 2, "high": 1},
  "by_attack_type": {"ssh_brute_force": 1, "ssh_password_spraying": 1, "suspicious_login_success": 1}
}
```

### Events

```bash
curl -sS 'http://127.0.0.1:8000/api/events?agent_id=web-01&outcome=failure&limit=20'
```

Filters: `agent_id`, `source_ip`, `outcome`, `username`, `since`, `limit`
(1–500), `offset`. Events are sorted newest first and exclude internal
`features` and `raw_event` payloads.

### Incidents

```bash
curl -sS 'http://127.0.0.1:8000/api/incidents?severity=high&host=web-01'
```

Filters: `severity`, `attack_type`, `status`, `source_ip`, `host`, `limit`,
`offset`. Sorted by `last_seen` descending.

### Incident detail and timeline

```bash
curl -sS http://127.0.0.1:8000/api/incidents/inc_05e8c6e202cfa68ade28196663105251
```

The response adds:

- `timeline` — supporting events sorted by timestamp, resolved from
  `ids-events`.
- `evidence_event_count` — number of incident event IDs found.
- `missing_event_ids` — IDs that could not be resolved. This is empty for a
  healthy pipeline; a non-empty list means evidence is missing and the
  incident should be treated as incomplete.

### Hosts

```bash
curl -sS 'http://127.0.0.1:8000/api/hosts?limit=50'
curl -sS http://127.0.0.1:8000/api/hosts/web-01
```

The list uses a `terms` aggregation over `agent.agent_id.keyword` with each
host's event count and last-seen timestamp. Host history combines recent events
and incidents whose `hosts` array contains that host.

## Tests

```bash
cd central-server-py
python -m unittest discover -s tests -v
```

The incident fixtures cover:

- brute force across two hosts,
- spraying against five users,
- suspicious success after three failures,
- benign negatives: four failures, a clean success, and two failures followed
  by a success.

Replay is tested by evaluating every fixture three times; incidents stay at one
document and evidence counts do not change.

## Limitations

- Detection is threshold logic over stored events. It is not an attack
  classifier and makes no accuracy claim.
- Window evaluation issues Elasticsearch queries per authentication event. It
  is stateless and simple, but not high-throughput; batching or caching is a
  later optimization.
- Field queries assume the dynamic `.keyword` subfields produced by the default
  mapping (`src_ip.keyword`, `outcome.keyword`, etc.).
- Incidents are keyed by source IP and time bucket. Different attack campaigns
  from one IP inside one bucket merge into one incident, by design.
- Detection is evaluated when an authentication event is processed, anchored at
  that event's log timestamp. Per-source ordering is guaranteed by the Kafka
  key, but the same source IP reported by different agents can arrive out of
  order across partitions. A severely late event can therefore miss a window
  that a later event already closed. Cross-host correlation is reliable for
  events that arrive in timestamp order; global ordering is not guaranteed.
- Only schema-v2 authentication events create v2 incidents. v1 events keep the
  existing consumer path and v1 `ids-incidents` behavior.
- API authentication, TLS termination, and rate limiting are deployment
  concerns and are not implemented here.
- The API is read-only; incident status changes are not part of this milestone.
- Not implemented: React, LLM, ML, or additional telemetry.
