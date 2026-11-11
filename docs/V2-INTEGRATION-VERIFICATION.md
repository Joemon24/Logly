# v2 integration and reliability verification (Milestone C)

Script: `scripts/verify-v2-integration.sh`

This is an opt-in, reproducible end-to-end check of:

```
auth.log -> agent-v2 -> SQLite outbox -> Kafka -> Python consumer -> Elasticsearch
```

It uses the repository's local `docker-compose.yml` services and a separate v2
Kafka topic per scenario. It does not commit, deploy, delete indices/topics, or
touch v1 topics. The only infrastructure change is a temporary compose
override that publishes Elasticsearch on `127.0.0.1:9200`; the override lives
outside the repository and is removed afterwards.

## Prerequisites

- Docker with Compose, and the repository's images available.
- The local `kafka` and `elasticsearch` services can be started.
- A Python environment with `central-server-py/requirements.txt` installed.

```bash
python3 -m venv /tmp/logly-venv
/tmp/logly-venv/bin/pip install -r central-server-py/requirements.txt
```

## Run

```bash
PYTHON=/tmp/logly-venv/bin/python bash scripts/verify-v2-integration.sh
```

The script builds `agent-v2` into a temporary directory, then runs the
scenarios below. It stops/starts the local Kafka and Elasticsearch containers to
simulate outages, restores them on exit, and returns non-zero if any assertion
fails. Logs are kept in the temporary directory on failure.

## Scenarios

| # | Scenario | Assertion |
|---|---|---|
| 1 | Normal SSH failure over IPv4 and success over IPv6 | Both schema-v2 documents arrive; outcomes/IPs are right; Kafka key is `<agent_id>/auth-log`. |
| 2 | Duplicate Kafka delivery | Replaying the captured payload leaves the Elasticsearch document version and count unchanged; the consumer logs a duplicate skip. |
| 3 | Kafka outage and recovery | The event stays pending with zero acknowledgements; after Kafka returns it is delivered. |
| 4 | Consumer restart | Stop the consumer, append a record, restart the same group; the second event is processed without loss. |
| 5 | Elasticsearch outage and recovery | The consumer fails during the outage and Kafka has no committed offset. After a successful Elasticsearch query confirms zero records, restarting the consumer stores exactly one record and commits offset 1 with zero lag. |
| 6 | Outbox capacity reached | With capacity 1 and one record per poll, the first event stays durable under backpressure; both events arrive after recovery. |
| 7 | Rotation and truncation | Rotation drains the old file and continues in the new one; truncation starts a new source generation; all four events arrive. |
| 8 | Multiple source identities | Two sources produce two documents and two distinct Kafka keys. |
| 9 | Brute force and replay | Five failures produce one incident with five event IDs; replaying the topic keeps one incident, five evidence IDs, and five documents. |
| 10 | Password spraying | Five failures against five users produce one `ssh_password_spraying` incident with five usernames. |
| 11 | Suspicious success | Three failures followed by a success produce one `suspicious_login_success` incident with three failures and one success event ID. |
| 12 | Benign negative | Four failures, a clean success, and two failures followed by a success create no incident. |
| 13 | Read API | Overview, event search, incident search, incident detail/timeline, host list, and host history return the stored evidence. |
| 14 | Expanded telemetry and Investigator | Four fixture records commit with sanitized evidence and audit provenance. The category filter returns the expected executable metadata. Investigator reproduces the rule with retrieved event citations. |

## Incident checks

Scenarios 9–13 use RFC3339-prefixed log lines so incident windows are
deterministic. Each scenario uses a unique source IP derived from the run ID, so
incidents from previous runs cannot merge into the current assertions. Incident
lookups are filtered by `hosts.keyword`, which is unique per run.

The replay check in scenario 9 re-produces every captured Kafka message and
then waits for consumer-group lag to reach zero. It asserts that the incident
count, evidence count, and event document count are unchanged.

The API check starts `uvicorn api.main:app` against the same Elasticsearch,
waits for readiness, and queries the endpoints over HTTP. The server is stopped
afterwards.

## Source ordering decision

Kafka message keys are `<agent_id>/<source_id>`, not `event_id`. Every event
from one agent/source pair hashes to the same partition, so consecutive source
events keep their relative order. `event_id` stays inside the payload and is
the only deduplication identity. Hashing on `event_id` would scatter one
source's events across partitions and lose that order.

Global ordering across different sources is intentionally not guaranteed.

## What the script does not prove

- It runs on the development host. Linux inode-based rotation/truncation is
  covered by the collector's unit tests and by this script's mv/truncate
  behavior, but production `auth.log` semantics, systemd lifecycle, logrotate
  configuration, permissions, and SELinux/AppArmor still require a Linux host
  check.
- It does not benchmark throughput or prove capacity under sustained load.
- It does not test Kafka broker failover, replication, TLS/SASL, or multiple
  partitions' consumer rebalancing with more than one consumer process.
- It does not test a v1 consumer and v2 consumer sharing one topic; they use
  separate topics by design.
- At-least-once delivery is assumed and duplicates are handled by design, not
  prevented.
- Incident detection is exercised with deterministic threshold fixtures, not
  against real attack traffic. False-positive rates and threshold calibration
  are not measured.
- The read API is exercised over plain HTTP on localhost; authentication, TLS,
  pagination under load, and concurrent readers are not tested.
