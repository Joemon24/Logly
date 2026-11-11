# Logly v2 completion guide

## Architecture

Linux auth log and optional audit log → Go collectors → SQLite outbox → Kafka → Python consumer → Elasticsearch → private FastAPI → React. Each collector uses its own durable source generation and physical record offset. The consumer commits offsets after required storage and incident work completes. Browser clients access the FastAPI service through a same-origin proxy. The standalone Lab serves synthetic recordings as static assets.

## Linux sources

| Source | Accepted coverage | Data retained |
| --- | --- | --- |
| Auth log | Existing SSH records, supported sudo command authorization, common useradd/usermod/userdel messages | SSH evidence as before. For new categories, sanitized message type, outcome and relevant actor or target. |
| Optional Linux audit log | SYSCALL records tagged `logly-exec`, `logly-passwd` or `logly-group` | Audit key, outcome, parsed timestamp, selected executable path or the configured watch path. No EXECVE arguments or PATH records. |

Use `LOGLY_V2_AUDIT_PATH` to enable the audit reader, which reserves the stable source ID `linux-audit`. Missing/unreadable sources are reported on state transitions. A readable audit file does not prove the kernel is emitting the selected rules. File watches are indicators of access under an operator-provisioned rule. They do not compare file contents or establish that a modification occurred. The auth log parser supports selected file formats and does not consume journald directly. A sudo authorization record does not establish command completion. Non-authentication categories are indexed and filterable; the v2 incident rules remain SSH-specific.

For a dedicated local v2 topic and consumer group, configure the Go agent with the values in `deploy/logly-agent-v2.env.example` and start `./bin/logly-agent-v2`. With Kafka listening on loopback and Elasticsearch ready, start the v2 consumer from `central-server-py`.

```sh
KAFKA_BROKERS=127.0.0.1:19092 KAFKA_TOPIC=ids.events.v2.stage \
KAFKA_GROUP_ID=logly-v2-stage ES_HOST=http://127.0.0.1:9200 \
../.venv/bin/python -m api.kafka_consumer
```

## Investigator and API

The existing GET routes are `/api/overview`, `/api/events`, `/api/incidents`, `/api/incidents/{id}`, `/api/hosts` and `/api/hosts/{host}`. Event search accepts a `category` filter. Search windows are capped at 10,000 records. Incident details display up to 500 referenced events and report how many references were omitted.

`POST /api/incidents/{id}/investigate` accepts a JSON object with optional `question` (maximum 400 characters). Use the Incident Details panel to run an initial investigation or ask about source IPs, affected hosts, related incidents, evidence gaps or next steps. Follow-ups are stateless and scoped to the selected incident.

The server owns four bounded read operations: get the incident, fetch up to 50 valid referenced event IDs, retrieve aggregate history for up to three hosts, and list up to five related incidents for the stored source IP. It reuses the production rule predicates and emits only citations present in retrieved records. Incident merging, unavailable references and caps can prevent independent reproduction. Storage failures surface as errors instead of being reported as empty evidence. Raw evidence needed by the existing predicates stays in the API process.

Set `LOGLY_OLLAMA_MODEL` when running FastAPI locally to opt into topic classification with a small installed Ollama model. The provider sends the analyst question to loopback Ollama. It gives Ollama no log text, tools or storage access. A two-request concurrency limit, short socket timeout, output cap and topic allowlist constrain the request. Python renders facts and citations. Unavailable providers or invalid output fall back to deterministic topic selection. Load the model before the walkthrough if cold startup exceeds the request timeout. Do not include credentials in analyst questions. Free-form model-written narratives are outside this implementation.

## Experimental anomaly detection

From `central-server-py`, run `python -m experiments.evaluate`. The default report evaluates the production SSH predicates on four existing synthetic fixtures. The exact-rule check returned 4/4 (3 true positives, 1 true negative, 0 false positives, 0 false negatives). These small, rule-authored fixtures measure regression behavior only.

The feature definition `ssh-window-v1` uses five counts for the final SSH source in each case: five-minute failures, distinct failed usernames, distinct failed hosts, ten-minute successes and ten-minute attempts. Run `python -m experiments.evaluate --train train.json --holdout heldout.json --artifact candidate.joblib` with separate JSON arrays of objects containing `id`, boolean `label` and timestamped `events` with unique IDs, one known source IP per case and a window of at most ten minutes. Training uses only normal windows, requires 20 such windows and at least five feature vectors, and rejects overlapping case IDs, event IDs and identical windows. This is a minimum for exercising the pipeline, not a sample-size recommendation. Split real data by time and collection group before export, de-identify it, and document the label source. Holdout must contain both classes. The report records split hashes, feature order, scikit-learn version and both classifiers' precision, recall and false-positive rate.

The experimental model has no production loader or effect on incident decisions. There is no representative labeled dataset in this repository. The existing legacy pickle loader is opt-in via `LOGLY_ENABLE_LEGACY_ML=1`; use artifacts you control, since pickle can execute code.

## Portfolio walkthrough

1. Open `lab.html` or the dashboard’s Simulation Lab. Select SSH password spraying and advance through the recorded events. Inspect cited IDs and then switch to the benign case to see the negative result.
2. In a private running stack, open Incidents, select a known test incident, inspect the timeline and use Investigator to ask about hosts, gaps or related incidents.
3. Show Events category filters and Host history. Explain the source coverage and whether displayed records came from the verification suite.
4. Discuss durable IDs, offset commits, retries and how missing evidence affects conclusions.

Regenerate the lab with `python -m experiments.evaluate --record-lab ../frontend/src/lab-recordings.json` from `central-server-py`. Publish only the contents of `frontend/dist-lab` to a static host after reviewing the recordings and granting deployment approval. The static build does not require Kafka, Elasticsearch, FastAPI or Ollama. Sudo, account, process and integrity records do not have dedicated detection predicates and are therefore absent from the scenario picker.

## Reliability and security review

Run Python unit tests, Go race tests, Go vet, frontend lint/typecheck/tests and both builds, then the existing opt-in local integration suite. The collector tests exercise checkpoint persistence, duplicate replay, capacity and rotation. The runtime tests exercise outage retry and graceful shutdown. Offline rotation may report lost coverage if a prior generation is unavailable, and copytruncate can lose writes between copy and truncate. Late events may attach to existing incident buckets or be missed when the qualifying anchor is never replayed. Existing per-query evidence caps and legacy in-memory correlation are additional limits.

FastAPI is an unauthenticated local development service. For shared use, add authentication, per-user authorization, request limits and audited access at a reviewed trust boundary. Kafka uses plaintext in development and Elasticsearch security is disabled in Compose. Keep them private and use TLS and credentials before crossing hosts. Scope the API's Elasticsearch key to the read operations it requires, and use separate credentials for ingestion. The public artifact is the synthetic static Lab only.

Set an approved retention interval for events, rejection records and incidents before deployment. Restrict rejected/raw evidence to authorized operators, preserve incident references only as long as policy allows, and audit evidence expiration separately from routine queries. Expiring event documents can leave missing citations in surviving incidents. Rejections and legacy logs may contain source input. There is no automated retention or deletion job in this branch.

## Completion verification

| Check | Observed result |
| --- | --- |
| Docker-backed integration on macOS | 53 passed, 0 failed across 14 scenarios. Elasticsearch outage withheld the Kafka offset. Recovery stored exactly one event and committed offset 1 with zero lag. |
| Python with pinned requirements | 59 passed. Investigator tests cover positive rule fixtures, unavailable references, injection text, provider fallback and failed retrieval. |
| Go race tests and vet | 106 top-level tests passed across 8 test packages. Go vet and a Linux ARM64 cross-build passed. |
| Frontend | 10 tests passed. Clean npm install, lint, TypeScript, both production builds and online npm audit passed. The build reports React Router module directive warnings. |
| Browser | Simulation Lab reviewed at 390, 768 and 1280 pixels. Live Investigator rendered citations, and a mobile citation link reached its evidence row. |
| Local Ollama | With Ollama 0.40.6 and qwen3.5:2b, four of five hand-written follow-ups routed through Ollama correctly. The fifth used the correct deterministic route. A live API follow-up also returned Ollama mode with nine stored citations. This measures a small routing smoke check. |
| Offline ML pipeline | Synthetic smoke data exercised fitting, manifest generation, artifact output and rejection of overlapping data. Representative accuracy remains unmeasured. |

The real-stack suite used the existing local Docker services and an available development Python environment. The pinned Python environment was used separately for the full unit suite. Ubuntu auditd, systemd, real host file watches, and multi-host security configuration remain unverified.
