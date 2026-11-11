# Logly v2.0

Logly collects Linux security telemetry and ties SSH detections to their supporting events. The React SOC dashboard supports event search, incident timelines, host history and an evidence-grounded Investigator. The standalone Simulation Lab uses four recorded synthetic fixtures.

| Component | Verification status |
| --- | --- |
| Go v2 file checkpoints, SQLite outbox, Kafka dispatch and replay | VERIFIED by automated and local real-stack tests. |
| SSH brute force, password spraying and suspicious successful login | VERIFIED by labeled fixtures and local real-stack tests. |
| Sudo, account changes, selected audit executions and watched-file access | VERIFIED through the local stack with fixture records. Live Ubuntu auditd and systemd remain unverified. |
| Incident Investigator and deterministic fallback | VERIFIED by fixture tests, live Elasticsearch queries and a local Ollama routing smoke check. |
| Simulation Lab | VERIFIED by recorded rule replay and standalone build. Public deployment is pending. |
| Isolation Forest | EXPERIMENTAL. The offline pipeline is available. Representative training and evaluation data have not been supplied. |
| Authenticated public SOC, automatic response, and comprehensive Linux audit coverage | NOT IMPLEMENTED. |

## Local development

Use Python 3.12 or 3.13, Go 1.23+, Node compatible with the pinned Vite version, and Docker Compose for Kafka and Elasticsearch. Use a dedicated local test environment. The default Compose detector consumes the v1 topic. The v2 integration script launches its own v2 consumer and agent.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r central-server-py/requirements.txt
PYTHON="$PWD/.venv/bin/python" bash scripts/verify-v2-integration.sh
```

The opt-in suite stops and restarts local Kafka and Elasticsearch as part of outage tests. It needs Docker access and leaves synthetic verification records in the configured local stack. See [stack verification](docs/V2-INTEGRATION-VERIFICATION.md) before running.

With Elasticsearch accessible on a trusted loopback endpoint, start the read API and dashboard in separate terminals.

```sh
cd central-server-py
ES_HOST=http://127.0.0.1:9200 ../.venv/bin/python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

```sh
cd frontend
npm ci
npm run dev
```

For a standalone walkthrough, run `npm run build:lab` in `frontend`, then serve only `dist-lab` with a static server and open `lab.html`. Its data is generated from checked-in synthetic fixtures. For local preview use `python3 -m http.server 4173 --bind 127.0.0.1 --directory dist-lab`.

## Engineering and release review

- [Completion guide and known limitations](docs/V2-COMPLETION.md)
- [Ubuntu v2 validation](docs/LINUX-DEPLOYMENT.md)
- [Go v2 runtime](docs/AGENT-V2-RUNTIME.md) and [schema](docs/AGENT-SCHEMA-V2.md)
- [Python delivery and deduplication](docs/PYTHON-CONSUMER-V2.md)
- [Incident rules and evidence](docs/INCIDENT-INTELLIGENCE.md)
- [Dashboard development](frontend/README.md)
