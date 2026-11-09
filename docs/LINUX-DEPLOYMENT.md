# Linux single-server release candidate

This build collects actual host logs. It is detection-only, with no firewall enforcement. It is not yet a production-certified release. Use a controlled staging host first.

## Scope and prerequisites

Target: a Linux server with Docker Engine/Compose, Go 1.23+, and file-based authentication logs readable by the agent. The supplied service template assumes a Debian/Ubuntu-style `adm` group. Journald-only hosts require a collector/export path; do not invent empty auth logs and assume coverage. Server capacity and operating system have not yet been supplied or validated.

This Compose stack is restricted to one trusted server: Kafka and Kibana bind to loopback; Elasticsearch has no published host port. Internal broker/database traffic is unauthenticated. Do not expose these services to other hosts or the Internet. Multi-host production needs authenticated TLS connections and a reviewed access policy first. No paid API or model subscription is required; server resources and operation remain your responsibility.

## Stage the services

Place this source tree at `/opt/distributed-ids`, inspect the configuration, then run from that directory:

```sh
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 detection-engine
```

The engine waits for Kafka and Elasticsearch health checks. It restarts on failure. Data uses named volumes. Never run `docker compose down -v` unless intentionally deleting the stored data. Old image versions have been retained; vulnerability scanning and supported-version upgrades are release gates, not completed work.

## Build and configure the actual agent

```sh
cd /opt/distributed-ids/agent
go test -race ./...
go build -o ../bin/ids-agent ./cmd/agent
```

Create a dedicated `ids` service account, `/var/lib/ids-agent` owned by that account, and `/etc/ids/agent.env` from `deploy/agent.env.example`. Set a unique agent ID and existing log paths. Verify the service account can read each intended log and write its state directory. On distributions without `adm`, adjust the group in the service template appropriately.

Install `deploy/ids-agent.service` under `/etc/systemd/system/`, then:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now ids-agent
sudo journalctl -u ids-agent -n 100 --no-pager
```

A missing log currently disables that monitor until restart. Rotation handling and persistent read positions remain outstanding: do not interpret a running process as proof of continuous collection.

## Access and verify

From your workstation, forward Kibana through your normal SSH connection:

```sh
ssh -L 5601:127.0.0.1:5601 YOUR_SERVER
```

Open `http://localhost:5601`, then import `dashboards/export.ndjson` through Saved Objects. Inspect data view compatibility; dashboard import has not been validated against a live stack here.

Observe your own legitimate SSH login on the server. Confirm the correct username/address/outcome appears in `ids-events`. Verify that host heartbeats are retained without authentication scoring. Detection alerts appear in `ids-alerts` and incidents in `ids-incidents`; external notifications are not implemented.

## Release gates

Before protection use: complete durable agent queue/replay, log rotation/checkpoint recovery, authenticated TLS for required trust boundaries, ingestion validation/quarantine, state recovery, retention/backups, operational alerting, dependency scans, full build and race checks, live outage/restart tests, and measured detection accuracy. The Python regression suite uses fake transports for delivery checks; it does not establish live Kafka/Elasticsearch compatibility or throughput.

## Rollback

Stop the agent, stop the detector, restore the previous reviewed application/image version, and preserve named data volumes. Back up data before upgrades; application rollback does not automatically reverse Elasticsearch migrations. Record the consumer group and offsets before a staged upgrade. Do not reset offsets or change groups casually: those operations change what is replayed.
