# IDS quick hardening report — 9 November 2026

Source: Joemon24/distributed-ids main at bfd4b1a6b6c0dd38207078ce9275931cf2c87918. Local release candidate, not deployed or published. The separate agent-rework branch was inspected but not merged; it has incompatible integrity/parsing behavior and random identity fallbacks.

## Good

Modular Go collection pipeline; Kafka transport; human-readable YAML rules; bounded time windows for feature calculations; dashboard export; pinned Python dependencies; existing separation of alert/incident functions. These are useful foundations, not evidence of validated security effectiveness.

## Changes delivered

- Environment-based Kafka and Elasticsearch connections; shared Elasticsearch client with certificate verification enabled and optional API key/CA configuration.
- Separate Kafka host/container listeners, service health dependencies, restart policies, persistent volumes, private database access, and loopback dashboard access.
- Non-root detector image with explicit source copies and no host virtual environment in the image.
- Kafka auto-commit disabled; exact consumed record offset acknowledged after its documents are stored. Small batches are flushed per Kafka record instead of waiting for 100 events.
- Deterministic event/alert/incident document IDs based on Kafka location to reduce duplicates when replaying the same record. This is not end-to-end exactly-once delivery; producer duplicates at different offsets remain distinct.
- Storage/processing errors stop consumption without acknowledging the failed record. A malformed raw Kafka JSON record still blocks progress and requires operator intervention; a dead-letter design remains outstanding.
- Alert cooldown no longer skips evidence storage; high-severity rules can trigger alerts; rule selection is deterministic by severity.
- Simulated blocking replaced by explicit recommendations. No traffic is hidden after a recommendation and no firewall action is claimed.
- OpenSSH IPv4/IPv6 authentication parsing, safer raw-data handling, unknown outcomes preserved, and heartbeat/non-auth envelopes kept out of authentication scoring.
- Go HTTP sender rejects non-success statuses. Print-only HTTP receiver returns 503 instead of pretending to accept events.
- Agent defaults to Kafka and configurable real Linux log paths; Kafka send has a timeout; feature counting aligns with parser outcomes.
- Linux service/config templates and automated check workflow included.

## Bad / still incomplete

The agent still drops a batch after exhausted retries; its disk buffer is not integrated. Log rotation, truncation, partial-line recovery, durable file positions, and graceful drain need implementation. Pipeline shared hash/trust state needs race testing and synchronization. The system uses processing time rather than a robust event-time/watermark design. Per-process correlation/trust state is not durable or correctly distributed. Some state maps are not bounded. Missing identities can still share an unknown bucket. YAML schema validation, general envelope validation, and safe dead-letter handling are incomplete. A persistent poison record can restart-loop the detector.

Lateral movement currently measures source-IP diversity rather than genuine movement among destination hosts. Notifications remain console-only. Model artifacts and reproducible training/evaluation are absent; the default is rules/heuristics, not a trained ML detector. The compound anomaly score is not a probability. There are no measured precision, recall, false-positive rate, load limits, or detection latency results.

## Worst / production blockers

1. Incomplete agent durability and collection continuity can lose real security evidence.
2. Internal services lack authenticated TLS; loopback/private binding reduces exposure but does not supply a complete trust model.
3. No tested recovery, retention, backups, operational alerting, or deployment rollback exercise.
4. No live service validation on the target Linux host; full dependency/build and vulnerability checks remain required.
5. Replayed events can change in-memory features and response timing; deterministic storage IDs alone do not make detection state transactional.

## Verification

Eleven Python regressions pass: OpenSSH parsing, unknown inputs, severity ordering, cooldown, agent isolation, recommendation behavior, explicit offset acknowledgement after storage, and no acknowledgement on storage failure. Delivery tests use fake transports and installed local ML dependencies, not a pinned clean environment. Python syntax compilation, Compose configuration validation, and whitespace checks pass. Targeted Go HTTP-status and feature-counting tests pass (two tests, with six HTTP status cases). Docker daemon is unavailable, so image build, full-stack execution, dashboards, and Linux service behavior are unverified. Full Go suite previously could not download dependencies under the available network restrictions. No claim of a successful live deployment is made.

## Next milestone

Implement durable agent outbox + rotation/checkpoints with failure tests first. Then finish validation, authenticate service boundaries, add real notifications/health metrics/retention, and run the staging acceptance tests in LINUX-DEPLOYMENT.md. Only after those gates should this be called production-ready.
