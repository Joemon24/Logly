#!/usr/bin/env bash
# Opt-in end-to-end verification for the Logly v2 path:
#   auth.log -> Go agent -> SQLite outbox -> Kafka -> Python consumer -> Elasticsearch
#
# It needs Docker with the repository's docker-compose services and a Python
# environment with central-server-py/requirements.txt installed.
#
#   PYTHON=/path/to/venv/bin/python scripts/verify-v2-integration.sh
#
# Nothing is committed or deployed. The script creates uniquely named Kafka
# topics and event IDs, and never deletes existing indices or Kafka data. It
# stops and starts the local kafka/elasticsearch containers to simulate
# outages. Override the Python interpreter with PYTHON=... when needed.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
TS="$(date +%s)"
RUN_DIR="${TMPDIR:-/tmp}/logly-v2-verify-$TS"
ES="${ES_URL:-http://localhost:9200}"
OVERRIDE="$RUN_DIR/compose-override.yml"
AGENT_BIN="$RUN_DIR/agent-v2"

PASS=0
FAIL=0
PIDS=()
SERVICES_STARTED=0

pass() { PASS=$((PASS + 1)); printf 'PASS: %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); printf 'FAIL: %s\n' "$1"; }
check() { if eval "$2"; then pass "$1"; else fail "$1"; fi; }

cleanup() {
  local status=$?
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
  done
  if [ "$SERVICES_STARTED" = 1 ]; then
    compose start kafka elasticsearch >/dev/null 2>&1
  fi
  if [ "$status" = 0 ]; then rm -rf "$RUN_DIR"; else echo "Logs kept in $RUN_DIR"; fi
}
trap cleanup EXIT

mkdir -p "$RUN_DIR"
cat > "$OVERRIDE" <<'YAML'
services:
  elasticsearch:
    ports:
      - "127.0.0.1:9200:9200"
YAML

compose() { docker compose -f "$ROOT/docker-compose.yml" -f "$OVERRIDE" "$@"; }
kafka_exec() { compose exec -T kafka "$@"; }

if ! docker info >/dev/null; then
  echo "FAIL: Docker daemon is unavailable" >&2
  exit 1
fi

if ! "$PYTHON" -c 'import kafka, elasticsearch, sklearn' >/dev/null 2>&1; then
  echo "SKIP: $PYTHON lacks central-server-py dependencies (see README)." >&2
  exit 3
fi

echo "== starting services =="
compose up -d kafka elasticsearch || { echo "FAIL: docker compose up"; exit 1; }
SERVICES_STARTED=1
for _ in $(seq 1 60); do
  curl -fsS "$ES" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$ES" >/dev/null || { echo "FAIL: Elasticsearch never became reachable at $ES"; exit 1; }

echo "== building agent-v2 =="
(cd "$ROOT/agent" && go build -o "$AGENT_BIN" ./cmd/agent-v2) || { echo "FAIL: go build"; exit 1; }

new_topic() {
  local topic="ids.events.v2.verify.$TS.$1"
  kafka_exec kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null || return 1
  printf '%s' "$topic"
}

es_count() { # index and curl arguments
  local index="$1"
  shift
  curl -fsS "$ES/$index/_count" -H 'Content-Type: application/json' "$@" |
    jq -er 'select(._shards.failed == 0) | .count | numbers | select(. >= 0 and . == floor)'
}

wait_docs() { # agent_id count timeout_seconds
  local agent="$1" want="$2" timeout="${3:-30}" body count
  body=$(printf '{"query":{"term":{"agent.agent_id.keyword":"%s"}}}' "$agent")
  for _ in $(seq 1 "$((timeout * 5))"); do
    count=$(es_count ids-events -d "$body") || count=
    [ "$count" = "$want" ] && return 0
    sleep 0.2
  done
  return 1
}

wait_lag_zero() { # group topic expected_committed_offsets timeout
  local group="$1" topic="$2" expected="$3" timeout="${4:-30}"
  for _ in $(seq 1 "$((timeout * 2))"); do
    if kafka_exec kafka-consumer-groups --bootstrap-server kafka:9092 \
      --describe --group "$group" 2>/dev/null |
      awk -v t="$topic" -v expected="$expected" '
        $2 == t && $4 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {
          committed += $4
          lag += $6
        }
        END { exit !(committed == expected && lag == 0) }
      '; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

count_docs() { # agent_id [curl arguments]
  local agent="$1"
  shift
  es_count ids-events "$@" -d "{\"query\":{\"term\":{\"agent.agent_id.keyword\":\"$agent\"}}}"
}

wait_for_event_count() { # agent_id; return the first successful count
  local agent="$1" count
  for _ in $(seq 1 60); do
    if count=$(count_docs "$agent" --max-time 3 2>/dev/null); then
      printf '%s\n' "$count"
      return 0
    fi
    sleep 1
  done
  return 1
}

assert_no_committed_offsets() { # group topic
  "$PYTHON" - "$1" "$2" <<'PYTHON_OFFSETS'
import sys

from kafka.admin import KafkaAdminClient
from kafka.structs import TopicPartition

group, topic = sys.argv[1:]
admin = KafkaAdminClient(bootstrap_servers="localhost:19092", request_timeout_ms=10000)
try:
    metadata, = admin.describe_topics([topic])
    partitions = {TopicPartition(topic, item["partition"]) for item in metadata["partitions"]}
    if metadata["error_code"] or not partitions:
        raise SystemExit(f"Could not verify partitions for {topic}")
    offsets = admin.list_consumer_group_offsets(group, partitions=sorted(partitions))
    if offsets.keys() != partitions or any(value.offset != -1 for value in offsets.values()):
        raise SystemExit(f"Expected no committed offsets for {group}: {offsets}")
finally:
    admin.close()
PYTHON_OFFSETS
}

wait_incidents() { # host want timeout
  local host="$1" want="$2" timeout="${3:-30}" body count
  body=$(printf '{"query":{"term":{"hosts.keyword":"%s"}}}' "$host")
  for _ in $(seq 1 "$((timeout * 5))"); do
    count=$(es_count ids-incidents-v2 -d "$body") || count=
    [ "$count" = "$want" ] && return 0
    sleep 0.2
  done
  return 1
}

incident_source() { # host
  curl -fsS "$ES/ids-incidents-v2/_search" -H 'Content-Type: application/json' \
    -d "{\"query\":{\"term\":{\"hosts.keyword\":\"$1\"}},\"size\":1}" |
    jq -er 'select(.timed_out == false and ._shards.failed == 0) | .hits.hits[0]._source | objects'
}

start_api() { # port (sets APIPID)
  ( cd "$ROOT/central-server-py" && \
    ES_HOST="$ES" exec "$PYTHON" -m uvicorn api.main:app \
      --host 127.0.0.1 --port "$1" --log-level warning ) \
    >"$RUN_DIR/api.log" 2>&1 &
  APIPID=$!
  PIDS+=("$APIPID")
}


start_consumer() { # topic group logfile
  ( cd "$ROOT/central-server-py" && \
    PYTHONUNBUFFERED=1 KAFKA_BROKERS=localhost:19092 KAFKA_TOPIC="$1" \
    KAFKA_GROUP_ID="$2" ES_HOST="$ES" exec "$PYTHON" -m api.kafka_consumer ) \
    >"$3" 2>&1 &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
}

stop_pid() {
  local pid="$1" i
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  for i in "${!PIDS[@]}"; do
    if [ "${PIDS[$i]}" = "$pid" ]; then
      unset 'PIDS[i]'
      break
    fi
  done
}

start_agent() { # topic log db source agent_id [extra env...]
  local topic="$1" log="$2" db="$3" source="$4" agent="$5"
  shift 5
  (
    exec env LOGLY_V2_LOG_PATH="$log" \
      LOGLY_V2_DB_PATH="$db" \
      LOGLY_V2_SOURCE_ID="$source" \
      LOGLY_V2_AGENT_ID="$agent" \
      LOGLY_V2_AGENT_TYPE=linux \
      LOGLY_V2_KAFKA_BROKERS=localhost:19092 \
      LOGLY_V2_KAFKA_TOPIC="$topic" \
      LOGLY_V2_POLL_INTERVAL=200ms \
      LOGLY_V2_STATS_INTERVAL=500ms \
      "$@" \
      "$AGENT_BIN"
  ) >>"$db.log" 2>&1 &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
}

# ---------------------------------------------------------------- scenario 1
echo "== 1. normal SSH failure (IPv4) and success (IPv6) =="
T1=$(new_topic one) || { echo "FAIL: create topic"; exit 1; }
A1="linux-verify-$TS-one"
G1="logly-verify-$TS-one"
L1="$RUN_DIR/auth-one.log"
cat > "$L1" <<'LOG'
Nov 10 10:00:01 web-01 sshd[100]: Failed password for alice from 203.0.113.9 port 50000 ssh2
Nov 10 10:00:02 web-01 sshd[101]: Accepted publickey for bob from 2001:db8::5 port 50001 ssh2
LOG
start_consumer "$T1" "$G1" "$RUN_DIR/consumer-one.log"
C1=$LAST_PID
start_agent "$T1" "$L1" "$RUN_DIR/one.db" auth-log "$A1"
AG1=$LAST_PID
if wait_docs "$A1" 2 30; then
  pass "two schema-v2 documents reached Elasticsearch"
else
  fail "two schema-v2 documents reached Elasticsearch"
fi
FAILURE_COUNT=$(es_count ids-events -d "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"agent.agent_id.keyword\":\"$A1\"}},{\"term\":{\"event.outcome.keyword\":\"failure\"}}]}}}")
IPV6_COUNT=$(es_count ids-events -d "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"agent.agent_id.keyword\":\"$A1\"}},{\"term\":{\"source.ip.keyword\":\"2001:db8::5\"}}]}}}")
check "IPv4 failure stored" "[ \"$FAILURE_COUNT\" = 1 ]"
check "IPv6 success stored" "[ \"$IPV6_COUNT\" = 1 ]"
KEYS=$(kafka_exec kafka-console-consumer --bootstrap-server kafka:9092 --topic "$T1" \
  --from-beginning --max-messages 2 --timeout-ms 10000 --property print.key=true \
  --property key.separator='|' 2>/dev/null | cut -d'|' -f1 | sort -u)
check "Kafka key is the stable agent/source identity" "[ \"$KEYS\" = \"$A1/auth-log\" ]"
stop_pid "$C1"; stop_pid "$AG1"

# ---------------------------------------------------------------- scenario 2
echo "== 2. duplicate Kafka delivery is deduplicated =="
T2=$(new_topic dup) || exit 1
A2="linux-verify-$TS-dup"; G2="logly-verify-$TS-dup"
L2="$RUN_DIR/auth-dup.log"
echo 'Nov 10 10:01:01 web-01 sshd[110]: Failed password for mallory from 198.51.100.7 port 51000 ssh2' > "$L2"
start_consumer "$T2" "$G2" "$RUN_DIR/consumer-dup.log"
C2=$LAST_PID
start_agent "$T2" "$L2" "$RUN_DIR/dup.db" auth-log "$A2"
AG2=$LAST_PID
wait_docs "$A2" 1 30; sleep 1
kafka_exec kafka-console-consumer --bootstrap-server kafka:9092 --topic "$T2" \
  --from-beginning --max-messages 1 --timeout-ms 10000 > "$RUN_DIR/one.json" 2>/dev/null
EVENT_ID=$(jq -r .event_id "$RUN_DIR/one.json")
VERSION_BEFORE=$(curl -fsS "$ES/ids-events/_doc/$EVENT_ID" | jq -er 'select(.found == true) | ._version | numbers | select(. > 0)')
kafka_exec kafka-console-producer --bootstrap-server kafka:9092 --topic "$T2" < "$RUN_DIR/one.json"
wait_lag_zero "$G2" "$T2" 2 30 || fail "duplicate delivery was not committed"
sleep 1
VERSION_AFTER=$(curl -fsS "$ES/ids-events/_doc/$EVENT_ID" | jq -er 'select(.found == true) | ._version | numbers | select(. > 0)')
COUNT2=$(es_count ids-events -d "{\"query\":{\"term\":{\"agent.agent_id.keyword\":\"$A2\"}}}")
check "replay does not overwrite the document (version $VERSION_BEFORE -> $VERSION_AFTER)" \
  "[ -n \"$VERSION_BEFORE\" ] && [ \"$VERSION_BEFORE\" = \"$VERSION_AFTER\" ]"
check "replay does not add a document (count $COUNT2)" "[ \"$COUNT2\" = 1 ]"
check "duplicate was skipped" "grep -q 'Duplicate event skipped' '$RUN_DIR/consumer-dup.log'"
stop_pid "$C2"; stop_pid "$AG2"

# ---------------------------------------------------------------- scenario 3
echo "== 3. Kafka outage, pending outbox, recovery =="
T3=$(new_topic outage) || exit 1
A3="linux-verify-$TS-outage"; G3="logly-verify-$TS-outage"
L3="$RUN_DIR/auth-outage.log"
echo 'Nov 10 10:02:01 web-01 sshd[120]: Accepted password for carol from 192.0.2.44 port 52000 ssh2' > "$L3"
compose stop kafka >/dev/null 2>&1
start_agent "$T3" "$L3" "$RUN_DIR/outage.db" auth-log "$A3"
AG3=$LAST_PID
PENDING=0
for _ in $(seq 1 40); do
  if grep -q '"pending_events":1' "$RUN_DIR/outage.db.log" 2>/dev/null; then PENDING=1; break; fi
  sleep 0.5
done
check "event retained in the outbox while Kafka is down" "[ \"$PENDING\" = 1 ]"
check "acknowledgement did not happen without a broker" \
  "[ \"\$(grep -o '\"acknowledged\":[0-9]*' '$RUN_DIR/outage.db.log' | tail -1 | cut -d: -f2)\" = 0 ]"
compose up -d kafka >/dev/null 2>&1
for _ in $(seq 1 60); do
  compose ps kafka 2>/dev/null | grep -q healthy && break
  sleep 1
done
start_consumer "$T3" "$G3" "$RUN_DIR/consumer-outage.log"
C3=$LAST_PID
if wait_docs "$A3" 1 45; then pass "pending event delivered after Kafka recovery"; else fail "pending event delivered after Kafka recovery"; fi
stop_pid "$C3"; stop_pid "$AG3"

# ---------------------------------------------------------------- scenario 4
echo "== 4. Python consumer restart =="
T4=$(new_topic restart) || exit 1
A4="linux-verify-$TS-restart"; G4="logly-verify-$TS-restart"
L4="$RUN_DIR/auth-restart.log"
echo 'Nov 10 10:03:01 web-01 sshd[130]: Failed password for dave from 203.0.113.20 port 53000 ssh2' > "$L4"
start_consumer "$T4" "$G4" "$RUN_DIR/consumer-restart.log"
C4=$LAST_PID
start_agent "$T4" "$L4" "$RUN_DIR/restart.db" auth-log "$A4"
AG4=$LAST_PID
wait_docs "$A4" 1 30
stop_pid "$C4"
echo 'Nov 10 10:03:02 web-01 sshd[131]: Accepted publickey for erin from 203.0.113.21 port 53001 ssh2' >> "$L4"
sleep 2
start_consumer "$T4" "$G4" "$RUN_DIR/consumer-restart-2.log"
C4B=$LAST_PID
if wait_docs "$A4" 2 30; then pass "consumer restart resumed without losing the second event"; else fail "consumer restart resumed without losing the second event"; fi
stop_pid "$C4B"; stop_pid "$AG4"

# ---------------------------------------------------------------- scenario 5
echo "== 5. Elasticsearch outage and recovery =="
T5=$(new_topic esdown) || exit 1
A5="linux-verify-$TS-esdown"; G5="logly-verify-$TS-esdown"
L5="$RUN_DIR/auth-esdown.log"
echo 'Nov 10 10:04:01 web-01 sshd[140]: Failed password for frank from 203.0.113.30 port 54000 ssh2' > "$L5"
start_consumer "$T5" "$G5" "$RUN_DIR/consumer-esdown.log"
C5=$LAST_PID
compose stop elasticsearch >/dev/null || { fail "could not stop Elasticsearch"; exit 1; }
start_agent "$T5" "$L5" "$RUN_DIR/esdown.db" auth-log "$A5"
AG5=$LAST_PID

# Keep Elasticsearch down until the consumer has actually failed the claim.
for _ in $(seq 1 45); do
  kill -0 "$C5" 2>/dev/null || break
  sleep 1
done
if kill -0 "$C5" 2>/dev/null; then
  fail "consumer did not exit during the Elasticsearch outage"
  exit 1
fi
if wait "$C5" || ! grep -q 'elastic_transport\.ConnectionError:' "$RUN_DIR/consumer-esdown.log" ||
  ! grep -q 'in claim_v2_event' "$RUN_DIR/consumer-esdown.log"; then
  fail "consumer did not fail at the Elasticsearch claim write"
  exit 1
fi
stop_pid "$C5"
pass "consumer failed at the Elasticsearch claim write during the outage"

if assert_no_committed_offsets "$G5" "$T5"; then
  pass "Kafka has no committed offset for the failed delivery"
else
  fail "could not verify that Kafka left the failed delivery uncommitted"
  exit 1
fi

compose up -d elasticsearch || { fail "could not restart Elasticsearch"; exit 1; }
# The root endpoint can respond before the ids-events shards can serve counts.
if ! COUNT5=$(wait_for_event_count "$A5"); then
  fail "Elasticsearch did not return a valid pre-restart event count"
  exit 1
fi
if [ "$COUNT5" != 0 ]; then
  fail "expected zero stored events before consumer restart, got $COUNT5"
  exit 1
fi
pass "Elasticsearch confirms zero stored events before consumer restart"

start_consumer "$T5" "$G5" "$RUN_DIR/consumer-esdown-2.log"
C5B=$LAST_PID
if wait_lag_zero "$G5" "$T5" 1 45; then
  pass "Kafka committed offset 1 with zero lag after recovery"
else
  fail "Kafka did not commit the recovered delivery at offset 1"
fi
if COUNT5=$(wait_for_event_count "$A5") && [ "$COUNT5" = 1 ]; then
  pass "Elasticsearch confirms exactly one stored event after consumer restart"
else
  fail "could not verify exactly one stored event after consumer restart"
fi
stop_pid "$C5B"; stop_pid "$AG5"

# ---------------------------------------------------------------- scenario 6
echo "== 6. outbox capacity backpressure =="
T6=$(new_topic capacity) || exit 1
A6="linux-verify-$TS-capacity"; G6="logly-verify-$TS-capacity"
L6="$RUN_DIR/auth-capacity.log"
printf '%s\n%s\n' \
  'Nov 10 10:05:01 web-01 sshd[150]: Failed password for gina from 203.0.113.40 port 55000 ssh2' \
  'Nov 10 10:05:02 web-01 sshd[151]: Failed password for gina from 203.0.113.40 port 55001 ssh2' > "$L6"
compose stop kafka >/dev/null 2>&1
start_agent "$T6" "$L6" "$RUN_DIR/capacity.db" auth-log "$A6" \
  LOGLY_V2_CAPACITY=1 LOGLY_V2_MAX_RECORDS_PER_POLL=1
AG6=$LAST_PID
PENDING=0
for _ in $(seq 1 40); do
  grep -q '"pending_events":1' "$RUN_DIR/capacity.db.log" 2>/dev/null && { PENDING=1; break; }
  sleep 0.5
done
check "capacity pressure left the first event durable" "[ \"$PENDING\" = 1 ]"
compose up -d kafka >/dev/null 2>&1
for _ in $(seq 1 60); do compose ps kafka 2>/dev/null | grep -q healthy && break; sleep 1; done
start_consumer "$T6" "$G6" "$RUN_DIR/consumer-capacity.log"
C6=$LAST_PID
if wait_docs "$A6" 2 45; then pass "both events delivered after backpressure cleared"; else fail "both events delivered after backpressure cleared"; fi
stop_pid "$C6"; stop_pid "$AG6"

# ---------------------------------------------------------------- scenario 7
echo "== 7. file rotation and truncation =="
T7=$(new_topic rotation) || exit 1
A7="linux-verify-$TS-rotation"; G7="logly-verify-$TS-rotation"
L7="$RUN_DIR/auth-rotation.log"
echo 'Nov 10 10:06:01 web-01 sshd[160]: Failed password for henry from 203.0.113.50 port 56000 ssh2' > "$L7"
start_consumer "$T7" "$G7" "$RUN_DIR/consumer-rotation.log"
C7=$LAST_PID
start_agent "$T7" "$L7" "$RUN_DIR/rotation.db" auth-log "$A7"
AG7=$LAST_PID
wait_docs "$A7" 1 30
mv "$L7" "$L7.1"
echo 'Nov 10 10:06:02 web-01 sshd[161]: Accepted password for iris from 203.0.113.51 port 56001 ssh2' > "$L7"
if wait_docs "$A7" 2 30; then pass "rotation drained and continued in the new file"; else fail "rotation drained and continued in the new file"; fi
echo 'Nov 10 10:06:03 web-01 sshd[162]: Failed password for jack from 203.0.113.52 port 56002 ssh2' >> "$L7"
wait_docs "$A7" 3 30
: > "$L7"
sleep 1
echo 'Nov 10 10:06:04 web-01 sshd[163]: Accepted password for kate from 203.0.113.53 port 56003 ssh2' > "$L7"
if wait_docs "$A7" 4 30; then pass "truncation started a new generation without loss"; else fail "truncation started a new generation without loss"; fi
stop_pid "$C7"; stop_pid "$AG7"

# ---------------------------------------------------------------- scenario 8
echo "== 8. multiple independent source identities =="
T8=$(new_topic multisource) || exit 1
A8="linux-verify-$TS-multi"; G8="logly-verify-$TS-multi"
L8A="$RUN_DIR/auth-a.log"; L8B="$RUN_DIR/auth-b.log"
echo 'Nov 10 10:07:01 web-01 sshd[170]: Failed password for leo from 203.0.113.60 port 57000 ssh2' > "$L8A"
echo 'Nov 10 10:07:02 web-01 sshd[171]: Failed password for leo from 203.0.113.61 port 57001 ssh2' > "$L8B"
start_consumer "$T8" "$G8" "$RUN_DIR/consumer-multi.log"
C8=$LAST_PID
start_agent "$T8" "$L8A" "$RUN_DIR/source-a.db" source-a "$A8"
AG8A=$LAST_PID
start_agent "$T8" "$L8B" "$RUN_DIR/source-b.db" source-b "$A8"
AG8B=$LAST_PID
if wait_docs "$A8" 2 30; then pass "both source identities reached Elasticsearch"; else fail "both source identities reached Elasticsearch"; fi
MULTI_KEYS=$(kafka_exec kafka-console-consumer --bootstrap-server kafka:9092 --topic "$T8" \
  --from-beginning --max-messages 2 --timeout-ms 10000 --property print.key=true \
  --property key.separator='|' 2>/dev/null | cut -d'|' -f1 | sort -u | tr '\n' ' ')
check "each source has its own Kafka key ($MULTI_KEYS)" \
  "[ \"$MULTI_KEYS\" = \"$A8/source-a $A8/source-b \" ]"
stop_pid "$C8"; stop_pid "$AG8A"; stop_pid "$AG8B"

# ---------------------------------------------------------------- scenario 9
echo "== 9. SSH brute force incident and Kafka replay =="
T9=$(new_topic brute) || exit 1
A9="linux-verify-$TS-brute"; G9="logly-verify-$TS-brute"
OCTET=$(( (TS + $$) % 200 + 10 ))
IP9="198.18.$OCTET.9"
L9="$RUN_DIR/auth-brute.log"
cat > "$L9" <<LOG
2026-11-09T10:00:00Z web-01 sshd[900]: Failed password for alice from $IP9 port 50000 ssh2
2026-11-09T10:00:30Z web-01 sshd[901]: Failed password for alice from $IP9 port 50001 ssh2
2026-11-09T10:01:00Z web-01 sshd[902]: Failed password for alice from $IP9 port 50002 ssh2
2026-11-09T10:01:30Z web-01 sshd[903]: Failed password for alice from $IP9 port 50003 ssh2
2026-11-09T10:02:00Z web-01 sshd[904]: Failed password for alice from $IP9 port 50004 ssh2
LOG
start_consumer "$T9" "$G9" "$RUN_DIR/consumer-brute.log"
C9=$LAST_PID
start_agent "$T9" "$L9" "$RUN_DIR/brute.db" auth-log "$A9"
AG9=$LAST_PID
if wait_docs "$A9" 5 30; then pass "five brute-force events reached Elasticsearch"; else fail "five brute-force events reached Elasticsearch"; fi
if wait_incidents "$A9" 1 30; then pass "brute force produced one incident"; else fail "brute force produced one incident"; fi
INC9=$(incident_source "$A9")
ATTACK9=$(jq -r '.attack_type // empty' <<<"$INC9")
FAILURES9=$(jq -r '.failure_event_ids | length' <<<"$INC9")
HOSTS9=$(jq -r '.hosts | length' <<<"$INC9")
check "brute force is classified from evidence" "[ \"$ATTACK9\" = ssh_brute_force ]"
check "brute force incident stores five event IDs" "[ \"$FAILURES9\" = 5 ]"
check "brute force incident records the host" "[ \"$HOSTS9\" = 1 ]"

kafka_exec kafka-console-consumer --bootstrap-server kafka:9092 --topic "$T9" \
  --from-beginning --max-messages 5 --timeout-ms 10000 > "$RUN_DIR/brute-replay.json" 2>/dev/null ||
  fail "could not capture five incident events for replay"
kafka_exec kafka-console-producer --bootstrap-server kafka:9092 --topic "$T9" < "$RUN_DIR/brute-replay.json"
wait_lag_zero "$G9" "$T9" 10 30 || fail "incident replay was not fully committed"
sleep 1
INC9B=$(incident_source "$A9")
INC_COUNT9B=$(es_count ids-incidents-v2 -d "{\"query\":{\"term\":{\"hosts.keyword\":\"$A9\"}}}")
ATTACK9B=$(jq -r '.attack_type // empty' <<<"$INC9B")
FAILURES9B=$(jq -r '.failure_event_ids | length' <<<"$INC9B")
COUNT9=$(count_docs "$A9")
check "replay keeps the same single incident" "[ \"$INC_COUNT9B\" = 1 ] && [ \"$ATTACK9B\" = ssh_brute_force ]"
check "replay does not duplicate evidence" "[ \"$FAILURES9B\" = 5 ]"
check "replay does not duplicate events" "[ \"$COUNT9\" = 5 ]"
stop_pid "$C9"; stop_pid "$AG9"

# ---------------------------------------------------------------- scenario 10
echo "== 10. password spraying incident =="
T10=$(new_topic spray) || exit 1
A10="linux-verify-$TS-spray"; G10="logly-verify-$TS-spray"
IP10="198.18.$OCTET.10"
L10="$RUN_DIR/auth-spray.log"
cat > "$L10" <<LOG
2026-11-09T10:05:00Z bastion-01 sshd[910]: Failed password for alice from $IP10 port 50100 ssh2
2026-11-09T10:05:01Z bastion-01 sshd[911]: Failed password for bob from $IP10 port 50101 ssh2
2026-11-09T10:05:02Z bastion-01 sshd[912]: Failed password for carol from $IP10 port 50102 ssh2
2026-11-09T10:05:03Z bastion-01 sshd[913]: Failed password for dave from $IP10 port 50103 ssh2
2026-11-09T10:05:04Z bastion-01 sshd[914]: Failed password for erin from $IP10 port 50104 ssh2
LOG
start_consumer "$T10" "$G10" "$RUN_DIR/consumer-spray.log"
C10=$LAST_PID
start_agent "$T10" "$L10" "$RUN_DIR/spray.db" auth-log "$A10"
AG10=$LAST_PID
wait_docs "$A10" 5 30
if wait_incidents "$A10" 1 30; then pass "spraying produced one incident"; else fail "spraying produced one incident"; fi
INC10=$(incident_source "$A10")
ATTACK10=$(jq -r '.attack_type // empty' <<<"$INC10")
USERS10=$(jq -r '.usernames | length' <<<"$INC10")
check "spraying is classified from distinct users" "[ \"$ATTACK10\" = ssh_password_spraying ]"
check "spraying incident stores five usernames" "[ \"$USERS10\" = 5 ]"
stop_pid "$C10"; stop_pid "$AG10"

# ---------------------------------------------------------------- scenario 11
echo "== 11. suspicious success after failures =="
T11=$(new_topic success) || exit 1
A11="linux-verify-$TS-success"; G11="logly-verify-$TS-success"
IP11="198.18.$OCTET.11"
L11="$RUN_DIR/auth-success.log"
cat > "$L11" <<LOG
2026-11-09T10:10:00Z app-01 sshd[920]: Failed password for bob from $IP11 port 50200 ssh2
2026-11-09T10:10:30Z app-01 sshd[921]: Failed password for bob from $IP11 port 50201 ssh2
2026-11-09T10:11:00Z app-01 sshd[922]: Failed password for bob from $IP11 port 50202 ssh2
2026-11-09T10:12:00Z app-01 sshd[923]: Accepted password for bob from $IP11 port 50203 ssh2
LOG
start_consumer "$T11" "$G11" "$RUN_DIR/consumer-success.log"
C11=$LAST_PID
start_agent "$T11" "$L11" "$RUN_DIR/success.db" auth-log "$A11"
AG11=$LAST_PID
wait_docs "$A11" 4 30
if wait_incidents "$A11" 1 30; then pass "suspicious success produced one incident"; else fail "suspicious success produced one incident"; fi
INC11=$(incident_source "$A11")
ATTACK11=$(jq -r '.attack_type // empty' <<<"$INC11")
SUCCESS11=$(jq -r '.success_event_ids | length' <<<"$INC11")
FAILURES11=$(jq -r '.failure_event_ids | length' <<<"$INC11")
check "success after failures is classified from evidence" "[ \"$ATTACK11\" = suspicious_login_success ]"
check "suspicious success incident stores the success event" "[ \"$SUCCESS11\" = 1 ]"
check "suspicious success incident stores three failures" "[ \"$FAILURES11\" = 3 ]"
stop_pid "$C11"; stop_pid "$AG11"

# ---------------------------------------------------------------- scenario 12
echo "== 12. benign authentication produces no incident =="
T12=$(new_topic benign) || exit 1
A12="linux-verify-$TS-benign"; G12="logly-verify-$TS-benign"
IP12="198.18.$OCTET.12"
IP12B="198.18.$OCTET.13"
IP12C="198.18.$OCTET.14"
L12="$RUN_DIR/auth-benign.log"
cat > "$L12" <<LOG
2026-11-09T10:20:00Z web-03 sshd[930]: Failed password for frank from $IP12 port 50300 ssh2
2026-11-09T10:20:01Z web-03 sshd[931]: Failed password for frank from $IP12 port 50301 ssh2
2026-11-09T10:20:02Z web-03 sshd[932]: Failed password for frank from $IP12 port 50302 ssh2
2026-11-09T10:20:03Z web-03 sshd[933]: Failed password for frank from $IP12 port 50303 ssh2
2026-11-09T10:21:00Z web-03 sshd[934]: Accepted publickey for grace from $IP12B port 50304 ssh2
2026-11-09T10:22:00Z web-03 sshd[935]: Failed password for heidi from $IP12C port 50305 ssh2
2026-11-09T10:22:30Z web-03 sshd[936]: Failed password for heidi from $IP12C port 50306 ssh2
2026-11-09T10:23:00Z web-03 sshd[937]: Accepted password for heidi from $IP12C port 50307 ssh2
LOG
start_consumer "$T12" "$G12" "$RUN_DIR/consumer-benign.log"
C12=$LAST_PID
start_agent "$T12" "$L12" "$RUN_DIR/benign.db" auth-log "$A12"
AG12=$LAST_PID
if wait_docs "$A12" 8 30; then pass "benign events reached Elasticsearch"; else fail "benign events reached Elasticsearch"; fi
wait_lag_zero "$G12" "$T12" 8 30 || fail "benign authentication was not fully analyzed"
if wait_incidents "$A12" 0 10; then pass "below-threshold authentication created no incident"; else fail "below-threshold authentication created no incident"; fi
BENIGN_INCIDENTS=$(es_count ids-incidents-v2 -d "{\"query\":{\"term\":{\"hosts.keyword\":\"$A12\"}}}")
check "benign host has no incidents" "[ \"$BENIGN_INCIDENTS\" = 0 ]"
stop_pid "$C12"; stop_pid "$AG12"

# ---------------------------------------------------------------- scenario 13
echo "== 13. FastAPI overview, events, incidents and hosts =="
API_PORT=$((18080 + (TS % 500)))
start_api "$API_PORT"
READY=0
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$API_PORT/api/overview" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.3
done
check "FastAPI started and answered overview" "[ \"$READY\" = 1 ]"
OVERVIEW=$(curl -fsS "http://127.0.0.1:$API_PORT/api/overview") || fail "HTTP request for OVERVIEW failed"
if ! jq -e . >/dev/null 2>&1 <<<"$OVERVIEW"; then
  echo "API overview body: $OVERVIEW"
  tail -20 "$RUN_DIR/api.log"
fi
API_INC=$(curl -fsS "http://127.0.0.1:$API_PORT/api/incidents?host=$A9") || fail "HTTP request for API_INC failed"
INC_ID=$(jq -r '.incidents[0].incident_id // empty' <<<"$API_INC")
DETAIL=$(curl -fsS "http://127.0.0.1:$API_PORT/api/incidents/$INC_ID") || fail "HTTP request for DETAIL failed"
EVENTS=$(curl -fsS "http://127.0.0.1:$API_PORT/api/events?agent_id=$A9&outcome=failure&limit=10") || fail "HTTP request for EVENTS failed"
HOSTS=$(curl -fsS "http://127.0.0.1:$API_PORT/api/hosts?limit=200") || fail "HTTP request for HOSTS failed"
HOST_DETAIL=$(curl -fsS "http://127.0.0.1:$API_PORT/api/hosts/$A9") || fail "HTTP request for HOST_DETAIL failed"
HOST_PRESENT=$(jq -r --arg h "$A9" '[.hosts[].host] | index($h) != null' <<<"$HOSTS")
OVERVIEW_EVENTS=$(jq -r '.events' <<<"$OVERVIEW")
OVERVIEW_INCIDENTS=$(jq -r '.incidents' <<<"$OVERVIEW")
INC_TOTAL=$(jq -r '.total' <<<"$API_INC")
DETAIL_COUNT=$(jq -r '.evidence_event_count' <<<"$DETAIL")
DETAIL_MISSING=$(jq -r '.missing_event_ids | length' <<<"$DETAIL")
EVENT_TOTAL=$(jq -r '.total' <<<"$EVENTS")
HOST_EVENTS=$(jq -r '.event_total' <<<"$HOST_DETAIL")
HOST_INCIDENTS=$(jq -r '.incident_total' <<<"$HOST_DETAIL")
check "overview reports events and incidents" \
  "[ \"$OVERVIEW_EVENTS\" -ge 5 ] && [ \"$OVERVIEW_INCIDENTS\" -ge 1 ]"
check "incident search by host returns one incident" "[ \"$INC_TOTAL\" = 1 ]"
check "incident detail timeline has five events" "[ \"$DETAIL_COUNT\" = 5 ]"
check "incident detail has no missing evidence" "[ \"$DETAIL_MISSING\" = 0 ]"
check "event search filters by host and outcome" "[ \"$EVENT_TOTAL\" = 5 ]"
check "host list contains the incident host" "[ \"$HOST_PRESENT\" = true ]"
check "host history combines events and incidents" \
  "[ \"$HOST_EVENTS\" = 5 ] && [ \"$HOST_INCIDENTS\" = 1 ]"
# ---------------------------------------------------------------- scenario 14
echo "== 14. expanded security telemetry and grounded Investigator =="
T14=$(new_topic telemetry) || { echo "FAIL: create telemetry topic"; exit 1; }
A14="linux-verify-$TS-telemetry"
G14="logly-verify-$TS-telemetry"
L14="$RUN_DIR/security.log"
AUDIT14="$RUN_DIR/audit.log"
cat > "$L14" <<'LOG'
Nov 10 10:00:01 web-01 sudo: alice : TTY=pts/0 ; USER=root ; COMMAND=/bin/echo REDACT_THIS_ARGUMENT
Nov 10 10:00:02 web-01 useradd[42]: new user: name=bob, UID=1001, home=/home/bob
LOG
cat > "$AUDIT14" <<'LOG'
type=SYSCALL msg=audit(1762673744.123:42): success=yes exe="/usr/bin/id" key="logly-exec"
type=EXECVE msg=audit(1762673744.123:42): argc=1 a0="REDACT_THIS_ARGUMENT"
type=SYSCALL msg=audit(1762673745.123:43): success=no key=logly-passwd
LOG
start_consumer "$T14" "$G14" "$RUN_DIR/consumer-telemetry.log"
C14=$LAST_PID
start_agent "$T14" "$L14" "$RUN_DIR/telemetry.db" auth-log "$A14" "LOGLY_V2_AUDIT_PATH=$AUDIT14"
AG14=$LAST_PID
if wait_docs "$A14" 4 30; then
  pass "all four security records were indexed"
else
  fail "all four security records were indexed"
fi
if wait_lag_zero "$G14" "$T14" 4 30; then
  pass "all four security records were committed with no extra deliveries"
else
  fail "all four security records were committed with no extra deliveries"
fi
QUERY14=$(jq -nc --arg host "$A14" '{size:5,query:{term:{"agent.agent_id.keyword":$host}}}')
if curl -fsS "$ES/ids-events/_search" -H 'Content-Type: application/json' -d "$QUERY14" |
  jq -e 'select(.timed_out == false and ._shards.failed == 0) | [.hits.hits[]._source] as $events |
    ($events | length) == 4 and
    ([$events[].event.category] | sort) == ["account","file_integrity","process","sudo"] and
    ([$events[] | select(.collection.source_type == "audit")] | length) == 2 and
    all($events[]; (.raw_event.data | contains("REDACT_THIS_ARGUMENT") | not))' >/dev/null; then
  pass "security documents preserve category and audit provenance with sanitized evidence"
else
  fail "security documents preserve category and audit provenance with sanitized evidence"
fi
if curl -fsS "http://127.0.0.1:$API_PORT/api/events?agent_id=$A14&category=process" |
  jq -e '.total == 1 and .events[0].target.path == "/usr/bin/id" and (.events[0] | has("raw_event") | not)' >/dev/null; then
  pass "FastAPI category filter exposes process metadata without raw evidence"
else
  fail "FastAPI category filter exposes process metadata without raw evidence"
fi
if curl -fsS "http://127.0.0.1:$API_PORT/api/incidents/$INC_ID/investigate" -H 'Content-Type: application/json' -d '{}' |
  jq -e --argjson detail "$DETAIL" '
    .reproduced == true and .mode == "deterministic" and (.observations | length) > 0 and
    (([.observations[].event_ids[]] - [$detail.timeline[].event_id]) | length) == 0' >/dev/null; then
  pass "Investigator reproduces the rule and cites only stored timeline events"
else
  fail "Investigator reproduces the rule and cites only stored timeline events"
fi
stop_pid "$AG14"
stop_pid "$C14"
stop_pid "$APIPID"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] || exit 1
