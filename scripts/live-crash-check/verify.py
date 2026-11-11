import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4

from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import TopicPartition

from api.schema_v2 import compute_event_id, validate_event_v2

root = Path(os.environ["LOGLY_TEST_ROOT"])
scratch = Path(__file__).resolve().parent
suffix = uuid4().hex[:12]
topic = "ids.events.v2.crash." + suffix
group = "logly-crash-" + suffix
host = "fixture-" + suffix
broker = os.getenv("LOGLY_TEST_BROKER", "127.0.0.1:29092")
es_url = os.getenv("LOGLY_TEST_ES", "http://127.0.0.1:29200")
es = Elasticsearch(es_url, request_timeout=5, max_retries=0)
partition = TopicPartition(topic, 0)


def wait_for(check, label, seconds=75):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.5)
    raise AssertionError("Timed out waiting for " + label)


def connect():
    try:
        return KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=5000)
    except Exception as error:
        if type(error).__name__ != "NoBrokersAvailable":
            raise
        return None


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def event_count():
    result = es.count(index="ids-events", query={"term": {"agent.agent_id.keyword": host}})
    check(result["_shards"]["failed"] == 0, "Event count had failed shards")
    return result["count"]


def incident():
    if not es.indices.exists(index="ids-incidents-v2"):
        return None
    result = es.search(
        index="ids-incidents-v2", query={"term": {"hosts.keyword": host}}, track_total_hits=True
    )
    check(not result["timed_out"] and result["_shards"]["failed"] == 0, "Incomplete incident search")
    if result["hits"]["total"]["value"] == 0:
        return None
    check(result["hits"]["total"]["value"] == 1, "More than one incident")
    return result["hits"]["hits"][0]["_source"]


def offset():
    result = admin.list_consumer_group_offsets(group, partitions=[partition])
    return result[partition].offset


def start(name, *arguments):
    env = dict(os.environ)
    env.pop("ES_API_KEY", None)
    env.pop("ES_CA_CERTS", None)
    env.update(
        KAFKA_BROKERS=broker, KAFKA_TOPIC=topic, KAFKA_GROUP_ID=group,
        ES_HOST=es_url, PYTHONUNBUFFERED="1", LOGLY_TEST_CRASH_EVENT=events[-1]["event_id"],
    )
    with (scratch / name).open("w") as log:
        return subprocess.Popen([sys.executable, *arguments], cwd=scratch, env=env, stdout=log, stderr=subprocess.STDOUT)


def check_alive(process, name):
    if process.poll() is not None:
        raise RuntimeError(f"Consumer exited with {process.returncode}\n" + (scratch / name).read_text()[-5000:])


fixture = json.loads((root / "agent/events/testdata/authentication.json").read_text())
events = []
for number in range(5):
    event = copy.deepcopy(fixture)
    event["agent"]["agent_id"] = host
    event["collection"]["position"]["byte_offset"] = 100 * number
    event["collection"]["position"]["record_number"] = number + 1
    event["sequence"] = number + 1
    event["timestamp"] = event["ingest_timestamp"] = f"2026-11-09T10:00:{number:02}Z"
    event["event_id"] = compute_event_id(event)
    validate_event_v2(event)
    events.append(event)

print("Waiting for Kafka and Elasticsearch", flush=True)
admin = wait_for(connect, "Kafka broker")
wait_for(es.ping, "Elasticsearch")
admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda x: json.dumps(x).encode())

def send(event, expected_offset):
    meta = producer.send(topic, partition=0, value=event).get(timeout=20)
    check(meta.offset == expected_offset, f"Unexpected producer offset {meta.offset}")


first = replay = None
try:
    first = start("consumer-crash.log", str(scratch / "crash.py"))
    for number, event in enumerate(events[:4]):
        send(event, number)

    def warmup():
        check_alive(first, "consumer-crash.log")
        return offset() == 4

    wait_for(warmup, "the first four records to commit")
    check(event_count() == 4 and incident() is None, "Unexpected warmup event or incident count")
    print("PASS: four events persisted, no incident, committed next offset 4", flush=True)
    send(events[-1], 4)
    check(first.wait(timeout=60) == -signal.SIGKILL, "Consumer did not exit via SIGKILL")
    check("Crash test reached incident-analysis entry" in (scratch / "consumer-crash.log").read_text(),
          "Consumer did not reach the intended crash point")
    stored = es.get(index="ids-events", id=events[-1]["event_id"])
    check(stored["_source"]["processed"] is True and event_count() == 5, "Fifth event was not durable before crash")
    check(offset() == 4 and incident() is None, "Crash committed the fifth record or wrote an incident")
    print("PASS: SIGKILL after fifth event write; no incident; committed next offset still 4", flush=True)
    replay = start("consumer-replay.log", "-m", "api.kafka_consumer")

    def recovered():
        check_alive(replay, "consumer-replay.log")
        return offset() == 5 and incident()

    result = wait_for(recovered, "replay to persist the incident and commit next offset 5")
    check(result["attack_type"] == "ssh_brute_force", "Wrong incident classification")
    evidence = result["failure_event_ids"]
    check(len(evidence) == 5 and set(evidence) == {e["event_id"] for e in events}, "Wrong incident evidence")
    after = es.get(index="ids-events", id=events[-1]["event_id"])
    check(event_count() == 5 and after["_seq_no"] == stored["_seq_no"] and after["_source"] == stored["_source"],
          "Replay rewrote or duplicated a stored event")
    print("PASS: restart created one ssh_brute_force incident with five unique event IDs; five event documents; next offset 5", flush=True)
finally:
    for process in (first, replay):
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    producer.close()
    admin.close()
    es.close()
