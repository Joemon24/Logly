"""Schema-v2 event contract validation and deterministic event IDs.

The Go agent derives ``event_id`` from a canonical JSON document. Recomputing
it here catches truncated or tampered payloads and gives the consumer a stable
Elasticsearch document ID and deduplication key.
"""
import hashlib
import json
import re
from datetime import datetime

EVENT_ID_PREFIX = "evt2_"
EVENT_ID_PATTERN = re.compile(r"^evt2_[0-9a-f]{64}$")
MAX_RAW_BYTES = 64 * 1024

CATEGORIES = {
    "authentication", "sudo", "account", "process", "file_integrity",
    "heartbeat",
}
TIMESTAMP_SOURCES = {"log", "ingest"}
SOURCE_TYPES = {"file", "journald", "audit", "agent"}
RAW_FORMATS = {"text", "json", "audit"}
ENTITY_TYPES = {"user", "ip", "host", "process", "file", "session"}


class SchemaV2Error(ValueError):
    """A payload claims schema v2 but does not satisfy the contract."""


def is_v2_event(value):
    return isinstance(value, dict) and value.get("schema_version") == 2


def compute_event_id(event):
    """Recompute the canonical event ID exactly as ``agent/events`` does."""
    agent = event["agent"]
    collection = event["collection"]
    position = collection["position"]
    raw = event["raw_event"]["data"]

    canonical = {
        "agent_id": agent["agent_id"],
        "agent_generation": agent["generation"],
        "source_id": collection["source_id"],
        "source_type": collection["source_type"],
        "source_generation": collection["generation"],
    }
    byte_offset = position.get("byte_offset")
    if byte_offset is not None:
        canonical["byte_offset"] = byte_offset
    canonical["record_number"] = position["record_number"]
    canonical["raw_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    encoded = json.dumps(
        canonical, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return EVENT_ID_PREFIX + hashlib.sha256(encoded).hexdigest()


def _require(mapping, key, where):
    value = mapping.get(key)
    if value is None or value == "":
        raise SchemaV2Error(f"{where}.{key} is required")
    return value


def _timestamp(event, key):
    value = _require(event, key, "event")
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SchemaV2Error(f"{key} must be a UTC RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise SchemaV2Error(f"{key} is not a valid timestamp: {exc}") from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise SchemaV2Error(f"{key} must be UTC")
    return parsed


def _entity(event, key):
    entity = event.get(key)
    if entity is None:
        return
    if not isinstance(entity, dict):
        raise SchemaV2Error(f"{key} must be an object")
    if entity.get("entity_type") not in ENTITY_TYPES:
        raise SchemaV2Error(f"{key}.entity_type is invalid")


def validate_event_v2(event):
    """Raise SchemaV2Error unless event satisfies the schema-v2 contract."""
    if not isinstance(event, dict):
        raise SchemaV2Error("event must be an object")
    if event.get("schema_version") != 2:
        raise SchemaV2Error("schema_version must be 2")

    event_id = _require(event, "event_id", "event")
    if not isinstance(event_id, str) or not EVENT_ID_PATTERN.match(event_id):
        raise SchemaV2Error("event_id must be evt2_ followed by 64 lowercase hex characters")

    _timestamp(event, "timestamp")
    _timestamp(event, "ingest_timestamp")
    if event.get("timestamp_source") not in TIMESTAMP_SOURCES:
        raise SchemaV2Error("timestamp_source is invalid")

    agent = event.get("agent")
    if not isinstance(agent, dict):
        raise SchemaV2Error("agent is required")
    for key in ("agent_id", "agent_type", "agent_version", "generation"):
        _require(agent, key, "agent")

    metadata = event.get("event")
    if not isinstance(metadata, dict):
        raise SchemaV2Error("event metadata is required")
    if metadata.get("category") not in CATEGORIES:
        raise SchemaV2Error("event.category is invalid")
    for key in ("action", "outcome"):
        _require(metadata, key, "event")

    _entity(event, "source")
    _entity(event, "target")

    raw = event.get("raw_event")
    if not isinstance(raw, dict):
        raise SchemaV2Error("raw_event is required")
    if raw.get("format") not in RAW_FORMATS:
        raise SchemaV2Error("raw_event.format is invalid")
    data = raw.get("data")
    if not isinstance(data, str):
        raise SchemaV2Error("raw_event.data must be a string")
    if len(data.encode("utf-8")) > MAX_RAW_BYTES:
        raise SchemaV2Error("raw_event.data exceeds the raw evidence limit")

    collection = event.get("collection")
    if not isinstance(collection, dict):
        raise SchemaV2Error("collection is required")
    _require(collection, "source_id", "collection")
    if collection.get("source_type") not in SOURCE_TYPES:
        raise SchemaV2Error("collection.source_type is invalid")
    _require(collection, "generation", "collection")
    position = collection.get("position")
    if not isinstance(position, dict):
        raise SchemaV2Error("collection.position is required")
    record_number = position.get("record_number")
    if not isinstance(record_number, int) or isinstance(record_number, bool) or record_number < 1:
        raise SchemaV2Error("collection.position.record_number must be 1 or greater")
    byte_offset = position.get("byte_offset")
    if byte_offset is not None and (
        not isinstance(byte_offset, int) or isinstance(byte_offset, bool) or byte_offset < 0
    ):
        raise SchemaV2Error("collection.position.byte_offset must not be negative")

    if metadata["category"] == "heartbeat":
        if collection.get("source_type") != "agent":
            raise SchemaV2Error("heartbeat requires collection.source_type agent")
        if event.get("timestamp_source") != "ingest":
            raise SchemaV2Error("heartbeat requires timestamp_source ingest")
        if byte_offset is not None:
            raise SchemaV2Error("heartbeat must not set collection.position.byte_offset")
        if collection.get("path"):
            raise SchemaV2Error("heartbeat must not set collection.path")
        if event.get("source") is not None or event.get("target") is not None:
            raise SchemaV2Error("heartbeat must not have source or target entities")

    computed = compute_event_id(event)
    if computed != event_id:
        raise SchemaV2Error(
            f"event_id mismatch: have {event_id}, computed {computed}"
        )
    return event
