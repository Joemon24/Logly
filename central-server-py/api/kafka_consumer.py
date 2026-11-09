# =========================================================
# IMPORTS
# =========================================================

import json
import random
import re
import os
import joblib
import numpy as np

from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict, Counter

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
from api.settings import elasticsearch_client
from api.soar.policy import alert_due
from elasticsearch import Elasticsearch, helpers

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# =========================================================
# SOAR IMPORTS
# =========================================================

from api.soar.alert_manager import create_alert

from api.soar.incident_manager import (
    create_incident
)

from api.soar.response_engine import (
    block_ip,
)

from api.soar.notifier import (
    send_notification
)
from api.rule_engine import (
    load_rules,
    evaluate_rules
)
# =========================================================
# SOAR
# =========================================================

def trigger_soar(event):

    create_alert(event)
    create_incident(event)
    block_ip(event.get("src_ip"))
    send_notification(event)

# =========================================================
# CONFIG
# =========================================================

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092").split(",")

TOPIC = os.getenv("KAFKA_TOPIC", "ids.events.v1")

GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ids-detector-v1")

ES_HOST = "http://localhost:9200"

ES_INDEX = "ids-events"

MODEL_PATH = "models/isolation_forest.pkl"

SCALER_PATH = "models/scaler.pkl"

TRAINING_DATA_PATH = "models/training_data.pkl"

# =========================================================
# ELASTICSEARCH
# =========================================================

es = elasticsearch_client()

bulk_buffer = []

BULK_SIZE = 100

# =========================================================
# MODEL
# =========================================================

scaler = StandardScaler()

model = IsolationForest(
    n_estimators=300,
    contamination=0.02,
    random_state=42
)

training_data = []

model_trained = False
print("ML status: rules-only unless validated model artifacts load successfully")

TRAINING_LIMIT = 500

# =========================================================
# WINDOWS
# =========================================================

WINDOW_5M = timedelta(minutes=5)

WINDOW_1M = timedelta(minutes=1)

# =========================================================
# STATE
# =========================================================

user_history = {}

ip_history = {}

fail_streak = defaultdict(int)

trust_scores = defaultdict(lambda: 1.0)

correlation_state = {}
last_alert_time = {}
ALERT_COOLDOWN = 60

MAX_HISTORY_KEYS = 10000

# =========================================================
# MODEL DIRECTORY
# =========================================================

os.makedirs("models", exist_ok=True)

# =========================================================
# LOAD TRAINING DATA
# =========================================================

if os.path.exists(TRAINING_DATA_PATH):

    try:

        training_data = joblib.load(
            TRAINING_DATA_PATH
        )

        print(
            f"✅ Loaded training data: "
            f"{len(training_data)} samples"
        )

    except Exception as e:

        print(
            f"❌ Failed loading training data: {e}"
        )

# =========================================================
# LOAD MODEL
# =========================================================

if (
    os.path.exists(MODEL_PATH)
    and os.path.exists(SCALER_PATH)
):

    try:

        model = joblib.load(MODEL_PATH)

        scaler = joblib.load(SCALER_PATH)

        model_trained = True

        print("✅ Loaded trained model")

    except Exception as e:

        print(
            f"❌ Failed loading model: {e}"
        )
# =========================================================
# LOAD RULES
# =========================================================

load_rules()
# =========================================================
# HELPERS
# =========================================================

def safe_div(a, b):

    return round(a / max(b, 1), 2)

# =========================================================
# PARSERS
# =========================================================

from api.normalization import extract_ip, extract_user, extract_outcome

# =========================================================
# FEATURE ENGINE
# =========================================================

def compute_features(event, now):

    agent_id = event["agent"]["agent_id"]

    ip = extract_ip(event)

    user = extract_user(event)

    outcome = extract_outcome(event)

    # =====================================================
    # USER HISTORY
    # =====================================================

    user_key = f"{agent_id}:{user}"

    if user_key not in user_history:
        user_history[user_key] = deque()

    u_history = user_history[user_key]

    u_history.append((now, ip, outcome))

    while (
        u_history
        and u_history[0][0] < now - WINDOW_5M
    ):
        u_history.popleft()

    # =====================================================
    # IP HISTORY
    # =====================================================

    ip_key = f"{agent_id}:{ip}"

    if ip_key not in ip_history:
        ip_history[ip_key] = deque()

    i_history = ip_history[ip_key]

    i_history.append((now, user, outcome))

    while (
        i_history
        and i_history[0][0] < now - WINDOW_5M
    ):
        i_history.popleft()

    # =====================================================
    # FAIL STREAK
    # =====================================================

    if outcome == "failure":
        fail_streak[user_key] += 1
    else:
        fail_streak[user_key] = 0

    # =====================================================
    # USER FEATURES
    # =====================================================

    failed_auth_count_5m = sum(
        1 for _, _, o in u_history
        if o == "failure"
    )

    event_rate_1m = sum(
        1 for ts, _, _ in u_history
        if ts >= now - WINDOW_1M
    )

    failed_ratio = safe_div(
        failed_auth_count_5m,
        len(u_history)
    )

    ips = [ip for _, ip, _ in u_history]

    ip_counts = Counter(ips)

    top_ip_count = (
        ip_counts.most_common(1)[0][1]
        if ip_counts else 0
    )

    dominance_ratio = safe_div(
        top_ip_count,
        len(u_history)
    )

    unique_ips = len(set(ips))

    # =====================================================
    # IP FEATURES
    # =====================================================

    unique_users = len(set(
        user for _, user, _ in i_history
    ))

    ip_failures = sum(
        1 for _, _, o in i_history
        if o == "failure"
    )

    ip_failed_ratio = safe_div(
        ip_failures,
        len(i_history)
    )

    ip_rate_1m = sum(
        1 for ts, _, _ in i_history
        if ts >= now - WINDOW_1M
    )

    hour_of_day = now.hour

    return {

        "failed_auth_count_5m":
            failed_auth_count_5m,

        "event_rate_1m":
            event_rate_1m,

        "failed_ratio":
            failed_ratio,

        "fail_streak":
            fail_streak[user_key],

        "dominance_ratio":
            dominance_ratio,

        "unique_ips":
            unique_ips,

        "unique_users":
            unique_users,

        "ip_failed_ratio":
            ip_failed_ratio,

        "ip_rate_1m":
            ip_rate_1m,

        "hour_of_day":
            hour_of_day,
    }

# =========================================================
# VECTOR
# =========================================================

def extract_vector(f):

    return [

        f["failed_ratio"],

        min(f["fail_streak"] / 20, 1),

        min(f["event_rate_1m"] / 200, 1),

        f["dominance_ratio"],

        min(f["unique_ips"] / 20, 1),

        min(f["unique_users"] / 20, 1),

        f["ip_failed_ratio"],

        min(f["ip_rate_1m"] / 200, 1),

        min(f["hour_of_day"] / 24, 1),
    ]

# =========================================================
# TRUST ENGINE
# =========================================================

def update_trust_score(
    current_trust,
    features,
    anomaly_score,
    outcome,
    alert_type
):

    trust = current_trust

    if (
        outcome == "success"
        and anomaly_score < 0.20
        and features["failed_ratio"] < 0.10
    ):
        trust += 0.03
    if (

    outcome == "success"

    and anomaly_score < 0.10

    and features["fail_streak"] == 0

):

        trust += 0.05

    if outcome == "failure":

        if features["failed_auth_count_5m"] <= 2:
            trust -= 0.01
        else:
            trust -= 0.05

    if features["fail_streak"] >= 5:
        trust -= 0.08

    if features["fail_streak"] >= 10:
        trust -= 0.15

    if anomaly_score > 0.50:
        trust -= 0.10

    if anomaly_score > 0.75:
        trust -= 0.15

    if alert_type == "password_spraying":
        trust -= 0.20

    elif alert_type == "brute_force":
        trust -= 0.30

    elif alert_type == "lateral_movement":
        trust -= 0.20

    trust = np.clip(trust, 0.0, 1.0)

    return round(float(trust), 2)

# =========================================================
# KAFKA CONSUMER
# =========================================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(
        v.decode("utf-8")
    ),
)

print("🚀 Consumer started...")

# =========================================================
# MAIN LOOP
# =========================================================

try:

    for msg in consumer:

        payload = msg.value

        if not isinstance(payload, list):
            payload = [payload]

        for event_position, event in enumerate(payload):

            try:

                if not isinstance(event, dict) or not event:
                    bulk_buffer.append({
                        "_index": "ids-rejected",
                        "_id": f"{msg.topic}:{msg.partition}:{msg.offset}:{event_position}",
                        "_source": {"reason": "Event must be a nonempty object", "payload": json.dumps(event)},
                    })
                    continue

                event["storage_id"] = f"{msg.topic}:{msg.partition}:{msg.offset}:{event_position}"
                now = datetime.now(timezone.utc)


                if "agent" not in event:

                    event["agent"] = {
                        "agent_id": "default"
                    }

                metadata = event.get("event", {})
                if isinstance(metadata, dict) and (
                    metadata.get("type") == "heartbeat" or
                    metadata.get("category") not in (None, "auth")
                ):
                    event["@timestamp"] = now.isoformat()
                    bulk_buffer.append({"_index": ES_INDEX, "_id": event["storage_id"], "_source": event})
                    continue

                # =================================================
                # CLEANUP
                # =================================================

                if len(user_history) > MAX_HISTORY_KEYS:

                    oldest_keys = list(
                        user_history.keys()
                    )[:1000]

                    for key in oldest_keys:
                        user_history.pop(key, None)

                if len(ip_history) > MAX_HISTORY_KEYS:

                    oldest_keys = list(
                        ip_history.keys()
                    )[:1000]

                    for key in oldest_keys:
                        ip_history.pop(key, None)

                # =================================================
                # FEATURES
                # =================================================

                features = compute_features(
                    event,
                    now
                )

                event["src_ip"] = extract_ip(event)

                # =================================================
                # BLOCKED IP CHECK
                # =================================================

                event["username"] = extract_user(event)

                event["outcome"] = extract_outcome(event)
                # =================================================

                # EVENT TYPE

                # =================================================

                event_type = "unknown"

                if event["outcome"] == "success":
                    event_type = "login_success"

                if event["outcome"] == "failure":

                    event_type = "login_failure"

                event["event_type"] = event_type

                event["features"] = features

                vector = extract_vector(features)

                # =================================================
                # MODEL
                # =================================================

                if not model_trained:

                    anomaly_score = 0.0

                else:

                    X = scaler.transform([vector])

                    score = model.decision_function(
                        X
                    )[0]

                    base_score = float(
                        np.clip(
                            -score * 1.8,
                            0,
                            1
                        )
                    )

                    if (
                        features["fail_streak"] <= 2
                        and features["failed_ratio"] < 0.60
                    ):
                        base_score *= 0.30

                    if (
                        features["event_rate_1m"] <= 3
                        and features["ip_rate_1m"] <= 3
                    ):
                        base_score *= 0.50

                    anomaly_score = round(
                        min(base_score, 1.0),
                        2
                    )

                # =================================================
                # BOOSTS
                # =================================================

                boost = 0

                if features["fail_streak"] >= 5:
                    boost += 0.10

                if features["fail_streak"] >= 10:
                    boost += 0.20

                if (
                    features["failed_ratio"] > 0.30
                    and features["failed_auth_count_5m"] >= 3
                ):
                    boost += 0.10

                if (
                    features["failed_ratio"] > 0.60
                    and features["failed_auth_count_5m"] >= 5
                ):
                    boost += 0.20

                if (
                    features["unique_users"] >= 6
                    and features["ip_failed_ratio"] > 0.50
                    and features["ip_rate_1m"] >= 6
                ):
                    boost += 0.40

                if (
                    features["unique_ips"] >= 6
                    and features["event_rate_1m"] > 12
                ):
                    boost += 0.30

                anomaly_score = round(
                    min(anomaly_score + boost, 1.0),
                    2
                )
                # =========================================================
                # RULE ENGINE
                # =========================================================

                matched_rules = evaluate_rules(
                    features
                )
                print("\n========== FEATURES ==========")
                print(features)

                print("========== MATCHED RULES ==========")
                print(matched_rules)
                print("===================================\n")

                if matched_rules:

                    top_rule = matched_rules[0]

                    alert_type = top_rule["type"]

                    severity = top_rule["severity"]

                    mitre_technique = top_rule["mitre"]

                else:

                    alert_type = "normal"

                    mitre_technique = "none"

                    if anomaly_score >= 0.85:
                        severity = "critical"

                    elif anomaly_score >= 0.60:
                        severity = "high"

                    elif anomaly_score >= 0.35:
                        severity = "medium"

                    else:
                        severity = "low"

                    if anomaly_score > 0.70:
                        alert_type = "anomaly"

                event["matched_rules"] = matched_rules              
                # =================================================
                # TRUST
                # =================================================

                trust_key = (
                    f"{event['agent']['agent_id']}:"
                    f"{event['username']}"
                )

                current_trust = trust_scores[
                    trust_key
                ]

                updated_trust = update_trust_score(
                    current_trust,
                    features,
                    anomaly_score,
                    event["outcome"],
                    alert_type
                )

                trust_scores[
                    trust_key
                ] = updated_trust

                # =================================================
                # RISK
                # =================================================

                event["risk"] = {

                    "anomaly_score":
                        anomaly_score,

                    "trust_score":
                        updated_trust,

                    "alert_type":
                        alert_type,

                    "severity":
                        severity,

                    "mitre_technique":
                        mitre_technique,
                }

                # =================================================
                # =================================================
                # SOAR EVENT DATA
                # =================================================

                event["alert_type"] = alert_type

                event["severity"] = severity

                event["mitre_technique"] = (
                    mitre_technique
                )

                event["anomaly_score"] = (
                    anomaly_score
                )

                event["trust_score"] = (
                    updated_trust
                )

                
                event["soar_triggered"] = False

                # =================================================
                # CORRELATION ENGINE
                # =================================================

                username = event["username"]

                correlation_key = (
                    f"{username}:{event['src_ip']}"
                )


                if alert_type == "brute_force":

                    correlation_state[correlation_key] = {

                        "stage": "brute_force",

                        "time": now
                    }

                if (
                    correlation_key in correlation_state
                    and event["event_type"] == "login_success"
                ):

                    previous = correlation_state[
                        correlation_key
                    ]
                    if previous["stage"] == "brute_force":

                        delta = now - previous["time"]

                        if delta.total_seconds() <= 120:

                            print(
                                f"🔥 ACCOUNT COMPROMISE: "
                                f"{username}"
                            )

                            event["alert_type"] = (
                                "account_compromise"
                            )

                            event["severity"] = "critical"

                            event["mitre_technique"] = "T1078"

                            alert_type = "account_compromise"

                            severity = "critical"

                            mitre_technique = "T1078"


                            del correlation_state[
                                correlation_key
                            ]
                # =================================================
                # =================================================
                # SOAR EXECUTION
                # =================================================

                event["risk"].update(
                    alert_type=event["alert_type"], severity=event["severity"],
                    mitre_technique=event["mitre_technique"]
                )
                alert_key, due = alert_due(event, now, last_alert_time, ALERT_COOLDOWN)
                if due:
                    trigger_soar(event)
                    last_alert_time[alert_key] = now
                    event["soar_triggered"] = True

                # =================================================
                # TIMESTAMP
                # =================================================

                event["@timestamp"] = (
                    now.isoformat()
                )

                # =================================================
                # OUTPUT
                # =================================================

                print(
                    f"🚨 "
                    f"A={anomaly_score:.2f} | "
                    f"T={updated_trust:.2f} | "
                    f"S={event['severity']} | "
                    f"Type={event['alert_type']} | "
                    f"MITRE={event['mitre_technique']}"
                )

                # =================================================
                # ELASTICSEARCH
                # =================================================

                bulk_buffer.append({

                    "_index": ES_INDEX,
                    "_id": event["storage_id"],

                    "_source": event
                })

            except Exception:
                # Fail closed: do not acknowledge a record whose processing failed.
                # Restart replays this record; deterministic IDs avoid duplicate documents.
                raise

        if bulk_buffer:
            helpers.bulk(es, bulk_buffer)
            bulk_buffer.clear()
        consumer.commit({
            TopicPartition(msg.topic, msg.partition): OffsetAndMetadata(msg.offset + 1, "", -1)
        })

except KeyboardInterrupt:

    print("\n🛑 Stopping consumer...")

finally:
    # Failed/partial records remain uncommitted and are replayed on restart.
    consumer.close()
