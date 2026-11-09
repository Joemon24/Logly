# normal_test.py

from kafka import KafkaProducer
import json
import time
import random

# =========================================================
# KAFKA PRODUCER
# =========================================================

producer = KafkaProducer(
    bootstrap_servers='localhost:19092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# =========================================================
# USERS & IPS
# =========================================================

users = [
    f"user{i}" for i in range(1, 51)
]

ips = [
    f"10.0.0.{i}" for i in range(10, 60)
]

# =========================================================
# DEVICE TYPES
# =========================================================

devices = [
    "windows",
    "mac",
    "linux"
]

# =========================================================
# TOTAL EVENTS
# =========================================================

TOTAL_EVENTS = 500

print(f"🚀 Sending {TOTAL_EVENTS} realistic normal logs...\n")

# =========================================================
# MAIN LOOP
# =========================================================

for i in range(TOTAL_EVENTS):

    user = random.choice(users)

    ip = random.choice(ips)

    device = random.choice(devices)

    # =====================================================
    # MOSTLY SUCCESSFUL LOGINS
    # =====================================================

    outcome = random.choices(
        ["success", "failure"],
        weights=[97, 3]
    )[0]

    # =====================================================
    # EVENT MESSAGE
    # =====================================================

    event = {

        "raw":
            f"{outcome} login "
            f"user={user} "
            f"ip={ip} "
            f"device={device}",

        "agent": {
            "agent_id": "server-1"
        }
    }

    producer.send(
        "ids.events.v1",
        event
    )

    print(
        f"[{i+1}/{TOTAL_EVENTS}] "
        f"{outcome} login | "
        f"user={user} | "
        f"ip={ip} | "
        f"device={device}"
    )

    # =====================================================
    # REALISTIC HUMAN DELAY
    # =====================================================

    time.sleep(
        random.uniform(0.8, 2.5)
    )

# =========================================================
# FLUSH
# =========================================================

producer.flush()

print(
    f"\n✅ {TOTAL_EVENTS} realistic normal logs sent successfully"
)
