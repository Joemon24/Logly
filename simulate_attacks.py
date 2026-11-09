# =========================================================
# IMPORTS
# =========================================================

import json
import random
import sys
import time
import atexit

from datetime import datetime, timezone

from kafka import KafkaProducer

# =========================================================
# CONFIG
# =========================================================

KAFKA_BROKERS = ["localhost:19092"]

TOPIC = "ids.events.v1"

# =========================================================
# KAFKA PRODUCER
# =========================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
    retries=5
)

# =========================================================
# CLEAN SHUTDOWN
# =========================================================

def cleanup():

    try:

        producer.flush(timeout=10)

        producer.close(timeout=10)

        print("✅ Kafka producer closed cleanly")

    except Exception as e:

        print(f"⚠️ Cleanup error: {e}")

atexit.register(cleanup)

# =========================================================
# DATA
# =========================================================

users = [
    "alice",
    "bob",
    "charlie",
    "david",
    "eve",
    "frank",
    "admin",
    "root",
    "testuser",
    "finance",
    "hr",
    "devops",
]

normal_ips = [
    "192.168.1.10",
    "192.168.1.11",
    "192.168.1.12",
    "192.168.1.13",
    "10.0.0.5",
    "10.0.0.6",
    "172.16.1.5",
    "172.16.1.6",
]

lateral_ips = [
    "10.0.0.21",
    "10.0.0.22",
    "10.0.0.23",
    "10.0.0.24",
    "10.0.0.25",
    "10.0.0.26",
]

agents = [
    "agent-1",
    "agent-2",
    "agent-3",
]

# =========================================================
# SEND EVENT
# =========================================================

def send_event(raw):

    with open("/tmp/auth.log", "a") as f:
        f.write(raw + "\n")

# =========================================================
# NORMAL MODE
# =========================================================

def normal_mode():

    print("🚀 Running NORMAL traffic...")

    while True:

        user = random.choice(users)

        ip = random.choice(normal_ips)

        success = random.random() < 0.94

        if success:

            raw = (
                f"login success "
                f"user={user} "
                f"ip={ip}"
            )

        else:

            raw = (
                f"login failed "
                f"user={user} "
                f"ip={ip}"
            )

        send_event(raw)

        print(raw)

        time.sleep(
            random.uniform(0.8, 2.5)
        )

# =========================================================
# BRUTE FORCE
# =========================================================

def brute_force_mode():

    print("🔥 Running BRUTE FORCE attack...")

    target_user = "admin"

    attacker_ip = "203.0.113.10"

    while True:

        raw = (
            f"login failed "
            f"user={target_user} "
            f"ip={attacker_ip}"
        )

        send_event(raw)

        print(raw)

        time.sleep(0.25)

# =========================================================
# PASSWORD SPRAYING
# =========================================================

def spraying_mode():

    print("⚠️ Running PASSWORD SPRAYING attack...")

    attacker_ip = "198.51.100.25"

    spray_users = [
        "alice",
        "bob",
        "charlie",
        "david",
        "eve",
        "finance",
        "hr",
        "devops",
    ]

    while True:

        user = random.choice(spray_users)

        raw = (
            f"login failed "
            f"user={user} "
            f"ip={attacker_ip}"
        )

        send_event(raw)

        print(raw)

        time.sleep(0.45)

# =========================================================
# LATERAL MOVEMENT
# =========================================================

def lateral_mode():

    print("🚨 Running LATERAL MOVEMENT simulation...")

    user = "admin"

    while True:

        ip = random.choice(
            lateral_ips
        )

        raw = (
            f"login success "
            f"user={user} "
            f"ip={ip}"
        )

        send_event(raw)

        print(raw)

        # slower to reduce flooding
        time.sleep(1.2)

# =========================================================
# ACCOUNT COMPROMISE
# =========================================================

def compromise_mode():

    print(
        "💀 Running ACCOUNT "
        "COMPROMISE simulation..."
    )

    target_user = "admin"

    attacker_ip = "203.0.113.200"

    # =====================================================
    # STAGE 1 — BRUTE FORCE
    # =====================================================

    for _ in range(12):

        raw = (
            f"login failed "
            f"user={target_user} "
            f"ip={attacker_ip}"
        )

        send_event(raw)

        print(raw)

        time.sleep(0.25)

    # =====================================================
    # STAGE 2 — SUCCESSFUL LOGIN
    # =====================================================

    time.sleep(3)

    raw = (
        f"login success "
        f"user={target_user} "
        f"ip={attacker_ip}"
    )

    send_event(raw)

    print(raw)

    print(
        "🔥 Compromise chain completed"
    )

# =========================================================
# MIXED MODE
# =========================================================

def mixed_mode():

    print("💀 Running MIXED ATTACK simulation...")

    brute_ip = "45.67.23.99"

    spray_ip = "198.51.100.90"

    while True:

        mode = random.choice([

            "normal",
            "normal",
            "normal",
            "normal",
            "brute",
            "spray",
            "lateral"
        ])

        # =================================================
        # NORMAL
        # =================================================

        if mode == "normal":

            user = random.choice(users)

            ip = random.choice(normal_ips)

            success = random.random() < 0.92

            if success:

                raw = (
                    f"login success "
                    f"user={user} "
                    f"ip={ip}"
                )

            else:

                raw = (
                    f"login failed "
                    f"user={user} "
                    f"ip={ip}"
                )

        # =================================================
        # BRUTE FORCE
        # =================================================

        elif mode == "brute":

            raw = (
                f"login failed "
                f"user=admin "
                f"ip={brute_ip}"
            )

        # =================================================
        # PASSWORD SPRAYING
        # =================================================

        elif mode == "spray":

            user = random.choice([
                "alice",
                "bob",
                "charlie",
                "finance",
                "hr",
                "devops",
            ])

            raw = (
                f"login failed "
                f"user={user} "
                f"ip={spray_ip}"
            )

        # =================================================
        # LATERAL MOVEMENT
        # =================================================

        else:

            ip = random.choice(
                lateral_ips
            )

            raw = (
                f"login success "
                f"user=admin "
                f"ip={ip}"
            )

        send_event(raw)

        print(raw)

        time.sleep(0.8)

# =========================================================
# MAIN
# =========================================================

if len(sys.argv) < 2:

    print(

        "Usage:\n"

        "python3 simulate_attacks.py normal\n"

        "python3 simulate_attacks.py brute_force\n"

        "python3 simulate_attacks.py spraying\n"

        "python3 simulate_attacks.py lateral\n"

        "python3 simulate_attacks.py compromise\n"

        "python3 simulate_attacks.py mixed"
    )

    sys.exit(1)

mode = sys.argv[1]

try:

    if mode == "normal":

        normal_mode()

    elif mode == "brute_force":

        brute_force_mode()

    elif mode == "spraying":

        spraying_mode()

    elif mode == "lateral":

        lateral_mode()

    elif mode == "compromise":

        compromise_mode()

    elif mode == "mixed":

        mixed_mode()

    else:

        print("❌ Invalid mode")

except KeyboardInterrupt:

    print("\n🛑 Simulation stopped")
