> **Release candidate:** See [readiness report](docs/READINESS.md) and [Linux deployment guide](docs/LINUX-DEPLOYMENT.md). This build uses real log inputs, defaults to detection-only recommendations, and does not implement firewall blocking. Historical feature claims below are not a production-readiness guarantee.

# Distributed AI-Powered IDS + SOAR Platform

A distributed Intrusion Detection and Response System built using:

- Go Agents
- Apache Kafka
- Python ML Detection Engine
- Elasticsearch
- Kibana
- SOAR Automation

This project detects:
- Brute Force Attacks
- Password Spraying
- Lateral Movement
- Behavioral Anomalies

using Machine Learning + Rule-Based Detection.

---

# Architecture

Attack Simulator
        ↓
Kafka Producer
        ↓
Kafka Broker
        ↓
Python Detection Engine
        ↓
Feature Engineering
        ↓
Isolation Forest ML
        ↓
Risk Scoring Engine
        ↓
SOAR Automation
        ↓
Elasticsearch
        ↓
Kibana Dashboard

---

# Features

## Distributed Agents
- Go-based telemetry agents
- Kafka streaming
- Heartbeat monitoring

## AI Detection Engine
- Isolation Forest anomaly detection
- Behavioral analytics
- Dynamic trust scoring

## Attack Detection
- Brute Force Detection
- Password Spraying Detection
- Lateral Movement Detection
- Generic Anomaly Detection

## SOAR Automation
- Alert creation
- Incident creation
- Automatic IP blocking
- Notification system

## MITRE ATT&CK Mapping
- T1110
- T1110.003
- TA0008
- T1078

## Dashboarding
- Kibana visualizations
- Real-time security analytics
- Alerts & incident tracking

---

# Technologies Used

- Python
- Go
- Apache Kafka
- Elasticsearch
- Kibana
- Scikit-learn
- NumPy

---

# Setup Instructions

## 1. Clone Repository

```bash
git clone <YOUR_REPO_URL>
cd distributed-ids
```
2. Install Python Dependencies
```bash
cd central-server-py
pip install -r requirements.txt
```
3. Start Kafka

Ensure Kafka is running on:
```
localhost:19092
```
4. Start Elasticsearch
```
http://localhost:9200
```
5. Start Kibana
```
http://localhost:5601
```
6. Run Detection Engine
```
cd central-server-py

python -m api.kafka_consumer
```
7. Run Agent
```
cd agent

go run cmd/agent/main.go
```
8. Simulate Attacks

Normal Traffic
```
python3 simulate_attacks.py normal
```
Brute Force
```
python3 simulate_attacks.py brute_force
```
Password Spraying
```
python3 simulate_attacks.py spraying
```
Lateral Movement
```
python3 simulate_attacks.py lateral
```
Mixed Attack Mode
```
python3 simulate_attacks.py mixed
```
Elasticsearch Indices

* ids-events
* ids-alerts
* ids-incidents
* ids-blocked

Kibana Dashboard

Import the exported Kibana .ndjson dashboard file from:

```
Stack Management → Saved Objects → Import
```
Detection Logic

Brute Force

* High fail streak
* High failed ratio
* Repeated failures against same user

Password Spraying

* Same IP attacking many users
* High failed ratio
* Rapid authentication attempts

Lateral Movement

* Same user accessing many internal systems rapidly

⸻

Future Improvements

* Docker Compose deployment
* Real-time WebSocket notifications
* Firewall integration
* Slack/Telegram alerts
* Advanced ML models
* Threat intelligence integration
* MITRE ATT&CK heatmaps

Author

Joemon Joy



