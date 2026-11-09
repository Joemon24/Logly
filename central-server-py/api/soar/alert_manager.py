from elasticsearch import Elasticsearch
from datetime import datetime, timezone

from api.settings import elasticsearch_client

es = elasticsearch_client()

ALERT_INDEX = "ids-alerts"


def create_alert(event):

    alert = {

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "attack_type": event.get(
            "alert_type"
        ),

        "severity": event.get(
            "severity"
        ),

        "source_ip": event.get(
            "src_ip"
        ),

        "username": event.get(
            "username"
        ),

        "mitre": event.get(
            "mitre_technique"
        ),

        "anomaly_score": event.get(
            "anomaly_score"
        ),

        "trust_score": event.get(
            "trust_score"
        ),
    }

    es.index(
        index=ALERT_INDEX,
        id=event["storage_id"],
        document=alert
    )

    print(
        f"[SOAR] Alert created for "
        f"{alert['source_ip']}"
    )
