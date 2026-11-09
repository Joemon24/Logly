import uuid

from elasticsearch import Elasticsearch
from datetime import datetime, timezone

from api.settings import elasticsearch_client

es = elasticsearch_client()

INCIDENT_INDEX = "ids-incidents"


def create_incident(event):

    incident_id = str(uuid.uuid4())[:8]

    incident = {

        "incident_id": incident_id,

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "status": "open",

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
    }

    es.index(
        index=INCIDENT_INDEX,
        id=event["storage_id"],
        document=incident
    )

    print(
        f"[SOAR] Incident created: "
        f"{incident_id}"
    )
