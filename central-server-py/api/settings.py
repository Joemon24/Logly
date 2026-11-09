"""Shared connection settings. TLS verification is always enabled."""
import os
from elasticsearch import Elasticsearch


def elasticsearch_client():
    options = {"verify_certs": True, "request_timeout": 30}
    if os.getenv("ES_API_KEY"):
        options["api_key"] = os.environ["ES_API_KEY"]
    if os.getenv("ES_CA_CERTS"):
        options["ca_certs"] = os.environ["ES_CA_CERTS"]
    return Elasticsearch(os.getenv("ES_HOST", "http://localhost:9200"), **options)
