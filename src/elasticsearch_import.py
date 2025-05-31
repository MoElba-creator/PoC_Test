"""
Script: elasticsearch_import.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script pulls the most recent logs from an Elasticsearch index.
It uses a tracking index to remember the last fetch time so we only grab new logs.
The output is saved to JSON and used as input for downstream model evaluation.

This is part of the ETL flow before ML_batch_scan.py runs.
"""

import os
import json
import sys
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

# Main log index and metadata tracking index
INDEX = "logs-*"
TRACKING_INDEX = "etl-log-tracking"
PIPELINE_NAME = "vives-etl"

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Return the last timestamp this pipeline was executed
def get_last_run_time():
    try:
        res = es.search(index=TRACKING_INDEX, body={
            "size": 1,
            "sort": [{"last_run_time": "desc"}],
            "query": {"term": {"pipeline": PIPELINE_NAME}}
        })
        if res["hits"]["hits"]:
            return res["hits"]["hits"][0]["_source"]["last_run_time"]
    except:
        pass
    # If tracking index fails or is empty then fallback to last 10 minutes
    return (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

# Write a new timestamp after successful fetch
def store_last_run_time(end_time):
    es.index(index=TRACKING_INDEX, document={
        "pipeline": PIPELINE_NAME,
        "last_run_time": end_time,
        "status": "success"
    })

start_time_str = get_last_run_time()
start_time_dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
FETCH_UP_TO_NOW_MARGIN = timedelta(seconds=10)
end_time_dt = datetime.now(timezone.utc) - FETCH_UP_TO_NOW_MARGIN
end_time_str = end_time_dt.isoformat(timespec='milliseconds') + 'Z'

if start_time_dt >= end_time_dt:
    print(f"No new logs to fetch: start time ({start_time_dt.isoformat()}) is equal or later than ({end_time_dt.isoformat()}).")
    sys.exit(0)

start_time_iso = start_time_dt.isoformat(timespec='milliseconds') + 'Z'
end_time_iso = end_time_dt.isoformat(timespec='milliseconds') + 'Z'


print(f"Fetching logs from {start_time_iso} to {end_time_iso}")

# Build Elasticsearch query to filter by @timestamp
query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": start_time_iso,
                "lte": end_time_iso
            }
        }
    },
    "_source": True
}

# Output path for the pulled logs
OUTPUT_PATH = "../data/validation_logs_latest.json"


try:
    # Stream through the result set
    results = scan(es, query=query, index=INDEX, size=5000)
    docs = list(results)
    print(f"Retrieved {len(docs)} logs.")

    # Make sure output dir exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Write to file as pretty JSON
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)
    print(f"Saved logs to {OUTPUT_PATH}")

except Exception as e:
    print(f"Error fetching logs: {e}")

# Register this run in the tracking index
print(f"Storing run for pipeline {PIPELINE_NAME} at {start_time_iso}")
store_last_run_time(end_time_iso)
