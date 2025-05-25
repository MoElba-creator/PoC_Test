"""
Script: elasticsearch_export.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script exports user feedback logs from Elasticsearch.
It checks when the last feedback export ran, then pulls all feedback labeled as "correct" or "incorrect" since that timestamp.
Two output files are created:
- a timestamped snapshot for backup
- a stable latest_feedback.json for retraining pipelines

Used as part of retrain_pipeline to extract feedback for model updates.
"""

import os
import json
from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import shutil
from pathlib import Path

# Load environment variables
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

INDEX_NAME = "network-anomalies"
TRACKING_INDEX = "etl-log-tracking"
PIPELINE_NAME = "vives-feedback-export"

# Generate file paths
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
SNAPSHOT_FILE = DATA_DIR / f"feedback_snapshot_{timestamp}.json"
LATEST_FILE = DATA_DIR / "latest_feedback.json"

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
)

# Retrieve timestamp of last successful export
def get_last_export_time():
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
    return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

# Save current run timestamp
def store_export_time(end_time):
    es.index(index=TRACKING_INDEX, document={
        "pipeline": PIPELINE_NAME,
        "last_run_time": end_time,
        "status": "success"
    })

# Define query range and feedback filter
start_time = get_last_export_time()
end_time = datetime.now(timezone.utc).isoformat()
print(f"Fetching feedback between {start_time} and {end_time}")

query = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "feedback_timestamp": {
                            "gte": start_time,
                            "lte": end_time
                        }
                    }
                },
                {
                    "terms": {
                        "user_feedback.keyword": ["correct", "incorrect"]
                    }
                }
            ]
        }
    }
}

# Execute query and collect hits
try:
    resp = es.search(index=INDEX_NAME, body=query, scroll="2m", size=1000)
    sid = resp["_scroll_id"]
    all_hits = resp["hits"]["hits"]
    scroll_size = len(all_hits)

    while scroll_size > 0:
        resp = es.scroll(scroll_id=sid, scroll="2m")
        sid = resp["_scroll_id"]
        hits = resp["hits"]["hits"]
        scroll_size = len(hits)
        all_hits.extend(hits)

    print(f"Retrieved {len(all_hits)} feedback logs")

except Exception as e:
    print(f"Failed to fetch logs from Elasticsearch: {e}")
    exit(1)

# Save snapshot and copy to latest if logs exist
if not all_hits:
    print("No feedback logs found — skipping export.")
    exit(0)

try:
    os.makedirs(DATA_DIR, exist_ok=True)
    logs = []
    for hit in all_hits:
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        logs.append(doc)

    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)
    print(f"Feedback exported to: {SNAPSHOT_FILE}")

    shutil.copy(SNAPSHOT_FILE, LATEST_FILE)
    print(f"Copied to latest: {LATEST_FILE}")

    store_export_time(end_time)

except Exception as e:
    print(f"Failed to write feedback JSON: {e}")
    exit(1)
