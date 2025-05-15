import os
import json
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from dotenv import load_dotenv

# Load Elasticsearch config from .env
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX = "logs-*"
TRACKING_INDEX = "etl-log-tracking"  # New index to track last successful run
PIPELINE_NAME = "vives-etl"          # Identifier for this pipeline

# Initialize client
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Get last successful run timestamp
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
    return (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()  # fallback

# NEW: Save this run's end timestamp
def store_last_run_time(end_time):
    es.index(index=TRACKING_INDEX, document={
        "pipeline": PIPELINE_NAME,
        "last_run_time": end_time,
        "status": "success"
    })

# ─────────────────────────────────────────────────────
# Use real last run time
start_time = get_last_run_time()
end_time = datetime.now(timezone.utc).isoformat()

print(f"Fetching logs from {start_time} to {end_time}")

query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": start_time,
                "lte": end_time
            }
        }
    },
    "_source": True
}

OUTPUT_PATH = "../data/validation_logs_latest.json"


try:
    results = scan(es, query=query, index=INDEX, size=5000)
    docs = list(results)
    print(f"Retrieved {len(docs)} logs.")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Save logs to timestamped JSON
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)
    print(f"Saved logs to {OUTPUT_PATH}")

except Exception as e:
    print(f"Error fetching logs: {e}")

print(f"Storing run for pipeline {PIPELINE_NAME} at {end_time}")
store_last_run_time(end_time)
