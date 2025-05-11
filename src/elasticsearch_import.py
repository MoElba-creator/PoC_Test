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

# Initialize client
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Calculate time window: now - 5 minutes
now = datetime.now(timezone.utc)
start_time = now - timedelta(minutes=5)
end_time = now

print(f"Fetching logs from {start_time.isoformat()} to {end_time.isoformat()}")

query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": start_time.isoformat(),
                "lte": end_time.isoformat()
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

    # Save logs to timestamped JSON
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)
    print(f"Saved logs to {OUTPUT_PATH}")

except Exception as e:
    print(f"Error fetching logs: {e}")
