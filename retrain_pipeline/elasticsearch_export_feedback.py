import os
import json
from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# === 1. Load environment variables ===
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
TRACKING_INDEX = "etl-log-tracking"
PIPELINE_NAME = "vives-feedback-export"
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_FILE = f"data/feedback_snapshot_{timestamp}.json"
LATEST_SYMLINK = "data/latest_feedback.json"

# === 2. Connect to Elasticsearch ===
es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
)

# === 3. Fetch last export timestamp ===
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
    return (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()  # fallback: 1 day ago

# === 4. Store new export timestamp ===
def store_export_time(end_time):
    es.index(index=TRACKING_INDEX, document={
        "pipeline": PIPELINE_NAME,
        "last_run_time": end_time,
        "status": "success"
    })

# === 5. Define time range and query ===
start_time = get_last_export_time()
end_time = datetime.now(timezone.utc).isoformat()
print(f"Fetching feedback between {start_time} and {end_time}")

query = {
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "@timestamp": {
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

# === 6. Execute query and scroll through results ===
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

# === 7. Save to JSON file ===
try:
    os.makedirs("data", exist_ok=True)
    logs = []
    for hit in all_hits:
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        logs.append(doc)

    # Save timestamped version
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)
    print(f"Feedback successfully exported to: {OUTPUT_FILE}")

except Exception as e:
    print(f"Failed to write JSON file: {e}")
    exit(1)
