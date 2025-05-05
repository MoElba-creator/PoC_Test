import os
import json
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load new environment
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
OUTPUT_FILE = "gelabelde_anomalieën.json"

# Elasticsearch connection
es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
)

# Fetch logs with feedback
query = {
    "query": {
        "bool": {
            "should": [
                {"term": {"user_feedback.keyword": "correct"}},
                {"term": {"user_feedback.keyword": "incorrect"}}
            ]
        }
    },
    "size": 100000
}

# Execution of query
res = es.search(index=INDEX_NAME, body=query)
hits = res["hits"]["hits"]
print(f"{len(hits)} logs found with feedback.")

# === 5. Exporteren naar JSON ===
data = [hit["_source"] | {"_id": hit["_id"]} for hit in hits]

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Feedback exported to: {OUTPUT_FILE}")
