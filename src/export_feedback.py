import os
import json
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# === 1. Laden van omgeving ===
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
OUTPUT_FILE = "gelabelde_anomalieën.json"

# === 2. Connectie met Elasticsearch ===
es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
)

# === 3. Query: alleen logs met feedback ===
query = {
    "query": {
        "bool": {
            "should": [
                {"term": {"user_feedback.keyword": "correct"}},
                {"term": {"user_feedback.keyword": "incorrect"}}
            ]
        }
    },
    "size": 10000  # pas aan indien je meer verwacht
}

# === 4. Uitvoeren van zoekopdracht ===
res = es.search(index=INDEX_NAME, body=query)
hits = res["hits"]["hits"]
print(f"📄 Gevonden logs met feedback: {len(hits)}")

# === 5. Exporteren naar JSON ===
data = [hit["_source"] | {"_id": hit["_id"]} for hit in hits]

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"✅ Feedback geëxporteerd naar: {OUTPUT_FILE}")
