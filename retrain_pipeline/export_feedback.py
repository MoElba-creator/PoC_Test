import os
import json
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import pandas as pd

# === 1. Laad omgevingsvariabelen ===
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
OUTPUT_FILE = "data/gelabelde_anomalieën.json"

# === 2. Verbinden met Elasticsearch ===
try:
    es = Elasticsearch(
        hosts=[ES_HOST],
        api_key=ES_API_KEY,
        headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    )
    if not es.ping():
        raise ValueError("❌ Kan geen verbinding maken met Elasticsearch.")
    print("✅ Verbonden met Elasticsearch.")
except Exception as e:
    print(f"❌ Elasticsearch connectieprobleem: {e}")
    exit(1)

# === 3. Query: haal alleen gelabelde feedback op ===
query = {
    "query": {
        "bool": {
            "should": [
                {"term": {"user_feedback.keyword": "correct"}},
                {"term": {"user_feedback.keyword": "incorrect"}}
            ]
        }
    }
}

# === 4. Scroll gebruiken om grote datasets op te halen ===
try:
    page = es.search(
        index=INDEX_NAME,
        body=query,
        scroll="2m",
        size=1000
    )
    sid = page["_scroll_id"]
    scroll_size = len(page["hits"]["hits"])
    all_hits = page["hits"]["hits"]

    while scroll_size > 0:
        page = es.scroll(scroll_id=sid, scroll="2m")
        sid = page["_scroll_id"]
        hits = page["hits"]["hits"]
        scroll_size = len(hits)
        all_hits.extend(hits)

    print(f"📄 Aantal logs met feedback gevonden: {len(all_hits)}")

except Exception as e:
    print(f"❌ Fout bij ophalen van feedback logs: {e}")
    exit(1)

# === 5. JSON normaliseren en exporteren ===
try:
    from pandas import json_normalize

    sources = [hit["_source"] | {"_id": hit["_id"]} for hit in all_hits]
    df = json_normalize(sources)

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, indent=2, ensure_ascii=False)

    print(f"✅ Feedback geëxporteerd naar: {OUTPUT_FILE}")
except Exception as e:
    print(f"❌ Fout bij schrijven van JSON-bestand: {e}")
    exit(1)
