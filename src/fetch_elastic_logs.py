import os
import json
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from dotenv import load_dotenv

# === 1. Configuratie inladen uit .env ===
load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX = "logs-*"
OUTPUT_PATH = "../data/validation_dataset.json"

# === 2. Elasticsearch client instellen ===
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# === 3. Tijdsblokken definiëren ===
# 10 blokken van 10 minuten op verschillende dagen/tijdstippen
# Pas dit handmatig aan voor realistische spreiding
start_times = [
    datetime(2025, 4, 15, 10, 0, tzinfo=timezone.utc),  # werkdag ochtend
    datetime(2025, 4, 16, 15, 0, tzinfo=timezone.utc),  # werkdag namiddag
    datetime(2025, 4, 17, 20, 0, tzinfo=timezone.utc),  # werkdag avond
    datetime(2025, 4, 18, 2, 0, tzinfo=timezone.utc),   # nacht
    datetime(2025, 4, 20, 11, 0, tzinfo=timezone.utc),  # weekenddag
    datetime(2025, 4, 22, 9, 0, tzinfo=timezone.utc),   # werkdag ochtend
    datetime(2025, 4, 23, 13, 0, tzinfo=timezone.utc),  # lunch
    datetime(2025, 4, 24, 17, 30, tzinfo=timezone.utc), # avondspits
    datetime(2025, 4, 25, 0, 0, tzinfo=timezone.utc),   # middernacht
    datetime(2025, 4, 27, 18, 0, tzinfo=timezone.utc)   # weekend avond
]

duration = timedelta(minutes=10)  # elk blok = 10 minuten

all_docs = []

# === 4. Logs ophalen per tijdsblok ===
for start_time in start_times:
    end_time = start_time + duration
    print(f"📡 Logs ophalen van {start_time.isoformat()} tot {end_time.isoformat()}...")

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

    try:
        results = scan(es, query=query, index=INDEX, size=5000)
        docs = list(results)
        print(f"✅ Gevonden: {len(docs)} logs")
        all_docs.extend(docs)

    except Exception as e:
        print(f"⚠️  Fout bij ophalen blok {start_time} – {end_time}: {e}")

# === 5. Alles opslaan naar één JSON-bestand ===
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_docs, f, indent=2)

print(f"✅ Volledige validatieset opgeslagen naar: {OUTPUT_PATH}")
