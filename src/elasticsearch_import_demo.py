"""
Script: elasticsearch_import_demo.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
FOR DEMO AND TESTING PURPOSES ONLY. This script pulls the most recent logs from an Elasticsearch index.
It uses a tracking index to remember the last fetch time so we only grab new logs.
The output is saved to JSON and used as input for downstream model evaluation.

This is part of the ETL flow before ML_batch_scan.py runs.
"""

import os
import json
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

# Main log index
INDEX = "logs-*"

# Where to save the output
OUTPUT_PATH = "../data/validation_dataset.json"

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# PoC testset
# 10 blocks of  15 minutes spread over weeks
start_times = [
    datetime(2025, 3, 8, 10, 0, tzinfo=timezone.utc),
    datetime(2025, 3, 16, 15, 0, tzinfo=timezone.utc),
    datetime(2025, 3, 25, 20, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 2, 2, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 9, 11, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 16, 9, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 23, 13, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 29, 17, 30, tzinfo=timezone.utc),
    datetime(2025, 5, 12, 0, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 14, 18, 0, tzinfo=timezone.utc)
]
''' [
    datetime(2025, 3, 8, 10, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 4, 15, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 3, 20, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 5, 2, 0, tzinfo=timezone.utc),
    datetime(2025, 5, 5, 11, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 22, 9, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 23, 13, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 24, 17, 30, tzinfo=timezone.utc),
    datetime(2025, 4, 25, 0, 0, tzinfo=timezone.utc),
    datetime(2025, 4, 27, 18, 0, tzinfo=timezone.utc)
]'''

duration = timedelta(minutes=15)

all_docs = []

# Fetch logs for each block
for start_time in start_times:
    end_time = start_time + duration
    print(f"Logs fetching from {start_time.isoformat()} until {end_time.isoformat()}...")

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
        print(f"Found logs: {len(docs)} logs")
        all_docs.extend(docs)

    except Exception as e:
        print(f"Error  {start_time} – {end_time}: {e}")

# Export combined logs to file in JSON
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_docs, f, indent=2)

print(f"Validation set saved to: {OUTPUT_PATH}")
