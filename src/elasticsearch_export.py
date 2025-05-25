"""
Script: elasticsearch_export.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script exports anomaly detection results back into Elasticsearch.
It pushes two sets of data:
1. Anomalies that were predicted as suspicious.
2. All evaluated logs so normal and anomalous.

Used for visualizations, feedback loops and a Kibana dashboard.
"""

import os
import json
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from dotenv import load_dotenv

# Load environment config
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

# Target indices in Elasticsearch
INDEX_NAME = "network-anomalies-realtime"
ALL_LOGS_INDEX = "network-anomalies-all-realtime"

# Automatically select the latest files
INPUT_JSON = "../data/predicted_anomalies_latest.json"
ALL_LOGS_JSON = "../data/all_evaluated_logs_latest.json"

# Fallback check
if not os.path.exists(INPUT_JSON):
    print(f"File not found: {INPUT_JSON}")
    exit(1)
if not os.path.exists(ALL_LOGS_JSON):
    print(f"File not found: {ALL_LOGS_JSON}")
    exit(1)

print(f"Using anomaly file: {INPUT_JSON}")
print(f"Using full logs file: {ALL_LOGS_JSON}")

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Load anomaly records and prepare them
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    records = json.load(f)
print(f"Anomaly records loaded: {len(records)}")

df = pd.DataFrame(records)
df.columns = [col.replace(".", "_") for col in df.columns]
df["user_feedback"] = df.get("user_feedback", "unknown")
df["reviewed"] = df.get("reviewed", False)
df["batch_timestamp"] = datetime.utcnow().isoformat()
df = df.fillna("unknown")

# Upload anomaly data to index
try:
    success, _ = bulk(es, ({"_index": INDEX_NAME, "_source": row.to_dict()} for _, row in df.iterrows()))
    print(f"{success} anomaly records uploaded to: {INDEX_NAME}")
except BulkIndexError as e:
    print(f"{len(e.errors)} anomaly records failed.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))

# Load full evaluated logs @todo: performance
with open(ALL_LOGS_JSON, "r", encoding="utf-8") as f:
    full_records = json.load(f)
print(f"All evaluated records loaded: {len(full_records)}")

df_all = pd.DataFrame(full_records)
df_all.columns = [col.replace(".", "_") for col in df_all.columns]
df_all["user_feedback"] = df_all.get("user_feedback", "unknown")
df_all["reviewed"] = df_all.get("reviewed", False)
df_all["batch_timestamp"] = datetime.utcnow().isoformat()
df_all = df_all.fillna("unknown")

# Upload all evaluated logs
try:
    success, _ = bulk(es, ({"_index": ALL_LOGS_INDEX, "_source": row.to_dict()} for _, row in df_all.iterrows()))
    print(f"{success} full records uploaded to: {ALL_LOGS_INDEX}")
except BulkIndexError as e:
    print(f"{len(e.errors)} full records failed.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))
