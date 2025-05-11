import os
import json
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from dotenv import load_dotenv
from pathlib import Path

# Configuration
load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies-realtime"
ALL_LOGS_INDEX = "network-anomalies-all-realtime"

# Automatically select the latest files
INPUT_JSON = "../data/predicted_anomalies_latest.json"
ALL_LOGS_JSON = "../data/all_evaluated_logs_latest.json"

if not os.path.exists(INPUT_JSON):
    print(f"File not found: {INPUT_JSON}")
    exit(1)
if not os.path.exists(ALL_LOGS_JSON):
    print(f"File not found: {ALL_LOGS_JSON}")
    exit(1)

print(f"Using anomaly file: {INPUT_JSON}")
print(f"Using full logs file: {ALL_LOGS_JSON}")

# Elasticsearch connection
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Load anomaly records
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    records = json.load(f)
print(f"Anomaly records loaded: {len(records)}")

df = pd.DataFrame(records)
df.columns = [col.replace(".", "_") for col in df.columns]
df["user_feedback"] = df.get("user_feedback", "unknown")
df["reviewed"] = df.get("reviewed", False)
df["batch_timestamp"] = datetime.utcnow().isoformat()
df = df.fillna("unknown")

# Send anomalies to Elasticsearch
try:
    success, _ = bulk(es, ({"_index": INDEX_NAME, "_source": row.to_dict()} for _, row in df.iterrows()))
    print(f"{success} anomaly records uploaded to: {INDEX_NAME}")
except BulkIndexError as e:
    print(f"{len(e.errors)} anomaly records failed.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))

# Load all evaluated logs
with open(ALL_LOGS_JSON, "r", encoding="utf-8") as f:
    full_records = json.load(f)
print(f"All evaluated records loaded: {len(full_records)}")

df_all = pd.DataFrame(full_records)
df_all.columns = [col.replace(".", "_") for col in df_all.columns]
df_all["user_feedback"] = df_all.get("user_feedback", "unknown")
df_all["reviewed"] = df_all.get("reviewed", False)
df_all["batch_timestamp"] = datetime.utcnow().isoformat()
df_all = df_all.fillna("unknown")

# Send full evaluated logs to Elasticsearch
try:
    success, _ = bulk(es, ({"_index": ALL_LOGS_INDEX, "_source": row.to_dict()} for _, row in df_all.iterrows()))
    print(f"{success} full records uploaded to: {ALL_LOGS_INDEX}")
except BulkIndexError as e:
    print(f"{len(e.errors)} full records failed.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))
