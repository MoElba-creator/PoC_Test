"""
Script: elasticsearch_export_demo.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script exports DEMO anomaly detection results back into Elasticsearch.
It pushes two sets of data:
1. Anomalies that were predicted as suspicious.
2. All evaluated logs so normal and anomalous.

Used for visualizations, feedback loops and a Kibana dashboard.
"""

import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from dotenv import load_dotenv

# Load config from .env
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

# Elasticsearch target indices for demo
INDEX_NAME = "network-anomalies"
ALL_LOGS_INDEX = "network-anomalies-all"

# Input files for demo export
INPUT_JSON = "../data/predicted_anomalies.json"
ALL_LOGS_JSON = "../data/all_evaluated_logs.json"

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Load anomaly records
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    records = json.load(f)

print(f"JSON records loaded: {len(records)}")

df = pd.DataFrame(records)
# Underscore replace for Elasticserach export
df.columns = [col.replace(".", "_") for col in df.columns]

# User feedback
if "user_feedback" not in df.columns:
    df["user_feedback"] = "unknown"
else:
    df["user_feedback"] = df["user_feedback"].fillna("unknown")

if "reviewed" not in df.columns:
    df["reviewed"] = False
else:
    df["reviewed"] = df["reviewed"].fillna(False)

df = df.fillna("unknown")

# Conversion to elasticsearch format
def df_to_elastic_format(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

# Upload anomaly data to index
try:
    success, _ = bulk(es, df_to_elastic_format(df, INDEX_NAME))
    print(f""
          f"{success} records succesfully uploaded to : {INDEX_NAME}")
except BulkIndexError as e:
    print(f"{len(e.errors)} failed to send.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))

# Upload all evaluated logs
with open(ALL_LOGS_JSON, "r", encoding="utf-8") as f:
    full_records = json.load(f)

print(f"All evaluated records loaded: {len(full_records)}")

df_all = pd.DataFrame(full_records)
df_all.columns = [col.replace(".", "_") for col in df_all.columns]

# Add default feedback if missing
if "user_feedback" not in df_all.columns:
    df_all["user_feedback"] = "unknown"
else:
    df_all["user_feedback"] = df_all["user_feedback"].fillna("unknown")

if "reviewed" not in df_all.columns:
    df_all["reviewed"] = False
else:
    df_all["reviewed"] = df_all["reviewed"].fillna(False)

df_all = df_all.fillna("unknown")

# Convert to Elasticsearch bulk format
def df_to_elastic_format(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

# Upload all logs to separate index
try:
    success, _ = bulk(es, df_to_elastic_format(df_all, ALL_LOGS_INDEX))
    print(f"{success} records uploaded to: {ALL_LOGS_INDEX}")
except BulkIndexError as e:
    print(f"{len(e.errors)} failed to send to {ALL_LOGS_INDEX}.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))
