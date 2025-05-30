"""
Script: ML_batch_scan.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script performs batch classification of incoming logs using trained models.
It predicts if traffic is anomalous or normal and writes results to output files.

What it does:
1. Loads SELECTED validation logs exported from elasticsearch.
2. Does inline feature engineering via build_df.
3. Encodes fields using pretrained hashing encoder.
4. Adds isolation forest anomaly score.
5. Loads three trained models and predicts labels.
6. Combines predictions using majority voting.
7. Filters out false positives using trusted ip and port filters.
"""

import os
import pandas as pd
import joblib
import json
from sklearn.ensemble import IsolationForest
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from synthetic_data_creation import build_df


# Config
DATA_DIR = Path("../data")
LATEST_LOGS_FILE = str(DATA_DIR / "validation_logs_latest.json")
PATH_OUTPUT_ALL = str(DATA_DIR / "all_evaluated_logs_latest.json")
PATH_OUTPUT_ANOMALIES = str(DATA_DIR / "predicted_anomalies_latest.json")

# Feature columns
RELEVANT_COLUMNS = [
    "source.ip", "destination.ip", "source.port", "destination.port", "network.transport",
    "session.iflow_bytes", "session.iflow_pkts", "event.action", "session.id",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair", "flow_count_per_minute", "unique_dst_ports",
    "bytes_ratio", "port_entropy", "flow.duration", "bytes_per_pkt", "msg_code",
    "is_suspicious_ratio"
]

NUMERIC_COLUMNS = [
    "source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts",
    "flow_count_per_minute", "unique_dst_ports", "bytes_ratio", "port_entropy",
    "flow.duration", "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

ENCODER_INPUT_COLUMNS = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair"
]

LOW_RISK_PORTS = {67, 68, 123, 161, 162, 443, 53, 9200}
TRUSTED_SOURCE_IPS = {"10.192.96.7", "10.192.96.8", "10.192.96.4"}
TRUSTED_DEST_IPS = {"193.190.77.36", "10.192.72.4", "193.190.147.185"}
# ──────────────────────────────────────────────

# Load data
with open(LATEST_LOGS_FILE, "r", encoding="utf-8") as f:
    records = json.load(f)
flattened_data = [r["_source"] for r in records if "_source" in r]
df = pd.json_normalize(flattened_data)
print("Records loaded:", len(df))

# Enrich logs with engineered features
df = build_df(df)

# Drop if critical fields missing
critical = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts"
]
print("Missing values before drop:")
print(df[critical].isnull().sum())

df.dropna(subset=critical, inplace=True)
print("Number of rSows after cleanup:", len(df))

# Encode features with pre-trained encoder
MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
xgb_bundle = joblib.load(os.path.join(MODEL_DIR, "xgboost_model.pkl"))
xgb_model = xgb_bundle["model"]
xgb_encoder = xgb_bundle["encoder"]
xgb_expected_columns = xgb_bundle["columns"]

# Prepare encoded features
df[ENCODER_INPUT_COLUMNS] = df[ENCODER_INPUT_COLUMNS].astype(str)
X_encoded = xgb_encoder.transform(df[ENCODER_INPUT_COLUMNS])

# Add numeric features
for col in NUMERIC_COLUMNS:
    X_encoded[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# Add anomaly score using Isolation Forest
iso_forest = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso_forest.fit(X_encoded)
df["isoforest_score"] = iso_forest.decision_function(X_encoded)
X_encoded["isoforest_score"] = df["isoforest_score"]

# Prepare XGBoost input
X_encoded_xgb = X_encoded.copy()
X_encoded_xgb = X_encoded_xgb[xgb_expected_columns]

# Load other trained models
rf_model = joblib.load(os.path.join(MODEL_DIR, "random_forest_model.pkl"))
log_model = joblib.load(os.path.join(MODEL_DIR, "logistic_regression_model.pkl"))

# Make predictions
df["RF_pred"] = rf_model.predict(X_encoded)
df["LOG_pred"] = log_model.predict(X_encoded)
df["XGB_pred"] = xgb_model.predict(X_encoded_xgb)

df["RF_score"] = rf_model.predict_proba(X_encoded)[:, 1]
df["LOG_score"] = log_model.predict_proba(X_encoded)[:, 1]
df["XGB_score"] = xgb_model.predict_proba(X_encoded_xgb)[:, 1]

# Combine scores to get an average confidence
df["model_score"] = df[["RF_score", "LOG_score", "XGB_score"]].mean(axis=1)

# Add feedback placeholders
df["user_feedback"] = None
df["reviewed"] = False

# Save full output
df.to_json(PATH_OUTPUT_ALL, orient="records", indent=2)
print(f"✔ All evaluated logs saved to: {PATH_OUTPUT_ALL}")

# Apply majority voting: Anomaly if 2 out of 3 models say so.
model_preds = df[["RF_pred", "LOG_pred", "XGB_pred"]].sum(axis=1)
df_anomalies = df[model_preds >= 2]
print(f"\nTotal anomalies predicted by majority voting: {len(df_anomalies)}")

# Filter out safe traffic
df_anomalies_filtered = df_anomalies[
    ~df_anomalies["destination.port"].isin(LOW_RISK_PORTS) &
    ~df_anomalies["source.ip"].isin(TRUSTED_SOURCE_IPS) &
    ~df_anomalies["destination.ip"].isin(TRUSTED_DEST_IPS)
].copy()
print(f"Final filtered anomalies: {len(df_anomalies_filtered)}")

df_anomalies_filtered["user_feedback"] = None
df_anomalies_filtered["reviewed"] = False
df_anomalies_filtered.to_json(PATH_OUTPUT_ANOMALIES, orient="records", indent=2)
print(f"Anomalies saved to: {PATH_OUTPUT_ANOMALIES}")
