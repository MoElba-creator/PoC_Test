"""
Script: elasticsearch_export.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script retrains the classification models using labeled feedback from analysts.
It loads the latest feedback logs, corrects column names, encodes features, adds an unsupervised anomaly score,
trains Random Forest, Logistic Regression, and XGBoost, and stores all models for validation and review.

The output includes candidate models and a validation set for offline evaluation.
"""

import os
import json
import pandas as pd
import joblib
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from category_encoders import HashingEncoder
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# Setup folder for this training run
today = datetime.now().strftime("%Y%m%d_%Hh")
RUN_DIR = Path(f"data/training_runs/{today}_candidate")
RUN_DIR.mkdir(parents=True, exist_ok=True)

# Input here is the feedback file exported via elasticsearch_export_feedback
INPUT_FILE = DATA_DIR / "latest_feedback.json"
if not INPUT_FILE.exists():
    print(f"File not found: {INPUT_FILE}")
    exit(1)

with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)

# Store raw copy in run folder
with open(RUN_DIR / "feedback.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
print(f"Feedback saved to: {RUN_DIR}/feedback.json")

# Load into DataFrame
df = pd.DataFrame(data)

# Restore original dot-named columns if they were flattened
RENAME_TO_DOT = {
    "source_ip": "source.ip",
    "destination_ip": "destination.ip",
    "network_transport": "network.transport",
    "event_action": "event.action",
    "tcp_flags": "tcp.flags",
    "agent_version": "agent.version",
    "fleet_action_type": "fleet.action.type",
    "source_port": "source.port",
    "destination_port": "destination.port",
    "session_iflow_bytes": "session.iflow_bytes",
    "session_iflow_pkts": "session.iflow_pkts",
    "flow_duration": "flow.duration"
}
df.rename(columns=RENAME_TO_DOT, inplace=True)

print(f"Columns available:\n{df.columns.tolist()}")

# Feature definitions
categorical = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair"
]

numeric = [
    "source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts",
    "flow_count_per_minute", "unique_dst_ports", "bytes_ratio",
    "port_entropy", "flow.duration", "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

required = categorical + numeric

# Check column presence
missing = [col for col in required if col not in df.columns]
if missing:
    print(f"Required columns missing: {missing}")
    exit(1)

# Filter usable feedback
df = df[df["user_feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user_feedback"].map({"correct": 1, "incorrect": 0})
df = df[required + ["label"]].dropna()
df[categorical] = df[categorical].astype(str)

# Encode high-cardinality features
encoder = HashingEncoder(cols=categorical, n_components=32)
X_cat = encoder.fit_transform(df[categorical])
X = pd.concat([X_cat.reset_index(drop=True), df[numeric].reset_index(drop=True)], axis=1)

# Add unsupervised feature which is Isolation Forest anomaly score
iso = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso.fit(X)
df["isoforest_score"] = iso.decision_function(X)
X["isoforest_score"] = df["isoforest_score"]

y = df["label"]

# Train/test split
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Train and export candidate models
models = {
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "logistic_regression": LogisticRegression(max_iter=1000),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="logloss")
}
MODEL_DIR.mkdir(parents=True, exist_ok=True)

for name, model in models.items():
    try:
        model.fit(X_train, y_train)
        model_path = MODEL_DIR / f"{name}_candidate.pkl"
        if name == "xgboost":
            joblib.dump({
                "model": model,
                "encoder": encoder,
                "columns": X_train.columns.tolist()
            }, model_path)
        else:
            joblib.dump(model, model_path)
        print(f" {name} model saved.")
    except Exception as e:
        print(f"Failed to train {name}: {e}")

# Save validation set
joblib.dump((X_val, y_val), RUN_DIR / "validation_set.pkl")
print(f"Validation set saved.")
