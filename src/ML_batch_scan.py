import pandas as pd
import joblib
import json
import os
from category_encoders import HashingEncoder
from sklearn.ensemble import IsolationForest

# Read JSON file
with open("../data/validation_dataset.json", "r", encoding="utf-8") as f:
    records = json.load(f)

print("Amount of records loaded: ", len(records))

# Extract _source from each record
flattened_data = [r["_source"] for r in records if "_source" in r]
df = pd.json_normalize(flattened_data)

# Choose relevant fields
relevant_columns =  [
    "source.ip", "destination.ip",
    "source.port", "destination.port",
    "network.transport",
    "session.iflow_bytes", "session.iflow_pkts",
    "event.action", "session.id", "label"
]

# Only select columns that are present in the actual JSON data
available_columns = [col for col in relevant_columns if col in df.columns]
if not available_columns:
    raise ValueError("None of the expected columns are found in the JSON.")

df_selected = df[available_columns].copy()

print("Selected columns:", available_columns)
print("Sample data:\n", df_selected.head())

# Keep only available fields
available_columns = [col for col in relevant_columns if col in df.columns]
df_selected = df[available_columns].copy()

# Clean numerical columns
numeric_columns = [col for col in ["source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts"] if col in df_selected.columns]
for col in numeric_columns:
    df_selected[col] = pd.to_numeric(df_selected[col], errors="coerce")

df_selected = df_selected.dropna()
print("Number of rows after cleanup: ", len(df_selected))

# Encode for unsupervised model
X_unlabeled = df_selected.copy()
for col in X_unlabeled.select_dtypes(include=["object"]).columns:
    X_unlabeled[col] = X_unlabeled[col].astype(str)

encoder_unsupervised = HashingEncoder(n_components=8)
X_unlabeled_encoded = encoder_unsupervised.fit_transform(X_unlabeled)

# Isolation Forest scoring
iso_forest = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
df.loc[df_selected.index, "isoforest_score"] = iso_forest.fit_predict(X_unlabeled_encoded)
df.loc[df_selected.index, "isoforest_score"] = iso_forest.decision_function(X_unlabeled_encoded)

# Retrain encoder directly on relevant fields
encoder_input_columns = ["source.ip", "destination.ip", "network.transport", "event.action"]

encoder = HashingEncoder(cols=encoder_input_columns, n_components=8)
encoder.fit(df_selected[encoder_input_columns])


# Select only the columns the encoder was trained on
encoder_input_columns = ["source.ip", "destination.ip", "network.transport", "event.action"]
X_encoded = encoder.transform(df_selected[encoder_input_columns])

# Adding remaining numeric features now
X_encoded["source.port"] = df_selected["source.port"]
X_encoded["destination.port"] = df_selected["destination.port"]
X_encoded["session.iflow_bytes"] = df_selected["session.iflow_bytes"]
X_encoded["session.iflow_pkts"] = df_selected["session.iflow_pkts"]

X_encoded["isoforest_score"] = df.loc[df_selected.index, "isoforest_score"]


rf_model = joblib.load("../models/random_forest_model.pkl")
log_model = joblib.load("../models/logistic_regression_model.pkl")
xgb_model = joblib.load("../models/xgboost_model.pkl")

df.loc[df_selected.index, "RF_pred"] = rf_model.predict(X_encoded)
df.loc[df_selected.index, "LOG_pred"] = log_model.predict(X_encoded)
df.loc[df_selected.index, "XGB_pred"] = xgb_model.predict(X_encoded)

# Calculate probability scores
df.loc[df_selected.index, "RF_score"] = rf_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "LOG_score"] = log_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "XGB_score"] = xgb_model.predict_proba(X_encoded)[:, 1]

# AVerage model score
df["model_score"] = df[["RF_score", "LOG_score", "XGB_score"]].mean(axis=1)

# Add default feedback fields for future manual review and false negatives
df.loc[df_selected.index, "user_feedback"] = None
df.loc[df_selected.index, "reviewed"] = False

# Export full evaluated logs
df_all_evaluated = df.loc[df_selected.index, [
    *available_columns,
    "isoforest_score",
    "RF_pred", "LOG_pred", "XGB_pred",
    "RF_score", "LOG_score", "XGB_score",
    "model_score",
    "user_feedback", "reviewed"
]]

# Save it as a separate file
all_logs_output_path = "../data/all_evaluated_logs.json"
df_all_evaluated.to_json(all_logs_output_path, orient="records", indent=2)

print(f"Full evaluated logs saved for post-hoc review: {all_logs_output_path}")

# Select flags by models
df_anomalies = df[
    (df["RF_pred"] == 1) |
    (df["LOG_pred"] == 1) |
    (df["XGB_pred"] == 1)
].copy()
print(f"Total number of anomalies predicted: {len(df_anomalies)}")

# Filter out low risk ports & IP's.
LOW_RISK_PORTS = {67, 68, 123, 161, 162, 443, 53, 9200}
df_anomalies_filtered = df_anomalies[
    ~df_anomalies["destination.port"].isin(LOW_RISK_PORTS)
]

TRUSTED_SOURCE_IPS = {"10.192.96.7", "10.192.96.8", "10.192.96.4"}
df_anomalies_filtered = df_anomalies_filtered[
    ~df_anomalies_filtered["source.ip"].isin(TRUSTED_SOURCE_IPS)
]

TRUSTED_DEST_IPS = {"193.190.77.36"}
df_anomalies_filtered = df_anomalies_filtered[
    ~df_anomalies_filtered["destination.ip"].isin(TRUSTED_DEST_IPS)
]

print(f"Final amount of anomalies: {len(df_anomalies_filtered)}")

# Attributes for feedback
df_anomalies_filtered["user_feedback"] = None
df_anomalies_filtered["reviewed"] = False

# Export JSON
output_path = "../data/predicted_anomalies.json"
records = df_anomalies_filtered.to_dict(orient="records")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print(f"Anomalies successfully exported to: {output_path}")
