import pandas as pd
import joblib
import json
from sklearn.ensemble import IsolationForest

# ──────────────────────────────────────────────
# CONFIGURATION
RELEVANT_COLUMNS = [
    "source.ip", "destination.ip",
    "source.port", "destination.port",
    "network.transport",
    "session.iflow_bytes", "session.iflow_pkts",
    "event.action", "session.id",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair",
    "flow_count_per_minute", "unique_dst_ports",
    "bytes_ratio", "port_entropy", "flow.duration",
    "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

NUMERIC_COLUMNS = [
    "source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts",
    "flow_count_per_minute", "unique_dst_ports", "bytes_ratio",
    "port_entropy", "flow.duration", "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

ENCODER_INPUT_COLUMNS = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair"
]

LOW_RISK_PORTS = {67, 68, 123, 161, 162, 443, 53, 9200}
TRUSTED_SOURCE_IPS = {"10.192.96.7", "10.192.96.8", "10.192.96.4"}
TRUSTED_DEST_IPS = {"193.190.77.36"}

PATH_VALIDATION = "../data/validation_dataset.json"
PATH_OUTPUT_ALL = "../data/all_evaluated_logs.json"
PATH_OUTPUT_ANOMALIES = "../data/predicted_anomalies.json"
# ──────────────────────────────────────────────

# LOAD DATA
with open(PATH_VALIDATION, "r", encoding="utf-8") as f:
    records = json.load(f)
flattened_data = [r["_source"] for r in records if "_source" in r]
df = pd.json_normalize(flattened_data)
print("Records loaded:", len(df))

# FILL MISSING COLUMNS WITH DEFAULTS
safe_fill = {
    "tcp.flags": "UNKNOWN",
    "proto_port_pair": "UNKNOWN",
    "version_action_pair": "UNKNOWN",
    "flow_count_per_minute": 0,
    "unique_dst_ports": 0,
    "bytes_ratio": 0.0,
    "port_entropy": 0.0,
    "flow.duration": 0,
    "bytes_per_pkt": 0.0,
    "msg_code": 0,
    "is_suspicious_ratio": False
}

for col, default in safe_fill.items():
    if col not in df.columns:
        df[col] = default
    df[col] = df[col].fillna(default)

# DROP ONLY CRITICAL MISSING VALUES
critical = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts"
]
print("Missing values before drop:")
print(df[critical].isnull().sum())

df.dropna(subset=critical, inplace=True)
print("Number of rSows after cleanup:", len(df))

# 4. ENCODE CATEGORICAL FEATURES
encoder = joblib.load("../models/ip_encoder_hashing.pkl")
df[ENCODER_INPUT_COLUMNS] = df[ENCODER_INPUT_COLUMNS].astype(str)
X_encoded = encoder.transform(df[ENCODER_INPUT_COLUMNS])

for col in NUMERIC_COLUMNS:
    X_encoded[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# 5. UNSUPERVISED ANOMALY SCORE
iso_forest = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso_forest.fit(X_encoded)
df["isoforest_score"] = iso_forest.decision_function(X_encoded)
X_encoded["isoforest_score"] = df["isoforest_score"]

# 6. XGBOOST ENCODING
xgb_bundle = joblib.load("../models/xgboost_model.pkl")
xgb_model = xgb_bundle["model"]
xgb_encoder = xgb_bundle["encoder"]
xgb_expected_columns = xgb_bundle["columns"]

X_encoded_xgb = xgb_encoder.transform(df[ENCODER_INPUT_COLUMNS])
for col in NUMERIC_COLUMNS:
    X_encoded_xgb[col] = df[col]
X_encoded_xgb["isoforest_score"] = df["isoforest_score"]
X_encoded_xgb = X_encoded_xgb[xgb_expected_columns]

# 7. LOAD SUPERVISED MODELS
rf_model = joblib.load("../models/random_forest_model.pkl")
log_model = joblib.load("../models/logistic_regression_model.pkl")

# 8. PREDICT
df["RF_pred"] = rf_model.predict(X_encoded)
df["LOG_pred"] = log_model.predict(X_encoded)
df["XGB_pred"] = xgb_model.predict(X_encoded_xgb)

df["RF_score"] = rf_model.predict_proba(X_encoded)[:, 1]
df["LOG_score"] = log_model.predict_proba(X_encoded)[:, 1]
df["XGB_score"] = xgb_model.predict_proba(X_encoded_xgb)[:, 1]
df["model_score"] = df[["RF_score", "LOG_score", "XGB_score"]].mean(axis=1)

# 9. FEEDBACK PLACEHOLDERS
df["user_feedback"] = None
df["reviewed"] = False

# 10. EXPORT FULL EVALUATED
df.to_json(PATH_OUTPUT_ALL, orient="records", indent=2)
print(f"✔ Full evaluated logs saved to: {PATH_OUTPUT_ALL}")

# 11. ANOMALY SELECTION
# Apply voting-based anomaly filter: at least 2 out of 3 models must agree
model_preds = df[["RF_pred", "LOG_pred", "XGB_pred"]].sum(axis=1)
df_anomalies = df[model_preds >= 2]
print(f"\nTotal anomalies predicted by majority voting: {len(df_anomalies)}")

# 12. FILTERED ANOMALIES
df_anomalies_filtered = df_anomalies[
    ~df_anomalies["destination.port"].isin(LOW_RISK_PORTS) &
    ~df_anomalies["source.ip"].isin(TRUSTED_SOURCE_IPS) &
    ~df_anomalies["destination.ip"].isin(TRUSTED_DEST_IPS)
].copy()
print(f"Final filtered anomalies: {len(df_anomalies_filtered)}")

df_anomalies_filtered["user_feedback"] = None
df_anomalies_filtered["reviewed"] = False
df_anomalies_filtered.to_json(PATH_OUTPUT_ANOMALIES, orient="records", indent=2)
print(f"✔ Anomalies exported to: {PATH_OUTPUT_ANOMALIES}")
