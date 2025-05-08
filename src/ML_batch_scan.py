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
    "event.action", "session.id", "label"
]
NUMERIC_COLUMNS = ["source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts"]
ENCODER_INPUT_COLUMNS = ["source.ip", "destination.ip", "network.transport", "event.action"]
LOW_RISK_PORTS = {67, 68, 123, 161, 162, 443, 53, 9200}
TRUSTED_SOURCE_IPS = {"10.192.96.7", "10.192.96.8", "10.192.96.4"}
TRUSTED_DEST_IPS = {"193.190.77.36"}

PATH_VALIDATION = "../data/validation_dataset.json"
PATH_OUTPUT_ALL = "../data/all_evaluated_logs.json"
PATH_OUTPUT_ANOMALIES = "../data/predicted_anomalies.json"
# ──────────────────────────────────────────────

# 1. LOAD DATA
with open(PATH_VALIDATION, "r", encoding="utf-8") as f:
    records = json.load(f)
flattened_data = [r["_source"] for r in records if "_source" in r]
df = pd.json_normalize(flattened_data)
print("Amount of records loaded:", len(df))

# 2. FILTER COLUMNS
available_columns = [col for col in RELEVANT_COLUMNS if col in df.columns]
df_selected = df[available_columns].copy()

# 3. NUMERIC CLEANUP
for col in NUMERIC_COLUMNS:
    if col in df_selected.columns:
        df_selected[col] = pd.to_numeric(df_selected[col], errors="coerce")
df_selected = df_selected.dropna()
print("Number of rows after cleanup:", len(df_selected))

# 4. ENCODE FOR RF & LOG
encoder = joblib.load("../models/ip_encoder_hashing.pkl")
X_encoded = encoder.transform(df_selected[ENCODER_INPUT_COLUMNS])
for col in NUMERIC_COLUMNS:
    X_encoded[col] = df_selected[col]

# 5. ADD UNSUPERVISED SCORE
iso_forest = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso_forest.fit(X_encoded)
df.loc[df_selected.index, "isoforest_score"] = iso_forest.decision_function(X_encoded)
X_encoded["isoforest_score"] = df.loc[df_selected.index, "isoforest_score"]

# 6. ENCODE FOR XGBOOST
xgb_bundle = joblib.load("../models/xgboost_model.pkl")
xgb_model = xgb_bundle["model"]
xgb_encoder = xgb_bundle["encoder"]
xgb_expected_columns = xgb_bundle["columns"]

X_encoded_xgb = xgb_encoder.transform(df_selected[ENCODER_INPUT_COLUMNS])
for col in NUMERIC_COLUMNS:
    X_encoded_xgb[col] = df_selected[col]
X_encoded_xgb["isoforest_score"] = df.loc[df_selected.index, "isoforest_score"]
X_encoded_xgb = X_encoded_xgb[xgb_expected_columns]

# 7. LOAD SUPERVISED MODELS
rf_model = joblib.load("../models/random_forest_model.pkl")
log_model = joblib.load("../models/logistic_regression_model.pkl")

# 8. PREDICTIONS
df.loc[df_selected.index, "RF_pred"] = rf_model.predict(X_encoded)
df.loc[df_selected.index, "LOG_pred"] = log_model.predict(X_encoded)
df.loc[df_selected.index, "XGB_pred"] = xgb_model.predict(X_encoded_xgb)

df.loc[df_selected.index, "RF_score"] = rf_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "LOG_score"] = log_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "XGB_score"] = xgb_model.predict_proba(X_encoded_xgb)[:, 1]
df["model_score"] = df[["RF_score", "LOG_score", "XGB_score"]].mean(axis=1)

# 9. ADD FEEDBACK PLACEHOLDERS
df.loc[df_selected.index, "user_feedback"] = None
df.loc[df_selected.index, "reviewed"] = False

# 10. EXPORT FULL EVALUATED LOGS
df_all_evaluated = df.loc[df_selected.index, [
    *available_columns,
    "isoforest_score",
    "RF_pred", "LOG_pred", "XGB_pred",
    "RF_score", "LOG_score", "XGB_score",
    "model_score",
    "user_feedback", "reviewed"
]]
df_all_evaluated.to_json(PATH_OUTPUT_ALL, orient="records", indent=2)
print(f"✔ Full evaluated logs saved to: {PATH_OUTPUT_ALL}")

# 11. SELECT ANOMALIES
df_anomalies = df[
    (df["RF_pred"] == 1) |
    (df["LOG_pred"] == 1) |
    (df["XGB_pred"] == 1)
].copy()
print(f"Total anomalies predicted: {len(df_anomalies)}")

# 12. FILTER ANOMALIES
df_anomalies_filtered = df_anomalies[
    ~df_anomalies["destination.port"].isin(LOW_RISK_PORTS) &
    ~df_anomalies["source.ip"].isin(TRUSTED_SOURCE_IPS) &
    ~df_anomalies["destination.ip"].isin(TRUSTED_DEST_IPS)
].copy()
print(f"Final filtered anomalies: {len(df_anomalies_filtered)}")

df_anomalies_filtered["user_feedback"] = None
df_anomalies_filtered["reviewed"] = False

# 13. EXPORT ANOMALIES
df_anomalies_filtered.to_json(PATH_OUTPUT_ANOMALIES, orient="records", indent=2)
print(f"✔ Anomalies exported to: {PATH_OUTPUT_ANOMALIES}")
