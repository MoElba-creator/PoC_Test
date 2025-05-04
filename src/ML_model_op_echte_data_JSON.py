import pandas as pd
import joblib
import json
import os
from category_encoders import HashingEncoder

# === 1. JSON-bestand inlezen ===
with open("../data/validation_dataset.json", "r", encoding="utf-8") as f:
    records = json.load(f)

print("Aantal JSON-records ingeladen:", len(records))

# === 2. Extract relevantie velden uit _source ===
flattened_data = [r["_source"] for r in records if "_source" in r]
df = pd.json_normalize(flattened_data)

# === 3. Relevante features kiezen ===
relevant_columns = [
    "source.ip", "destination.ip", "source.port", "destination.port",
    "network.transport", "session.iflow_bytes", "session.iflow_pkts"
]
df_selected = df[relevant_columns].copy()

# === 4. Numerieke velden proper maken ===
for col in ["source.port", "destination.port", "session.iflow_bytes", "session.iflow_pkts"]:
    df_selected[col] = pd.to_numeric(df_selected[col], errors="coerce")

# === 5. Verwijder onbruikbare rijen ===
df_selected = df_selected.dropna()
print("Aantal bruikbare rijen na opschoning:", len(df_selected))

# === 6. Laad encoder en modellen ===
encoder = joblib.load("../models/ip_encoder_hashing.pkl")
rf_model = joblib.load("../models/random_forest_model.pkl")
log_model = joblib.load("../models/logistic_regression_model.pkl")
xgb_model = joblib.load("../models/xgboost_model.pkl")

# === 7. Transformeer features met encoder ===
X_encoded = encoder.transform(df_selected)

# === 8. Bereken voorspellingen en scores ===
df.loc[df_selected.index, "RF_pred"] = rf_model.predict(X_encoded)
df.loc[df_selected.index, "LOG_pred"] = log_model.predict(X_encoded)
df.loc[df_selected.index, "XGB_pred"] = xgb_model.predict(X_encoded)

# === 9. Bereken waarschijnlijkheden voor klasse 1 (anomalie) ===
df.loc[df_selected.index, "RF_score"] = rf_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "LOG_score"] = log_model.predict_proba(X_encoded)[:, 1]
df.loc[df_selected.index, "XGB_score"] = xgb_model.predict_proba(X_encoded)[:, 1]

# === 10. Gemiddelde model_score berekenen ===
df["model_score"] = df[["RF_score", "LOG_score", "XGB_score"]].mean(axis=1)

# === 11. Selecteer records met minstens 1 anomaly-predictie ===
df_anomalies = df[
    (df["RF_pred"] == 1) |
    (df["LOG_pred"] == 1) |
    (df["XGB_pred"] == 1)
].copy()
print(f"Totaal aantal ruwe anomalieën: {len(df_anomalies)}")

# === 12. Filtering: poorten & IP’s uitsluiten ===
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

print(f"✅ Finale aantal anomalieën: {len(df_anomalies_filtered)}")

# === 13. Extra metadata voor feedback & validatie toevoegen ===
df_anomalies_filtered["user_feedback"] = None
df_anomalies_filtered["reviewed"] = False

# === 14. Exporteer naar JSON ===
output_path = "../data/voorspelde_anomalieën_gefilterd.json"
records = df_anomalies_filtered.to_dict(orient="records")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print(f"✅ Anomalieën geëxporteerd naar: {output_path}")
