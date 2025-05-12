import os
import json
import pandas as pd
import joblib
from datetime import datetime
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.ensemble import IsolationForest
from xgboost import XGBClassifier
from category_encoders import HashingEncoder
from sklearn.metrics import f1_score

# === 1. Bestandsinstellingen ===
today = datetime.now().strftime("%Y%m%d_%Hh")
RUN_DIR = Path(f"data/training_runs/{today}_candidate")
RUN_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = "data/gelabelde_anomalieën.json"
if not os.path.exists(INPUT_FILE):
    print(f"❌ Bestand niet gevonden: {INPUT_FILE}")
    exit(1)

with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)

with open(RUN_DIR / "feedback.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
print(f"✅ Feedback opgeslagen onder: {RUN_DIR}/feedback.json")

# === 2. Data voorbereiden ===
df = pd.json_normalize(data)
if "user_feedback" not in df.columns:
    print("❌ Kolom 'user_feedback' ontbreekt.")
    exit(1)

df = df[df["user_feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user_feedback"].map({"correct": 1, "incorrect": 0})

categorical = [
    "source_ip", "destination_ip", "network_transport", "event_action",
    "tcp_flags", "agent_version", "fleet_action_type", "message",
    "proto_port_pair", "version_action_pair"
]
numeric = [
    "source_port", "destination_port", "session_iflow_bytes", "session_iflow_pkts",
    "flow_count_per_minute", "unique_dst_ports", "bytes_ratio",
    "port_entropy", "flow.duration", "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

required = categorical + numeric
if not all(col in df.columns for col in required):
    print("❌ Vereiste kolommen ontbreken.")
    exit(1)

df = df[required + ["label"]].dropna()
df[categorical] = df[categorical].astype(str)

# === 3. Encode categorical features
encoder = HashingEncoder(cols=categorical, n_components=32)
X_cat = encoder.transform(df[categorical])
X = pd.concat([X_cat.reset_index(drop=True), df[numeric].reset_index(drop=True)], axis=1)

# === 4. Voeg isoforest_score toe
iso = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso.fit(X)
df["isoforest_score"] = iso.decision_function(X)
X["isoforest_score"] = df["isoforest_score"]

y = df["label"]

# === 5. Split train/test
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# === 6. Train modellen
models = {
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "logistic_regression": LogisticRegression(max_iter=1000),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="logloss")
}
os.makedirs("models", exist_ok=True)

for name, model in models.items():
    try:
        model.fit(X_train, y_train)
        if name == "xgboost":
            bundle = {
                "model": model,
                "encoder": encoder,
                "columns": X_train.columns.tolist()
            }
            joblib.dump(bundle, f"models/{name}_candidate.pkl")
        else:
            joblib.dump(model, f"models/{name}_candidate.pkl")
        print(f"✅ {name} model opgeslagen.")
    except Exception as e:
        print(f"❌ Fout bij trainen van {name}: {e}")

# === 7. Save val set
joblib.dump((X_val, y_val), RUN_DIR / "validation_set.pkl")
print(f"📦 Validatieset opgeslagen.")
