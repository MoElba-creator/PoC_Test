import os
import json
import pandas as pd
import joblib
from datetime import datetime
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
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
flattened_data = [r["_source"] for r in data if "_source" in r]
df = pd.json_normalize(flattened_data)
print("Kolommen gevonden:", df.columns.tolist())
if "user.feedback" not in df.columns:
    print("❌ Kolom 'user_feedback' ontbreekt in data.")
    exit(1)

df = df[df["user.feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user.feedback"].map({"correct": 1, "incorrect": 0})

features = [
    "source.ip", "destination.ip", "source.port", "destination.port",
    "network.transport", "session.iflow_bytes", "session.iflow_pkts"
]
if not all(f in df.columns for f in features):
    print("❌ Sommige vereiste features ontbreken in de JSON.")
    exit(1)

df_selected = df[features + ["label"]].dropna()

# === 3. Encodeer features ===
try:
    encoder = joblib.load("models/ip_encoder_hashing.pkl")
    print("✅ Encoder geladen.")
except Exception as e:
    print(f"❌ Kan encoder niet laden: {e}")
    exit(1)

try:
    X_encoded = encoder.transform(df_selected[features])
except Exception as e:
    print(f"❌ Fout tijdens transformeren met encoder: {e}")
    exit(1)

y = df_selected["label"]

# === 4. Train/test split ===
X_train, X_val, y_train, y_val = train_test_split(X_encoded, y, test_size=0.2, random_state=42)

# === 5. Definieer en train modellen ===
models = {
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "logistic_regression": LogisticRegression(max_iter=1000),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="logloss")
}

os.makedirs("models", exist_ok=True)

for name, model in models.items():
    try:
        model.fit(X_train, y_train)
        candidate_path = f"models/{name}_candidate.pkl"
        joblib.dump(model, candidate_path)
        print(f"✅ {name} kandidaat-model opgeslagen.")
    except Exception as e:
        print(f"❌ Fout bij trainen/saven van {name}: {e}")

# === 6. Save val set for later evaluation ===
joblib.dump((X_val, y_val), RUN_DIR / "validation_set.pkl")
print(f"📦 Validatieset opgeslagen voor evaluatie.")
