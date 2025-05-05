import os
import json
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import f1_score

# === 1. Laad gelabelde feedbackdata ===
INPUT_FILE = "data/gelabelde_anomalieën.json"

if not os.path.exists(INPUT_FILE):
    print(f"❌ Bestand niet gevonden: {INPUT_FILE}")
    exit(1)

with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)

df = pd.DataFrame(data)

# === 2. Label voorbereiden ===
if "user_feedback" not in df.columns:
    print("❌ Kolom 'user_feedback' ontbreekt in data.")
    exit(1)

df = df[df["user_feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user_feedback"].map({"correct": 1, "incorrect": 0})

# === 3. Selecteer relevante features ===
features = ["source.ip", "destination.ip", "network.transport", "bytes", "connections"]
if not all(f in df.columns for f in features):
    print("❌ Sommige vereiste features ontbreken in de JSON.")
    exit(1)

df_selected = df[features + ["label"]].dropna()

# === 4. Laad hashing encoder ===
try:
    encoder = joblib.load("models/ip_encoder_hashing.pkl")
    print("✅ Encoder geladen.")
except Exception as e:
    print(f"❌ Kan encoder niet laden: {e}")
    exit(1)

# === 5. Pas encoder toe ===
try:
    X_encoded = encoder.transform(df_selected[features])
except Exception as e:
    print(f"❌ Fout tijdens transformeren met encoder: {e}")
    exit(1)

y = df_selected["label"]

# === 6. Split in train/val ===
X_train, X_val, y_train, y_val = train_test_split(X_encoded, y, test_size=0.2, random_state=42)

# === 7. Definieer modellen ===
models = {
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "logistic_regression": LogisticRegression(max_iter=1000),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="logloss")
}

# === 8. Train en bewaar kandidaten ===
os.makedirs("models", exist_ok=True)

for name, model in models.items():
    try:
        model.fit(X_train, y_train)
        joblib.dump(model, f"models/{name}_candidate.pkl")
        print(f"✅ {name} kandidaat-model opgeslagen.")
    except Exception as e:
        print(f"❌ Fout bij trainen/saven van {name}: {e}")
