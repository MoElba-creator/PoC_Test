import os
import json
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder

# === 1. Laad originele trainingsdata ===
original_data = pd.read_csv("../data/dummy_network_logs.csv")
original_data["label"] = 0  # originele logs zijn niet bevestigd

# === 2. Laad feedbackdata ===
with open("../train_model/gelabelde_anomalieën.json", "r", encoding="utf-8") as f:
    feedback_data = json.load(f)
feedback_df = pd.DataFrame(feedback_data)
feedback_df = feedback_df[feedback_df["user_feedback"] == "correct"]
feedback_df["label"] = 1  # bevestigde anomalieën

# === 3. Combineer en verwerk ===
full_data = pd.concat([original_data, feedback_df], ignore_index=True)

# === 4. Voorverwerking ===
features = ["source_ip", "destination_ip", "network_transport", "packet_size", "duration"]
X = full_data[features].copy()
y = full_data["label"]

# Encodeer categorische kolommen
encoders = {}
for col in ["source_ip", "destination_ip", "network_transport"]:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))
    encoders[col] = le

# === 5. Train/test split ===
X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2, random_state=42)

# === 6. Train meerdere modellen ===

# Random Forest
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)
print("\n=== Random Forest ===")
print(classification_report(y_test, rf_model.predict(X_test)))

# Logistic Regression
lr_model = LogisticRegression(max_iter=1000)
lr_model.fit(X_train, y_train)
print("\n=== Logistic Regression ===")
print(classification_report(y_test, lr_model.predict(X_test)))

# XGBoost
xgb_model = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
xgb_model.fit(X_train, y_train)
print("\n=== XGBoost ===")
print(classification_report(y_test, xgb_model.predict(X_test)))

# === 7. Opslaan modellen ===
os.makedirs("../models", exist_ok=True)

joblib.dump(rf_model, "../models/random_forest_model.pkl")
joblib.dump(lr_model, "../models/logistic_regression_model.pkl")
joblib.dump(xgb_model, "../models/xgboost_model.pkl")

for col, le in encoders.items():
    joblib.dump(le, f"../models/encoder_{col}.pkl")

print("✅ Alle modellen en encoders opgeslagen.")
