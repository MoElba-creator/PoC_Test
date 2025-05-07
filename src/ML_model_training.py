import pandas as pd
import json
import joblib
from category_encoders import HashingEncoder
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

# Load data
with open("../data/dummy_network_logs.json", "r", encoding="utf-8") as f:
    records = json.load(f)
df = pd.DataFrame(records)

# Clean data
relevant_columns = [
    "source.ip", "destination.ip",
    "source.port", "destination.port",
    "network.transport",
    "session.iflow_bytes", "session.iflow_pkts",
    "event.action", "label"
]
df = df[[col for col in relevant_columns if col in df.columns]].dropna()

# Encoders
encoder_input_columns = ["source.ip", "destination.ip", "network.transport", "event.action"]
encoder = HashingEncoder(cols=encoder_input_columns, n_components=8)
df[encoder_input_columns] = df[encoder_input_columns].astype(str)
X_cat_encoded = encoder.fit_transform(df[encoder_input_columns])

# Add numeric features
X = X_cat_encoded.copy()
X["source.port"] = df["source.port"]
X["destination.port"] = df["destination.port"]
X["session.iflow_bytes"] = df["session.iflow_bytes"]
X["session.iflow_pkts"] = df["session.iflow_pkts"]

# Add isoforest_score for supervised models
iso = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso.fit(X)
df["isoforest_score"] = iso.decision_function(X)
X["isoforest_score"] = df["isoforest_score"]

# Prepare target
y = df["label"]

# Train models
rf = RandomForestClassifier(random_state=42)
log = LogisticRegression(max_iter=1000)
xgb = XGBClassifier(use_label_encoder=False, eval_metric="logloss")

rf.fit(X, y)
log.fit(X, y)
xgb.fit(X, y)

#  Save models and encoder
joblib.dump(rf, "../models/random_forest_model.pkl")
joblib.dump(log, "../models/logistic_regression_model.pkl")
joblib.dump(xgb, "../models/xgboost_model.pkl")
joblib.dump(encoder, "../models/ip_encoder_hashing.pkl")

print("All models trained and saved with isoforest_score included.")
