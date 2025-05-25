"""
Script: ML_model_training.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script trains machine learning models to classify normal vs anomalous network logs.
It uses both supervised and unsupervised logic and stores the results for later prediction use.

What it does:
1. Loads the synthetic dataset created via synthetic_data_creation.py.
2. Defines relevant feature columns incl. numeric and categorical.
3. Encodes string fields using hashing so we dont need category list.
4. Enriches the features with isolation forest anomaly score.
5. Trains three classifiers. Random forest, logistic regression and xgboost.
6. Evaluates the models using standard metrics.
7. Saves trained models to a shared location for reuse.
"""


import pandas as pd
import json
import os
import joblib
from category_encoders import HashingEncoder
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


# load network logs from synthetic data file
with open("../data/dummy_network_logs.json", "r", encoding="utf-8") as f:
    records = json.load(f)
df = pd.DataFrame(records)

# define which features will be used
categorical_features = [
    "source.ip", "destination.ip", "network.transport", "event.action",
    "tcp.flags", "agent.version", "fleet.action.type", "message",
    "proto_port_pair", "version_action_pair"
]

numeric_features = [
    "source.port", "destination.port",
    "session.iflow_bytes", "session.iflow_pkts",
    "flow_count_per_minute", "unique_dst_ports",
    "bytes_ratio", "port_entropy", "flow.duration",
    "bytes_per_pkt", "msg_code", "is_suspicious_ratio"
]

# remove rows that have missing stuff in these columns
required_columns = categorical_features + numeric_features + ["label"]
df = df[required_columns].dropna()

# convert all categorical fields to string
# needed before encoding
df[categorical_features] = df[categorical_features].astype(str)

# use hashing encoder so we don't need to track the possible values
encoder = HashingEncoder(cols=categorical_features, n_components=32)
X_cat_encoded = encoder.fit_transform(df[categorical_features])

# Combine numeric + encoded categorical into one big feature set
X = pd.concat([X_cat_encoded, df[numeric_features].reset_index(drop=True)], axis=1)

# Add anomaly score from isolation forest for hybrid learning
iso = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso.fit(X)
df["isoforest_score"] = iso.decision_function(X)
X["isoforest_score"] = df["isoforest_score"]

# Tis is the column we're trying to predict
y = df["label"]

# Cut data into training and testing so we can evaluate. Industry standard is 80/20 split.
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Define models we want to train
rf = RandomForestClassifier(random_state=42)
log = LogisticRegression(max_iter=3000)
xgb = XGBClassifier(eval_metric="logloss")

# Train models using our training set
rf.fit(X_train, y_train)
log.fit(X_train, y_train)
xgb.fit(X_train, y_train)

# Make predictions using the test data
y_rf = rf.predict(X_test)
y_log = log.predict(X_test)
y_xgb = xgb.predict(X_test)

# Helper to print model performance
def evaluate(name, y_true, y_pred):
    print(f"\n--- {name} ---")
    print("Accuracy :", accuracy_score(y_true, y_pred))
    print("Precision:", precision_score(y_true, y_pred, zero_division=0))
    print("Recall   :", recall_score(y_true, y_pred, zero_division=0))
    print("F1 Score :", f1_score(y_true, y_pred, zero_division=0))

# Show scores for each model
evaluate("Random Forest", y_test, y_rf)
evaluate("Logistic Regression", y_test, y_log)
evaluate("XGBoost", y_test, y_xgb)

# Save trained models so we can load them later
MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# Save random forest and logistic model as joblib
joblib.dump(rf, os.path.join(MODEL_DIR, "random_forest_model.pkl"))
joblib.dump(log, os.path.join(MODEL_DIR, "logistic_regression_model.pkl"))

# Save xgboost but also the encoder and column order so we can reload it properly
xgb_bundle = {
    "model": xgb,
    "encoder": encoder,
    "columns": X_train.columns.tolist()
}
joblib.dump(xgb_bundle, os.path.join(MODEL_DIR, "xgboost_model.pkl"))

print("\nAll models are trained and saved with expanded features!")
