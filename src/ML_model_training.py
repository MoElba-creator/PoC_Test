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

# ---------------------------------------------
# Load and preprocess network log data
# ---------------------------------------------

# Load synthetic network logs from JSON
with open("../data/dummy_network_logs.json", "r", encoding="utf-8") as f:
    records = json.load(f)
df = pd.DataFrame(records)

# Define feature columns
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

# Drop rows with missing values in essential columns
required_columns = categorical_features + numeric_features + ["label"]
df = df[required_columns].dropna()

# Convert all categorical fields to string
# Needed before encoding
df[categorical_features] = df[categorical_features].astype(str)

# Encode high-cardinality categorical features using hashing
encoder = HashingEncoder(cols=categorical_features, n_components=32)
X_cat_encoded = encoder.fit_transform(df[categorical_features])

# Combine encoded categorical and numeric features
X = pd.concat([X_cat_encoded, df[numeric_features].reset_index(drop=True)], axis=1)

# ---------------------------------------------
# Add unsupervised anomaly score via Isolation Forest
# ---------------------------------------------
iso = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
iso.fit(X)
df["isoforest_score"] = iso.decision_function(X)
X["isoforest_score"] = df["isoforest_score"]

# Define the target label
y = df["label"]

# Split dataset into training and test sets (80/20)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ---------------------------------------------
# Train supervised ML models
# ---------------------------------------------

# Initialize classifiers
rf = RandomForestClassifier(random_state=42)
log = LogisticRegression(max_iter=3000)
xgb = XGBClassifier(eval_metric="logloss")

# Fit models on training data
rf.fit(X_train, y_train)
log.fit(X_train, y_train)
xgb.fit(X_train, y_train)

# Predict labels on test data
y_rf = rf.predict(X_test)
y_log = log.predict(X_test)
y_xgb = xgb.predict(X_test)

# ---------------------------------------------
# Evaluate models using standard classification metrics
# ---------------------------------------------
def evaluate(name, y_true, y_pred):
    print(f"\n--- {name} ---")
    print("Accuracy :", accuracy_score(y_true, y_pred))
    print("Precision:", precision_score(y_true, y_pred, zero_division=0))
    print("Recall   :", recall_score(y_true, y_pred, zero_division=0))
    print("F1 Score :", f1_score(y_true, y_pred, zero_division=0))

evaluate("Random Forest", y_test, y_rf)
evaluate("Logistic Regression", y_test, y_log)
evaluate("XGBoost", y_test, y_xgb)

# ---------------------------------------------
# Save models and encoder for later use
# ---------------------------------------------
# Always save to root-level models/ directory
MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# Save Random Forest and Logistic Regression
joblib.dump(rf, os.path.join(MODEL_DIR, "random_forest_model.pkl"))
joblib.dump(log, os.path.join(MODEL_DIR, "logistic_regression_model.pkl"))

# Save XGBoost model with encoder and feature metadata
xgb_bundle = {
    "model": xgb,
    "encoder": encoder,
    "columns": X_train.columns.tolist()
}
joblib.dump(xgb_bundle, os.path.join(MODEL_DIR, "xgboost_model.pkl"))

print("\nAll models are trained and saved with expanded features!")
