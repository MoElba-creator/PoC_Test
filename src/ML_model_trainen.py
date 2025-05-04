import pandas as pd
import numpy as np
import joblib
from category_encoders import HashingEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.metrics import classification_report

df = pd.read_csv("../data/dummy_network_logs.csv")

# Selecteer relevante kolommen
features = [
    "source.ip", "destination.ip", "source.port", "destination.port",
    "network.transport", "session.iflow_bytes", "session.iflow_pkts"
]
X = df[features]
y = df["label"]

#Zorg dat alle kolommen numeriek zijn
X["source.port"] = pd.to_numeric(X["source.port"], errors="coerce")
X["destination.port"] = pd.to_numeric(X["destination.port"], errors="coerce")
X["session.iflow_bytes"] = pd.to_numeric(X["session.iflow_bytes"], errors="coerce")
X["session.iflow_pkts"] = pd.to_numeric(X["session.iflow_pkts"], errors="coerce")
X = X.dropna()

# HashingEncoder toepassen
categorical_cols = ["source.ip", "destination.ip", "network.transport"]
encoder = HashingEncoder(cols=categorical_cols, n_components=8)
X_encoded = encoder.fit_transform(X)

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X_encoded, y, test_size=0.2, random_state=42
)

# Modellen trainen
print("Training Random Forest...")
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)

print("Training Logistic Regression...")
lr_model = LogisticRegression(max_iter=1000)
lr_model.fit(X_train, y_train)

print("Training XGBoost...")
xgb_model = XGBClassifier(use_label_encoder=False, eval_metric="logloss")
xgb_model.fit(X_train, y_train)

# Evaluatie vh model
print("\nRandom Forest:\n", classification_report(y_test, rf_model.predict(X_test)))
print("\nLogistic Regression:\n", classification_report(y_test, lr_model.predict(X_test)))
print("\nXGBoost:\n", classification_report(y_test, xgb_model.predict(X_test)))

# OPslaan modellen en encoders
joblib.dump(rf_model, "../models/random_forest_model.pkl")
joblib.dump(lr_model, "../models/logistic_regression_model.pkl")
joblib.dump(xgb_model, "../models/xgboost_model.pkl")
joblib.dump(encoder, "../models/ip_encoder_hashing.pkl")

print("\n Modellen en encoder succesvol opgeslagen.")
