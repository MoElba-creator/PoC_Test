import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
import json

from xgboost import XGBClassifier

# Load labeled data
with open("data/gelabelde_feedback.json", encoding="utf-8") as f:
    data = json.load(f)

df = pd.DataFrame(data)
df = df[df["user_feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user_feedback"].map({"correct": 1, "incorrect": 0})

# 2. Encoders
enc_src = joblib.load("models/encoder_source.ip.pkl")
enc_dst = joblib.load("models/encoder_destination.ip.pkl")
enc_proto = joblib.load("models/encoder_network.transport.pkl")

df["source.ip"] = enc_src.transform(df["source.ip"])
df["destination.ip"] = enc_dst.transform(df["destination.ip"])
df["network.transport"] = enc_proto.transform(df["network.transport"])

# Feature engineering
features = ["source.ip", "destination.ip", "network.transport", "bytes", "connections"]
df = df.dropna(subset=features + ["label"])

# Dummy encoding IP's en transport als string (gebruik je eigen encoders indien nodig)
X = pd.get_dummies(df[features])
y = df["label"]

X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

models = {
    "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
    "logistic_regression": LogisticRegression(max_iter=1000),
    "xgboost": XGBClassifier(use_label_encoder=False, eval_metric="logloss")
}

for name, model in models.items():
    model.fit(X_train, y_train)
    joblib.dump(model, f"models/{name}_candidate.pkl")
    print(f"✅ {name} kandidaat-model opgeslagen.")

