import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
import xgboost as xgb
import joblib
from category_encoders import HashingEncoder
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# 1. Connectie
es = Elasticsearch(
    "https://uat.elastic.vives.cloud:9200",
    basic_auth=("Laurens", "A,knS%q8M{H2/YR$]_)Z;4+KcxzWp"),
    verify_certs=False
)

# 2. Ophalen van feedbackrecords
query = {
    "query": {
        "bool": {
            "should": [
                {"term": {"feedback_label": "correct"}},
                {"term": {"feedback_label": "incorrect"}}
            ]
        }
    }
}

records = [hit["_source"] for hit in scan(es, index="network-anomalies", query=query)]
df = pd.DataFrame(records)

if "feedback_label" not in df.columns or df.empty:
    raise ValueError("Geen feedbackgegevens beschikbaar.")

# 3. Mapping en selectie
df["label"] = df["feedback_label"].map({"correct": 1, "incorrect": 0})
relevant_cols = [
    "source_ip", "destination_ip", "source_port", "destination_port",
    "network_transport", "session_iflow_bytes", "session_iflow_pkts"
]

df = df[relevant_cols + ["label"]].dropna()
for col in ["source_port", "destination_port", "session_iflow_bytes", "session_iflow_pkts"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.dropna()

X = df[relevant_cols]
y = df["label"]

# 4. Encoding
encoder = HashingEncoder(cols=["source_ip", "destination_ip", "network_transport"])
X_encoded = encoder.fit_transform(X)

# 5. Training
rf = RandomForestClassifier(n_estimators=100, random_state=42)
log = LogisticRegression(max_iter=1000)
xgb_model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')

rf.fit(X_encoded, y)
log.fit(X_encoded, y)
xgb_model.fit(X_encoded, y)

# 6. Opslaan
joblib.dump(rf, "../models/random_forest_model.pkl")
joblib.dump(log, "../models/logistic_regression_model.pkl")
joblib.dump(xgb_model, "../models/xgboost_model.pkl")
joblib.dump(encoder, "../models/ip_encoder_hashing.pkl")

print("✅ Opnieuw getraind en opgeslagen!")
