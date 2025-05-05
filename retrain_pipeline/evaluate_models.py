import joblib
import json
import pandas as pd
from sklearn.metrics import f1_score

with open("data/voorspelde_anomalieën_gefilterd.json", encoding="utf-8") as f:
    val_data = json.load(f)

df = pd.DataFrame(val_data)
df = df[df["user_feedback"].isin(["correct", "incorrect"])]
df["label"] = df["user_feedback"].map({"correct": 1, "incorrect": 0})

# encoderen
enc_src = joblib.load("models/encoder_source.ip.pkl")
enc_dst = joblib.load("models/encoder_destination.ip.pkl")
enc_proto = joblib.load("models/encoder_network.transport.pkl")

df["source.ip"] = enc_src.transform(df["source.ip"])
df["destination.ip"] = enc_dst.transform(df["destination.ip"])
df["network.transport"] = enc_proto.transform(df["network.transport"])

features = ["source.ip", "destination.ip", "network.transport", "bytes", "connections"]
X = df[features]
y = df["label"]

model_names = ["random_forest", "logistic_regression", "xgboost"]

for name in model_names:
    current = joblib.load(f"models/{name}_model.pkl")
    candidate = joblib.load(f"models/{name}_candidate.pkl")

    f1_old = f1_score(y, current.predict(X))
    f1_new = f1_score(y, candidate.predict(X))

    print(f"\n📌 {name.upper()} — oud: {f1_old:.4f} | nieuw: {f1_new:.4f}")
    if f1_new > f1_old + 0.01:
        joblib.dump(candidate, f"models/{name}_model.pkl")
        print("✅ Vervangen: nieuw model presteert beter.")
    else:
        print("🚫 Geen vervanging: huidig model blijft.")
