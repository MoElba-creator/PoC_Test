import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from dotenv import load_dotenv

# === 1. Configuratie ===
load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
INPUT_JSON = "../data/voorspelde_anomalieën_gefilterd.json"

# === 2. Elasticsearch connectie via API key ===
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True  # zorg dat SSL correct werkt, anders tijdelijk False in test
)

# === 3. JSON-data inladen ===
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    records = json.load(f)

print(f"📥 Ingeladen records uit JSON: {len(records)}")

# === 4. Conversie naar Pandas DataFrame ===
df = pd.DataFrame(records)

# === 5. Kolomtransformaties en defaults ===
# Puntjes in kolomnamen vervangen voor Elasticsearch
df.columns = [col.replace(".", "_") for col in df.columns]

# Voeg eventueel ontbrekende kolommen toe voor feedback loop
if "user_feedback" not in df.columns:
    df["user_feedback"] = None

if "reviewed" not in df.columns:
    df["reviewed"] = False

# Vermijd lege waarden
df = df.fillna("onbekend")

# === 6. Conversie naar Elasticsearch-bulkformaat ===
def df_to_elastic_format(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

# === 7. Verzenden naar Elasticsearch ===
try:
    success, _ = bulk(es, df_to_elastic_format(df, INDEX_NAME))
    print(f"✅ {success} records succesvol geüpload naar: {INDEX_NAME}")
except BulkIndexError as e:
    print(f"❌ {len(e.errors)} documenten konden niet geüpload worden.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))
