import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from dotenv import load_dotenv

# Configuration
load_dotenv()

ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
INPUT_JSON = "../data/predicted_anomalies.json"

#lasticsearch connection with API key
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# JSON load
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    records = json.load(f)

print(f"JSON records loaded: {len(records)}")

# Panda dataframe
df = pd.DataFrame(records)

# Underscore replace for Elasticserach export
df.columns = [col.replace(".", "_") for col in df.columns]

# User feedback
if "user_feedback" not in df.columns:
    df["user_feedback"] = "unknown"
else:
    df["user_feedback"] = df["user_feedback"].fillna("unknown")

if "reviewed" not in df.columns:
    df["reviewed"] = False
else:
    df["reviewed"] = df["reviewed"].fillna(False)

df = df.fillna("unknown")

df = df.fillna("unknown")

# Conversion to elasticsearch format
def df_to_elastic_format(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

# Send to Elasticsearch
try:
    success, _ = bulk(es, df_to_elastic_format(df, INDEX_NAME))
    print(f""
          f"{success} records succesfully uploaded to : {INDEX_NAME}")
except BulkIndexError as e:
    print(f"{len(e.errors)} failed to send.")
    for err in e.errors[:5]:
        print(json.dumps(err, indent=2))
