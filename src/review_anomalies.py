import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from datetime import datetime

# === 1. Omgeving (Streamlit Cloud of lokaal .env) ===
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]
INDEX_NAME = "network-anomalies"

# === 2. Elasticsearch connectie ===
es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

# === 3. Streamlit UI ===
st.set_page_config(page_title="Anomalieën Review", layout="wide")
st.title("🔍 Review netwerk anomalieën")
st.info("Geef feedback op anomalies. Enkel logs met `user_feedback = onbekend` worden getoond.")

# === 4. Filters ===
with st.sidebar:
    st.header("🔎 Filters")

    max_results = st.slider("Max. aantal anomalies", min_value=10, max_value=1000, step=10, value=100)

    filter_source_ip = st.text_input("Filter op bron IP (source_ip)")
    filter_destination_ip = st.text_input("Filter op bestemming IP (destination_ip)")

    start_date = st.date_input("Vanaf datum (timestamp)", value=None)
    end_date = st.date_input("Tot datum (timestamp)", value=None)

# === 5. Dynamische query bouwen ===
must_conditions = [{"term": {"user_feedback.keyword": "onbekend"}}]

# IP-filters
if filter_source_ip:
    must_conditions.append({"match": {"source_ip": filter_source_ip}})
if filter_destination_ip:
    must_conditions.append({"match": {"destination_ip": filter_destination_ip}})

# Datumfilter op `timestamp` (ISO-formaat nodig)
range_filter = {}
if start_date:
    range_filter["gte"] = start_date.strftime("%Y-%m-%dT00:00:00")
if end_date:
    range_filter["lte"] = end_date.strftime("%Y-%m-%dT23:59:59")
if range_filter:
    must_conditions.append({
        "range": {
            "timestamp": range_filter
        }
    })

query = {
    "query": {
        "bool": {
            "must": must_conditions
        }
    },
    "size": max_results
}

# === 6. Ophalen en tonen ===
try:
    res = es.search(index=INDEX_NAME, body=query)
    hits = res["hits"]["hits"]
    st.caption(f"Gevonden anomalies: {len(hits)}")

    if not hits:
        st.success("✅ Geen anomalies meer of filters geven geen resultaat.")
    else:
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]

            with st.expander(f"{source.get('timestamp', '?')} | {source.get('source_ip')} ➜ {source.get('destination_ip')}"):
                st.json(source)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Verdacht", key=f"yes_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={
                            "doc": {
                                "user_feedback": "correct",
                                "reviewed": True
                            }
                        })
                        st.success("✔️ Geregistreerd als verdacht")
                        st.rerun()
                with col2:
                    if st.button("❌ Niet verdacht", key=f"no_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={
                            "doc": {
                                "user_feedback": "incorrect",
                                "reviewed": True
                            }
                        })
                        st.warning("❗ Geregistreerd als niet-verdacht")
                        st.rerun()

except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' bestaat niet.")
except Exception as e:
    st.exception(e)
