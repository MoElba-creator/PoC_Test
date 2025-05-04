import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from datetime import datetime, timedelta
from collections import defaultdict
import json

# === 1. Omgeving (werkt ook met Streamlit Cloud Secrets) ===
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
st.info("Geef feedback op anomalieën. Enkel logs met user_feedback = onbekend worden getoond.")

# === 4. Filters ===
st.sidebar.title("🔎 Filters")
max_logs = st.sidebar.slider("Max. aantal anomalies", min_value=10, max_value=1000, value=100)
filter_source_ip = st.sidebar.text_input("Filter op bron IP (source_ip)")
filter_destination_ip = st.sidebar.text_input("Filter op bestemming IP (destination_ip)")
filter_protocol = st.sidebar.text_input("Filter op protocol (network_transport)")
score_threshold = st.sidebar.slider("Minimum gemiddelde score", min_value=0.0, max_value=1.0, value=0.5, step=0.01)

# === 5. Download gelabelde feedback ===
if st.sidebar.button("📥 Download alle feedback als JSON"):
    try:
        feedback_query = {
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {"user_feedback.keyword": "onbekend"}}
                    ]
                }
            },
            "size": 10000
        }
        res = es.search(index=INDEX_NAME, body=feedback_query)
        labeled_hits = [hit["_source"] for hit in res["hits"]["hits"]]
        feedback_json = json.dumps(labeled_hits, indent=2)
        st.sidebar.download_button("Download feedback.json", feedback_json, file_name="feedback.json", mime="application/json")
    except Exception as e:
        st.sidebar.error(f"Download mislukt: {e}")

# === 6. Ophalen van niet-gereviewde anomalies ===
try:
    base_query = {
        "bool": {
            "must": [
                {"term": {"user_feedback.keyword": "onbekend"}}
            ]
        }
    }

    if filter_source_ip:
        base_query["bool"]["must"].append({"term": {"source_ip.keyword": filter_source_ip}})
    if filter_destination_ip:
        base_query["bool"]["must"].append({"term": {"destination_ip.keyword": filter_destination_ip}})
    if filter_protocol:
        base_query["bool"]["must"].append({"term": {"network_transport.keyword": filter_protocol}})

    query = {
        "query": base_query,
        "size": max_logs,
        "sort": [{"@timestamp": {"order": "desc"}}]
    }

    res = es.search(index=INDEX_NAME, body=query)
    hits = res["hits"]["hits"]

    if not hits:
        st.success("✅ Alle anomalies zijn al beoordeeld.")
    else:
        # === 7. Clustering op basis van source/dest IP, protocol en timestamp-binning ===
        groups = defaultdict(list)
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]
            timestamp = datetime.strptime(source["@timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            bucket_time = timestamp.replace(second=0, microsecond=0)
            group_key = (source.get("source_ip"), source.get("destination_ip"), source.get("network_transport"), bucket_time)
            groups[group_key].append((doc_id, source))

        for (src_ip, dst_ip, proto, group_time), items in groups.items():
            if len(items) == 0:
                continue

            scores = [s.get("RF_score", 0) for _, s in items if isinstance(s.get("RF_score", 0), (int, float))]
            avg_score = sum(scores) / len(scores) if scores else 0

            if avg_score < score_threshold:
                continue

            with st.expander(f"{proto} | {src_ip} ➜ {dst_ip} | logs: {len(items)} | RF avg: {avg_score:.2f}"):
                for doc_id, source in items:
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
