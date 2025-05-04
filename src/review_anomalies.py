import os
import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from datetime import datetime

# === 1. Connectie met Elasticsearch ===
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]
INDEX_NAME = "network-anomalies"

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

# === 2. Pagina-configuratie ===
st.set_page_config(page_title="Review netwerk anomalieën", layout="wide")
st.title("🔍 Review netwerk anomalieën")
st.info("Geef feedback op anomalies. Enkel logs met `user_feedback = onbekend` worden getoond.")

# === 3. Sidebar filters ===
st.sidebar.header("🔎 Filters")
max_logs = st.sidebar.slider("Max. aantal anomalies", 10, 1000, 100)
src_ip_filter = st.sidebar.text_input("Filter op bron IP (source_ip)")
dst_ip_filter = st.sidebar.text_input("Filter op bestemming IP (destination_ip)")
min_rf_score = st.sidebar.slider("Minimum RF score", 0.0, 1.0, 0.0, 0.01)
min_xgb_score = st.sidebar.slider("Minimum XGB score", 0.0, 1.0, 0.0, 0.01)
min_log_score = st.sidebar.slider("Minimum LOG score", 0.0, 1.0, 0.0, 0.01)
start_date = st.sidebar.text_input("Vanaf datum (timestamp, YYYY-MM-DD)")

# === 4. Dynamische query bouwen ===
filters = [
    {"term": {"user_feedback.keyword": "onbekend"}},
    {"range": {"RF_score": {"gte": min_rf_score}}},
    {"range": {"XGB_score": {"gte": min_xgb_score}}},
    {"range": {"LOG_score": {"gte": min_log_score}}}
]

if src_ip_filter:
    filters.append({"term": {"source_ip.keyword": src_ip_filter}})
if dst_ip_filter:
    filters.append({"term": {"destination_ip.keyword": dst_ip_filter}})
if start_date:
    try:
        date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        filters.append({"range": {"timestamp": {"gte": date_obj.isoformat()}}})
    except ValueError:
        st.sidebar.error("❌ Ongeldige datum. Gebruik formaat: YYYY-MM-DD")

query = {
    "query": {"bool": {"must": filters}},
    "size": max_logs
}

# === 5. Data ophalen ===
try:
    res = es.search(index=INDEX_NAME, body=query)
    hits = res["hits"]["hits"]

    st.markdown(f"**Gevonden anomalies:** {len(hits)}")

    if not hits:
        st.success("✅ Alle anomalies zijn al beoordeeld.")
    else:
        unique_set = set()
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]

            unique_key = (source.get("timestamp"), source.get("source_ip"), source.get("destination_ip"),
                          source.get("source_port"), source.get("destination_port"))
            if unique_key in unique_set:
                continue
            unique_set.add(unique_key)

            beschrijving = f"{source.get('timestamp', '?')} | {source.get('network_transport', '?').upper()} | {source.get('source_ip')}:{source.get('source_port')} ➜ {source.get('destination_ip')}:{source.get('destination_port')} | " \
                          f"RF: {source.get('RF_score', 0):.2f} | XGB: {source.get('XGB_score', 0):.2f} | LOG: {source.get('LOG_score', 0):.2f}"

            with st.expander(beschrijving):
                st.json(source)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Dit is effectief verdacht", key=f"yes_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "correct", "reviewed": True}})
                        st.rerun()

                with col2:
                    if st.button("❌ Geen echte anomaly", key=f"no_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "incorrect", "reviewed": True}})
                        st.rerun()

except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' bestaat niet.")
except Exception as e:
    st.exception(e)
