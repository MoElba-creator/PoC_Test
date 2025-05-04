import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from datetime import datetime, time
from collections import defaultdict
import json

# === 1. Elasticsearch connectie ===
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]
INDEX_NAME = "network-anomalies"

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

# === 2. UI instellingen ===
st.set_page_config(page_title="Anomalieën Review", layout="wide")
st.title("🔍 Review netwerk anomalieën")
st.info("Geef feedback op anomalieën. Enkel logs met user_feedback = onbekend worden getoond.")

# === 3. Sidebar filters ===
st.sidebar.title("🔎 Filters")

max_logs = st.sidebar.slider("Max. aantal anomalies", min_value=10, max_value=1000, value=100)
source_ip = st.sidebar.text_input("Filter op bron IP (source_ip)")
destination_ip = st.sidebar.text_input("Filter op bestemming IP (destination_ip)")
protocol = st.sidebar.text_input("Filter op protocol (network_transport)")
score_threshold = st.sidebar.slider("Minimum gemiddelde score", min_value=0.0, max_value=1.0, value=0.5, step=0.01)

st.sidebar.markdown("### 📅 Filter op tijdsinterval")
start_date = st.sidebar.date_input("Startdatum")
start_time = st.sidebar.time_input("Starttijd", value=time(0, 0))
end_date = st.sidebar.date_input("Einddatum")
end_time = st.sidebar.time_input("Eindtijd", value=time(23, 59))

start_dt = datetime.combine(start_date, start_time)
end_dt = datetime.combine(end_date, end_time)

doc_id_filter = st.sidebar.text_input("📄 Zoek op document _id")

# === 4. Feedback download ===
if st.sidebar.button("📥 Download alle feedback als JSON"):
    feedback_query = {
        "query": {
            "bool": {
                "must_not": [{"term": {"user_feedback.keyword": "onbekend"}}]
            }
        },
        "size": 10000
    }
    try:
        res = es.search(index=INDEX_NAME, body=feedback_query)
        labeled_hits = [hit["_source"] for hit in res["hits"]["hits"]]
        feedback_json = json.dumps(labeled_hits, indent=2)
        st.sidebar.download_button("Download feedback.json", feedback_json, file_name="feedback.json", mime="application/json")
    except Exception as e:
        st.sidebar.error(f"Download mislukt: {e}")

# === 5. Ophalen van data ===
try:
    if doc_id_filter:
        # Zoek exact op document _id
        query = {
            "query": {
                "ids": {
                    "values": [doc_id_filter]
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        hits = res["hits"]["hits"]
    else:
        # Normale query met filters
        base_query = {
            "bool": {
                "must": [
                    {"term": {"user_feedback.keyword": "onbekend"}},
                    {"range": {"@timestamp": {"gte": start_dt.isoformat(), "lte": end_dt.isoformat()}}}
                ]
            }
        }
        if source_ip:
            base_query["bool"]["must"].append({"term": {"source_ip.keyword": source_ip}})
        if destination_ip:
            base_query["bool"]["must"].append({"term": {"destination_ip.keyword": destination_ip}})
        if protocol:
            base_query["bool"]["must"].append({"term": {"network_transport.keyword": protocol}})

        query = {
            "query": base_query,
            "size": max_logs,
            "sort": [{"@timestamp": {"order": "desc"}}]
        }
        res = es.search(index=INDEX_NAME, body=query)
        hits = res["hits"]["hits"]

    if not hits:
        st.success("✅ Geen anomalies gevonden met deze filters.")
    else:
        groups = defaultdict(list)
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]
            raw_ts = source["@timestamp"]
            if "." in raw_ts:
                raw_ts = raw_ts.split(".")[0] + "." + raw_ts.split(".")[1][:6] + "Z"
            timestamp = datetime.strptime(raw_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
            bucket_time = timestamp.replace(second=0, microsecond=0)
            group_key = (source.get("source_ip"), source.get("destination_ip"), source.get("network_transport"), bucket_time)
            groups[group_key].append((doc_id, source))

        for (src_ip, dst_ip, proto, group_time), items in groups.items():
            if len(items) == 0:
                continue

            scores = [s.get("RF_score", 0) for _, s in items if isinstance(s.get("RF_score", 0), (int, float))]
            avg_score = sum(scores) / len(scores) if scores else 0

            if not doc_id_filter and avg_score < score_threshold:
                continue

            color = "🟢"
            if avg_score > 0.9:
                color = "🔴"
            elif avg_score > 0.75:
                color = "🟠"

            group_title = f"{color} {group_time.strftime('%Y-%m-%d %H:%M')} | {proto} | {src_ip} ➜ {dst_ip} | logs: {len(items)} | RF avg: {avg_score:.2f}"
            with st.expander(group_title):
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button(f"✅ Markeer als verdacht", key=f"group_yes_{src_ip}_{dst_ip}_{group_time}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={
                                "doc": {"user_feedback": "correct", "reviewed": True}
                            })
                        st.success("✔️ Gemarkeerd als verdacht")
                        st.rerun()
                with col2:
                    if st.button(f"❌ Markeer als NIET verdacht", key=f"group_no_{src_ip}_{dst_ip}_{group_time}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={
                                "doc": {"user_feedback": "incorrect", "reviewed": True}
                            })
                        st.warning("❗ Gemarkeerd als NIET verdacht")
                        st.rerun()

                for doc_id, source in items:
                    st.markdown(f"**📄 Log** `{source.get('@timestamp', '?')}` — 🆔 `{doc_id}`")
                    st.json(source)

except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' bestaat niet.")
except Exception as e:
    st.exception(e)
