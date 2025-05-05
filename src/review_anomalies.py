import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from datetime import datetime, time
from collections import defaultdict
import json
from PIL import Image

# Elasticsearch connectie aanmaken
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]
INDEX_NAME = "network-anomalies"

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

#2 UI-instellingen gebruiksvriendelijk maken
st.set_page_config(page_title="VIVES network logging anomalies review", layout="wide")
col1, col2 = st.columns([2, 10])
with col1:
    st.image("images/logo_vives.png", use_container_width=True)
with col2:
    st.title("VIVES network logging anomalies review")
st.info("Consult anomaly logging. Once feedback is given the log is not visible anymore.")

# === 3. Sidebar filters ===
st.sidebar.title("Filtering")

max_logs = st.sidebar.slider("Maximum shown logs", min_value=1, max_value=1000, value=100)
doc_id_filter = st.sidebar.text_input("Search on unique log ID")
source_ip = st.sidebar.text_input("Filter on Source IP")
destination_ip = st.sidebar.text_input("Filter on Destination IP")
protocol = st.sidebar.text_input("Filter on Network Protocol")
score_threshold = st.sidebar.slider("Minimum gemiddelde score", min_value=0.0, max_value=1.0, value=0.5, step=0.01)

st.sidebar.markdown("📅 Filter on log date")
start_date = st.sidebar.date_input("Start date")
start_time = st.sidebar.time_input("Start Time", value=time(0, 0))
end_date = st.sidebar.date_input("End date")
end_time = st.sidebar.time_input("End Time", value=time(23, 59))

start_dt = datetime.combine(start_date, start_time)
end_dt = datetime.combine(end_date, end_time)

# === 4. Feedback download ===
if st.sidebar.button("📥 Download given feedback"):
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
        st.sidebar.error(f"Download failed: {e}")

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
        st.success("✅ No anomalies were found. Consider adjusting the filters.")
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
                    if st.button(f"✅ Mark as suspicious", key=f"group_yes_{src_ip}_{dst_ip}_{group_time}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={
                                "doc": {"user_feedback": "correct", "reviewed": True}
                            })
                        st.success("✔️ Marked as suspicious successful")
                        st.rerun()
                with col2:
                    if st.button(f"❌ Mark as normal behavior", key=f"group_no_{src_ip}_{dst_ip}_{group_time}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={
                                "doc": {"user_feedback": "incorrect", "reviewed": True}
                            })
                        st.warning("️️️✔️ Marked as normal behavior")
                        st.rerun()

                for doc_id, source in items:
                    st.markdown(f"**📄 Log** `{source.get('@timestamp', '?')}` — 🆔 `{doc_id}`")
                    st.json(source)

except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' does not exist.")
except Exception as e:
    st.exception(e)
