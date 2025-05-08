import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from pathlib import Path
from datetime import datetime, time as dt_time
import time
from collections import defaultdict
import json
from PIL import Image
import bcrypt
from dotenv import load_dotenv

st.set_page_config(page_title="VIVES Network logging anomalies review", layout="wide")

load_dotenv()

def check_login():
    correct_username = os.getenv("LOGIN_USER")
    raw_hash = os.getenv("LOGIN_PASS_HASH")
    if not raw_hash:
        st.error("LOGIN_PASS_HASH environment variable is not set.")
        st.stop()
    correct_password_hash = raw_hash.encode("utf-8")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "login_attempts" not in st.session_state:
        st.session_state.login_attempts = 0
    if "last_attempt_time" not in st.session_state:
        st.session_state.last_attempt_time = 0

    if st.session_state.authenticated:
        st.sidebar.success(f"Logged in as {correct_username}")
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()
        return

    with st.form("Login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            now = time.time()
            if now - st.session_state.last_attempt_time < 5:
                st.error("Please wait  before trying again.")
                st.stop()

            if username == correct_username and bcrypt.checkpw(password.encode("utf-8"), correct_password_hash):
                st.session_state.authenticated = True
                st.session_state.login_attempts = 0
                st.rerun()
            else:
                st.session_state.last_attempt_time = now
                st.session_state.login_attempts += 1
                st.error("Invalid credentials")
                st.stop()

    st.stop()

check_login()

# Index names
ANOMALY_INDEX = "network-anomalies"
ALL_LOGS_INDEX = "network-anomalies-all"

# 🔌 Elasticsearch connection
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

# 🖼 Logo
logo_path = Path(__file__).resolve().parent.parent / "images" / "logo_vives.png"
if not logo_path.exists():
    st.error(f"Logo not found at: {logo_path}")
    st.stop()

logo = Image.open(logo_path).convert("RGBA")
white_bg = Image.new("RGBA", logo.size, (255, 255, 255, 255))
white_logo = Image.alpha_composite(white_bg, logo)

col1, col2 = st.columns([2, 10])
with col1:
    st.image(white_logo, use_container_width=True)
with col2:
    st.title("Network logging anomalies review")

#  Toggle false negative view
show_unflagged_logs = st.sidebar.checkbox("Show all evaluated logs", value=False)

if show_unflagged_logs:
    st.info("Showing logs that were not flagged by the model. You can mark them as false negatives to move them to the anomaly index for retraining.")
    INDEX_NAME = ALL_LOGS_INDEX
else:
    st.info("Showing logs flagged by the model as an anomaly. Once feedback is given the log disappears from this view.")
    INDEX_NAME = ANOMALY_INDEX

# Sidebar filters
st.sidebar.title("Filtering")

if st.sidebar.button("🔄 Reset filters"):
    st.session_state["group_filter_option"] = "Show all"
    st.session_state["doc_id_filter"] = ""
    st.session_state["source_ip"] = ""
    st.session_state["destination_ip"] = ""
    st.session_state["protocol"] = ""
    st.session_state["score_threshold"] = 0.0

group_filter_option = st.sidebar.selectbox("Group filter", ["Show all", "Only grouped logs", "Only ungrouped logs (single-log groups)"], key="group_filter_option")
doc_id_filter = st.sidebar.text_input("Search on unique log ID", key="doc_id_filter")
source_ip = st.sidebar.text_input("Filter on Source IP", key="source_ip")
destination_ip = st.sidebar.text_input("Filter on Destination IP", key="destination_ip")
protocol = st.sidebar.text_input("Filter on Network Protocol", key="protocol")
score_type = st.sidebar.selectbox("Select ML-modelscore for filtering", options=["No filtering", "RF", "ISO", "XGBoost", "Logistic", "Average of all"], index=0, key="score_type")

score_threshold = st.sidebar.slider("Minimum average score", min_value=0.0, max_value=1.0, step=0.01, key="score_threshold")
max_logs = st.sidebar.slider("Maximum shown logs", min_value=1, max_value=1000, value=100)
MAX_SAFE_LOGS = 200
if max_logs > MAX_SAFE_LOGS:
    st.warning(f"Showing more than {MAX_SAFE_LOGS} logs may slow down performance.")
    max_logs = MAX_SAFE_LOGS

st.sidebar.markdown("📅 Filter on log date")
start_date = st.sidebar.date_input("Start date")
start_time = st.sidebar.time_input("Start Time", value=dt_time(0, 0))
end_date = st.sidebar.date_input("End date")
end_time = st.sidebar.time_input("End Time", value=dt_time(23, 59))

start_dt = datetime.combine(start_date, start_time)
end_dt = datetime.combine(end_date, end_time)
if end_dt < start_dt:
    end_dt = start_dt

# Query Elasticsearch
try:
    if doc_id_filter:
        query = { "query": { "ids": { "values": [doc_id_filter] } } }
        indexes = [ANOMALY_INDEX, ALL_LOGS_INDEX]
        res = es.search(index=indexes, body=query)
        hits = res["hits"]["hits"]
    else:
        base_query = {
            "bool": {
                "must": [
                    {"term": {"user_feedback.keyword": "unknown"}},
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

        query = { "query": base_query, "size": max_logs, "sort": [{"@timestamp": {"order": "desc"}}] }


        @st.cache_data(ttl=300)
        def query_elasticsearch(index_name, query):
            return es.search(index=index_name, body=query)["hits"]["hits"]

        hits = query_elasticsearch(INDEX_NAME, query)

    if not hits:
        st.success("✅ No anomalies were found. Consider adjusting the filters.")
    else:
        groups = defaultdict(list)
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]
            index_name = hit["_index"]
            source["_origin_index"] = index_name
            raw_ts = source["@timestamp"]
            if "." in raw_ts:
                raw_ts = raw_ts.split(".")[0] + "." + raw_ts.split(".")[1][:6] + "Z"
            timestamp = datetime.strptime(raw_ts, "%Y-%m-%dT%H:%M:%S.%fZ")
            bucket_time = timestamp.replace(second=0, microsecond=0)
            group_key = (source.get("source_ip"), source.get("destination_ip"), source.get("network_transport"), bucket_time)
            groups[group_key].append((doc_id, source))

        for (src_ip, dst_ip, proto, group_time), items in groups.items():
            if group_filter_option == "Only grouped logs" and len(items) == 1:
                continue
            elif group_filter_option == "Only ungrouped logs (single-log groups)" and len(items) > 1:
                continue

            rf_scores = [s.get("RF_score", 0) for _, s in items if isinstance(s.get("RF_score", 0), (int, float))]
            iso_scores = [s.get("isoforest_score", 0) for _, s in items if isinstance(s.get("isoforest_score", 0), (int, float))]
            xgb_scores = [s.get("XGB_score", 0) for _, s in items if isinstance(s.get("XGB_score", 0), (int, float))]
            log_scores = [s.get("logistic_score", 0) for _, s in items if isinstance(s.get("logistic_score", 0), (int, float))]

            avg_rf = sum(rf_scores) / len(rf_scores) if rf_scores else 0
            avg_iso = sum(iso_scores) / len(iso_scores) if iso_scores else 0
            avg_xgb = sum(xgb_scores) / len(xgb_scores) if xgb_scores else 0
            avg_log = sum(log_scores) / len(log_scores) if log_scores else 0
            avg_all = (avg_rf + avg_iso + avg_xgb + avg_log) / 4

            score_map = {
                "RF": avg_rf,
                "ISO": avg_iso,
                "XGBoost": avg_xgb,
                "Logistic": avg_log,
                "Average of all": avg_all,
                "No filtering": None
            }
            selected_score = score_map.get(score_type, None)

            if score_type != "No filtering" and selected_score is not None and selected_score < score_threshold:
                continue

            color = "🟢"
            if selected_score is not None and selected_score > 0.9:
                color = "🔴"
            elif selected_score is not None and selected_score > 0.75:
                color = "🟠"

            orphan_label = "Single log | " if len(items) == 1 else "Grouped logs | "
            if selected_score is not None:
                score_text = f"{score_type}: {selected_score:.2f}"
            else:
                score_text = "No score filtering"

            group_title = (
                f"{orphan_label}{color} {group_time.strftime('%Y-%m-%d %H:%M')} | "
                f"{proto} | {src_ip} ➜ {dst_ip} | logs: {len(items)} | {score_text}"
            )

            with st.expander(group_title):
                group_id = f"{src_ip}_{dst_ip}_{proto}_{group_time.strftime('%Y-%m-%d_%H:%M:%S')}"
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button(f"🕵️ Mark as suspicious", key=f"group_yes_{group_id}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "correct", "reviewed": True}})
                        st.success("✔️ Marked as suspicious")
                        st.rerun()
                with col2:
                    if st.button(f"✅ Mark as normal", key=f"group_no_{group_id}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "incorrect", "reviewed": True}})
                        st.warning("✔️ Marked as normal")
                        st.rerun()

                if show_unflagged_logs:
                    if score_threshold < 0.7:
                        st.warning(
                            "⚠️ Viewing false negatives requires a higher minimum score (>= 0.7) to avoid slowdowns.")
                        st.stop()

                    if st.button(f"🕵️ Mark as missed anomaly", key=f"group_fn_{group_id}"):
                        for doc_id, log in items:
                            es.index(index=ANOMALY_INDEX, document={**log, "user_feedback": "correct", "reviewed": True})
                            es.update(index=ALL_LOGS_INDEX, id=doc_id, body={"doc": {"user_feedback": "correct", "reviewed": True}})
                        st.success("✔️ False negative promoted to anomaly index.")
                        st.rerun()

                for doc_id, source in items:
                    index_label = source.get("_origin_index", "?")
                    label = "Unflagged (evaluated)" if index_label == ALL_LOGS_INDEX else "Flagged (anomaly)"
                    label = "Unflagged (evaluated)" if index_label == ALL_LOGS_INDEX else "Flagged (anomaly)"
                    st.markdown(
                        f"** Log** `{source.get('@timestamp', '?')}` —  `{doc_id}` — {label} —  Index: `{index_label}`")
                    st.code(json.dumps(source, indent=2), language="json")



except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' does not exist.")
except Exception as e:
    st.exception(e)
