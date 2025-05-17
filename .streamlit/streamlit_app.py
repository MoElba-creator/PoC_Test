import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from pathlib import Path
from datetime import datetime, timezone, time as dt_time
from collections import defaultdict
import json
from PIL import Image
from core.auth import check_login

check_login()

st.set_page_config(page_title="VIVES Network logging anomalies review", layout="wide")


# --- Sidebar Filters First (important for datetime vars) ---
st.sidebar.title("🔄 Filtering")

col_reset, col_clear = st.sidebar.columns(2)

with col_reset:
    if st.button("Reset filters"):
        st.session_state["group_filter_option"] = "Show all"
        st.session_state["doc_id_filter"] = ""
        st.session_state["source_ip"] = ""
        st.session_state["destination_ip"] = ""
        st.session_state["protocol"] = ""
        st.session_state["score_threshold"] = 0.0

with col_clear:
    if st.button("Clear dashboard filter"):
        st.experimental_set_query_params()
        st.rerun()

group_filter_option = st.sidebar.selectbox("Group filter", ["Show all", "Only grouped logs", "Only single logs"], key="group_filter_option")
doc_id_filter = st.sidebar.text_input("Search on unique log ID", key="doc_id_filter").strip()
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

# --- URL-based timestamp filtering (from dashboard) ---
if "url_params_buffer" not in st.session_state:
    st.session_state["url_params_buffer"] = st.query_params

qs = st.session_state["url_params_buffer"]
from_ts = qs.get("from_ts", [None])[0]
to_ts = qs.get("to_ts", [None])[0]
try:
    if from_ts and to_ts:
        start_dt = datetime.fromisoformat(from_ts)
        end_dt = datetime.fromisoformat(to_ts)

        start_date = start_dt.date()
        start_time = start_dt.time().replace(second=0, microsecond=0)
        end_date = end_dt.date()
        end_time = end_dt.time().replace(second=0, microsecond=0)

        st.sidebar.date_input("Start date", value=start_date, key="url_start_date")
        st.sidebar.time_input("Start Time", value=start_time, key="url_start_time")
        st.sidebar.date_input("End date", value=end_date, key="url_end_date")
        st.sidebar.time_input("End Time", value=end_time, key="url_end_time")

        st.sidebar.info(
            f"📅 Filter auto-set from dashboard: {start_dt.strftime('%Y-%m-%d %H:%M')} → {end_dt.strftime('%H:%M')}"
        )

        if st.sidebar.button("Clear dashboard filter"):
            st.experimental_set_query_params()  # Clears all query params
            st.rerun()
    else:
        raise ValueError("Missing params")
except Exception:
    st.sidebar.markdown("📅 Filter on log date")
    start_date = st.sidebar.date_input("Start date")
    start_time = st.sidebar.time_input("Start Time", value=dt_time(0, 0))
    end_date = st.sidebar.date_input("End date")
    end_time = st.sidebar.time_input("End Time", value=dt_time(23, 59))

    start_dt = datetime.combine(start_date, start_time)
    end_dt = datetime.combine(end_date, end_time)
    if end_dt < start_dt:
        end_dt = start_dt

# --- Indexes and connection ---
ANOMALY_INDEX = "network-anomalies"
ALL_LOGS_INDEX = "network-anomalies-all"

ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

# --- Logo ---
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

    base_query = {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [
                            {"term": {"user_feedback.keyword": "unknown"}},
                            {"bool": {"must_not": {"exists": {"field": "user_feedback"}}}}
                        ]
                    }
                },
                {
                    "bool": {
                        "must_not": [
                            {"exists": {"field": "RF_pred"}},
                            {"exists": {"field": "LOG_pred"}},
                            {"exists": {"field": "XGB_pred"}}
                        ]
                    }
                },
                {"range": {"@timestamp": {"gte": start_dt.isoformat(), "lte": end_dt.isoformat()}}}
            ]
        }
    }
else:
    st.info("Showing logs flagged by the model as an anomaly. Once feedback is given the log disappears from this view.")
    INDEX_NAME = ANOMALY_INDEX


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

        query = { "query": base_query, "size": max_logs, "sort": [
    {"@timestamp": {"order": "desc", "unmapped_type": "date"}}] }


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
            elif group_filter_option == "Only single logs" and len(items) > 1:
                continue

            rf_scores = [s.get("RF_score", 0) for _, s in items if isinstance(s.get("RF_score", 0), (int, float))]
            iso_scores = [s.get("isoforest_score", 0) for _, s in items if isinstance(s.get("isoforest_score", 0), (int, float))]
            xgb_scores = [s.get("XGB_score", 0) for _, s in items if isinstance(s.get("XGB_score", 0), (int, float))]
            log_scores = [s.get("LOG_score", 0) for _, s in items if isinstance(s.get("LOG_score", 0), (int, float))]

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
                feedback_time = datetime.now(timezone.utc).isoformat()
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button(f"🕵️ Mark as suspicious", key=f"group_yes_{group_id}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "correct", "reviewed": True, "feedback_timestamp": feedback_time}})
                        st.success("✔️ Marked as suspicious")
                        st.rerun()
                with col2:
                    if st.button(f"✅ Mark as normal", key=f"group_no_{group_id}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={"doc": {"user_feedback": "incorrect", "reviewed": True, "feedback_timestamp": feedback_time }})
                        st.warning("✔️ Marked as normal")
                        st.rerun()

                if show_unflagged_logs:
                    if score_threshold < 0.7:
                        st.warning(
                            "Viewing false negatives requires a higher minimum score (>= 0.7) to avoid slowdowns.")
                        st.stop()

                    if st.button(f"🕵️ Mark as missed anomaly", key=f"group_fn_{group_id}"):
                        for doc_id, log in items:
                            es.index(index=ANOMALY_INDEX, document={**log, "user_feedback": "correct", "reviewed": True, "feedback_timestamp": feedback_time})
                            es.update(index=ALL_LOGS_INDEX, id=doc_id, body={"doc": {"user_feedback": "correct", "reviewed": True, "feedback_timestamp": feedback_time }})
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
