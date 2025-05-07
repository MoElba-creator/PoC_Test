import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os
from datetime import datetime, time
from collections import defaultdict
import json
from PIL import Image

# Login for security reasons
def check_login():
    correct_username = os.getenv("LOGIN_USER")
    correct_password = os.getenv("LOGIN_PASS")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        with st.form("Login"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
            if submitted and username == correct_username and password == correct_password:
                st.session_state.authenticated = True
            else:
                st.error("Invalid credentials")
        st.stop()

check_login()

# Elasticsearch connection
ES_HOST = os.getenv("ES_HOST") or st.secrets["ES_HOST"]
ES_API_KEY = os.getenv("ES_API_KEY") or st.secrets["ES_API_KEY"]
INDEX_NAME = "network-anomalies"

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

#2 UX

logo = Image.open("images/logo_vives.png").convert("RGBA")
white_bg = Image.new("RGBA", logo.size, (255, 255, 255, 255))
white_logo = Image.alpha_composite(white_bg, logo)

st.set_page_config(page_title="VIVES Network logging anomalies review", layout="wide")
col1, col2 = st.columns([2, 10])
with col1:
    st.image(white_logo, use_container_width=True)
with col2:
    st.title("Network logging anomalies review")
st.info("Consult anomaly logging. Once feedback is given the log is not visible anymore.")

# Sidebar filters
st.sidebar.title("Filtering")

# Sidebar: Reset filters
if st.sidebar.button("🔄 Reset filters"):
    st.session_state["group_filter_option"] = "Show all"
    st.session_state["doc_id_filter"] = ""
    st.session_state["source_ip"] = ""
    st.session_state["destination_ip"] = ""
    st.session_state["protocol"] = ""
    st.session_state["score_threshold"] = 0.0


# Sidebar: Define filters with session state keys
group_filter_option = st.sidebar.selectbox(
    "Group filter",
    ["Show all", "Only grouped logs", "Only ungrouped logs (single-log groups)"],
    key="group_filter_option"
)
doc_id_filter = st.sidebar.text_input("Search on unique log ID", value=st.session_state.get("doc_id_filter", ""), key="doc_id_filter")
source_ip = st.sidebar.text_input("Filter on Source IP", value=st.session_state.get("source_ip", ""), key="source_ip")
destination_ip = st.sidebar.text_input("Filter on Destination IP", value=st.session_state.get("destination_ip", ""), key="destination_ip")
protocol = st.sidebar.text_input("Filter on Network Protocol", value=st.session_state.get("protocol", ""), key="protocol")
score_type = st.sidebar.selectbox(
    "Select ML-modelscore for filtering",
    options=["No filtering", "RF", "ISO", "XGBoost", "Logistic", "Average of all"],
    index=0,
    key="score_type"
)

if "score_threshold" not in st.session_state:
    st.session_state["score_threshold"] = 0.0

score_threshold = st.sidebar.slider(
    "Minimum average score", min_value=0.0, max_value=1.0,
    step=0.01, key="score_threshold"
)

max_logs = st.sidebar.slider("Maximum shown logs", min_value=1, max_value=1000, value=100)

st.sidebar.markdown("📅 Filter on log date")
start_date = st.sidebar.date_input("Start date")
start_time = st.sidebar.time_input("Start Time", value=time(0, 0))
end_date = st.sidebar.date_input("End date")
end_time = st.sidebar.time_input("End Time", value=time(23, 59))

start_dt = datetime.combine(start_date, start_time)
end_dt = datetime.combine(end_date, end_time)
if end_dt < start_dt:
    end_dt = start_dt


# Feedback download in JSON
if st.sidebar.button("📥 Download filtered feedback"):
    feedback_query = {
        "bool": {
            "must_not": [{"term": {"user_feedback.keyword": "unknown"}}],
            "filter": [
                {"range": {"@timestamp": {"gte": start_dt.isoformat(), "lte": end_dt.isoformat()}}}
            ]
        }
    }

    if doc_id_filter:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"ids": {"values": [doc_id_filter]}},
                        {"bool": {"must_not": [{"term": {"user_feedback.keyword": "unknown"}}]}}
                    ]
                }
            },
            "size": 1
        }

    else:
        if source_ip:
            feedback_query["filter"].append({"term": {"source_ip.keyword": source_ip}})
        if destination_ip:
            feedback_query["filter"].append({"term": {"destination_ip.keyword": destination_ip}})
        if protocol:
            feedback_query["filter"].append({"term": {"network_transport.keyword": protocol}})
        if score_threshold > 0:
            feedback_query["filter"].append({"range": {"RF_score": {"gte": score_threshold}}})

        query = {
            "query": feedback_query,
            "size": 10000,
            "sort": [{"@timestamp": {"order": "desc"}}]
        }

    try:
        res = es.search(index=INDEX_NAME, body=query)
        filtered_hits = [
            {**hit["_source"], "_id": hit["_id"]}
            for hit in res["hits"]["hits"]
        ]
        feedback_json = json.dumps(filtered_hits, indent=2)

        st.sidebar.download_button(
            label="Download filtered_feedback.json",
            data=feedback_json,
            file_name="filtered_feedback.json",
            mime="application/json"
        )
    except Exception as e:
        st.sidebar.error(f"Download failed: {e}")


# Retrieve flagged logging data from Elasticsearch index
try:
    if doc_id_filter:
        # Filter on id
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
        # Normal query
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
            if group_filter_option == "Only grouped logs" and len(items) == 1:
                continue
            elif group_filter_option == "Only ungrouped logs (single-log groups)" and len(items) > 1:
                continue

            rf_scores = [s.get("RF_score", 0) for _, s in items if isinstance(s.get("RF_score", 0), (int, float))]
            iso_scores = [s.get("isoforest_score", 0) for _, s in items if
                          isinstance(s.get("isoforest_score", 0), (int, float))]
            xgb_scores = [s.get("xgboost_score", 0) for _, s in items if
                          isinstance(s.get("xgboost_score", 0), (int, float))]
            log_scores = [s.get("logistic_score", 0) for _, s in items if
                          isinstance(s.get("logistic_score", 0), (int, float))]

            # Gemiddelde score per model
            avg_rf = sum(rf_scores) / len(rf_scores) if rf_scores else 0
            avg_iso = sum(iso_scores) / len(iso_scores) if iso_scores else 0
            avg_xgb = sum(xgb_scores) / len(xgb_scores) if xgb_scores else 0
            avg_log = sum(log_scores) / len(log_scores) if log_scores else 0

            # Gemiddelde over alle modellen
            avg_all = (avg_rf + avg_iso + avg_xgb + avg_log) / 4

            # Bepaal geselecteerde score op basis van de keuze in de sidebar
            score_map = {
                "RF": avg_rf,
                "ISO": avg_iso,
                "XGBoost": avg_xgb,
                "Logistic": avg_log,
                "Average of all": source.get("model_score", 0),
                "No filtering": None
            }
            selected_score = score_map.get(score_type, None)

            # Filter uitschakelen als gekozen is voor "No filtering"
            if score_type != "No filtering" and selected_score is not None and selected_score < score_threshold:
                continue

            # Kleurindicator gebaseerd op geselecteerde score
            color = "🟢"
            if selected_score is not None and selected_score > 0.9:
                color = "🔴"
            elif selected_score is not None and selected_score > 0.75:
                color = "🟠"

            orphan_label = "Single log | " if len(items) == 1 else "Grouped logs | "
            group_title = (
                f"{orphan_label}{color} {group_time.strftime('%Y-%m-%d %H:%M')} | "
                f"{proto} | {src_ip} ➜ {dst_ip} | logs: {len(items)} | "
                f"{score_type}: {selected_score:.2f}"
            )

            with st.expander(group_title):
                # create a unique, reproducible key
                group_id = f"{src_ip}_{dst_ip}_{proto}_{group_time.strftime('%Y-%m-%d_%H:%M:%S')}"

                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button(f"🕵️ Mark as suspicious", key=f"group_yes_{group_id}"):
                        for doc_id, _ in items:
                            es.update(index=INDEX_NAME, id=doc_id, body={
                                "doc": {"user_feedback": "correct", "reviewed": True}
                            })
                        st.success("✔️ Marked as suspicious successful")
                        st.rerun()
                with col2:
                    if st.button(f"✅ Mark as normal behavior", key=f"group_no_{group_id}"):
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
