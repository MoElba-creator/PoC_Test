import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import altair as alt
from urllib.parse import urlencode

load_dotenv()

# ──────────────────────────────────────────────
# AUTH CHECK
from core.auth import check_login
check_login()

# ──────────────────────────────────────────────
# ES Setup
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

es = Elasticsearch(
    hosts=[ES_HOST],
    api_key=ES_API_KEY,
    headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"},
    request_timeout=30
)

INDEX = "network-anomalies"
MAX_DOCS = 5000

# ──────────────────────────────────────────────
# Sidebar Filters
st.sidebar.header("🔍 Query Filters")
max_logs = st.sidebar.slider("Max logs to fetch", min_value=100, max_value=5000, value=1000, step=100)
min_score = st.sidebar.slider("Minimum model score", 0.0, 1.0, 0.0, 0.01)
only_with_feedback = st.sidebar.checkbox("Only logs with user feedback")

# Time range (fixed to last 5 days)
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=5)

# ──────────────────────────────────────────────
# Query Elasticsearch
@st.cache_data(ttl=300)
def fetch_logs():
    query = {
        "size": max_logs,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": start_time.isoformat(), "lte": end_time.isoformat()}}},
                    {"range": {"model_score": {"gte": min_score}}}
                ]
            }
        }
    }
    if only_with_feedback:
        query["query"]["bool"]["must"].append({"exists": {"field": "user_feedback"}})
    try:
        res = es.search(index=INDEX, body=query)
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        st.error(f"Elasticsearch query failed: {e}")
        return []

records = fetch_logs()
if not records:
    st.warning("No logs found for the selected filters.")
    st.stop()

# DataFrame
df = pd.DataFrame(records)
df["@timestamp"] = pd.to_datetime(df["@timestamp"], errors="coerce")
df = df.dropna(subset=["@timestamp"])
df["@timestamp"] = pd.to_datetime(df["@timestamp"], utc=True, errors="coerce")
df = df[df["@timestamp"] >= pd.Timestamp(start_time).tz_localize("UTC")]

# ──────────────────────────────────────────────
# METRICS
col1, col2, col3 = st.columns(3)
col1.metric("Logs loaded", len(df))
col2.metric("Avgerage model score", round(df["model_score"].mean(), 4))
col3.metric("Unique Source IPs", df["source_ip"].nunique() if "source_ip" in df else "N/A")

# ──────────────────────────────────────────────
# CHART: Anomalies Over Time with click support
st.markdown("### ⏱️ Anomalies Over Time (Click to review)")
if not df.empty:
    df["hour"] = df["@timestamp"].dt.floor("H")
    time_hist = df.groupby("hour").size().reset_index(name="count")
    time_hist["hour_str"] = time_hist["hour"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    click = alt.selection_single(fields=["hour_str"], empty="none", on="click")
    chart = alt.Chart(time_hist).mark_bar().encode(
        x=alt.X("hour:T", title="Hour"),
        y=alt.Y("count:Q", title="# Anomalies"),
        tooltip=["hour:T", "count:Q"],
        opacity=alt.condition(click, alt.value(1), alt.value(0.5))
    ).add_selection(click).properties(height=250)

if not df.empty:
    st.altair_chart(chart, use_container_width=True)

    # Show top 10 busiest hours with links
    st.markdown("###Explore busiest hours")
    top_hours = time_hist.sort_values("count", ascending=False).head(10)
    for _, row in top_hours.iterrows():
        from_ts = row["hour"].isoformat()
        to_ts = (row["hour"] + timedelta(hours=1)).isoformat()
        hour_str = row["hour"].strftime("%Y-%m-%d %H:%M")
        query = urlencode({"from_ts": from_ts, "to_ts": to_ts})
        link = f"/?{query}"
        st.markdown(f"🕒 **{hour_str}** — {row['count']} logs &nbsp;&nbsp; [🔍 View logs]({link})", unsafe_allow_html=True)
else:
    st.info("No data to plot.")

# ──────────────────────────────────────────────
if "source_ip" in df:
    st.markdown("###Top 10 Source IPs")
    top_ips = df["source_ip"].value_counts().head(10).reset_index()
    top_ips.columns = ["source_ip", "count"]
    st.bar_chart(top_ips.set_index("source_ip"))

# ──────────────────────────────────────────────
if "model_score" in df:
    st.markdown("### Model Score Distribution")
    hist = alt.Chart(df).mark_bar().encode(
        alt.X("model_score:Q", bin=alt.Bin(maxbins=20), title="Model Score"),
        y=alt.Y("count():Q", title="Count"),
        tooltip=["count():Q"]
    ).properties(height=250)
    st.altair_chart(hist, use_container_width=True)

# ──────────────────────────────────────────────
if "user_feedback" in df:
    st.markdown("### User Feedback Overview")
    fb_counts = df["user_feedback"].fillna("unknown").value_counts().reset_index()
    fb_counts.columns = ["Feedback", "Count"]
    chart = alt.Chart(fb_counts).mark_arc(innerRadius=40).encode(
        theta="Count:Q",
        color="Feedback:N",
        tooltip=["Feedback:N", "Count:Q"]
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

# ──────────────────────────────────────────────
st.markdown("---")
