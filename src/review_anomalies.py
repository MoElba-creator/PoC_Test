import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch.exceptions import NotFoundError
from dotenv import load_dotenv
import os
from datetime import datetime

# === 1. Configuratie en connectie ===
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"

es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# === 2. Streamlit pagina ===
st.set_page_config(page_title="Anomalieën Review", layout="wide")
st.title("🔍 Feedback op Netwerk Anomalieën")

# === 3. Sidebar filters ===
st.sidebar.header("🔎 Filters")
min_score = st.sidebar.slider("Minimum model_score", 0.0, 1.0, 0.90)
max_items = st.sidebar.number_input("Maximaal aantal te tonen", min_value=10, max_value=5000, value=200)

# === 4. Totaal aantal nog te beoordelen records tonen ===
count_query = {
    "query": {
        "term": {
            "reviewed": False
        }
    }
}
total = es.count(index=INDEX_NAME, body=count_query)["count"]
st.sidebar.markdown(f"📝 Nog te labelen: **{total}**")

# === 5. Zoek alle ongeëvalueerde anomalies met scan helper ===
query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"reviewed": False}},
                {"range": {"model_score": {"gte": min_score}}}
            ]
        }
    }
}

try:
    scanned = scan(es, index=INDEX_NAME, query=query, size=500)
    docs = list(scanned)
    hits = docs[:max_items]

    if not hits:
        st.success("🎉 Geen nieuwe anomalies om te labelen.")
    else:
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]

            title = f"{source.get('source_ip', '?')} → {source.get('destination_ip', '?')} | Port: {source.get('destination_port', '?')} | Score: {source.get('model_score', '?'):.2f}"
            with st.expander(title):
                st.json(source)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Correct", key=f"correct_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, doc={
                            "doc": {
                                "user_feedback": "correct",
                                "reviewed": True,
                                "feedback_timestamp": datetime.utcnow().isoformat()
                            }
                        })
                        st.success("Gemarkeerd als correct")

                with col2:
                    if st.button("❌ Incorrect", key=f"incorrect_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, doc={
                            "doc": {
                                "user_feedback": "incorrect",
                                "reviewed": True,
                                "feedback_timestamp": datetime.utcnow().isoformat()
                            }
                        })
                        st.error("Gemarkeerd als incorrect")

except NotFoundError:
    st.warning(f"Index '{INDEX_NAME}' werd niet gevonden.")
