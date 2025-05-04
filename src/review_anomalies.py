import streamlit as st
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import os


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
st.info("Geef feedback op elke anomaly. Enkel anomalies zonder feedback worden weergegeven.")

# === 4. Ophalen van niet-gereviewde anomalies ===
try:
    query = {
        "query": {
             "term": {
                 "user_feedback.keyword": "onbekend"
                     }
                },
        "size": 1000
    }

    res = es.search(index=INDEX_NAME, body=query)
    hits = res["hits"]["hits"]

    if not hits:
        st.success("✅ Alle anomalies zijn al beoordeeld.")
    else:
        for hit in hits:
            source = hit["_source"]
            doc_id = hit["_id"]

            with st.expander(f"{source.get('timestamp', '?')} | {source.get('source_ip')} ➜ {source.get('destination_ip')}"):
                st.json(source)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Dit is effectief verdacht", key=f"yes_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={
                            "doc": {
                                "user_feedback": "correct",
                                "reviewed": True
                            }
                        })
                        st.success("✔️ Geregistreerd als verdacht")
                        st.experimental_rerun()

                with col2:
                    if st.button("❌ Geen echte anomaly", key=f"no_{doc_id}"):
                        es.update(index=INDEX_NAME, id=doc_id, body={
                            "doc": {
                                "user_feedback": "incorrect",
                                "reviewed": True
                            }
                        })
                        st.warning("❗ Geregistreerd als niet-verdacht")
                        st.experimental_rerun()

except NotFoundError:
    st.error(f"Index '{INDEX_NAME}' bestaat niet.")
except Exception as e:
    st.exception(e)
