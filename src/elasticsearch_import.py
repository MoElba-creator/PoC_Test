"""
Script: elasticsearch_import.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script pulls the most recent logs from an Elasticsearch index.
It uses a tracking index to remember the last fetch time so we only grab new logs.
The output is saved to JSON and used as input for downstream model evaluation.

This is part of the ETL flow before ML_batch_scan.py runs.
"""

import os
import json
import sys
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from dotenv import load_dotenv
import traceback
from dateutil import parser as dateutil_parser

# Load credentials from .env file
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")

# Main log index and metadata tracking index
INDEX = "logs-*"
TRACKING_INDEX = "etl-log-tracking"
PIPELINE_NAME = "vives-etl"

# Connect to Elasticsearch
es = Elasticsearch(
    ES_HOST,
    api_key=ES_API_KEY,
    verify_certs=True
)

# Return the last timestamp this pipeline was executed
def get_last_run_time():
    try:
        query_body = {
            "size": 1,
            "sort": [{"last_run_time": "desc"}],
            # Using .keyword is good practice for exact term matches if your mapping supports it
            "query": {"term": {"pipeline.keyword": PIPELINE_NAME}}
        }
        print(f"DEBUG: Querying TRACKING_INDEX with: {json.dumps(query_body)}")

        res = es.search(index=TRACKING_INDEX, body=query_body)

        try:
            response_summary_for_log = dict(res)
        except Exception:
            response_summary_for_log = {"hits_total": res.get("hits", {}).get("total", {}).get("value", "N/A"),
                                        "took_ms": res.get("took", "N/A")}
        print(f"DEBUG: Summary from TRACKING_INDEX search: {json.dumps(response_summary_for_log, default=str)}")

        hits_list = res.get("hits", {}).get("hits", [])
        if hits_list:
            print(f"DEBUG: Found {len(hits_list)} hit(s) in TRACKING_INDEX.")
            first_hit = hits_list[0]
            source = first_hit.get("_source")
            if source and "last_run_time" in source:
                last_run_time_value = source["last_run_time"]
                print(f"DEBUG: Successfully retrieved last_run_time: {last_run_time_value}")
                return last_run_time_value
            else:
                print(f"DEBUG: First hit found, but missing _source or 'last_run_time' field. Hit content: {first_hit}")
        else:
            print(f"DEBUG: No hits found in TRACKING_INDEX for pipeline {PIPELINE_NAME}.")

    except Exception as e:
        print(
            f"WARNING: get_last_run_time() fell back to fallback of 10 minutes. Problem: {e}\nTraceback: {traceback.format_exc()}")

    print(f"DEBUG: Proceeding with 10-minute fallback in get_last_run_time.")
    return (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

# Write a new timestamp after successful fetch
def store_last_run_time(run_end_time):
    try:
        es.index(index=TRACKING_INDEX, document={
            "pipeline": PIPELINE_NAME,
            "last_run_time": run_end_time,
            "status": "success"
        })
        es.indices.refresh(index=TRACKING_INDEX)
    except Exception as e:
        print(f"Error: Cannot save last run time: ({run_end_time}): {e}")
        sys.exit(1)


start_time_str = get_last_run_time()

try:
    start_time_dt = dateutil_parser.isoparse(start_time_str)
except ValueError as e_parse:
    print(
        f"FATAL: Could not parse retrieved start_time_str ('{start_time_str}') with dateutil.parser: {e_parse}. Exiting.")
    sys.exit(1)

FETCH_UP_TO_NOW_MARGIN = timedelta(seconds=10)
end_time_dt = datetime.now(timezone.utc) - FETCH_UP_TO_NOW_MARGIN

if start_time_dt.tzinfo is None:
    start_time_dt = start_time_dt.replace(tzinfo=timezone.utc)
else:
    start_time_dt = start_time_dt.astimezone(timezone.utc)

start_time_iso = start_time_dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
end_time_iso = end_time_dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

if start_time_dt >= end_time_dt:
    print(
        f"No new logs to fetch: start time ({start_time_dt.isoformat()}) is equal or later than ({end_time_dt.isoformat()}).")
    sys.exit(0)

print(f"Fetching logs from {start_time_iso} to {end_time_iso}")

query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": start_time_iso,
                "lt": end_time_iso
            }
        }
    },
    "_source": True
}

OUTPUT_PATH = "../data/validation_logs_latest.json"

try:
    results = scan(es, query=query, index=INDEX, size=5000)
    docs = list(results)
    print(f"Retrieved {len(docs)} logs.")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)
    print(f"Saved logs to {OUTPUT_PATH}")
except Exception as e:
    print(f"Error fetching logs: {e}")
    sys.exit(1)

# Register this run in the tracking index
if docs:
    try:
        valid_timestamps = [
            doc['_source']['@timestamp']
            for doc in docs
            if doc.get('_source') and isinstance(doc['_source'].get('@timestamp'), str) and doc['_source']['@timestamp']
        ]
        if not valid_timestamps:
            print(
                f"No valid @timestamp found in {len(docs)} retrieved logs. Storing calculated query end_time: {end_time_iso}")
            store_last_run_time(end_time_iso)
        else:
            max_ts_str_from_docs = max(valid_timestamps)  # Can have nanoseconds and 'Z'

            timestamp_to_store_str = ""
            try:
                dt_obj = dateutil_parser.isoparse(max_ts_str_from_docs)

                timestamp_to_store_str = dt_obj.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace(
                    '+00:00', 'Z')

                print(
                    f"Storing run for pipeline {PIPELINE_NAME} at {timestamp_to_store_str} (formatted to ms from max @timestamp: {max_ts_str_from_docs})")
                store_last_run_time(timestamp_to_store_str)

            except Exception as e_format_store:
                print(
                    f"Error preparing max_timestamp ('{max_ts_str_from_docs}') for storage: {e_format_store}. Storing calculated query end_time {end_time_iso} as fallback.")
                store_last_run_time(end_time_iso)

    except Exception as e:
        print(
            f"Error processing docs for max timestamp: {e}. Storing calculated query end_time {end_time_iso} as fallback.")
        store_last_run_time(end_time_iso)
else:
    print(f"No logs retrieved. Storing calculated query end_time: {end_time_iso}")
    store_last_run_time(end_time_iso)