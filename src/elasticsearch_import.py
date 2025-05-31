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
        # This debug line for the query_body is fine as query_body is a Python dict
        print(f"DEBUG: Querying TRACKING_INDEX with: {json.dumps(query_body)}")

        res = es.search(index=TRACKING_INDEX, body=query_body)

        # Safely print debug information from the response without causing serialization errors
        # You can convert the main body of the response to a dict for inspection if needed
        # For elasticsearch-py 8.x, the response object often acts like a dictionary for its main body.
        # Or access specific parts like res.get('hits')
        # Let's try to print a summary or specific parts safely:
        try:
            # Attempt to convert the main response body to dict for logging if it's not already.
            # The ObjectApiResponse often behaves like a dict for its primary data.
            response_summary_for_log = dict(res)
        except Exception:
            # If direct dict conversion fails, just get a string representation or specific safe fields
            response_summary_for_log = {"hits_total": res.get("hits", {}).get("total", {}).get("value", "N/A"),
                                        "took_ms": res.get("took", "N/A")}
        print(f"DEBUG: Summary from TRACKING_INDEX search: {json.dumps(response_summary_for_log, default=str)}")

        # Check for hits safely
        # res.get("hits", {}) ensures 'hits' key exists, .get("hits", []) ensures inner 'hits' list exists
        hits_list = res.get("hits", {}).get("hits", [])
        if hits_list:  # Check if the list of actual hit documents is not empty
            print(f"DEBUG: Found {len(hits_list)} hit(s) in TRACKING_INDEX.")
            first_hit = hits_list[0]
            source = first_hit.get("_source")  # Safely get _source
            if source and "last_run_time" in source:  # Check if _source exists and has last_run_time
                last_run_time_value = source["last_run_time"]
                print(f"DEBUG: Successfully retrieved last_run_time: {last_run_time_value}")
                return last_run_time_value
            else:
                print(f"DEBUG: First hit found, but missing _source or 'last_run_time' field. Hit content: {first_hit}")
        else:
            print(f"DEBUG: No hits found in TRACKING_INDEX for pipeline {PIPELINE_NAME}.")

    except Exception as e:
        # Make sure 'traceback' is imported to use traceback.format_exc()
        # If 'import traceback' is at the top of the file, it's available here.
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
start_time_dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
FETCH_UP_TO_NOW_MARGIN = timedelta(seconds=10)
end_time_dt = datetime.now(timezone.utc) - FETCH_UP_TO_NOW_MARGIN

start_time_iso = start_time_dt.isoformat(timespec='milliseconds') + 'Z'
end_time_iso = end_time_dt.isoformat(timespec='milliseconds') + 'Z'

if start_time_dt >= end_time_dt:
    print(f"No new logs to fetch: start time ({start_time_dt.isoformat()}) is equal or later than ({end_time_dt.isoformat()}).")
    sys.exit(0)

print(f"Fetching logs from {start_time_iso} to {end_time_iso}")

# Build Elasticsearch query to filter by @timestamp
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

# Output path for the pulled logs
OUTPUT_PATH = "../data/validation_logs_latest.json"


try:
    # Stream through the result set
    results = scan(es, query=query, index=INDEX, size=5000)
    docs = list(results)
    print(f"Retrieved {len(docs)} logs.")

    # Make sure output dir exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Write to file as pretty JSON
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
            print(f"No valid @timestamp found in {len(docs)} retrieved logs. Storing calculated query end_time: {end_time_iso}")
            store_last_run_time(end_time_iso)
        else:
            max_timestamp = max(valid_timestamps)
            print(f"Storing run for pipeline {PIPELINE_NAME} at {max_timestamp} (based on max @timestamp from docs)")
            store_last_run_time(max_timestamp)
    except Exception as e: # Catch broader errors during this block
        print(f"Error processing max timestamp: {e}. Storing calculated query end_time {end_time_iso} as fallback.")
        store_last_run_time(end_time_iso)
else:
    print(f"No logs found, storing fallback end_time: {end_time_iso}")
    store_last_run_time(end_time_iso)
