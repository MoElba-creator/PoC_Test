import os
import json
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from pathlib import Path
from scipy.stats import entropy  # Import entropy
import numpy as np  # Import numpy

# Load environment variables
load_dotenv()
ES_HOST = os.getenv("ES_HOST")
ES_API_KEY = os.getenv("ES_API_KEY")
INDEX_NAME = "network-anomalies"
DATA_DIR = Path("../data")
LATEST_LOGS_FILE = str(DATA_DIR / "validation_logs_latest.json")

def calculate_engineered_features(df):
    """Calculates the engineered features for the given DataFrame."""

    df["@timestamp"] = pd.to_datetime(df["@timestamp"])
    df["timestamp_minute"] = df["@timestamp"].dt.floor('min')

    # Flow statistics
    df["flow_count_per_minute"] = df.groupby(["source_ip", "timestamp_minute"])["session_id"].transform('count')
    df["unique_dst_ports"] = df.groupby(["source_ip", "timestamp_minute"])["destination_port"].transform('nunique')

    # Traffic shape metrics
    df["bytes_ratio"] = df["session_iflow_bytes"] / (df["session_iflow_pkts"] + 1)

    # Port entropy
    def port_entropy_calc(x):
        value_counts = x.value_counts(normalize=True)
        return entropy(value_counts)

    df["port_entropy"] = df.groupby(["source_ip", "timestamp_minute"])["destination_port"].transform(port_entropy_calc)

    return df

def load_logs(file_path):
    """Loads log data from a JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        records = json.load(f)
    df = pd.json_normalize(records)
    return df

def prepare_data_for_model(df):
    """Prepares the log data by calculating engineered features and handling missing values."""

    df = calculate_engineered_features(df)

    # FILL MISSING COLUMNS WITH DEFAULTS
    safe_fill = {
        "flow_count_per_minute": 0,
        "unique_dst_ports": 0,
        "bytes_ratio": 0.0,
        "port_entropy": 0.0,
        "flow_duration": 0,  # Assuming this exists
        "bytes_per_pkt": 0.0, # Assuming this exists
        "msg_code": 0,       # Assuming this exists
        "is_suspicious_ratio": False # Assuming this exists
    }

    for col, default in safe_fill.items():
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)
    return df

if __name__ == "__main__":
    # Example Usage
    try:
        df_logs = load_logs(LATEST_LOGS_FILE)  # Replace with your actual file path or data source

        # Ensure necessary columns exist before calculating features
        required_cols = ["@timestamp", "source.ip", "destination.ip", "destination.port", "session.iflow_bytes", "session.iflow_pkts"]
        for col in required_cols:
            if col not in df_logs.columns:
                raise ValueError(f"Required column '{col}' is missing from log data.")

        df_processed = prepare_data_for_model(df_logs.copy())  # Pass a copy to avoid modifying original

        print(df_processed.head())  # Print the first few rows to verify
        df_processed.to_json("../data/evaluated_logs_with_features.json", orient="records", indent=2)

    except FileNotFoundError:
        print(f"Error: Log file '{LATEST_LOGS_FILE}' not found.")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")