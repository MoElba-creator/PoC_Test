import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from scipy.stats import entropy
import hashlib

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

# Configuration
n_normal = 100000
n_vertical_scans = 1000
n_horizontal_scans = 1000
n_dst_ip_spikes = 1000
n_unusual_pairs = 1000
ports = list(range(1024, 1100))

# IP pool
real_ips = [
    "10.192.96.4", "10.192.96.8", "10.192.96.7", "10.195.192.71", "10.195.192.14", "10.195.208.20",
    "10.195.208.216", "172.16.1.69", "10.199.12.78", "10.195.224.9", "10.192.32.65",
    "10.195.211.176", "10.195.192.27", "10.199.5.96", "10.195.208.45", "10.195.224.37", "10.192.96.20",
    "10.195.208.47", "10.193.70.215", "10.195.193.203", "172.16.1.82", "10.199.200.1", "10.192.0.114"
]

def generate_timestamp(start_time, n, spacing=3):
    return [start_time + timedelta(seconds=i*spacing + random.randint(0, 2)) for i in range(n)]

def generate_session_id(row):
    hash_input = f"{row['source.ip']}-{row['destination.ip']}-{row['destination.port']}"
    return int(hashlib.sha256(hash_input.encode()).hexdigest(), 16) % 1000000

def build_df(base_df):
    base_df["@timestamp"] = pd.to_datetime(base_df["@timestamp"])
    df = base_df.copy()
    df["timestamp_minute"] = df["@timestamp"].dt.floor('min')
    df["flow_count_per_minute"] = df.groupby(["source.ip", "timestamp_minute"])["session.id"].transform('count')
    df["unique_dst_ports"] = df.groupby(["source.ip", "timestamp_minute"])["destination.port"].transform('nunique')
    df["bytes_ratio"] = df["session.iflow_bytes"] / (df["session.iflow_pkts"] + 1)

    def port_entropy(x):
        value_counts = x.value_counts(normalize=True)
        return entropy(value_counts)

    entropy_df = df.groupby(["source.ip", "timestamp_minute"])["destination.port"].apply(port_entropy).reset_index(name="port_entropy")
    df = df.merge(entropy_df, on=["source.ip", "timestamp_minute"], how="left")
    df.drop(columns=["timestamp_minute"], inplace=True)

    # New features
    df["flow.duration"] = np.random.randint(10, 1000, len(df))
    df["tcp.flags"] = np.random.choice(["SYN", "ACK", "RST", "FIN", "PSH"], len(df))
    df["agent.version"] = np.random.choice(["8.17.1", "8.16.2", "8.15.0"], len(df))
    df["fleet.action.type"] = np.random.choice(["POLICY_CHANGE", "ENROLL", "ACKNOWLEDGE", "NONE"], len(df), p=[0.2, 0.2, 0.2, 0.4])
    df["message"] = np.random.choice([
        "component model updated",
        "Updating running component model",
        "Action delivered to agent on checkin",
        "component started",
        "heartbeat"
    ], len(df))
    df["msg_code"] = df["message"].astype("category").cat.codes
    df["version_action_pair"] = df["agent.version"] + "-" + df["fleet.action.type"]
    df["proto_port_pair"] = df["network.transport"] + "-" + df["destination.port"].astype(str)
    df["bytes_per_pkt"] = df["session.iflow_bytes"] / (df["session.iflow_pkts"] + 1)
    df["is_suspicious_ratio"] = (df["bytes_per_pkt"] < 2) | (df["bytes_per_pkt"] > 1000)
    df["user_feedback"] = 0
    return df

def generate_traffic(n, label, pattern):
    base_time = datetime(2025, 4, 15) + timedelta(days=random.randint(0, 7))
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(base_time, n, spacing=pattern.get("spacing", 2)),
        "source.ip": pattern.get("src_ips", np.random.choice(real_ips, n)),
        "destination.ip": pattern.get("dst_ips", np.random.choice(real_ips, n)),
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": pattern.get("dst_ports", np.random.choice(ports, n)),
        "network.transport": np.random.choice(pattern.get("transports", ["tcp", "udp"]), size=n, p=pattern.get("transport_probs", None)),
        "session.iflow_bytes": np.random.randint(*pattern.get("bytes_range", (100, 20000)), n),
        "session.iflow_pkts": np.random.randint(*pattern.get("pkts_range", (1, 100)), n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

df = pd.concat([
    generate_traffic(n_normal, 0, {}),
    generate_traffic(n_vertical_scans, 1, {
        "src_ips": [np.random.choice(real_ips)] * n_vertical_scans,
        "dst_ips": [np.random.choice(real_ips)] * n_vertical_scans,
        "spacing": 1,
        "bytes_range": (0, 200),
        "pkts_range": (1, 3)
    }),
    generate_traffic(n_horizontal_scans, 1, {
        "src_ips": [np.random.choice(real_ips)] * n_horizontal_scans,
        "dst_ips": np.random.choice(real_ips, n_horizontal_scans),
        "dst_ports": [np.random.choice(ports)] * n_horizontal_scans,
        "spacing": 1,
        "bytes_range": (0, 150),
        "pkts_range": (1, 3)
    }),
    generate_traffic(n_dst_ip_spikes, 1, {
        "dst_ips": [np.random.choice(real_ips)] * n_dst_ip_spikes,
        "spacing": 0.5,
        "bytes_range": (50, 500),
        "pkts_range": (1, 10)
    }),
    generate_traffic(n_unusual_pairs, 1, {
        "src_ips": [f"192.168.100.{i % 255}" for i in range(n_unusual_pairs)],
        "dst_ips": [f"203.0.113.{i % 255}" for i in range(n_unusual_pairs)],
        "bytes_range": (10, 100),
        "pkts_range": (1, 3)
    })
]).sample(frac=1).reset_index(drop=True)

df = build_df(df)
df["@timestamp"] = df["@timestamp"].astype(str)

df.to_json("../data/dummy_network_logs.json", orient="records", indent=2)