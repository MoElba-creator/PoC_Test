import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from scipy.stats import entropy
import hashlib

# Configuration
n_normal = 100000
n_vertical_scans = 1000
n_horizontal_scans = 1000
n_dst_ip_spikes = 1000
n_unusual_pairs = 1000
ports = list(range(1024, 1100))

# Realistic IP-pool
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

    # Entropy calculation
    def port_entropy(x):
        value_counts = x.value_counts(normalize=True)
        return entropy(value_counts)

    entropy_df = df.groupby(["source.ip", "timestamp_minute"])["destination.port"].apply(port_entropy).reset_index(name="port_entropy")
    df = df.merge(entropy_df, on=["source.ip", "timestamp_minute"], how="left")
    df.drop(columns=["timestamp_minute"], inplace=True)
    return df

# Traffic Generators
def normal_traffic(n, label):
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(datetime.now(), n, spacing=2),
        "source.ip": np.random.choice(real_ips, n),
        "destination.ip": np.random.choice(real_ips, n),
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": np.random.choice(ports, n),
        "network.transport": np.random.choice(["tcp", "udp", "icmp"], n),
        "session.iflow_bytes": np.random.randint(100, 20000, n),
        "session.iflow_pkts": np.random.randint(1, 100, n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

def vertical_scan(n, label):
    src_ip = np.random.choice(real_ips)
    dst_ip = np.random.choice(real_ips)
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(datetime.now(), n, spacing=1),
        "source.ip": [src_ip] * n,
        "destination.ip": [dst_ip] * n,
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": np.random.choice(ports, n),
        "network.transport": np.random.choice(["tcp", "udp"], size=n, p=[0.9, 0.1]),
        "session.iflow_bytes": np.random.randint(0, 200, n),
        "session.iflow_pkts": np.random.randint(1, 3, n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

def horizontal_scan(n, label):
    src_ip = np.random.choice(real_ips)
    dst_port = np.random.choice(ports)
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(datetime.now(), n, spacing=1),
        "source.ip": [src_ip] * n,
        "destination.ip": np.random.choice(real_ips, n),
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": [dst_port] * n,
        "network.transport": np.random.choice(["tcp", "udp"], size=n, p=[0.9, 0.1]),
        "session.iflow_bytes": np.random.randint(0, 150, n),
        "session.iflow_pkts": np.random.randint(1, 3, n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

def dst_ip_spike(n, label):
    dst_ip = np.random.choice(real_ips)
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(datetime.now(), n, spacing=0.5),
        "source.ip": np.random.choice(real_ips, n),
        "destination.ip": [dst_ip] * n,
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": np.random.choice(ports, n),
        "network.transport": np.random.choice(["tcp", "udp"], n),
        "session.iflow_bytes": np.random.randint(50, 500, n),
        "session.iflow_pkts": np.random.randint(1, 10, n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

def unusual_pairs(n, label):
    df = pd.DataFrame({
        "@timestamp": generate_timestamp(datetime.now(), n),
        "source.ip": [f"192.168.100.{i % 255}" for i in range(n)],
        "destination.ip": [f"203.0.113.{i % 255}" for i in range(n)],
        "source.port": np.random.randint(10000, 65000, n),
        "destination.port": np.random.choice(ports, n),
        "network.transport": np.random.choice(["tcp", "udp"], n),
        "session.iflow_bytes": np.random.randint(10, 100, n),
        "session.iflow_pkts": np.random.randint(1, 3, n),
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df

# Combine and engineer
df = pd.concat([
    normal_traffic(n_normal, 0),
    vertical_scan(n_vertical_scans, 1),
    horizontal_scan(n_horizontal_scans, 1),
    dst_ip_spike(n_dst_ip_spikes, 1),
    unusual_pairs(n_unusual_pairs, 1)
]).sample(frac=1).reset_index(drop=True)

df = build_df(df)

# Save to JSON
df["@timestamp"] = df["@timestamp"].astype(str)
df.to_json("../data/dummy_network_logs.json", orient="records", indent=2)