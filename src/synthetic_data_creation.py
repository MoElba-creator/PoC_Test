import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from scipy.stats import entropy
import hashlib

# Configuration
random.seed(42)
np.random.seed(42)

# Size of generated dataset
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
    "10.195.211.176", "10.195.192.27", "10.199.5.96",
]

# 1.  Introduce IP Categories and Unlikely Connections
ip_categories = {
    "Web_Server": ["192.0.2.1", "192.0.2.254"],
    "DNS_Server": ["198.51.100.1", "198.51.100.10"],
    "Mail_Server": ["203.0.113.1", "203.0.113.50"],
    "App_Server": ["192.168.1.1", "192.168.1.200"],
    "Database_Server": ["10.0.0.1", "10.0.10.254"],
    "User_PC": ["192.168.10.1", "192.168.10.254"],
    "Unknown": ["172.217.0.0", "172.217.255.255"]
}
unlikely_connections = {
    "User_PC": ["Database_Server"],
    "Web_Server": ["Database_Server", "User_PC"],
    "App_Server": ["User_PC"],
    "DNS_Server": ["App_Server", "User_PC"],
    "Database_Server": ["Unknown", "User_PC"]
}

def generate_ip_from_range(start_ip, end_ip):
    """Generates a random IP address within a given range.

    Args:
        start_ip (str): The starting IP address.
        end_ip (str): The ending IP address.

    Returns:
        str: A random IP address within the range.
    """
    def ip_to_int(ip_str):
        parts = ip_str.split(".")
        return (int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3])

    def int_to_ip(ip_int):
        return ".".join([
            str((ip_int >> 24) & 255),
            str((ip_int >> 16) & 255),
            str((ip_int >> 8) & 255),
            str(ip_int & 255),
        ])

    start_int = ip_to_int(start_ip)
    end_int = ip_to_int(end_ip)
    return int_to_ip(random.randint(start_int, end_int))


def generate_timestamp(start_time, n, spacing=3):
    """Generates a list of timestamps.

       Args:
           start_time (datetime): The starting datetime.
           n (int): The number of timestamps to generate.
           spacing (int, optional): The spacing between timestamps in seconds. Defaults to 3.

       Returns:
           list: A list of datetime objects.
       """
    return [start_time + timedelta(seconds=i * spacing + random.randint(0, 2)) for i in range(n)]


def generate_session_id(row):
    """Create a stable session.id from IPs and port using a SHA256 hash.

    Args:
        row (pd.Series): A row of the DataFrame containing 'source.ip', 'destination.ip', and 'destination.port'.

    Returns:
        int: A unique session ID.
    """
    hash_input = f"{row['source.ip']}-{row['destination.ip']}-{row['destination.port']}"
    return int(hashlib.sha256(hash_input.encode()).hexdigest(), 16) % 1000000


def generate_traffic(n, label, pattern):
    """
    Generates network traffic data with specified characteristics.

    Args:
        n (int): Number of records to generate.
        label (int): 0 for normal, 1 for anomalous.
        pattern (dict): A dictionary specifying the traffic pattern.  Possible keys:
            'src_ips', 'dst_ips', 'src_ports', 'dst_ports', 'spacing',
            'bytes_range', 'pkts_range', 'traffic_type'

    Returns:
        pd.DataFrame: A DataFrame containing the generated traffic data.
    """

    base_time = datetime(2025, 4, 15) + timedelta(days=random.randint(0, 7))

    common_ports = [53, 9200, 67, 2048, 53202, 68, 443, 770, 769, 4122, 4120, 445, 135, 88, 389, 123, 771, 47808, 49670,
                    4118]
    weights = [
        0.55,  # 53 (DNS) — most common
        0.16,  # 9200 (Elasticsearch)
        0.10,  # 67 (DHCP)
        0.012,  # 2048
        0.01,  # 53202
        0.005,  # 68 (DHCP)
        0.004,  # 443 (HTTPS)
        0.002, 0.002, 0.0015, 0.0012,  # 770, 769, 4122, 4120
        0.001, 0.0008, 0.0007, 0.0005, 0.0005,  # 445, 135, 88, 389, 123
        0.0003, 0.0003, 0.0002,
        0.0001
    ]
    # Normalize weights so they sum to 1
    weights = [w / sum(weights) for w in weights]
    # Realistic destination port assignment based on known log frequencies
    destination_ports = np.random.choice(common_ports, size=n, p=weights)

    # Generate flow byte values using normal distribution
    # Why log-normal? Because real network traffic is *not* symmetric:
    # - Most logs have low byte counts
    # - A few logs have *very large* spikes
    mean_adjusted = 6.5  # Controls the center of distribution
    sigma_adjusted = 2.5  # Controls the spread
    raw_bytes = np.random.lognormal(mean=mean_adjusted, sigma=sigma_adjusted, size=n).astype(int)
    raw_bytes = np.clip(raw_bytes, 0, 2848576)

    # Make 50% of flows equal to 0 — this reflects real stats:
    # In the real dataset 50% of logs had 0 bytes transferred.
    zero_mask = np.random.rand(n) < 0.5
    raw_bytes[zero_mask] = 0

    session_iflow_bytes = raw_bytes

    # Packets follow similar distribution to bytes, but with slightly lower mean
    # This gives us values like 0, 1, 2, 5, 100, 1000, 60000, etc.
    mean_pkts = 1.5
    sigma_pkts = 2.4
    pkt_base = np.random.lognormal(mean=mean_pkts, sigma=sigma_pkts, size=n).astype(int)
    pkt_base = np.clip(pkt_base, 0, 78600)
    # Make 50% of them zero
    pkt_base[np.random.rand(n) < 0.5] = 0

    # Ports used for source: real logs show a lot of high random source ports (>30000),
    # but also some logs have 0 as source port (DHCP or malformed flows).
    # So we include a few zeros in the pool to reflect that.
    source_port_pool = (
            [0] * 2000 +  # boost zero to match spike
            list(np.random.randint(50000, 65536, 8000))  # sample only from high ports
    )

    df = pd.DataFrame({
        "@timestamp": generate_timestamp(base_time, n, spacing=pattern.get("spacing", 2)),
        "source.ip": pattern.get("src_ips", np.random.choice(real_ips, n)),
        "destination.ip": pattern.get("dst_ips", np.random.choice(real_ips, n)),
        "source.port": np.random.choice(source_port_pool, size=n),
        "destination.port": pattern.get("dst_ports", destination_ports),
        "network.transport": np.random.choice(pattern.get("transports", ["tcp", "udp"]), size=n,
                                              p=pattern.get("transport_probs", None)),
        "session.iflow_bytes": session_iflow_bytes,
        "session.iflow_pkts": pkt_base,
        "event.action": ["flow_create"] * n,
    })
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = label
    return df


# 2.  Modify the Unusual IP Pairs Generation
def generate_unusual_pairs(n):
    """Generates n "unusual IP pair" records."""
    data = []
    for _ in range(n):
        # Select source and destination categories, avoiding unlikely pairs
        src_category = random.choice(list(ip_categories.keys()))
        possible_dst_categories = [
            k for k in ip_categories.keys() if k not in unlikely_connections.get(src_category, [])
        ]
        dst_category = random.choice(possible_dst_categories)

        # Generate IPs from the selected categories
        src_ip = generate_ip_from_range(*ip_categories[src_category])
        dst_ip = generate_ip_from_range(*ip_categories[dst_category])

        #  Use the refined parameters (from DeepSeek suggestion and your feedback)
        spacing = np.random.choice([0.1, 0.5, 2], p=[0.7, 0.2, 0.1])
        network_transport = np.random.choice(["tcp", "udp"], p=[0.3, 0.7])
        bytes_range = (10, 5000)
        pkts_range = (1, 20)
        start_time = datetime.now()
        timestamp = start_time + timedelta(seconds=_)

        src_port = random.choice(ports)
        dst_port = random.choice(ports)
        event_action = "allowed"
        tcp_flags = "PA"
        agent_version = "8.10.0"
        fleet_action_type = "network"
        message = "Unusual traffic detected"
        proto_port_pair = f"{network_transport}:{src_port}-{dst_port}"
        version_action_pair = f"{agent_version}:{event_action}"
        flow_count_per_minute = random.randint(1, 100)
        unique_dst_ports = random.randint(1, 5)
        bytes_ratio = random.uniform(0, 1)
        port_entropy = random.uniform(0, 8)
        flow_duration = random.uniform(0.1, 60)
        bytes_per_pkt = random.randint(10, 1000)
        msg_code = random.randint(1000, 2000)
        is_suspicious_ratio = random.uniform(0, 0.5)
        record = {
            "@timestamp": timestamp.isoformat(),
            "source.ip": src_ip,
            "destination.ip": dst_ip,
            "source.port": src_port,
            "destination.port": dst_port,
            "network.transport": network_transport,
            "event.action": event_action,
            "tcp.flags": tcp_flags,
            "agent.version": agent_version,
            "fleet.action.type": fleet_action_type,
            "message": message,
            "proto_port_pair": proto_port_pair,
            "version_action_pair": version_action_pair,
            "session.iflow_bytes": random.randint(*bytes_range),
            "session.iflow_pkts": random.randint(*pkts_range),
            "flow_count_per_minute": flow_count_per_minute,
            "unique_dst_ports": unique_dst_ports,
            "bytes_ratio": bytes_ratio,
            "port_entropy": port_entropy,
            "flow.duration": flow_duration,
            "bytes_per_pkt": bytes_per_pkt,
            "msg_code": msg_code,
            "is_suspicious_ratio": is_suspicious_ratio,
        }
        data.append(record)

    df = pd.DataFrame(data)
    df["session.id"] = df.apply(generate_session_id, axis=1)
    df["label"] = 1  # Anomalous
    return df

# Core feature engineering
def build_df(base_df):
    """Add all engineered features and synthetic metadata fields to the dataset.

    Args:
        base_df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with added features.
    """
    base_df["@timestamp"] = pd.to_datetime(base_df["@timestamp"])
    df = base_df.copy()
    df["timestamp_minute"] = df["@timestamp"].dt.floor('min')

    # After extracting timestamp_minute, convert @timestamp to ISO 8601 string
    df["@timestamp"] = df["@timestamp"].apply(
        lambda ts: ts.strftime('%Y-%m-%dT%H:%M:%S.%f') + f"{random.randint(0, 999):03d}Z"
    )
    # Flow statistics: how many flows/IP/minute and unique port spread
    df["flow_count_per_minute"] = df.groupby(["source.ip", "timestamp_minute"])["session.id"].transform('count')
    df["unique_dst_ports"] = df.groupby(["source.ip", "timestamp_minute"])["destination.port"].transform('nunique')

    # Traffic shape metrics
    df["bytes_ratio"] = df["session.iflow_bytes"] / (df["session.iflow_pkts"] + 1)

    # Port entropy
    def port_entropy(x):
        value_counts = x.value_counts(normalize=True)
        return entropy(value_counts)

    entropy_series = df.groupby(["source.ip", "timestamp_minute"])["destination.port"].transform(port_entropy)
    df["port_entropy"] = entropy_series

    # Synthetic metadata fields
    df["flow.duration"] = np.random.randint(10, 1000, len(df))
    df["tcp.flags"] = np.random.choice(["SYN", "ACK", "RST", "FIN", "PSH"], len(df))
    df["agent.version"] = np.random.choice(["8.17.1", "8.16.2", "8.15.0"], len(df))
    df["fleet.action.type"] = np.random.choice(["POLICY_CHANGE", "ENROLL", "ACKNOWLEDGE", "NONE"], len(df),
                                              p=[0.2, 0.2, 0.2, 0.4])
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


# 3.  Combine Traffic Generation
def generate_combined_traffic():
    """Combines normal and anomalous traffic generation."""
    df = pd.concat([
        generate_traffic(n_normal, 0, {}),
        # One source IP targeting many different destination ports on a single destination IP.
        generate_traffic(n_vertical_scans, 1, {
            "src_ips": [np.random.choice(real_ips)] * n_vertical_scans,
            "dst_ips": [np.random.choice(real_ips)] * n_vertical_scans,
            "spacing": 1,
            "bytes_range": (0, 200),
            "pkts_range": (1, 3)
        }),
        # One source IP targeting the same port on many destination IPs.
        generate_traffic(n_horizontal_scans, 1, {
            "src_ips": [np.random.choice(real_ips)] * n_horizontal_scans,
            "dst_ips": np.random.choice(real_ips, n_horizontal_scans),
            "dst_ports": [np.random.choice(ports)] * n_horizontal_scans,
            "spacing": 1,
            "bytes_range": (0, 150),
            "pkts_range": (1, 3)
        }),
        # Simulates many connections to the same destination IP in a short time. DDoS e.g.
        generate_traffic(n_dst_ip_spikes, 1, {
            "dst_ips": [np.random.choice(real_ips)] * n_dst_ip_spikes,
            "spacing": 0.5,
            "bytes_range": (50, 500),
            "pkts_range": (1, 10)
        }),
        # Source and destination IPs are never-before-seen.
        generate_unusual_pairs(n_unusual_pairs)
    ]).sample(frac=1).reset_index(drop=True)

    df = build_df(df)
    return df


# 4. Main Execution
if __name__ == "__main__":
    df = generate_combined_traffic()
    df.to_json("../data/dummy_network_logs.json", orient="records", indent=2)
    print("✔ Successfully generated dummy network logs and saved to ../data/dummy_network_logs.json")
