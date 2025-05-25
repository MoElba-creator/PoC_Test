"""
Script: dummy_real_analysis.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script compares the dummy log data with real network logs exported from Elasticsearch.
It performs structural comparisons, statistical checks and visual inspections to ensure the dummy data
matches real-world traffic behavior. It helps validate that the logs are realistic enough to train
machine learning models and apply detection logic.

What this script does:
1. Loads dummy and real log datasets in JSON format. Run script dummy_data_creation.py and elasticsearch_export.py first.
2. Compares their column structures such as 'what fields exist in each?'
3. Compares data types and finds shared numerical columns.
4. Checks for missing values.
5. Shows descriptive statistics like min, max, mean for shared numerical fields.
6. Visualizes histograms to compare dummy vs. real data distributions.
7. Prints how often certain destination ports and IPs are seen.
8. Verifies whether 'trusted IPs' and 'low-risk ports' are actually dominant in real logs.
   These values are used for filtering in ML_batch_scan.py.
9. Shows how  labels are distributed over time.
10. Visualizes vertical scans.
11. Performs PCA projection to see if anomalies and normal traffic can be separated in 2D.

How to read the output:
- "Common columns" shows which fields exist in both real and dummy logs.
- "Columns unique to..." helps identify fields that are only in one of the sets.
- The numerical stats allow comparing means, max values and outliers between both sets.
- Histograms for each numerical feature appear side by side (dummy left, real right) — this
  helps  assess whether the dummy generation logic produces similar patterns.
- Destination port frequency confirms if ports like 53 (DNS) and 9200 (Elasticsearch) dominate.
- Trusted source and destination IP counts help validate if excluding them from anomaly detection makes sense.
- The flow volume over time plot helps visualize burst patterns or attack spikes.
- The port scan chart shows how many different destination ports each source IP hits — useful to identify vertical scans.
- The PCA plot attempts to reduce many dimensions (features) into two, and plots normal vs anomaly as separate colors.
  If you see some visible separation, it means your features may be useful for training a model.

Note:
- This script does not do any training or prediction — it's just for comparison and exploration.
- You can rerun this script after adjusting dummy data logic to revalidate realism.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data
try:
    dummy_df = pd.read_json("../data/dummy_network_logs.json")
    real_df = pd.read_json("../data/validation_dataset.json")
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure the files are in the correct location.")
    exit()

# Flatten real logs
if '_source' in real_df.columns:
    real_df = pd.json_normalize(real_df['_source'])

# Compare columns
dummy_cols = set(dummy_df.columns)
real_cols = set(real_df.columns)

common_cols = dummy_cols.intersection(real_cols)
dummy_unique_cols = dummy_cols.difference(real_cols)
real_unique_cols = real_cols.difference(dummy_cols)

print("--- Column Comparison ---")
print(f"Common columns: {common_cols}")
print(f"Columns unique to dummy data: {dummy_unique_cols}")
print(f"Columns unique to real data: {real_unique_cols}")

# Investigate Data Types of Common Columns
print("\n--- Data Types of Common Columns ---")
for col in common_cols:
    print(f"Column: {col}")
    print(f"  Dummy Data: {dummy_df[col].dtype}")
    print(f"  Real Data: {real_df[col].dtype}")

# Identify common numerical columns
numerical_cols_dummy = dummy_df.select_dtypes(include=np.number).columns
numerical_cols_real = real_df.select_dtypes(include=np.number).columns
common_numerical_cols = list(set(numerical_cols_dummy).intersection(numerical_cols_real))

print("\n--- Numerical Column Comparison ---")
print(f"Common numerical columns: {common_numerical_cols}")

# Missing Value Analysis
print("\n--- Missing Value Analysis ---")
print("Dummy Data:")
print(dummy_df[common_numerical_cols].isnull().sum())
print("\nReal Data:")
print(real_df[common_numerical_cols].isnull().sum())

# Descriptive Statistics and Visualizations
if common_numerical_cols:
    print("\n--- Dummy Data Statistics ---")
    print(dummy_df[common_numerical_cols].describe())

    print("\n--- Real Data Statistics ---")
    print(real_df[common_numerical_cols].describe())

    for col in common_numerical_cols:
        plt.figure(figsize=(10, 5))
        plt.subplot(1, 2, 1)
        dummy_sample = dummy_df[col].dropna()
        if len(dummy_sample) > 10000:
            dummy_sample = dummy_sample.sample(10000, random_state=42)
        sns.histplot(dummy_sample, bins=50, kde=False)
        plt.title(f"Dummy Data: {col}")

        plt.subplot(1, 2, 2)
        real_sample = real_df[col].dropna()
        if len(real_sample) > 10000:
            real_sample = real_sample.sample(10000, random_state=42)
        if col == "session.iflow_bytes":
            sns.histplot(real_sample, bins=50, kde=False)
            plt.xlim(0, 25000)  # Focus zoom for better visibility
        else:
            sns.histplot(real_sample, bins=50, kde=False)
        plt.title(f"Real Data: {col}")

        plt.tight_layout()
        plt.show()

# LOW-RISK PORT ANALYSIS
low_risk_ports = [67, 68, 123, 161, 162, 443, 53, 9200]
print("\n--- Destination Port Frequency (Real Logs) ---")
print(real_df["destination.port"].value_counts().head(20))
print("\nLOW_RISK_PORTS breakdown:")
print(real_df[real_df["destination.port"].isin(low_risk_ports)]["destination.port"].value_counts())

# TRUSTED IP ANALYSIS
trusted_source_ips = ["10.192.96.7", "10.192.96.8", "10.192.96.4"]
trusted_dest_ips = ["193.190.77.36"]

print("\n--- Source IP Frequency (Real Logs) ---")
print(real_df["source.ip"].value_counts().head(20))
print("\nTrusted source IP usage:")
print(real_df[real_df["source.ip"].isin(trusted_source_ips)]["source.ip"].value_counts())

print("\n--- Destination IP Frequency (Real Logs) ---")
print(real_df["destination.ip"].value_counts().head(20))
print("\nTrusted destination IP usage:")
print(real_df[real_df["destination.ip"].isin(trusted_dest_ips)]["destination.ip"].value_counts())

# Time-based density plot (label presence assumed)
# Time-based density plot
dummy_df["@timestamp"] = pd.to_datetime(dummy_df["@timestamp"])
if 'label' in dummy_df.columns:
    for label in [0, 1]:
        subset = dummy_df[dummy_df["label"] == label]
        counts = subset.set_index("@timestamp").resample("1min").size()
        counts.plot(label=f"Label {label}", figsize=(12, 4))

    plt.title("Flow volume over time by label")
    plt.legend()
    plt.tight_layout()
    plt.show()

# Port scan visualization
if 'label' in dummy_df.columns:
    scan_df = dummy_df[dummy_df["label"] == 1]
    scan_summary = scan_df.groupby("source.ip")["destination.port"].nunique()
    scan_summary = scan_summary[scan_summary > 1]
    scan_summary = scan_summary.clip(upper=50)  # avoid extreme outliers
    sns.histplot(scan_summary, bins=30)
    plt.title("Unique destination ports per source (Vertical Scans)")
    plt.tight_layout()
    plt.show()

else:
    print("\nNo common numerical columns to visualize.")