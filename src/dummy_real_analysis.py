import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

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

# Port scan visualization (optimized)
if 'label' in dummy_df.columns:
    scan_df = dummy_df[dummy_df["label"] == 1]
    scan_summary = scan_df.groupby("source.ip")["destination.port"].nunique()
    scan_summary = scan_summary[scan_summary > 1]
    scan_summary = scan_summary.clip(upper=50)  # avoid extreme outliers
    sns.histplot(scan_summary, bins=30)
    plt.title("Unique destination ports per source (Vertical Scans)")
    plt.tight_layout()
    plt.show()

# PCA projection (optimized)
features = [col for col in ["session.iflow_bytes", "session.iflow_pkts", "bytes_ratio", "bytes_per_pkt"] if col in dummy_df.columns]
if features and 'label' in dummy_df.columns:
    sampled = dummy_df[features + ["label"]].dropna()
    if len(sampled) > 10000:
        sampled = sampled.sample(10000, random_state=42)

    scaled = StandardScaler().fit_transform(sampled[features])
    proj = PCA(n_components=2).fit_transform(scaled)
    sampled["pca1"], sampled["pca2"] = proj[:, 0], proj[:, 1]
    sns.scatterplot(data=sampled, x="pca1", y="pca2", hue="label", alpha=0.4)
    plt.title("PCA Projection of Dummy Dataset")
    plt.tight_layout()
    plt.show()
else:
    print("\nNo common numerical columns to visualize.")