# VIVES PoC: Real-Time Network Anomaly Detection with Machine Learning

This project is a **Proof of Concept (PoC)** for real-time anomaly detection in network logs using a hybrid Machine Learning approach, built for the **IT Architecture & Security** department of **Hogeschool VIVES**.

## Project Overview
- **Goal**: Detect port scans, unusual data transfers, and abnormal connection volumes in network logs.
- **Environment**: Logs stored in Elasticsearch, processed with Python, and visualized through a Streamlit dashboard.
- **Features**:
  - Automatic anomaly detection using Isolation Forest + supervised classifiers (RF, LR, XGB).
  - Dashboard for anomaly review and feedback.
  - Automated retraining loop based on user feedback.
  - Integration with Elasticsearch.

---

## Project Structure

```
PoC_Test/
├── src/
│   ├── ML_batch_scan.py          # Runs anomaly detection using Isolation Forest
│   ├── ML_model_training.py      # Trains supervised models (RandomForest, XGBoost, LogisticRegression)
│   ├── dummy_data_creation.py    # Generates synthetic data with labeled anomalies
│   ├── elasticsearch_import.py   # Fetches logs from Elasticsearch over time windows
│   ├── elasticsearch_export.py   # Sends anomaly results back to Elasticsearch
│   ├── feedback_creator.py       # Combines original data with user feedback for retraining
│   └── streamlit_app.py          # Streamlit UI for viewing anomalies and collecting feedback
│
├── retrain_pipeline/
│   ├── export_feedback.py        # Queries Elasticsearch for labeled anomalies
│   ├── retrain_models.py         # Retrains models using feedback-enhanced dataset
│   └── evaluate_models.py        # Compares candidate models vs deployed ones
│
├── models/                       # Folder containing saved models (.pkl files)
├── data/                         # Contains training runs, exported datasets, feedback
├── .env                          # API keys and credentials (not checked into git)
├── requirements.txt             # Required Python packages
├── Dockerfile                   # Container setup for optional deployment
└── README.md                    # Project documentation
```

Each script is tied to a phase in the anomaly detection and feedback loop.

---

## How It Works

### Data Sources
- Logs are pulled from an Elasticsearch index (`logs-*`).
- Simulation logs can be created with `dummy_data_creation.py` for testing.

### Detection
- Batch-based detection using `ML_batch_scan.py`.
- Isolation Forest provides anomaly scores.
- Supervised models (trained on dummy + feedback data) classify anomalies.

### Feedback
- Detected anomalies are exported to Elasticsearch (`network-anomalies`).
- Users provide feedback via the Streamlit app.
- Labeled feedback is exported and used to retrain models.

### Retraining Pipeline
- `export_feedback.py`: Fetch feedback from Elasticsearch.
- `retrain_models.py`: Train new models on full dataset.
- `evaluate_models.py`: Compare new vs deployed models (F1-score).

---

## Model & Feature Details

### Models Used
- **Isolation Forest**: Unsupervised model for anomaly scoring.
- **Random Forest**: Robust ensemble classifier.
- **Logistic Regression**: Baseline linear classifier.
- **XGBoost**: High-performance gradient boosting model.

### Feature Engineering
- **Hashed IPs**: Source/destination IPs encoded using `HashingEncoder`.
- **Ports & Protocols**: Included as categorical and numeric features.
- **Session Stats**: Bytes and packets per session.
- **Entropy (optional)**: For testing information density.

---

## How to get started?
`
### Prerequisites
- Python 3.8+
- [Elasticsearch 8.x](https://www.elastic.co/elasticsearch/)
- `.env` file with:
  ```env
  ES_HOST=https://your-elasticsearch-host
  ES_API_KEY=your_api_key
  LOGIN_USER=admin
  LOGIN_PASS_HASH=hashed_bcrypt_password
  ```

### Installation
```bash
git clone https://github.com/MoElba-creator/PoC_Test.git
cd PoC_Test
pip install -r requirements.txt
```

### Optional: Build with Docker
```bash
docker build -t vives-anomaly-poc .
```

---

## Run the Pipeline

### Import Logs
```bash
python src/elasticsearch_import.py
```

### Run Anomaly Detection
```bash
python src/ML_batch_scan.py
```

### Export Results to Elasticsearch
```bash
python src/elasticsearch_export.py
```

### Launch Streamlit Dashboard
```bash
streamlit run src/streamlit_app.py
```

---

## Retraining with Feedback

### Step-by-step
```bash
python retrain_pipeline/export_feedback.py
python retrain_pipeline/retrain_models.py
python retrain_pipeline/evaluate_models.py
```
- If new models perform better you may **manually replace** deployed models.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ConnectionError` to Elasticsearch | Check `.env` file and internet access |
| `Missing columns in JSON` | Ensure correct structure in imported dataset |
| Streamlit app fails login | Verify bcrypt password and user in `.env` |

---

## 🌐 Deployment & CI

- GitHub Actions configured in `.github/workflows/retrain.yml` for automated retraining
- Secrets must be configured in the GitHub repo for Elasticsearch access

---

## Screenshots

_Add screenshots of the Streamlit dashboard here if available._

---

## Future Roadmap
- Integrate real-time streaming with Logstash, Kafka, or Beats
- Implement real-time alerting via Slack or email
- Add drift detection and automatic model promotion
- Replace manual retraining with fully automated CI/CD triggers

---

## Tips for VIVES
- Use the Streamlit UI to validate or reject detected anomalies.
- Periodically run the retraining pipeline to improve detection.
- Consider extending with real-time streaming (e.g., Logstash/Beats + Kafka + REST API).

---

## License
MIT License. Built as part of a Bachelor thesis at VIVES Hogeschool.
