"""
Script: evaluate_models.py
Authors: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelor thesis — data-driven anomaly detection

Purpose
This script compares newly trained candidate models against the currently deployed ones
using the validation set saved during retraining.

The metrics that compared are accuracy, precision, recall and f1.
If the candidate model outperforms the deployed one on f1 the model is promoted
and the feedback snapshot is marked as accepted. If not it is marked as rejected.

A candidate is only promoted if:
- f1 is higher.
- AND precision is not worse.
- AND recall is not worse.

All decisions and metrics are logged per training run.
"""

import joblib
import shutil
import json
from pathlib import Path
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
candidate_dirs = sorted((DATA_DIR / "training_runs").glob("*_candidate"))
if not candidate_dirs:
    print("No candidate training run found in data/training_runs.")
    exit(1)

TRAINING_RUN_DIR = candidate_dirs[-1]

# Load validation dataset
val_path = TRAINING_RUN_DIR / "validation_set.pkl"
if not val_path.exists():
    print(f"Validation set missing: {val_path}")
    exit(1)

X_val, y_val = joblib.load(val_path)
print("Validation set loaded.")


model_names = ["random_forest", "logistic_regression", "xgboost"]
metrics_log = {}

# Helper to compute all relevant metrics
def compute_metrics(y_true, y_pred):
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0)
    }

for name in model_names:
    candidate_path = MODEL_DIR / f"{name}_candidate.pkl"
    deployed_path = MODEL_DIR / f"{name}_deployed.pkl"

    if not candidate_path.exists():
        print(f"Candidate model missing: {candidate_path}")
        continue

    try:
        candidate_model = joblib.load(candidate_path)
        y_pred_cand = candidate_model.predict(X_val)
        cand_metrics = compute_metrics(y_val, y_pred_cand)
    except Exception as e:
        print(f"Error evaluating {name}_candidate: {e}")
        continue

    if deployed_path.exists():
        deployed_model = joblib.load(deployed_path)
        y_pred_depl = deployed_model.predict(X_val)
        depl_metrics = compute_metrics(y_val, y_pred_depl)
    else:
        print(f"No deployed model found for {name} — accepting candidate.")
        depl_metrics = {k: -1 for k in cand_metrics}

    # Log and print metrics
    metrics_log[name] = {
        "candidate": cand_metrics,
        "deployed": depl_metrics
    }

    print(f"\n=== {name.upper()} ===")
    for metric in ["accuracy", "precision", "recall", "f1"]:
        print(f"{metric.capitalize():<9} | Candidate: {cand_metrics[metric]:.3f} | Deployed: {depl_metrics[metric]:.3f}")

    promote = (
        cand_metrics["f1"] > depl_metrics["f1"] and
        cand_metrics["precision"] >= depl_metrics["precision"] and
        cand_metrics["recall"] >= depl_metrics["recall"]
    )

    snapshot_base = TRAINING_RUN_DIR.name.replace("_candidate", "")
    source_feedback = DATA_DIR / "latest_feedback.json"

    if promote:
        joblib.dump(candidate_model, deployed_path)
        print(f"{name} promoted: F1 improved, no drop in precision or recall.")

        # Label feedback as accepted
        accepted_feedback = DATA_DIR / f"feedback_snapshot_{snapshot_base}_accepted.json"
        if source_feedback.exists():
            shutil.copy(source_feedback, accepted_feedback)
            print(f"Feedback saved as: {accepted_feedback}")

        # Rename training folder
        accepted_path = Path(str(TRAINING_RUN_DIR).replace("_candidate", "_accepted"))
        TRAINING_RUN_DIR.rename(accepted_path)
        print(f"Training folder renamed to: {accepted_path}")
        TRAINING_RUN_DIR = accepted_path

    else:
        print(f"{name} not promoted because criteria were not met.")

        # Label feedback as rejected
        rejected_feedback = DATA_DIR / f"feedback_snapshot_{snapshot_base}_rejected.json"
        if source_feedback.exists():
            shutil.copy(source_feedback, rejected_feedback)
            print(f"Feedback saved as: {rejected_feedback}")

        # Rename training folder
        rejected_path = Path(str(TRAINING_RUN_DIR).replace("_candidate", "_rejected"))
        TRAINING_RUN_DIR.rename(rejected_path)
        print(f"Training folder renamed to: {rejected_path}")
        TRAINING_RUN_DIR = rejected_path

# Save metrics to file
metrics_path = TRAINING_RUN_DIR / "metrics.json"
with open(metrics_path, "w", encoding="utf-8") as f:
    json.dump(metrics_log, f, indent=2)
print(f"All metrics saved to: {metrics_path}")
