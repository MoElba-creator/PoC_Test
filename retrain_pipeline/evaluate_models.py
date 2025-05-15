import os
import joblib
import shutil
from sklearn.metrics import f1_score
from datetime import datetime
from pathlib import Path

MODEL_DIR = Path("models")
TRAINING_RUN_DIR = sorted(Path("data/training_runs").glob("*_candidate"))[-1]  # laatste run

# Load validation dataset
val_path = TRAINING_RUN_DIR / "validation_set.pkl"
if not val_path.exists():
    print(f"Validationset missing: {val_path}")
    exit(1)

X_val, y_val = joblib.load(val_path)
print("Validation set loaded.")


model_names = ["random_forest", "logistic_regression", "xgboost"]

# Compare models
for name in model_names:
    candidate_path = MODEL_DIR / f"{name}_candidate.pkl"
    deployed_path = MODEL_DIR / f"{name}_deployed.pkl"

    if not candidate_path.exists():
        print(f" Candidate model missing: {candidate_path}")
        continue

    try:
        candidate_model = joblib.load(candidate_path)
        y_pred_candidate = candidate_model.predict(X_val)
        f1_candidate = f1_score(y_val, y_pred_candidate)
    except Exception as e:
        print(f"Error during validation of {name}_candidate: {e}")
        continue

    if deployed_path.exists():
        deployed_model = joblib.load(deployed_path)
        y_pred_deployed = deployed_model.predict(X_val)
        f1_deployed = f1_score(y_val, y_pred_deployed)
    else:
        print(f"No deployed model found for  {name}. Accept new model.")
        f1_deployed = -1

    print(f" {name} Candidate F1: {f1_candidate:.3f} vs Deployed F1: {f1_deployed:.3f}")

    # === 4. Beslissing: kandidaat promoten? ===
    if f1_candidate > f1_deployed:
        joblib.dump(candidate_model, deployed_path)
        print(f"Deployed model {name} saved: F1 improved.")

        # === Copy feedback snapshot and label as accepted ===
        snapshot_basename = TRAINING_RUN_DIR.name.replace("_candidate", "")
        source_feedback = Path("data/latest_feedback.json")
        accepted_feedback = Path(f"data/feedback_snapshot_{snapshot_basename}_accepted.json")

        if source_feedback.exists():
            shutil.copy(source_feedback, accepted_feedback)
            print(f"Feedback snapshot labeled as: {accepted_feedback}")

        # Rename map name to accepted
        accepted_path = Path(str(TRAINING_RUN_DIR).replace("_candidate", "_accepted"))
        TRAINING_RUN_DIR.rename(accepted_path)
        print(f"Trainingmap renamed to: {accepted_path}")
    else:

        #  Copy feedback snapshot and label as rejected
        snapshot_basename = TRAINING_RUN_DIR.name.replace("_candidate", "")
        source_feedback = Path("data/latest_feedback.json")
        rejected_feedback = Path(f"data/feedback_snapshot_{snapshot_basename}_rejected.json")

        if source_feedback.exists():
            shutil.copy(source_feedback, rejected_feedback)
            print(f"Feedback labeled as: {rejected_feedback}")

        # raneme to rejected
        rejected_path = Path(str(TRAINING_RUN_DIR).replace("_candidate", "_rejected"))
        TRAINING_RUN_DIR.rename(rejected_path)
        print(f"Model didn't improve. Map renamed to: {rejected_path}")
