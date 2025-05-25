import unittest
import shutil
import os
import joblib
from pathlib import Path
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier
import subprocess

class TestEvaluateModels(unittest.TestCase):
    def setUp(self):
        # Define all relevant paths
        self.base_dir = Path(__file__).resolve().parents[1]
        self.data_dir = self.base_dir / "data"
        self.model_dir = self.base_dir / "models"
        self.run_dir = self.data_dir / "training_runs" / "test_20240609_14h_candidate"

        # Create required folders
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.run_dir.mkdir(parents=True, exist_ok=True)

        # Generate dummy validation data
        X, y = make_classification(n_samples=100, n_features=20, random_state=42)
        joblib.dump((X, y), self.run_dir / "validation_set.pkl")

        # Train and store dummy models
        model = RandomForestClassifier()
        model.fit(X, y)
        joblib.dump(model, self.model_dir / "random_forest_candidate.pkl")
        joblib.dump(model, self.model_dir / "logistic_regression_candidate.pkl")
        joblib.dump({"model": model, "encoder": None, "columns": list(range(20))}, self.model_dir / "xgboost_candidate.pkl")

    def tearDown(self):
        # Clean up all test folders after each test
        shutil.rmtree(self.run_dir.parent, ignore_errors=True)
        shutil.rmtree(self.model_dir, ignore_errors=True)

    def test_fails_without_candidate_folder(self):
        # Temporarily rename *_candidate folders so they are not detected
        candidates = list((self.data_dir / "training_runs").glob("*_candidate"))
        renamed = []
        for path in candidates:
            new_path = Path(str(path).replace("_candidate", "_backup"))
            shutil.move(str(path), new_path)
            renamed.append((new_path, path))

        # Run script — should fail with clear message
        result = subprocess.run(
            ["python", "retrain_pipeline/evaluate_models.py"],
            cwd=self.base_dir,
            capture_output=True,
            text=True
        )

        # Assert expected output
        self.assertIn("No candidate training run", result.stdout)

        # Restore renamed folders
        for old, new in renamed:
            shutil.move(str(old), str(new))

    def test_successful_evaluation_writes_metrics(self):
        # Run evaluation script with valid candidate setup
        result = subprocess.run(
            ["python", "retrain_pipeline/evaluate_models.py"],
            cwd=self.base_dir,
            capture_output=True,
            text=True
        )

        # Search for accepted or rejected output folders
        out_dirs = list((self.data_dir / "training_runs").glob("test_20240609_14h_*"))
        self.assertTrue(any((d / "metrics.json").exists() for d in out_dirs))
