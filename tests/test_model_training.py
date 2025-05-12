import os
import unittest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from ML_model_training import MODEL_DIR

class TestModelTraining(unittest.TestCase):
    def test_models_saved(self):
        self.assertTrue(os.path.exists(os.path.join(MODEL_DIR, "random_forest_model.pkl")))
        self.assertTrue(os.path.exists(os.path.join(MODEL_DIR, "logistic_regression_model.pkl")))
        self.assertTrue(os.path.exists(os.path.join(MODEL_DIR, "xgboost_model.pkl")))
