import unittest
import json
import os

class TestBatchScan(unittest.TestCase):
    def test_output_files_exist(self):
        self.assertTrue(os.path.exists("../data/all_evaluated_logs_latest.json"))
        self.assertTrue(os.path.exists("../data/predicted_anomalies_latest.json"))

    def test_anomalies_have_required_fields(self):
        with open("../data/predicted_anomalies_latest.json", encoding="utf-8") as f:
            records = json.load(f)
        self.assertGreater(len(records), 0)
        for record in records:
            self.assertIn("RF_pred", record)
            self.assertIn("XGB_score", record)
            self.assertIn("destination.port", record)
