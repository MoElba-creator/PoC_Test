import unittest
import json

class TestElasticsearchExport(unittest.TestCase):
    def test_anomalies_json_loads(self):
        with open("../data/predicted_anomalies_latest.json", encoding="utf-8") as f:
            records = json.load(f)
        self.assertIsInstance(records, list)
        self.assertTrue(len(records) > 0)
