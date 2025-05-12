import unittest
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from synthetic_data_creation import generate_combined_traffic

class TestSyntheticData(unittest.TestCase):
    def test_dummy_generation_shape(self):
        df = generate_combined_traffic()
        self.assertGreater(len(df), 5000)
        self.assertIn("label", df.columns)
        self.assertIn("session.id", df.columns)

    def test_no_nulls_in_critical_fields(self):
        df = generate_combined_traffic()
        critical = ["source.ip", "destination.ip", "session.iflow_bytes", "session.iflow_pkts"]
        self.assertTrue(df[critical].notnull().all().all())
