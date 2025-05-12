import unittest
from unittest.mock import patch
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from elasticsearch_import import get_last_run_time

class TestElasticImport(unittest.TestCase):
    @patch("elasticsearch_import.es.search")
    def test_last_run_fallback(self, mock_search):
        mock_search.side_effect = Exception("fail")
        ts = get_last_run_time()
        self.assertIsInstance(ts, str)
        self.assertIn("T", ts)  # ISO timestamp
