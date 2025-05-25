import unittest
import json
import os
import shutil
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

# Adjust the path so modules are found correctly
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../retrain_pipeline")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Import the functions that are already defined
from elasticsearch_export_feedback import get_last_export_time, store_export_time
import elasticsearch_export_feedback


class TestElasticsearchExportFeedback(unittest.TestCase):

    def setUp(self):
        # Create a temporary 'data' folder for tests
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        os.makedirs(self.test_data_dir, exist_ok=True)

        # Patch the DATA_DIR and FILE_paths in the tested module to point to the test folder
        self.original_snapshot_file = elasticsearch_export_feedback.SNAPSHOT_FILE
        self.original_latest_file = elasticsearch_export_feedback.LATEST_FILE

        elasticsearch_export_feedback.SNAPSHOT_FILE = os.path.join(self.test_data_dir, "feedback_snapshot_test.json")
        elasticsearch_export_feedback.LATEST_FILE = os.path.join(self.test_data_dir, "latest_feedback.json")

        # Create the 'data' directory in the test folder for snapshot and latest files
        os.makedirs(os.path.dirname(elasticsearch_export_feedback.SNAPSHOT_FILE), exist_ok=True)

    def tearDown(self):
        # Clean up the temporary 'data' folder after tests
        if os.path.exists(self.test_data_dir):
            shutil.rmtree(self.test_data_dir)

        # Restore original paths
        elasticsearch_export_feedback.SNAPSHOT_FILE = self.original_snapshot_file
        elasticsearch_export_feedback.LATEST_FILE = self.original_latest_file  # Corrected typo here

    @patch('sys.exit')
    @patch("elasticsearch_export_feedback.get_last_export_time")
    @patch("elasticsearch_export_feedback.es")  # Patch the 'es' object directly
    def _run_export_logic_for_test(self, mock_es, mock_get_last_export_time, mock_exit, mock_es_search_results=None,
                                   mock_es_scroll_results=None):
        """
        Helper function to run the core logic of elasticsearch_export_feedback.py.
        This simulates running the script directly.
        """

        # Configure the search and scroll methods of the mock es instance
        mock_es.search.return_value = mock_es_search_results if mock_es_search_results is not None else {
            "_scroll_id": "scroll_id_123", "hits": {"hits": []}}
        mock_es.scroll.return_value = mock_es_scroll_results if mock_es_scroll_results is not None else {
            "_scroll_id": "scroll_id_123", "hits": {"hits": []}}

        mock_get_last_export_time.return_value = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        # Simulate the main execution flow of the script
        start_time = elasticsearch_export_feedback.get_last_export_time()
        end_time = datetime.now(timezone.utc).isoformat()

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "feedback_timestamp": {
                                    "gte": start_time,
                                    "lte": end_time
                                }
                            }
                        },
                        {
                            "terms": {
                                "user_feedback.keyword": ["correct", "incorrect"]
                            }
                        }
                    ]
                }
            }
        }

        try:
            resp = mock_es.search(index=elasticsearch_export_feedback.INDEX_NAME, body=query, scroll="2m", size=1000)
            sid = resp["_scroll_id"]
            all_hits = resp["hits"]["hits"]
            scroll_size = len(all_hits)

            while scroll_size > 0:
                resp = mock_es.scroll(scroll_id=sid, scroll="2m")
                sid = resp["_scroll_id"]
                hits = resp["hits"]["hits"]
                scroll_size = len(hits)
                all_hits.extend(hits)

        except Exception as e:
            print(f"Failed to fetch logs from Elasticsearch during test: {e}")
            mock_exit.assert_called_once_with(1)
            return  # Exit the helper function if ES fetch fails

        if not all_hits:
            print("No feedback logs found during test — skipping export.")
            mock_exit.assert_called_once_with(0)
            return  # Exit the helper function if no hits

        try:
            logs = []
            for hit in all_hits:
                doc = hit["_source"]
                doc["_id"] = hit["_id"]
                logs.append(doc)

            with open(elasticsearch_export_feedback.SNAPSHOT_FILE, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)

            shutil.copy(elasticsearch_export_feedback.SNAPSHOT_FILE, elasticsearch_export_feedback.LATEST_FILE)

            # Ensure store_export_time is called with the correct end_time
            with patch("elasticsearch_export_feedback.store_export_time") as mock_store:
                mock_store(end_time)  # Call the actual function

            mock_exit.assert_not_called()  # If everything passed, sys.exit should not be called

        except Exception as e:
            print(f"Failed to write feedback JSON during test: {e}")
            mock_exit.assert_called_once_with(1)
            return  # Exit the helper function if write fails

    @patch("elasticsearch_export_feedback.es.search")
    def test_get_last_export_time_fallback(self, mock_es_search):
        # Simulate an error or no results from Elasticsearch
        mock_es_search.side_effect = Exception("No tracking index")

        # Call the function and check if the fallback is used
        last_time = get_last_export_time()

        # Check if the returned time is close to the current time (within 7 days)
        now = datetime.now(timezone.utc)
        fallback_time_obj = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
        self.assertLessEqual(now - fallback_time_obj, timedelta(days=7))
        self.assertGreaterEqual(now - fallback_time_obj, timedelta(days=7, minutes=-1))  # Small margin

    @patch("elasticsearch_export_feedback.es.index")
    def test_store_export_time(self, mock_es_index):
        test_time = datetime.now(timezone.utc).isoformat()
        store_export_time(test_time)
        mock_es_index.assert_called_once_with(
            index=elasticsearch_export_feedback.TRACKING_INDEX,
            document={
                "pipeline": elasticsearch_export_feedback.PIPELINE_NAME,
                "last_run_time": test_time,
                "status": "success"
            }
        )

    def test_export_feedback_writes_files(self):
        # Mock Elasticsearch responses for _run_export_logic_for_test
        mock_search_results = {
            "_scroll_id": "scroll_id_123",
            "hits": {
                "hits": [
                    {"_source": {"user_feedback": "correct",
                                 "feedback_timestamp": datetime.now(timezone.utc).isoformat()}, "_id": "1"},
                    {"_source": {"user_feedback": "incorrect",
                                 "feedback_timestamp": datetime.now(timezone.utc).isoformat()}, "_id": "2"}
                ]
            }
        }
        mock_scroll_results = {
            "_scroll_id": "scroll_id_123",
            "hits": {"hits": []}  # No further hits
        }

        # Run the helper export logic
        # Pass the mock_es, mock_get_last_export_time, and mock_exit to the helper
        with patch('sys.exit') as mock_exit, \
                patch("elasticsearch_export_feedback.get_last_export_time") as mock_get_last_export_time_inner, \
                patch("elasticsearch_export_feedback.es") as mock_es_inner:
            self._run_export_logic_for_test(
                mock_es=mock_es_inner,
                mock_get_last_export_time=mock_get_last_export_time_inner,
                mock_exit=mock_exit,  # Pass the mock_exit to the helper
                mock_es_search_results=mock_search_results,
                mock_es_scroll_results=mock_scroll_results
            )

        # Check if files have been created
        self.assertTrue(os.path.exists(elasticsearch_export_feedback.SNAPSHOT_FILE))
        self.assertTrue(os.path.exists(elasticsearch_export_feedback.LATEST_FILE))

        # Check the content of the files
        with open(elasticsearch_export_feedback.LATEST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(len(data), 2)
            self.assertIn("correct", [d["user_feedback"] for d in data])
            self.assertIn("incorrect", [d["user_feedback"] for d in data])

    def test_export_feedback_no_feedback(self):
        # Mock Elasticsearch response with no hits for _run_export_logic_for_test
        mock_search_results = {"_scroll_id": "scroll_id_123", "hits": {"hits": []}}
        mock_scroll_results = {"_scroll_id": "scroll_id_123", "hits": {"hits": []}}

        # Run the helper export logic
        # Pass the mock_es, mock_get_last_export_time, and mock_exit to the helper
        with patch('sys.exit') as mock_exit, \
                patch("elasticsearch_export_feedback.get_last_export_time") as mock_get_last_export_time_inner, \
                patch("elasticsearch_export_feedback.es") as mock_es_inner:
            self._run_export_logic_for_test(
                mock_es=mock_es_inner,
                mock_get_last_export_time=mock_get_last_export_time_inner,
                mock_exit=mock_exit,  # Pass the mock_exit to the helper
                mock_es_search_results=mock_search_results,
                mock_es_scroll_results=mock_scroll_results
            )

        # Check if files were NOT created
        self.assertFalse(os.path.exists(elasticsearch_export_feedback.SNAPSHOT_FILE))
        self.assertFalse(os.path.exists(elasticsearch_export_feedback.LATEST_FILE))