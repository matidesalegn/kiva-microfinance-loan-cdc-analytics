import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Ensure src directory is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from ingest_api import fetch_loans, upsert_loans

class TestKivaIngestion(unittest.TestCase):
    """Unit tests for the Kiva REST API ingestion pipeline."""

    @patch("ingest_api.requests.get")
    def test_fetch_loans_success(self, mock_get):
        """Test successful loan fetching from Kiva API."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "loans": [
                {"id": 101, "name": "Kigali Retail Store", "status": "funded", "loan_amount": 500, "funded_amount": 500},
                {"id": 102, "name": "Musanze Agriculture", "status": "funded", "loan_amount": 1000, "funded_amount": 800}
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        loans = fetch_loans(page=1, per_page=2)
        self.assertEqual(len(loans), 2)
        self.assertEqual(loans[0]["id"], 101)
        self.assertEqual(loans[0]["name"], "Kigali Retail Store")

    @patch("ingest_api.requests.get")
    def test_fetch_loans_retry_failure(self, mock_get):
        """Test exponential backoff retry behavior on API failure."""
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("API connection timeout")
        
        with self.assertRaises(Exception) as context:
            fetch_loans(page=1)
        self.assertIn("Failed to fetch data from API", str(context.exception))

    def test_upsert_loans_empty(self):
        """Test upsert behavior with empty input list."""
        mock_conn = MagicMock()
        count = upsert_loans(mock_conn, [])
        self.assertEqual(count, 0)

    def test_upsert_loans_success(self):
        """Test successful database upsert formatting."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        test_loans = [
            {
                "id": 201,
                "name": "Rubavu Tailoring",
                "status": "funded",
                "funded_amount": 300,
                "loan_amount": 300,
                "activity": "Tailoring",
                "sector": "Services",
                "location": {"country": "Rwanda", "town": "Rubavu"},
                "posted_date": "2026-08-10T10:00:00Z"
            }
        ]
        
        count = upsert_loans(mock_conn, test_loans)
        self.assertEqual(count, 1)
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

if __name__ == "__main__":
    unittest.main()
