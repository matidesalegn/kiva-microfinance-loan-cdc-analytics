import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

# Ensure src directory is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from cdc_monitor import (
    compute_drift,
    compute_lag_seconds,
    connector_state_value,
    count_failed_tasks,
)


class TestCdcReconciliation(unittest.TestCase):
    """Unit tests for the pure CDC drift/lag computations in cdc_monitor.py."""

    def test_compute_drift_in_sync(self):
        self.assertEqual(compute_drift(1000, 1000), 0)

    def test_compute_drift_clickhouse_behind(self):
        self.assertEqual(compute_drift(1000, 950), 50)

    def test_compute_drift_clickhouse_ahead_of_stale_snapshot(self):
        # Can legitimately go negative for an instant if Postgres is polled
        # slightly before ClickHouse during a burst of concurrent writes.
        self.assertEqual(compute_drift(950, 1000), -50)


class TestReplicationLag(unittest.TestCase):
    def test_lag_no_data_yet_returns_none(self):
        self.assertIsNone(compute_lag_seconds(datetime.now(timezone.utc), None))

    def test_lag_computed_from_naive_datetime(self):
        now = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)
        max_updated_at = datetime(2026, 8, 11, 11, 58, 30)  # naive, assumed UTC
        self.assertAlmostEqual(compute_lag_seconds(now, max_updated_at), 90.0)

    def test_lag_never_negative(self):
        now = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)
        future_row = now + timedelta(seconds=5)  # clock skew edge case
        self.assertEqual(compute_lag_seconds(now, future_row), 0.0)


class TestDebeziumConnectorHealth(unittest.TestCase):
    def test_all_running_is_healthy(self):
        status = {
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": 0, "state": "RUNNING"}],
        }
        self.assertEqual(connector_state_value(status), 1)

    def test_connector_paused_is_unhealthy(self):
        status = {
            "connector": {"state": "PAUSED"},
            "tasks": [{"id": 0, "state": "RUNNING"}],
        }
        self.assertEqual(connector_state_value(status), 0)

    def test_task_failed_is_unhealthy(self):
        status = {
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": 0, "state": "FAILED"}],
        }
        self.assertEqual(connector_state_value(status), 0)

    def test_no_tasks_is_unhealthy(self):
        status = {"connector": {"state": "RUNNING"}, "tasks": []}
        self.assertEqual(connector_state_value(status), 0)

    def test_count_failed_tasks(self):
        status = {
            "connector": {"state": "RUNNING"},
            "tasks": [
                {"id": 0, "state": "FAILED"},
                {"id": 1, "state": "RUNNING"},
                {"id": 2, "state": "FAILED"},
            ],
        }
        self.assertEqual(count_failed_tasks(status), 2)


if __name__ == "__main__":
    unittest.main()
