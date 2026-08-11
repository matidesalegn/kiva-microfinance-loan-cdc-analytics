"""
CDC Reconciliation & Freshness Monitor.

Answers the question every CDC pipeline eventually gets asked: "how do we know
Debezium didn't silently drop or fall behind on events?" It polls Postgres
(source of truth) and ClickHouse (CDC target) side by side and exposes the
comparison as Prometheus metrics:

  - cdc_postgres_row_count        : live row count in raw_data.kiva_loans
  - cdc_clickhouse_row_count      : deduplicated row count in ClickHouse
  - cdc_row_count_drift           : postgres_count - clickhouse_count
  - cdc_replication_lag_seconds   : now() - max(source_updated_at) seen in ClickHouse,
                                     i.e. how stale the newest replicated row is
  - debezium_connector_state      : 1 if the connector + all tasks are RUNNING, else 0
  - debezium_connector_failed_tasks : number of tasks currently in FAILED state
  - cdc_monitor_scrape_errors_total : counter of failed polling attempts, by source
  - cdc_monitor_last_success_timestamp_seconds : last time a full poll succeeded

This is what backs the "CDC lag" and "data freshness" observability signals
described in docs/design-report.md / docs/observability.md.
"""

import os
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg
import requests
from prometheus_client import Counter, Gauge, start_http_server

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_USER = os.getenv("DB_USER", "kiva_admin")
DB_PASS = os.getenv("DB_PASS", "kiva_password")
DB_NAME = os.getenv("DB_NAME", "kiva_oltp")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "kiva_admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "kiva_password")

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
DEBEZIUM_CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "kiva-postgres-connector")

POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "15"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9200"))

# --- Prometheus metrics -----------------------------------------------------

postgres_row_count = Gauge("cdc_postgres_row_count", "Row count in the Postgres source-of-truth table")
clickhouse_row_count = Gauge("cdc_clickhouse_row_count", "Deduplicated row count in the ClickHouse CDC raw table")
row_count_drift = Gauge("cdc_row_count_drift", "postgres_row_count - clickhouse_row_count")
replication_lag_seconds = Gauge("cdc_replication_lag_seconds", "Seconds between now() and the newest source_updated_at replicated into ClickHouse")
connector_state = Gauge("debezium_connector_state", "1 if the Debezium connector and all tasks are RUNNING, else 0", ["connector"])
connector_failed_tasks = Gauge("debezium_connector_failed_tasks", "Number of Debezium connector tasks currently in FAILED state", ["connector"])
scrape_errors_total = Counter("cdc_monitor_scrape_errors_total", "Number of failed polling attempts, by data source", ["source"])
last_success_timestamp = Gauge("cdc_monitor_last_success_timestamp_seconds", "Unix timestamp of the last fully successful poll of all sources")


# --- Pure helpers (unit-testable without live services) --------------------

def compute_drift(postgres_count: int, clickhouse_count: int) -> int:
    """Positive = ClickHouse is behind Postgres by this many rows."""
    return postgres_count - clickhouse_count


def compute_lag_seconds(now_utc: datetime, max_source_updated_at: Optional[datetime]) -> Optional[float]:
    """None means "no data has replicated yet", not "zero lag"."""
    if max_source_updated_at is None:
        return None
    if max_source_updated_at.tzinfo is None:
        max_source_updated_at = max_source_updated_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now_utc - max_source_updated_at).total_seconds())


def connector_state_value(status_json: dict) -> int:
    """1 only if the connector itself and every task report RUNNING."""
    connector_ok = status_json.get("connector", {}).get("state") == "RUNNING"
    tasks = status_json.get("tasks", [])
    tasks_ok = len(tasks) > 0 and all(t.get("state") == "RUNNING" for t in tasks)
    return 1 if (connector_ok and tasks_ok) else 0


def count_failed_tasks(status_json: dict) -> int:
    return sum(1 for t in status_json.get("tasks", []) if t.get("state") == "FAILED")


# --- Data source polling ----------------------------------------------------

def fetch_postgres_row_count() -> int:
    with psycopg.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_data.kiva_loans;")
            return cur.fetchone()[0]


def _clickhouse_query(query: str) -> str:
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    response = requests.get(
        url,
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=5,
    )
    response.raise_for_status()
    return response.text.strip()


def fetch_clickhouse_row_count() -> int:
    result = _clickhouse_query(
        "SELECT count() FROM raw_data.kiva_loans_raw FINAL WHERE is_deleted = 0"
    )
    return int(result) if result else 0


def fetch_clickhouse_max_updated_at() -> Optional[datetime]:
    result = _clickhouse_query(
        "SELECT toUnixTimestamp(max(source_updated_at)) FROM raw_data.kiva_loans_raw FINAL WHERE is_deleted = 0"
    )
    epoch_seconds = int(result) if result else 0
    if epoch_seconds <= 0:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def fetch_connector_status() -> dict:
    url = f"{DEBEZIUM_URL}/connectors/{DEBEZIUM_CONNECTOR_NAME}/status"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


# --- Poll loop ---------------------------------------------------------------

def poll_once() -> None:
    all_ok = True

    pg_count = None
    ch_count = None

    try:
        pg_count = fetch_postgres_row_count()
        postgres_row_count.set(pg_count)
    except Exception as exc:  # noqa: BLE001 - one bad source should not kill the loop
        print(f"[cdc_monitor] postgres poll failed: {exc}")
        scrape_errors_total.labels(source="postgres").inc()
        all_ok = False

    try:
        ch_count = fetch_clickhouse_row_count()
        clickhouse_row_count.set(ch_count)
    except Exception as exc:  # noqa: BLE001
        print(f"[cdc_monitor] clickhouse row count poll failed: {exc}")
        scrape_errors_total.labels(source="clickhouse").inc()
        all_ok = False

    if pg_count is not None and ch_count is not None:
        row_count_drift.set(compute_drift(pg_count, ch_count))

    try:
        max_updated_at = fetch_clickhouse_max_updated_at()
        lag = compute_lag_seconds(datetime.now(timezone.utc), max_updated_at)
        if lag is not None:
            replication_lag_seconds.set(lag)
    except Exception as exc:  # noqa: BLE001
        print(f"[cdc_monitor] clickhouse lag poll failed: {exc}")
        scrape_errors_total.labels(source="clickhouse_lag").inc()
        all_ok = False

    try:
        status_json = fetch_connector_status()
        connector_state.labels(connector=DEBEZIUM_CONNECTOR_NAME).set(connector_state_value(status_json))
        connector_failed_tasks.labels(connector=DEBEZIUM_CONNECTOR_NAME).set(count_failed_tasks(status_json))
    except Exception as exc:  # noqa: BLE001
        print(f"[cdc_monitor] debezium status poll failed: {exc}")
        scrape_errors_total.labels(source="debezium").inc()
        connector_state.labels(connector=DEBEZIUM_CONNECTOR_NAME).set(0)
        all_ok = False

    if all_ok:
        last_success_timestamp.set(time.time())


def main() -> None:
    print(f"[cdc_monitor] starting metrics server on :{METRICS_PORT}, polling every {POLL_INTERVAL_SECONDS}s")
    start_http_server(METRICS_PORT)
    while True:
        poll_once()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
