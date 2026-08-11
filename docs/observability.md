# Observability & System Monitoring Architecture

## Overview
Observability is a foundational pillar of the Kiva Microfinance Data Platform. In real-time streaming architectures (PostgreSQL CDC ➔ Redpanda ➔ ClickHouse), silent failures or unseen stream lag can corrupt downstream analytics and machine learning models.

This document details what is actually deployed and scraped in this repository — every metric named below has a corresponding scrape target in `config/prometheus.yml` and, where relevant, a Grafana alert rule in `config/grafana/provisioning/alerting/rules.yml`. See `docs/design-report.md` for the wider architecture and rationale.

---

## 1. What Is Monitored?

### 1. Pipeline Health & Streaming Throughput
- **Redpanda Ingestion Throughput** (`rate(vectorized_kafka_rpc_received_bytes[1m])`) — network payload flowing into the event bus from Debezium CDC. Scraped from `redpanda:9644`.
- **ClickHouse Active Queries / Memory** (`ClickHouseMetrics_Query`, `ClickHouseMetrics_MemoryTracking`) — scraped from ClickHouse's native `/metrics` endpoint on `clickhouse:9363` (enabled via `config/clickhouse_prometheus.xml`).
- **Postgres Health** (`pg_up`, connection/transaction stats) — via `prometheuscommunity/postgres-exporter`, scraped from `postgres-exporter:9187`.

### 2. Data Freshness & CDC Replication Lag
- **`cdc_replication_lag_seconds`** — `now() - max(source_updated_at)` of the newest row visible in `raw_data.kiva_loans_raw`. `source_updated_at` is Postgres's own `updated_at` column, carried through the Kafka-engine table and materialized view specifically so this is a real, row-level freshness measurement rather than an inferred proxy. Computed by `src/cdc_monitor.py`.
- **`cdc_row_count_drift`** — `cdc_postgres_row_count - cdc_clickhouse_row_count`, i.e. a direct reconciliation between the OLTP source of truth and the deduplicated CDC target. This is the most direct possible answer to "did we lose any CDC events?" and is deliberately independent of Debezium's own internal offset bookkeeping.
- Both metrics are polled every `POLL_INTERVAL_SECONDS` (default 15s) and exposed on `cdc-monitor:9200/metrics`.

### 3. CDC Connector Health
- **`debezium_connector_state`** (1 = connector and all tasks `RUNNING`, 0 otherwise) and **`debezium_connector_failed_tasks`** — polled by `cdc-monitor` from the Kafka Connect REST API (`GET /connectors/{name}/status`), not inferred from throughput. A connector that is `PAUSED` or has a `FAILED` task with zero traffic is caught even if every other metric looks quiet.

### 4. Container Resource Footprint
- **ClickHouse Memory Usage** (`ClickHouseMetrics_MemoryTracking`) — guards against OOM during heavy dbt transformation queries.
- **ClickHouse Active Queries** (`ClickHouseMetrics_Query`) — concurrent SQL execution gauge.
- Every service in `docker-compose.yml` has an explicit `deploy.resources.limits.memory`, so container-level exhaustion shows up as an OOM-killed container (visible via `docker compose ps`/`docker stats`) rather than silent host thrashing.

### 5. Exporter Self-Health
- **`cdc_monitor_scrape_errors_total`** (by source: `postgres`, `clickhouse`, `clickhouse_lag`, `debezium`) and **`cdc_monitor_last_success_timestamp_seconds`** — the monitor's own reliability is itself observable: a stuck or half-failing exporter is distinguishable from "everything is fine, zero drift."

---

## 2. Tooling Rationale & Selection Matrix

| Component | Tool Chosen | Why Chosen? | Key Alternatives Considered |
| :--- | :--- | :--- | :--- |
| **Metrics Collector** | **Prometheus** | Pull-based, zero external dependency, runs as one more `docker compose` service, so startup stays a single command. | Pushgateway, Telegraf |
| **Visualization & Alerting** | **Grafana** | Native Prometheus integration, dashboards *and* unified alerting provisioned entirely as code (`config/grafana/provisioning/`) — no manual UI clicking required after `docker compose up`. | Datadog, AWS CloudWatch |
| **Streaming Telemetry** | **Redpanda's built-in exporter** | Native C++ Prometheus endpoint, no separate JMX exporter needed. | JMX Exporter |
| **Database Telemetry (OLAP)** | **ClickHouse native `/metrics`** | Built-in HTTP endpoint exposing `ProfileEvents`/`SystemMetrics` with zero runtime overhead. | Custom exporter scripts |
| **Database Telemetry (OLTP)** | **`postgres_exporter`** | Standard, well-maintained community exporter; avoids hand-rolling Postgres internals metrics. | Custom SQL-based exporter |
| **CDC Reconciliation & Lag** | **Custom exporter (`src/cdc_monitor.py`)** | Neither Debezium nor ClickHouse natively expose "did every row arrive, and how stale is the newest one" as a single first-class metric for this schema — a purpose-built exporter answering exactly that question is more trustworthy than inferring it from throughput graphs. | Debezium JMX metrics (heavier: requires a JVM javaagent + separate JMX-to-Prometheus bridge for a resource-capped local stack) |

---

## 3. Alerting

Four alert rules are provisioned as code in `config/grafana/provisioning/alerting/rules.yml` (Grafana's unified alerting, evaluated every 1m):

| Alert | Condition | Severity |
|---|---|---|
| `cdc-row-drift-high` | `abs(cdc_row_count_drift) > 5` for 5m | warning |
| `cdc-replication-lag-high` | `cdc_replication_lag_seconds > 120` for 5m | warning |
| `debezium-connector-down` | `min(debezium_connector_state) < 1` for 2m | critical |
| `clickhouse-ingestion-stalled` | `rate(ClickHouseProfileEvents_InsertedRows[5m]) ≈ 0` for 5m | warning |

No external notification channel (Slack/email/PagerDuty) is configured, since that would require credentials outside the scope of a local demo stack — rules fire and are visible in the Grafana Alerting UI (`http://localhost:3001/alerting/list`), which is enough to demonstrate the alerting *design*; wiring a real contact point in production is a one-file change (`config/grafana/provisioning/alerting/contact-points.yml`).

---

## 4. Automated Grafana Provisioning

To enforce Infrastructure-as-Code (IaC) practices, everything below is created automatically on `docker compose up -d` — no manual dashboard import or datasource click-through:

- **Datasources**: `config/grafana/provisioning/datasources/prometheus.yml` and `clickhouse.yml`, both with explicit, stable `uid`s (`prometheus`, `clickhouse`) so dashboards and alert rules can reference them deterministically regardless of provisioning order.
- **Dashboards**: `config/grafana/dashboards/kiva_pipeline_observability.json` (11 panels: Redpanda throughput, ClickHouse memory/queries/ingestion rate, Postgres-vs-ClickHouse row reconciliation, CDC lag, Debezium connector state, Postgres exporter status) and `kiva_executive_analytics.json` (business KPIs from the marts).
- **Alert rules**: `config/grafana/provisioning/alerting/rules.yml`, described above.
