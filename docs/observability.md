# Observability & System Monitoring Architecture

## Overview
Observability is a foundational pillar of the Kiva Microfinance Data Platform. In real-time streaming architectures (PostgreSQL CDC ➔ Redpanda ➔ ClickHouse), silent failures or unseen stream lag can corrupt downstream analytics and machine learning models. 

This document details the observability design, what key telemetry signals are monitored, and the rationale behind tool selections.

---

## 1. What Is Monitored?

The platform continuously tracks signals across four core dimensions:

### 1. Pipeline Health & Streaming Throughput
- **Redpanda Ingestion Throughput (`rate(vectorized_kafka_rpc_received_bytes[1m])`):** Tracks real-time network payload volume flowing into the event bus from Debezium CDC.
- **Debezium Connector Status:** Monitors task health (`RUNNING`, `PAUSED`, `FAILED`) via Debezium REST API checks.

### 2. Data Freshness & Pipeline Latency
- **ClickHouse Ingestion Rate (`rate(ClickHouseProfileEvents_InsertedRows[1m])`):** Measures the row consumption speed into ClickHouse raw tables.
- **Data Ingestion Heartbeat:** Tracks elapsed time between source event generation in PostgreSQL and final materialization in ClickHouse analytics marts.

### 3. CDC Replication Lag
- **Offset Lag Monitoring:** Evaluates Debezium WAL replication slot lag in PostgreSQL (`my_connect_offsets` consumer position vs Postgres WAL LSN) to detect backpressure or network bottlenecks before message queue overflow.

### 4. Container Resource Footprint
- **ClickHouse Memory Usage (`ClickHouseMetrics_MemoryTracking`):** Tracks RAM consumption during heavy dbt transformation queries to prevent Out-Of-Memory (OOM) kernel kills.
- **Active ClickHouse Queries (`ClickHouseMetrics_Query`):** Real-time gauge of active concurrent SQL execution threads.

---

## 2. Tooling Rationale & Selection Matrix

| Component | Tool Chosen | Why Chosen? | Key Alternatives Considered |
| :--- | :--- | :--- | :--- |
| **Metrics Collector** | **Prometheus** | High-performance, pull-based time-series engine with native scrape targets for Redpanda, ClickHouse, and Postgres exporters. Zero external database dependencies. | Pushgateway, Telegraf |
| **Visualization & Alerting** | **Grafana** | Industry-standard open-source dashboarding engine with seamless Prometheus integration, alerting rules, and automated JSON provisioning via Docker Compose. | Datadog, AWS CloudWatch |
| **Streaming Telemetry** | **Redpanda Prometheus Exporter** | Built directly into Redpanda’s C++ core; exposes native Kafka RPC throughput without needing a separate Java JMX exporter. | JMX Exporter |
| **Database Telemetry** | **ClickHouse Native Prometheus Endpoint** | Built-in `/metrics` HTTP endpoint exposing internal `ProfileEvents` and `SystemMetrics` with zero runtime overhead. | Custom exporter scripts |

---

## 3. Automated Grafana Provisioning

To enforce Infrastructure-as-Code (IaC) best practices:
- **Datasource Provisioning:** Automatically binds Prometheus (`http://prometheus:9090`) on boot (`config/grafana/provisioning/datasources/prometheus.yml`).
- **Dashboard Provisioning:** Auto-loads the 6-panel **Kiva Microfinance Pipeline Observability** dashboard (`config/grafana/dashboards/kiva_pipeline_observability.json`) upon `docker compose up -d`.
