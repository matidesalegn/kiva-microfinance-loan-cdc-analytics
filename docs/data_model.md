# Data Model & Schema Documentation

## 1. Overview & Data Layering Architecture

The Kiva Microfinance Data Platform follows the modern **Medallion Data Architecture** tailored for high-performance ClickHouse OLAP analytics and real-time Change Data Capture (CDC).

```
PostgreSQL (OLTP) ➔ Debezium CDC ➔ Redpanda (Kafka) ➔ ClickHouse MV ➔ Staging ➔ Marts / ML Store
```

---

## 2. Layer Definitions

### Layer 1: Ingestion & CDC (`raw_data`)
- **`kafka_kiva_loans_cdc` (`ENGINE = Kafka`):** Non-persisted streaming buffer engine that connects directly to Redpanda broker topic `cdc.raw_data.kiva_loans` using `JSONEachRow` parsing.
- **`kiva_loans_raw` (`ENGINE = ReplacingMergeTree(_version)`):** Operational raw table optimized for deduplicating CDC update events using Postgres Primary Key (`id`) as `ORDER BY (id)`.
- **`kiva_loans_mv` (`MATERIALIZED VIEW`):** Zero-latency internal trigger that parses JSON payloads from `kafka_kiva_loans_cdc` and writes them into `kiva_loans_raw`.

### Layer 2: Staging Layer (`analytics.stg_kiva_loans`)
- Materialized as a dbt view.
- Cleans data types, standardizes column names, applies deduplication logic via `argMax()`, and filters out soft-deleted records (`is_deleted = 0`).

### Layer 3: Intermediate Layer (`analytics.int_loans_enriched`)
- Materialized as a dbt view.
- Enriches loans with business logic: calculates funding percentage (`funded_amount / loan_amount`), funding remaining, and funding tiers (`Fully Funded`, `Almost Funded`, `Needs Funding`).

### Layer 4: Analytics Marts (`analytics.mart_loans_by_sector`)
- Materialized as a ClickHouse `MergeTree` table.
- Aggregates loan metrics by country and industry sector for executive reporting.

### Layer 5: Machine Learning Feature Store (`analytics.mart_loan_features_ml`)
- Materialized as a ClickHouse `MergeTree` table.
- Produces normalized, encoded, and labeled feature vectors for ML model training:
  - **Feature Scaling:** `funding_ratio`, `log_loan_amount` (`log10(loan_amount + 1)`), `log_funded_amount`.
  - **One-Hot Encodings:** `is_sector_agriculture`, `is_sector_retail`, `is_sector_services`, `is_sector_food`, `is_country_rwanda`, `is_country_kenya`.
  - **Temporal Features:** `posted_month`, `posted_day_of_week`, `days_since_posted`.
  - **Target Label:** `target_is_fully_funded` (`1` if `funded_amount >= loan_amount` else `0`).

---

## 3. ClickHouse Design Rationale

| Design Choice | Selection | Engineering Rationale |
| :--- | :--- | :--- |
| **Raw Table Engine** | `ReplacingMergeTree(_version)` | Prevents duplicate row accumulation caused by CDC update/upsert stream events. Deduplicates on background merge based on `ORDER BY (id)`. |
| **Streaming Engine** | `Kafka` Engine + `Materialized View` | Lock-free, event-driven streaming ingestion without needing external Python consumer scripts. |
| **Sorting / Ordering Key** | `ORDER BY (id)` | Matches OLTP primary key lookup pattern, enabling instant index binary search scans in ClickHouse. |
