# Naming conventions

The rules the code already follows, written down so a new table, model, column, metric or
service fits in. Where this file and the code disagree, the code wins and this file is the bug.

## General

- snake_case for databases, tables, columns, dbt models, metrics and Python modules.
- A name says what the thing is first, then what it holds.

## Databases, schemas and topics

| Where | Name | Holds |
|---|---|---|
| PostgreSQL database | `kiva_oltp` | The transactional source |
| PostgreSQL schema | `raw_data` | Source tables, read by Debezium |
| Redpanda topic | `cdc.<schema>.<table>`, here `cdc.raw_data.kiva_loans` | Change events; `cdc` is Debezium's `topic.prefix` |
| ClickHouse database | `raw_data` | CDC landing |
| ClickHouse database | `analytics` | dbt models (the schema in `profiles.yml`) |

## Tables and views

- Source tables are named for the entity: `kiva_loans`.
- ClickHouse landing objects are named for the source table plus their role:

| Pattern | Role | Example |
|---|---|---|
| `kafka_<table>_cdc` | Kafka engine table reading the topic | `kafka_kiva_loans_cdc` |
| `<table>_mv` | Materialized view that writes the landing table | `kiva_loans_mv` |
| `<table>_raw` | Landing table, `ReplacingMergeTree` | `kiva_loans_raw` |

- dbt models carry a layer prefix:

| Prefix | Layer | Materialized | Example |
|---|---|---|---|
| `stg_<source>_<entity>` | Staging: current state, renamed | view | `stg_kiva_loans` |
| `int_<entity>_<what was done>` | Intermediate: business logic | view | `int_loans_enriched` |
| `mart_<entity>_by_<dimension>` | Mart for dashboards, at a named grain | table | `mart_loans_by_sector` |
| `mart_<entity>_features_ml` | Machine-learning feature table | table | `mart_loan_features_ml` |

- dbt sources are declared in `src_<source>.yml`, models in `schema.yml`.
- dbt singular tests state what must be true: `assert_<condition>.sql`, for example
  `assert_positive_loan_amount.sql`.

## Columns

| Pattern | Meaning | Examples |
|---|---|---|
| `<entity>_id` | Key, renamed from the source's `id` in staging | `loan_id` |
| `<entity>_<attribute>` | Renamed from a generic source name | `loan_status`, `entrepreneur_name` |
| `_amount` | Money, in the loan's currency | `loan_amount`, `funded_amount`, `amount_remaining` |
| `total_` | Sum in an aggregate | `total_loan_volume`, `total_funding_gap` |
| `_percentage`, `_ratio` | A percentage, or a ratio between zero and one | `funding_percentage`, `funding_ratio` |
| `is_<state>` | 0 or 1 flag | `is_deleted`, `is_sector_agriculture`, `is_country_kenya` |
| `log_` | Log transform | `log_loan_amount` |
| `target_` | Machine-learning label | `target_is_fully_funded` |
| `_date`, `_at` | Date or timestamp | `posted_date`, `source_updated_at` |
| leading underscore | CDC system column, never business data | `_op`, `_version` |

## Services, metrics and alerts

- Compose services and containers: `kiva_<service>`, for example `kiva_postgres`,
  `kiva_debezium`, `kiva_cdc_monitor`.
- Metrics from `cdc-monitor` start with what they measure: `cdc_` for pipeline signals,
  `debezium_connector_` for connector state, `cdc_monitor_` for the monitor's own health.
  Counters end in `_total`, durations in `_seconds`.
- Grafana alert rules have kebab-case ids that name the condition: `cdc-row-drift-high`,
  `cdc-replication-lag-high`, `debezium-connector-down`, `clickhouse-ingestion-stalled`.
