# Data catalogue

Every table and view, layer by layer, with its grain and its columns. The source of truth is
the code: [`config/init.sql`](../config/init.sql) for PostgreSQL,
[`config/clickhouse_init.sql`](../config/clickhouse_init.sql) for the ClickHouse landing layer,
and the dbt models under [`dbt_project/models`](../dbt_project/models). Tests listed here are
the ones declared in [`schema.yml`](../dbt_project/models/schema.yml) and
[`src_kiva.yml`](../dbt_project/models/staging/src_kiva.yml), plus the singular test in
[`dbt_project/tests`](../dbt_project/tests). [`data_model.md`](data_model.md) explains the
layering and the ClickHouse design choices.

| Layer | Where | Written by | Read by |
|---|---|---|---|
| Source | PostgreSQL `kiva_oltp`, schema `raw_data` | the ingestion job (`src/ingest_api.py`) | Debezium |
| Stream | Redpanda topic `cdc.raw_data.kiva_loans` | Debezium | ClickHouse |
| Landing | ClickHouse database `raw_data` | a materialized view over the Kafka engine table | dbt source, `cdc-monitor` |
| Models | ClickHouse database `analytics` | dbt | Grafana dashboards, the feature consumer |

## Source: PostgreSQL `raw_data.kiva_loans`

One row per loan. `REPLICA IDENTITY DEFAULT`, so update events carry the primary key.

| Column | Type | Meaning |
|---|---|---|
| `id` | int, primary key | Kiva loan id |
| `name` | varchar | Borrower or borrowing group (personal data) |
| `status` | varchar | Funding state, for example `funded` |
| `funded_amount` | numeric | Amount raised so far |
| `loan_amount` | numeric | Amount requested |
| `activity` | varchar | Business activity, for example Farming |
| `sector` | varchar | Sector, for example Agriculture |
| `country`, `town` | varchar | Where the borrower is |
| `posted_date` | timestamp | When the loan was published |
| `ingested_at` | timestamp | First load |
| `updated_at` | timestamp | Last change, carried through CDC as the freshness signal |

## Landing: ClickHouse `raw_data`

| Object | What it is |
|---|---|
| `kafka_kiva_loans_cdc` | Kafka engine table reading the topic as `JSONEachRow`, consumer group `clickhouse_consumer_group` |
| `kiva_loans_mv` | Materialized view that converts the timestamps and the delete flag, and writes the landing table |
| `kiva_loans_raw` | Landing table, `ReplacingMergeTree(_version)`, ordered by `id`, partitioned by month of `posted_date` |

`kiva_loans_raw` columns: the source columns above except `ingested_at`, with `posted_date` as
`DateTime`, plus:

| Column | Meaning |
|---|---|
| `source_updated_at` | The source `updated_at`; `cdc-monitor` measures replication lag from its newest value |
| `_op` | Debezium operation |
| `is_deleted` | 1 when the event is a delete |
| `_version` | The ClickHouse insert time (`now()` in the materialized view). The engine keeps the row with the highest version, so the latest arrival wins |

dbt test on the source: `id` not null.

## Models: ClickHouse `analytics` (dbt)

### `stg_kiva_loans` (view)

Current state of each loan: reads `kiva_loans_raw` with `FINAL` and drops rows where
`is_deleted = 1`. Grain: one row per loan. Owner: data engineering.

| Column | Meaning | Tests |
|---|---|---|
| `loan_id` | From `id` | unique, not null |
| `entrepreneur_name` | From `name`; tagged `contains_pii` | |
| `loan_status` | From `status` | not null; one of funded, fundraising, in_repayment, defaulted, refunded, expired |
| `funded_amount` | Amount raised | |
| `loan_amount` | Amount requested | not null |
| `amount_remaining` | `loan_amount` minus `funded_amount` | |
| `activity`, `sector`, `country`, `town` | Descriptors | |
| `posted_date` | Publication time | |

### `int_loans_enriched` (view)

Adds business categories to the staging view. Grain: one row per loan. Owner: analytics.

| Column | Meaning | Tests |
|---|---|---|
| all `stg_kiva_loans` columns | As above | |
| `funding_tier` | Fully Funded when nothing remains; Almost Funded when at most a tenth remains; otherwise Needs Funding | not null; one of the three |
| `funding_percentage` | `funded_amount` as a percentage of `loan_amount`, two decimals | |

### `mart_loans_by_sector` (table)

Grain: one row per country and sector. A `MergeTree` ordered by country and sector. Owner:
reporting.

| Column | Meaning | Tests |
|---|---|---|
| `country`, `sector` | The grain | `country` not null |
| `total_loans` | Distinct loans | |
| `total_loan_volume` | Sum of `loan_amount` | not null |
| `total_funded_volume` | Sum of `funded_amount` | |
| `total_funding_gap` | Sum of `amount_remaining` | |
| `funding_rate_percentage` | Funded volume as a percentage of loan volume | |

### `mart_loan_features_ml` (table)

The machine-learning feature table. Grain: one row per loan. A `MergeTree` ordered by
`loan_id`, partitioned by month of `posted_date`. Owner: ML engineering.

| Column | Meaning | Tests |
|---|---|---|
| `loan_id` | The grain | unique, not null |
| `loan_amount`, `funded_amount`, `amount_remaining`, `funding_percentage` | Amounts | |
| `funding_ratio` | `funded_amount` over `loan_amount`, four decimals | |
| `log_loan_amount`, `log_funded_amount` | Base-ten log of the amount plus one | |
| `sector`, `country` | Descriptors | |
| `is_sector_agriculture`, `is_sector_retail`, `is_sector_services`, `is_sector_food` | One-hot sector flags, 0 or 1 | |
| `is_country_rwanda`, `is_country_kenya` | One-hot country flags, 0 or 1 | |
| `posted_date`, `posted_month`, `posted_day_of_week` | Date features | |
| `days_since_posted` | Days from `posted_date` to the build time | |
| `target_is_fully_funded` | Label: 1 when `funded_amount` reaches `loan_amount` | not null; 0 or 1 |

Singular test: [`assert_positive_loan_amount.sql`](../dbt_project/tests/assert_positive_loan_amount.sql).

## Monitoring metrics (`cdc-monitor`)

Not tables, but part of the contract the dashboards and alerts read.

| Metric | Meaning |
|---|---|
| `cdc_postgres_row_count` | Rows in the PostgreSQL source table |
| `cdc_clickhouse_row_count` | Deduplicated rows in the ClickHouse landing table |
| `cdc_row_count_drift` | The first minus the second |
| `cdc_replication_lag_seconds` | Seconds since the newest replicated `source_updated_at`; no value until a row has replicated |
| `debezium_connector_state` | 1 when the connector and every task are running |
| `debezium_connector_failed_tasks` | Tasks in a failed state |
| `cdc_monitor_scrape_errors_total` | Failed polls, by data source |
| `cdc_monitor_last_success_timestamp_seconds` | Time of the last poll that reached every source |
