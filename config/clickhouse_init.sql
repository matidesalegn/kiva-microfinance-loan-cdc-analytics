CREATE DATABASE IF NOT EXISTS raw_data;

-- 1. Kafka Engine Table: Connects directly to Redpanda to consume CDC events.
-- `updated_at` is carried through even though it isn't used by any dbt model,
-- because it is what lets cdc-monitor compute a real, row-level CDC freshness
-- lag (now() - max(source_updated_at)) instead of a synthetic sleep/heuristic.
-- posted_date/updated_at are declared Nullable(Int64), NOT String or DateTime:
-- with schemas disabled, Debezium's default time.precision.mode encodes
-- Postgres TIMESTAMP columns as raw microseconds-since-epoch JSON integers
-- (e.g. 1786453340000000), never an ISO date string. Declaring them String
-- and casting with toDateTimeOrNull() -- the naive approach -- doesn't error
-- on this; it silently clamps every row to ClickHouse's DateTime32 max
-- (2106-02-07 06:28:15), corrupting every date-derived column downstream.
-- fromUnixTimestamp64Micro() below is the correct decode.
CREATE TABLE IF NOT EXISTS raw_data.kafka_kiva_loans_cdc (
    id Int64,
    name String,
    status String,
    funded_amount Float64,
    loan_amount Float64,
    activity String,
    sector String,
    country String,
    town String,
    posted_date Nullable(Int64),
    updated_at Nullable(Int64),
    __op String,
    __deleted String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'cdc.raw_data.kiva_loans',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow';

-- 2. Raw Table: ReplacingMergeTree handles duplicates and updates naturally in ClickHouse.
-- PARTITION BY toYYYYMM(posted_date) bounds part count to one per calendar month
-- (Kiva loan history spans years, not decades) and enables cheap partition-level
-- operations (TTL/drop/backfill) as volume grows -- see docs/design-report.md
-- for the full ClickHouse table-design rationale.
CREATE TABLE IF NOT EXISTS raw_data.kiva_loans_raw (
    id Int64,
    name String,
    status String,
    funded_amount Float64,
    loan_amount Float64,
    activity String,
    sector String,
    country String,
    town String,
    posted_date DateTime,
    source_updated_at DateTime,
    _op String,
    is_deleted UInt8,
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(posted_date)
ORDER BY (id);

-- 3. Materialized View: Moves data from Kafka stream into the Raw Table instantly
CREATE MATERIALIZED VIEW IF NOT EXISTS raw_data.kiva_loans_mv TO raw_data.kiva_loans_raw AS
SELECT
    id,
    name,
    status,
    funded_amount,
    loan_amount,
    activity,
    sector,
    country,
    town,
    toDateTime(fromUnixTimestamp64Micro(ifNull(posted_date, 0))) AS posted_date,
    toDateTime(fromUnixTimestamp64Micro(ifNull(updated_at, 0))) AS source_updated_at,
    __op AS _op,
    if(__deleted = 'true', 1, 0) AS is_deleted,
    toUInt64(now()) AS _version
FROM raw_data.kafka_kiva_loans_cdc;
