CREATE DATABASE IF NOT EXISTS raw_data;

-- 1. Kafka Engine Table: Connects directly to Redpanda to consume CDC events
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
    posted_date String,
    __op String,
    __deleted String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list = 'cdc.raw_data.kiva_loans',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow';

-- 2. Raw Table: ReplacingMergeTree handles duplicates and updates naturally in ClickHouse
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
    _op String,
    is_deleted UInt8,
    _version UInt64
) ENGINE = ReplacingMergeTree(_version)
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
    toDateTimeOrNull(posted_date) AS posted_date,
    __op AS _op,
    if(__deleted = 'true', 1, 0) AS is_deleted,
    toUInt64(now()) AS _version
FROM raw_data.kafka_kiva_loans_cdc;
