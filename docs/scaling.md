# Pipeline Scaling & Infrastructure Growth Strategy

## Overview
This report outlines how the Kiva Microfinance Data Platform can scale horizontally and vertically to support high-throughput data volumes (e.g. tens of millions of daily CDC transactions and real-time streaming queries).

---

## 1. Storage & Database Scaling (ClickHouse & Postgres)

### ClickHouse Cluster Horizontal Sharding
- **ReplicatedMergeTree & Sharding:** Transition from single-instance ClickHouse to a distributed ClickHouse cluster (`Distributed` engine) with `ReplicatedMergeTree` across 3+ data nodes.
- **Partitioning Strategy:** Partition large historical tab les by month (`PARTITION BY toYYYYMM(posted_date)`) to allow ClickHouse to drop or detach old data partitions efficiently without rewriting full table merges.
- **Deduplication at Scale:** Utilize ClickHouse `FINAL` modifier sparingly or leverage materialized view aggregations (`SummingMergeTree` / `AggregatingMergeTree`) to reduce read-time computation overhead.

### PostgreSQL Read Replicas & Connection Pooling
- **Logical Replication Offloading:** Move Debezium CDC WAL extraction from the primary OLTP database to a dedicated PostgreSQL standby replica to prevent WAL reading CPU spikes on primary write transactions.
- **PgBouncer:** Deploy PgBouncer connection pooling to handle high-concurrency ingestion script connections.

---

## 2. Ingestion & Streaming Scalability (Debezium & Redpanda)

### Redpanda Partition Parallelism
- Increase `cdc.raw_data.kiva_loans` topic partitions from 1 to `N` matching worker core count.
- ClickHouse `Kafka` engine consumer threads scale linearly with partition count, enabling parallel lock-free bulk inserts.

### Debezium Buffer Tuning
- Scale Debezium `max.batch.size` and `max.queue.size` to absorb high-throughput WAL spikes during batch loading operations.

---

## 3. Transformation & Feature Store Scaling (dbt & Feast)

### dbt Incremental Materializations
- Transition staging and intermediate models from views to `incremental` table materializations in dbt using watermark checks (`WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})`).

### Online/Offline Feature Store Integration
- Connect ClickHouse (`analytics.mart_loan_features_ml`) as an offline feature store to **Feast** or **Redis** for sub-millisecond online feature retrieval in production ML inference microservices.
