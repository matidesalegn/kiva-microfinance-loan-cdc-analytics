# Requirements

What this project had to do: the objective, then the functional and non-functional
requirements that follow from it, and what was left out on purpose. It is an independent
portfolio project on Kiva's public loan API and is not affiliated with or endorsed by Kiva.

## Objective

Keep an analytics warehouse in step with a transactional loans database, change by change, and
make sure a lost or delayed change cannot go unnoticed even while every service reports healthy.

## Functional requirements

1. Ingest funded loans from Kiva's public API (`/v1/loans/search.json`) into PostgreSQL,
   upserting on the loan id, so a re-run updates existing loans instead of duplicating them and
   the change stream carries real updates, not only inserts.
2. Capture every change from PostgreSQL's write-ahead log with Debezium and publish it to
   Redpanda as flattened JSON, with the operation and a deleted flag on each event.
3. Land the events in ClickHouse and keep the latest row for each loan.
4. Model the data with dbt:
   - a staging view of the current state, with deleted rows removed;
   - an intermediate view that adds a funding tier and a funding percentage;
   - a mart by country and sector for dashboards;
   - a per-loan machine-learning feature table with a fully-funded label.
5. Test the models: unique and not-null keys, and accepted values for the loan status, the
   funding tier and the label.
6. Orchestrate ingestion, then `dbt run` and `dbt test`, with Dagster, at startup and every
   fifteen minutes. The change path itself must not wait for that schedule.
7. Monitor the pipeline with signals that infrastructure metrics do not give:
   - row-count drift between PostgreSQL and the deduplicated ClickHouse table;
   - replication lag, measured from the newest replicated `updated_at`;
   - connector health from the Kafka Connect API, with every task counted.
8. Alert by email when any of those breaks, and show pipeline health and loan analytics on
   dashboards.

## Non-functional requirements

- **One command.** `docker compose up -d` starts every service, and the Debezium connector
  registers itself from a template rendered with the values in `.env`.
- **Laptop-sized.** Every service has an explicit memory limit, and the whole set needs about
  4 GB of free memory.
- **Polite to a free API.** A browser User-Agent, because the API's firewall rejects the
  default Python client; retries with exponential backoff; a fifteen-minute polling cadence.
- **No external accounts.** Monitoring and alert email (Mailpit) run inside Compose.
- **Tested in CI, in stages:** lint and unit tests, then Compose and dbt parse checks, then an
  end-to-end CDC and dbt test against the live stack. Each stage runs only if the previous
  one passed.
- **Personal data marked.** The borrower name is tagged as personal data in the dbt
  metadata.

## Out of scope

- **History of changes.** The warehouse keeps the latest state of each loan, not every
  version.
- **Deletes at the source.** The ingestion only inserts and updates. The pipeline still
  carries the deleted flag end to end, so a delete in PostgreSQL would be filtered out in
  staging.
- **More than one node per service.** Everything runs as a single node in Compose;
  [`scaling.md`](scaling.md) sets out how each part would scale.

See [`README.md`](README.md) for where each requirement is documented in more depth.
