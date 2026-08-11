{{ config(materialized='view') }}

WITH raw_data AS (
    -- The FINAL modifier ensures ClickHouse resolves the ReplacingMergeTree instantly,
    -- meaning we only see the absolute latest version of every CDC record!
    SELECT *
    FROM {{ source('raw_data', 'kiva_loans_raw') }} FINAL
    WHERE is_deleted = 0
)

SELECT
    id AS loan_id,
    name AS entrepreneur_name,
    status AS loan_status,
    funded_amount,
    loan_amount,
    (loan_amount - funded_amount) AS amount_remaining,
    activity,
    sector,
    country,
    town,
    posted_date
FROM raw_data
