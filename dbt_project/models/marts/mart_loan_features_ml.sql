{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['loan_id'],
    partition_by='toYYYYMM(posted_date)'
) }}
-- Row-per-loan grain that grows unbounded with ingestion volume, so it is
-- partitioned like the raw CDC table (see docs/design-report.md) to keep
-- feature-store rebuilds/backfills scoped to a single month of data.

WITH enriched_loans AS (
    SELECT *
    FROM {{ ref('int_loans_enriched') }}
)

SELECT
    loan_id,
    
    -- Numerical Feature Engineering & Scaling
    loan_amount,
    funded_amount,
    amount_remaining,
    funding_percentage,
    ROUND(funded_amount / NULLIF(loan_amount, 0), 4) AS funding_ratio,
    
    -- Log Transformation for skewed financial variables
    ROUND(log10(loan_amount + 1), 4) AS log_loan_amount,
    ROUND(log10(funded_amount + 1), 4) AS log_funded_amount,

    -- Categorical One-Hot / Binary Encodings
    sector,
    CASE WHEN sector = 'Agriculture' THEN 1 ELSE 0 END AS is_sector_agriculture,
    CASE WHEN sector = 'Retail' THEN 1 ELSE 0 END AS is_sector_retail,
    CASE WHEN sector = 'Services' THEN 1 ELSE 0 END AS is_sector_services,
    CASE WHEN sector = 'Food' THEN 1 ELSE 0 END AS is_sector_food,

    country,
    CASE WHEN country = 'Rwanda' THEN 1 ELSE 0 END AS is_country_rwanda,
    CASE WHEN country = 'Kenya' THEN 1 ELSE 0 END AS is_country_kenya,

    -- Temporal Features
    posted_date,
    toMonth(posted_date) AS posted_month,
    toDayOfWeek(posted_date) AS posted_day_of_week,
    dateDiff('day', posted_date, now()) AS days_since_posted,

    -- Machine Learning Target Label (Classification Target)
    CASE 
        WHEN funded_amount >= loan_amount THEN 1 
        ELSE 0 
    END AS target_is_fully_funded

FROM enriched_loans
