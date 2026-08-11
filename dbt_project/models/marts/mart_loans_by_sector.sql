{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['country', 'sector']
) }}

SELECT
    country,
    sector,
    COUNT(DISTINCT loan_id) AS total_loans,
    SUM(loan_amount) AS total_loan_volume,
    SUM(funded_amount) AS total_funded_volume,
    SUM(amount_remaining) AS total_funding_gap,
    ROUND(SUM(funded_amount) / SUM(loan_amount) * 100, 2) AS funding_rate_percentage
FROM {{ ref('int_loans_enriched') }}
GROUP BY country, sector
