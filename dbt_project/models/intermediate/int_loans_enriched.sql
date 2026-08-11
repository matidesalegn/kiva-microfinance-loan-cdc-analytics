{{ config(
    materialized='view'
) }}

WITH staged_data AS (
    SELECT *
    FROM {{ ref('stg_kiva_loans') }}
)

SELECT
    loan_id,
    entrepreneur_name,
    loan_status,
    funded_amount,
    loan_amount,
    amount_remaining,
    
    -- Business Logic Enrichment
    CASE 
        WHEN amount_remaining = 0 THEN 'Fully Funded'
        WHEN amount_remaining > 0 AND amount_remaining <= (loan_amount * 0.1) THEN 'Almost Funded'
        ELSE 'Needs Funding'
    END AS funding_tier,
    
    ROUND((funded_amount / NULLIF(loan_amount, 0)) * 100, 2) AS funding_percentage,

    activity,
    sector,
    country,
    town,
    posted_date
FROM staged_data
