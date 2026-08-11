-- This is a custom Singular Test
-- It ensures that we don't have corrupted data where loan amount is negative

SELECT 
    loan_id, 
    loan_amount
FROM {{ ref('stg_kiva_loans') }}
WHERE loan_amount < 0
