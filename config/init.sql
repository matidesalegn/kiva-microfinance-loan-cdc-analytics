-- Create schema for Kiva Microfinance raw data
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Set up the loans table to store Kiva micro-loans data
CREATE TABLE IF NOT EXISTS raw_data.kiva_loans (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(50),
    funded_amount NUMERIC(15, 2),
    loan_amount NUMERIC(15, 2),
    activity VARCHAR(150),
    sector VARCHAR(150),
    country VARCHAR(150),
    town VARCHAR(150),
    posted_date TIMESTAMP,
    -- Audit columns for CDC and debugging
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure replica identity is set to FULL or DEFAULT so Debezium can track deletes/updates correctly
ALTER TABLE raw_data.kiva_loans REPLICA IDENTITY DEFAULT;
