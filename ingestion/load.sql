-- ECB SAFE Microdata — SQL load script
-- Executed by .github/workflows/ingest-sql-test.yml via:
--   envsubst < ingestion/load.sql | ./duckdb
--
-- ${CSV_DIR} is substituted at runtime with the directory containing
-- the extracted CSV files.

ATTACH 'md:safe_ai';
USE safe_ai;

CREATE SCHEMA IF NOT EXISTS raw;

CREATE OR REPLACE TABLE raw.safe_microdata_sql AS
SELECT
    *,
    current_timestamp AS _ingested_at
FROM read_csv(
    '${CSV_DIR}/*.csv',
    header        = true,
    ignore_errors = true,
    union_by_name = true
);

SELECT 'Rows loaded: ' || COUNT(*)::VARCHAR AS result
FROM raw.safe_microdata_sql;
