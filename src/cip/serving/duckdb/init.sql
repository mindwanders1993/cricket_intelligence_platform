-- serving/duckdb/init.sql
--
-- One-time DuckDB bootstrap: install extensions and create schemas.
-- Executed by refresh.py before any view creation or dbt run.
-- S3 / MinIO credentials are NOT here — injected by Python via SET statements.

INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;
