-- serving/duckdb/init.sql
--
-- One-time DuckDB bootstrap: install extensions and create schemas.
-- Executed by refresh.py before any view creation or dbt run.
-- S3 / MinIO credentials are NOT here — injected by Python via SET statements.

INSTALL httpfs;
INSTALL iceberg;
INSTALL postgres;
LOAD httpfs;
LOAD iceberg;
LOAD postgres;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;
