# DuckDB `iceberg_scan` + Iceberg v2 row-level deletes

## TL;DR

**DuckDB's iceberg extension honours Iceberg v2 row-level delete files
correctly.** Plain `SELECT * FROM iceberg_scan(...)` is sufficient when
materialising Silver into DuckDB — no `QUALIFY ROW_NUMBER` dedup workaround
is needed.

## Why this matters

The match-data pipeline rework introduces incremental Silver via
`SparkIcebergWriter.delete_and_insert(key_cols=["match_id"])`. Under the
hood this issues Iceberg v2 row-level deletes (Spark SQL `DELETE FROM …
WHERE match_id IN (…)`) before each `INSERT INTO`. If DuckDB's
`iceberg_scan` ignored those delete files, every changed match would
appear twice in `silver.deliveries` (old rows + new rows), and the
`MAX(_snapshot_date)` filter in `DuckDBRefresh.create_silver_views` would
silently drop most data because `_snapshot_date` is now per-write rather
than per-snapshot.

The whole DuckDB Silver refresh path hinges on this question:

> When we issue Spark `DELETE FROM silver.X` + `INSERT INTO silver.X`,
> does DuckDB's `iceberg_scan` show the deletes as applied?

## Experiment

Versions used (2026-05-18):

- `pyspark` 3.5.8
- `iceberg-spark-runtime` 1.5.0 (from Maven, downloaded by the platform)
- `pyiceberg` 0.11.1
- `duckdb` 1.5.2 with the `iceberg` extension installed from the DuckDB
  community repository
- MinIO + Iceberg REST catalog (the platform's dev stack)

Steps reproduced from a fresh table:

1. `CREATE TABLE silver.delete_test ... TBLPROPERTIES ('format-version'='2')`
   via Spark SQL.
2. `INSERT INTO silver.delete_test VALUES ('A',1,'a1'), ('A',2,'a2'), ('B',1,'b1')`
   (initial 3 rows).
3. `DELETE FROM silver.delete_test WHERE match_id IN ('A')`.
4. `INSERT INTO silver.delete_test VALUES ('A',1,'a1_new'), ('A',2,'a2_new'), ('A',3,'a3_new')`.
5. Spark snapshot history shows `['append', 'overwrite', 'append']` — the
   DELETE compresses into an `overwrite` snapshot at the Iceberg metadata
   level. Spark sees 4 rows (1 of B + 3 of A_new).
6. From a separate DuckDB process:
   ```sql
   SELECT COUNT(*) FROM iceberg_scan('s3://cricket-lakehouse/silver/delete_test', allow_moved_paths=true);
   -- 4
   ```
7. Row contents:
   ```
   ('A', 1, 'a1_new')
   ('A', 2, 'a2_new')
   ('A', 3, 'a3_new')
   ('B', 1, 'b1')
   ```

Expected outcomes:

- **4** → DuckDB honours deletes (the case we observed).
- **7** → DuckDB does not honour deletes; we would have needed
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY {natural_keys} ORDER BY
  _snapshot_date DESC, _ingested_at DESC) = 1` in the refresh SQL to
  dedupe in DuckDB.

## Implications for the codebase

`src/cip/serving/duckdb/refresh.py:create_silver_views` ships with the
plain shape:

```sql
CREATE OR REPLACE TABLE silver.{table} AS
SELECT * FROM iceberg_scan('{path}', allow_moved_paths=true);
```

No `QUALIFY` clause, no `_NATURAL_KEYS_BY_TABLE` mapping required for
dedup purposes (the mapping may still be useful for other refresh-time
operations, but it is not load-bearing here).

If a future DuckDB upgrade regresses this behaviour, this runbook is the
hand-off — re-run the experiment, and if the count flips to 7, restore
the `QUALIFY` shape (the natural-keys mapping should still be
maintainable for that fallback).

## Reproducing the experiment

Save the following as a one-off scratch file and run it from the project
root. It is intentionally not checked in — there is no value in carrying
it around once the runbook records the outcome.

```python
# Part 1 — Spark side: create + delete + insert
import os
os.environ.setdefault("ICEBERG_REST_URI", "http://localhost:8181")
os.environ.setdefault("MINIO_S3_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("POSTGRES_HOST", "localhost")

from cip.transform.spark.session import get_or_create_spark

T = "silver.delete_test"
spark = get_or_create_spark("delete-insert-experiment")
spark.sql(f"DROP TABLE IF EXISTS {T}")
spark.sql(f"""
    CREATE TABLE {T} (match_id STRING, ball INT, v STRING)
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
""")
spark.sql(f"INSERT INTO {T} VALUES ('A',1,'a1'),('A',2,'a2'),('B',1,'b1')")
spark.sql(f"DELETE FROM {T} WHERE match_id IN ('A')")
spark.sql(f"INSERT INTO {T} VALUES ('A',1,'a1_new'),('A',2,'a2_new'),('A',3,'a3_new')")
print("Spark count:", spark.sql(f"SELECT COUNT(*) FROM {T}").first()[0])
print("Snapshots:", [r["operation"] for r in spark.sql(f"SELECT operation FROM {T}.snapshots").collect()])
spark.stop()
```

```python
# Part 2 — DuckDB side: query through iceberg_scan
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='localhost:9000'")
con.execute("SET s3_url_style='path'")
con.execute("SET s3_use_ssl=false")
con.execute("SET s3_region='us-east-1'")
con.execute("SET s3_access_key_id='<MINIO_ROOT_USER>'")
con.execute("SET s3_secret_access_key='<MINIO_ROOT_PASSWORD>'")
con.execute("SET unsafe_enable_version_guessing=true")
path = "s3://cricket-lakehouse/silver/delete_test"  # no trailing slash
print("DuckDB count:", con.execute(
    f"SELECT COUNT(*) FROM iceberg_scan('{path}', allow_moved_paths=true)"
).fetchone()[0])
```

After validating, drop the test table:

```python
from cip.transform.spark.session import get_or_create_spark
spark = get_or_create_spark("cleanup")
spark.sql("DROP TABLE IF EXISTS silver.delete_test PURGE")
spark.stop()
```
