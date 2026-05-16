---
name: cip-inspect-table
description: "Inspect any Bronze / Silver / Gold table in the Cricket Intelligence Platform lakehouse: row count, snapshot-date histogram, schema, sample rows. Use when the user asks 'what does silver.deliveries look like', 'how many rows in bronze.match_data', 'show me dim_player', or wants to verify a table after a pipeline run."
trigger: /cip-inspect-table
---

# /cip-inspect-table

Uniform inspection for any registered table across all three layers. Bronze + Silver use the Iceberg REST catalog via PyIceberg (read-only). Gold uses DuckDB.

## Usage

```
/cip-inspect-table                          # ask which table
/cip-inspect-table <fqn>                    # full inspect: count, snapshots, schema, 5 sample rows
/cip-inspect-table <fqn> --rows N           # change sample size (default 5; 0 = no samples)
/cip-inspect-table <fqn> --snapshot YYYY-MM-DD   # filter to one _snapshot_date partition
/cip-inspect-table <fqn> --schema-only      # skip data scans, only print schema
/cip-inspect-table <fqn> --where "<expr>"   # additional filter for sample rows (DuckDB SQL)
```

`<fqn>` is the 2-segment FQN like `bronze.match_data`, `silver.deliveries`, `gold.fact_delivery`. Reject 3-segment legacy forms (`cricket.bronze.X`) with a clear error — the catalog name was dropped from FQNs in Big Task 3.

## What You Must Do When Invoked

If invoked with `--help` or `-h`, print Usage and stop.

### Step 1 — Resolve and validate the FQN

Parse `<fqn>` into `(layer, table)`. Layer must be `bronze` / `silver` / `gold`. Look it up:

```bash
poetry run python -c "
from cip.common.contracts.naming import TableName
from cip.common.contracts.enums import Layer
layer, table = '<LAYER>', '<TABLE>'
valid = {'bronze': TableName.BRONZE_TABLES, 'silver': TableName.SILVER_TABLES, 'gold': TableName.GOLD_TABLES}[layer]
assert table in valid, f'Unknown table {table!r} in {layer}. Valid: {sorted(valid)}'
print('ok')
"
```

If validation fails, surface the list of valid tables for that layer and stop.

### Step 2 — Pick the engine

| Layer | Engine | Reason |
|---|---|---|
| `bronze`, `silver` | PyIceberg via Polars | Iceberg-native, no Spark needed for read |
| `gold` | DuckDB CLI on `storage/duckdb/cricket.duckdb` | Gold is materialised there |

**Gold-layer caveat:** DuckDB allows a single process to hold the file at a time. If `make duckdb-ui` is running, **even a `-readonly` connection from a separate CLI fails** with `IO Error: Could not set lock on file ... Conflicting lock is held`. (Verified against DuckDB v1.5.2 — the readonly flag does not grant read-alongside-writer semantics.) Before inspecting Gold, run `make duckdb-stop`; offer to restart the UI for the user when finished.

### Step 3 — Run the inspection

**For Bronze / Silver** (Iceberg, host-shell env):

`PolarsIcebergReader.read_table(...)` returns an **eager** `pl.DataFrame` (not lazy). It supports `row_filter` for pushdown — use it for snapshot filtering on large tables, otherwise the whole table is materialised in memory. For Silver tables (millions of rows) you almost always want `--snapshot`.

```bash
ICEBERG_REST_URI=http://localhost:8181 \
MINIO_S3_ENDPOINT=http://localhost:9000 \
POSTGRES_HOST=localhost \
poetry run python <<'PY'
from cip.transform.shared.readers import PolarsIcebergReader

fqn = "<LAYER>.<TABLE>"
reader = PolarsIcebergReader.from_settings()

# Build the row_filter: if --snapshot was given, push it down; else read all.
row_filter = "_snapshot_date = '<DATE>'" if "<DATE>" else None  # None = full table
df = reader.read_table(fqn, row_filter=row_filter)  # eager pl.DataFrame

print(f"=== {fqn} ===")
print(f"Row count: {df.height:,}")

print("\n=== Snapshot-date histogram ===")
print(df.group_by("_snapshot_date").len().sort("_snapshot_date"))

print("\n=== Schema ===")
for name, dtype in df.schema.items():
    print(f"  {name}: {dtype}")

print("\n=== Sample rows (head <N>) ===")
print(df.head(<N>))
PY
```

For `--where`, append a `df = df.filter(...)` after the read.
For `--schema-only`, pass `columns=["_snapshot_date"]` (cheapest valid scan) and print only `df.schema`.

**For Gold** (DuckDB):

Pre-flight: `pgrep -f "duckdb .*-ui" >/dev/null` — if the UI is running, tell the user and run `make duckdb-stop` (asking first). Don't attempt to inspect Gold without an exclusive lock; even `-readonly` will fail.

```bash
duckdb -readonly storage/duckdb/cricket.duckdb <<'SQL'
.mode markdown
SELECT '=== gold.<TABLE> ===' AS header;
SELECT count(*) AS row_count FROM gold.<TABLE>;

SELECT '=== Schema ===' AS header;
DESCRIBE gold.<TABLE>;

SELECT '=== Sample rows ===' AS header;
SELECT * FROM gold.<TABLE> [WHERE <expr>] LIMIT <N>;
SQL
```

(`-readonly` is still worth passing — it prevents accidental writes to the file. It just doesn't grant concurrent-with-writer access.)

For Gold there is **no** `_snapshot_date` partition — skip the snapshot histogram.

When done, offer to restart the UI: `make duckdb-ui` (don't auto-launch — it's an interactive command).

### Step 4 — Report

Present the results in clean markdown sections. Highlight:
- **Unusual partition count.** If Silver has more than one `_snapshot_date`, mention it — DuckDB consumers filter to MAX, but it's worth knowing.
- **Empty tables.** Row count of 0 → suggest running the corresponding `/cip-pipeline-run`.
- **Schema surprises.** If you see SCD2 columns (`_is_current`, `_valid_from`, `_valid_to`) in Silver, mention that the table is SCD2 and naive reads see all history.

## Pre-flight checks

- For Bronze/Silver: `docker ps --filter name=compose-iceberg-rest-1 --filter status=running -q` must be non-empty. If not, suggest `make up`.
- For Gold: `storage/duckdb/cricket.duckdb` must exist. If not, the user hasn't run `/cip-gold-refresh` yet.
- For Gold: `pgrep -f "duckdb .*-ui"` must return nothing. If the UI is up, stop it (`make duckdb-stop`) first — see the Gold-layer caveat above.

## Honesty rules

- 2-segment FQNs only. Reject `cricket.bronze.X` with a hint that the catalog name was dropped.
- Validate against `TableName.*_TABLES` — don't `iceberg_scan` blindly. Mistyped names should fail fast.
- For Gold, default to `-readonly` so this never collides with the UI or a running Gold DAG.
