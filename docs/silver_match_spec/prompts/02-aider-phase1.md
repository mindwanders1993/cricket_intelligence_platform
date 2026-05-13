# Stage B — Phase 0 + 1 prompt (Aider + Ollama)

After `aider` is open with the files listed in RUNBOOK.md Stage B, paste:

---

```
Read `docs/silver_match_spec/spec.md`:
- Section 1 (PySpark StructType)
- Section 2 entries for tables: teams, venues, competitions
- Section 5 (dependency order)
- Section 6 entries for teams, venues, competitions

Implement these four files following the SAME class pattern as
`src/cip/transform/polars/silver/persons.py` (class with `from_settings()`
factory, `run(snapshot_date, pipeline_run_id)` method, dataclass result).

## File 1: `src/cip/transform/spark/silver/_shared.py`

Module-level constants and shared helpers:

- `MATCH_JSON_SCHEMA: StructType` — exact StructType from spec section 1
- `_BRONZE_TABLE = TableName.bronze("match_documents")`
- `def read_bronze_matches(spark: SparkSession, snapshot_date: str) -> DataFrame`
  - reads `cricket.bronze.match_documents` filtered by snapshot_date
  - applies window function: ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY CAST(revision AS INT) DESC)
  - filters to row_number == 1 (MAX revision per match_id)
  - parses raw_json column using `from_json(col("raw_json"), MATCH_JSON_SCHEMA)` → exposes parsed struct as `parsed` column
  - returns DataFrame with original Bronze cols + `parsed` struct column
- `def silver_meta_columns(snapshot_date: str, pipeline_run_id: str) -> list[Column]`
  - returns Column expressions for _snapshot_date, _ingested_at, _pipeline_run_id,
    _is_current=true, _valid_from=current_timestamp, _valid_to=NULL
  - use META.* constants

## File 2: `src/cip/transform/spark/silver/teams.py`

```python
class TeamsTransform:
    """Extract unique teams from match_documents → silver.teams (SCD2-style)."""
    
    def __init__(self, spark, writer): ...
    
    @classmethod
    def from_settings(cls) -> "TeamsTransform": ...
    
    def run(self, snapshot_date: str, pipeline_run_id: str) -> TeamsResult:
        # 1. read bronze with read_bronze_matches
        # 2. extract distinct team names from parsed.info.teams[] array
        # 3. derive team_type from parsed.info.team_type (international/club/etc)
        # 4. add silver_meta_columns
        # 5. writer.dynamic_overwrite(df, fqn=TableName.silver("teams"), 
        #    snapshot_date=snapshot_date, partition_cols=["_snapshot_date"], 
        #    pipeline_run_id=pipeline_run_id)
        # 6. return TeamsResult(rows_written=..., snapshot_date=..., ...)
```

## File 3: `src/cip/transform/spark/silver/venues.py`

Same structure. Extract from `parsed.info.venue` and `parsed.info.city`.
Distinct on (venue, city). Schema per spec section 2.

## File 4: `src/cip/transform/spark/silver/competitions.py`

Same structure. Extract from `parsed.info.event` struct
(name, match_number, group, stage, sub_name). Skip rows where event is null
(friendlies). Distinct on (competition_name, season, gender, match_type).

## Strict rules

- Use `TableName.silver()` and `TableName.bronze()` — NEVER raw FQN strings
- Use `META.SNAPSHOT_DATE` etc. — NEVER literal "_snapshot_date"
- Use `SparkIcebergWriter.dynamic_overwrite()` — NOT plain DataFrame.write
- `partition_cols=["_snapshot_date"]` on every writer call
- No type casting in Silver if Bronze field is already string — exception:
  cast numeric fields (innings.overs, deliveries.runs, etc.) per the spec types
- Add docstrings only where business intent is non-obvious

Do NOT write tests yet. Implementations only.
```
