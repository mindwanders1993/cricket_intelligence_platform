# Stage F — Final consistency review prompt (Gemini CLI)

After Phases 1-6 are implemented, open `gemini` at the project root and paste:

---

```
@docs/silver_match_spec/spec.md
@CLAUDE.md
@src/cip/transform/spark/silver/
@src/cip/quality/checks/match_silver_dq.py
@src/cip/quality/checks/match_bronze_dq.py
@src/cip/quality/checks/register_dq.py
@src/cip/ingestion/jobs/build_match_silver.py
@orchestration/airflow/dags/dag_build_silver_match_data.py
@src/cip/common/contracts/naming.py
@tests/unit/transform/spark/silver/
@tests/unit/quality/test_match_silver_dq.py
@tests/unit/ingestion/jobs/test_build_match_silver.py

You are reviewing Big Task 5 (Match Silver pipeline) implementation against
the spec. The pipeline produces 12 Silver tables from cricket.bronze.match_documents.

Find every issue. Be exhaustive — this is the final gate before merge. Report
findings as a checklist of `FIX:` items, each with `<file>:<line>` or
`<file>` ref so the user can navigate directly.

Check categories:

## 1. Naming & contracts compliance

- Any raw FQN strings like "cricket.silver.matches" instead of 
  `TableName.silver("matches")`
- Any literal "_snapshot_date" / "_ingested_at" / "_pipeline_run_id" instead of 
  `META.SNAPSHOT_DATE` etc.
- Any new Silver table written but not present in `TableName.SILVER_TABLES`
- `unmatched_persons_audit` and `match_registry` must be in SILVER_TABLES

## 2. Writer / partition consistency

- Every `SparkIcebergWriter.dynamic_overwrite()` call must pass
  `partition_cols=["_snapshot_date"]` (or `partition_cols=[META.SNAPSHOT_DATE]`)
- No use of `df.write.format("iceberg")` direct — must go through writer

## 3. Spec coverage

For each of the 12 Silver tables in spec section 2:
- Does an implementation file exist?
- Does its schema match the spec (columns, types, nullability)?
- Are the listed edge cases (spec section 3) covered in tests?

For each test scenario in spec section 6:
- Is there a corresponding test in `tests/unit/transform/spark/silver/`?

## 4. Identity resolution correctness

In `identity_resolution.py`:
- Algorithm matches spec section 4 exactly: registry → name_variations → audit
- Unmatched rows are written to `silver.unmatched_persons_audit`, NOT dropped
- `match_players` and `match_officials` are rewritten with person_id (NULL when 
  unresolved), NOT filtered

## 5. DQ checks

- All 6 checks (MAT-SLV-001 … 006) present in `match_silver_dq.py`
- BLOCK vs WARN severity matches spec
- Results persist to `control.dq_results` via the same INSERT pattern as 
  `match_bronze_dq.py`
- `DQBlockingFailureError` raised after persisting (not before)

## 6. Job + DAG wiring

- 4 phases + DQ task all present in `build_match_silver.py`
- Idempotency check against `control.silver_match_build_log` before each phase
- Each phase logs RUNNING → SUCCESS/FAILED row with rows_written and duration_ms
- DAG `dag_build_silver_match_data`: task graph matches `check_infra → 
  wait_for_archive_dq → phase1_lookups → phase2_facts → phase3_participants → 
  resolve_identity → run_dq → done`
- `ExternalTaskSensor` on `dag_ingest_all_match_data.run_dq`

## 7. Spark-specific gotchas

- `posexplode_outer` vs `explode`: outer is needed where the array can be empty
  but the parent row should be preserved (e.g. wickets within deliveries)
- `from_json` schema parsing — check the StructType has nullable=True for every 
  optional field per the spec
- Window functions for MAX(revision) and legal_ball_num — partitioned correctly
- `coalesce(extras.wides, 0)` — every nullable int that's summed/counted 

## 8. Tests

- Mock all I/O (no real catalog calls)
- SparkSession fixture is `local[1]` and session-scoped
- Every edge case from spec section 3 has at least one test asserting it
- 80%+ branch coverage on the 4 fact transforms (matches, innings, deliveries, 
  wickets) and identity_resolution

## 9. CLAUDE.md & docs alignment

- New tables in `TableName.SILVER_TABLES` reflected in CLAUDE.md "Known tables"
- Any new env var or service dependency documented

## Output format

```
FIX: <file>:<line> — <one-line description of issue>
FIX: <file> — <description>
...
```

Group fixes by category (Naming, Writer, Spec coverage, Identity, DQ, DAG, Spark,
Tests, Docs). Sort by severity within each group (CRITICAL > MAJOR > MINOR).

If a category has zero issues, write `OK: <category>` to make that explicit.

Do not propose solutions — just identify problems. The user will take this back 
to Claude Code with the checklist.
```
