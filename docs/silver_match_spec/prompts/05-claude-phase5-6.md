# Stage E — Phase 5 (DAG + jobs) + Phase 6 (DQ) prompt (Claude Code)

Open a fresh Claude Code session (or continue from Stage D) and paste:

---

```
Big Task 5 — Phase 5 (job + DAG wiring) + Phase 6 (Silver DQ).

Read first:
- `docs/silver_match_spec/spec.md` (referenced as needed)
- `src/cip/ingestion/jobs/ingest_all_match_data.py` (job pattern for match pipeline)
- `src/cip/ingestion/jobs/ingest_people_and_names.py` (job pattern for register)
- `orchestration/airflow/dags/dag_ingest_all_match_data.py` (DAG pattern)
- `orchestration/airflow/dags/dag_build_silver_match_data.py` (existing skeleton — fill this in)
- `src/cip/quality/checks/match_bronze_dq.py` (DQ pattern to follow)
- `src/cip/quality/checks/register_dq.py` (DQResult / DQRunSummary / persistence helpers)
- `infra/bootstrap/init-metastore.sql` (control schema DDL — needs a new table)

## Phase 5.1 — Control DB table

Append to `infra/bootstrap/init-metastore.sql`:

```sql
CREATE TABLE IF NOT EXISTS control.silver_match_build_log (
    id              BIGSERIAL PRIMARY KEY,
    dag_run_id      TEXT,
    pipeline_run_id TEXT NOT NULL,
    snapshot_date   DATE NOT NULL,
    phase           TEXT NOT NULL,          -- 'phase1_lookups' | 'phase2_facts' | 'phase3_participants' | 'phase4_identity'
    status          control.ingestion_status NOT NULL,
    rows_written    BIGINT,
    duration_ms     BIGINT,
    detail_json     JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    error_message   TEXT,
    UNIQUE (snapshot_date, phase, pipeline_run_id)
);
```

Provide a SQL snippet I can apply with:
  psql ${POSTGRES_DSN:-postgresql://cricket_user:cricket_pass@localhost:5432/cricket_platform} -f infra/bootstrap/init-metastore.sql

## Phase 5.2 — Job module: `src/cip/ingestion/jobs/build_match_silver.py`

Pattern: thin Airflow callables, idempotency via control DB.

Functions (each accepts snapshot_date, pipeline_run_id, force, **context):

- `task_build_phase1_lookups` — runs TeamsTransform, VenuesTransform, 
  CompetitionsTransform (in series; lookups are small)
- `task_build_phase2_facts` — runs MatchesTransform → InningsTransform → 
  DeliveriesTransform → WicketsTransform (order matters per spec section 5)
- `task_build_phase3_participants` — runs MatchPlayersTransform, 
  MatchOfficialsTransform, MatchRegistryTransform
- `task_resolve_identity` — runs IdentityResolver

Each function:
1. Check `control.silver_match_build_log` for (snapshot_date, phase, status=SUCCESS)
   → SKIP unless force=True
2. INSERT a RUNNING row
3. Execute the transforms
4. UPDATE row to SUCCESS with rows_written totals, duration_ms, detail_json
5. On exception: UPDATE row to FAILED with error_message, then re-raise

Also add `task_run_dq` callable (delegates to MatchSilverDQChecker — Phase 6 below).

CLI entrypoint with argparse: `--task [phase1|phase2|phase3|identity|dq|all]`,
`--snapshot-date`, `--force`.

## Phase 5.3 — DAG: fill in `orchestration/airflow/dags/dag_build_silver_match_data.py`

dag_id: `dag_build_silver_match_data` (from DagNames.BUILD_SILVER)
schedule: triggered by `ExternalTaskSensor` watching `dag_ingest_all_match_data`
  task `run_dq` success, OR manual trigger. For now, use manual schedule (None).
catchup: False
max_active_runs: 1

Task graph:
```
check_infra
    └─► wait_for_archive_dq          (ExternalTaskSensor on dag_ingest_all_match_data.run_dq)
          └─► phase1_lookups
                └─► phase2_facts
                      └─► phase3_participants
                            └─► resolve_identity
                                  └─► run_dq
                                        └─► done
```

`check_infra` verifies MinIO, PostgreSQL, Iceberg REST connectivity, and that
`cricket.bronze.match_documents` exists for the snapshot_date.

Use the same Jinja-template pattern from dag_ingest_all_match_data.py for
snapshot_date, pipeline_run_id, force.

Execution timeouts:
- check_infra: 2 min
- phase1_lookups: 15 min
- phase2_facts: 90 min (deliveries is the heavy one — 21k matches × ~250 deliveries each)
- phase3_participants: 20 min
- resolve_identity: 20 min
- run_dq: 30 min

## Phase 6 — DQ checks: `src/cip/quality/checks/match_silver_dq.py`

Follow the pattern of `match_bronze_dq.py`. Reuse `DQCheckResult`, `DQRunSummary`,
`DQBlockingFailureError`, `_pct` from `register_dq.py`.

`MatchSilverDQChecker.run_all(snapshot_date, pipeline_run_id)` runs these 6 checks:

| ID | Check | Severity |
|---|---|---|
| MAT-SLV-001 | PK uniqueness on every Silver table (matches, innings (match_id+innings_number), deliveries (match_id+innings_number+over_num+ball_num+legal_ball_num — exact PK from spec), wickets, match_players, match_officials, match_registry) | BLOCK |
| MAT-SLV-002 | FK integrity: every innings.match_id ∈ matches; every deliveries.(match_id, innings_number) ∈ innings; every wicket.(match_id, innings_number, over_num, ball_num) ∈ deliveries | BLOCK |
| MAT-SLV-003 | deliveries.ball_num and legal_ball_num are monotonic within (match_id, innings_number, over_num) | BLOCK |
| MAT-SLV-004 | Reconciliation: SUM(deliveries.runs_total) per innings == outcome score where available; report mismatch rate. WARN if any mismatch > 0 runs (cricket is exact, but data quirks exist) | WARN |
| MAT-SLV-005 | Unmatched person rate <= 5% — count rows in unmatched_persons_audit / total match_players + match_officials rows | WARN |
| MAT-SLV-006 | Silver match count == DISTINCT Bronze match_documents count for same snapshot | WARN |

Each check returns a `DQCheckResult` with check_id, check_name, layer="SILVER",
source_file="match_documents JSON", table_name, status, severity, expected/actual
values, row counts, failure_pct.

Persist to `control.dq_results` via the same INSERT pattern as match_bronze_dq.

Add `task_run_dq` wrapper in `build_match_silver.py` that calls
`MatchSilverDQChecker.from_settings().run_all(...)` — raises DQBlockingFailureError
on BLOCK failure, which fails the Airflow task.

## Tests

For Phase 5:
- `tests/unit/ingestion/jobs/test_build_match_silver.py` — mock the 4 transforms,
  verify idempotency check, log row inserted then updated, force=True bypasses, 
  failure path updates error_message and re-raises

For Phase 6:
- `tests/unit/quality/test_match_silver_dq.py` — one TestClass per check, mock
  the reader to return synthetic DataFrames, verify each PASSED/FAILED/SKIPPED
  scenario as per match_bronze_dq tests.

## Final verification

After implementation:

```bash
# 1. Apply new control DDL
docker exec compose-postgres-1 psql -U cricket_user -d cricket_platform -f - < infra/bootstrap/init-metastore.sql

# 2. Lint
poetry run ruff check --fix --exclude .claude/ .
poetry run black --exclude .claude/ .

# 3. All tests green
poetry run pytest --tb=no -q

# 4. DAG validates
make dag-validate
```

## Strict rules

- Reuse DQ types from register_dq — do NOT duplicate
- Idempotency via control.silver_match_build_log — never assume re-runs are free
- XCom payloads = primitives only
- DAG callables: thin, no business logic
```
