# Stage D — Phase 3 + 4 prompt (Claude Code)

Continue the Claude session from Stage C (or open fresh) and paste:

---

```
Big Task 5 — Phase 3 (participants) + Phase 4 (identity resolution).

Read first:
- `docs/silver_match_spec/spec.md` sections 2 (match_players, match_officials,
  match_registry, unmatched_persons_audit), 3 (relevant edge cases), 4
  (identity algorithm)
- `src/cip/transform/polars/silver/persons.py` (how silver.persons and
  silver.name_variations are structured)
- `src/cip/transform/spark/silver/_shared.py`, `teams.py` (patterns established)

## Phase 3 — Participants

### File: `src/cip/transform/spark/silver/match_players.py`

`MatchPlayersTransform.run(snapshot_date, pipeline_run_id)`:
- Read Bronze, parse `parsed.info.players` (struct with team-named fields)
- Convert struct to array of (team, players[]) pairs — use `array(*[struct(...)])`
  or `map_from_entries` based on what works cleanly
- Explode to one row per (match_id, team, player_name)
- Columns: match_id, team, player_name, batting_order (1-based position in array),
  is_substitute (bool — true if name appears in supersubs)
- person_id: NULL for now (Phase 4 will populate)
- Add silver_meta_columns
- Write to `cricket.silver.match_players`, partition_cols=["_snapshot_date"]

### File: `src/cip/transform/spark/silver/match_officials.py`

`MatchOfficialsTransform.run(...)`:
- Read Bronze, parse `parsed.info.officials` (struct with sub-arrays:
  match_referees[], reserve_umpires[], tv_umpires[], umpires[])
- Stack/union all 4 sub-arrays with a `role` column
- One row per (match_id, role, person_name)
- person_id: NULL for now
- Add silver_meta_columns
- Write to `cricket.silver.match_officials`

## Phase 4 — Identity Resolution

### File: `src/cip/transform/spark/silver/match_registry.py`

`MatchRegistryTransform.run(...)`:
- Read Bronze, parse `parsed.info.registry.people` — this is a map<string, string>
  where key=display_name, value=cricsheet_id
- Convert map to array of (display_name, cricsheet_id) via `map_entries` + explode
- One row per (match_id, display_name, cricsheet_id)
- cricsheet_id may be NULL (cricsheet doesn't know the person)
- Add silver_meta_columns
- Write to `cricket.silver.match_registry`

### File: `src/cip/transform/spark/silver/identity_resolution.py`

`IdentityResolver.run(snapshot_date, pipeline_run_id)`:

Implements the spec section 4 algorithm. Reads from Silver tables already written
in earlier phases:

```python
match_players = read silver.match_players for this snapshot
match_officials = read silver.match_officials for this snapshot
match_registry = read silver.match_registry for this snapshot
silver_persons = read silver.persons (no snapshot filter — these are SCD2 current)
silver_name_variations = read silver.name_variations (current only)
```

Resolution flow (apply to match_players, then match_officials):

```
1. LEFT JOIN to match_registry on (match_id, display_name == person_name)
   → adds cricsheet_id

2. LEFT JOIN to silver_persons on cricsheet_id == silver_persons.cricsheet_id
   → adds person_id (call it pid_from_registry)

3. LEFT JOIN to silver_name_variations on lower(person_name) == lower(variation_name)
   AND _is_current = true
   → adds pid_from_name (the silver_name_variations.person_id)

4. resolved_person_id = COALESCE(pid_from_registry, pid_from_name)

5. resolution_method = 
     CASE 
       WHEN pid_from_registry IS NOT NULL THEN 'registry_cricsheet_id'
       WHEN pid_from_name IS NOT NULL THEN 'name_variation_fallback'
       ELSE NULL
     END

6. Rows where resolved_person_id IS NULL → write to silver.unmatched_persons_audit
   with columns: match_id, table_source ('match_players' or 'match_officials'),
   person_name, role_or_team, registry_present (bool), cricsheet_id_in_registry,
   _snapshot_date, _ingested_at, _pipeline_run_id

7. Rows with resolved_person_id (all rows, matched + unmatched) → overwrite
   silver.match_players and silver.match_officials with person_id populated.
   Use SparkIcebergWriter.dynamic_overwrite on the SAME partition. Do NOT drop
   unmatched rows — keep with person_id=NULL.
```

### File: `src/cip/transform/spark/silver/__init__.py` (update)

Export the 6 new classes (plus the 4 from Phase 2) so callers can do
`from cip.transform.spark.silver import MatchesTransform, ...`.

## Tests

For each of the 4 new transforms + IdentityResolver, write 
`tests/unit/transform/spark/silver/test_<name>.py`:

- match_players: happy path, empty players, supersubs, batting order
- match_officials: all 4 role types, empty officials, missing role
- match_registry: cricsheet_id present, cricsheet_id null, empty registry,
  missing registry block
- identity_resolution:
  - registry path success (cricsheet_id → silver.persons hit)
  - fallback path success (name_variations hit)
  - unmatched (no registry, no name match) → row in audit
  - person_name with different case → still matches via lower()
  - empty silver.persons → all rows unmatched
  - mixed scenario: some resolved, some unmatched

## Strict rules

- Use TableName.silver() / TableName.bronze() — never raw FQN
- Use META.* — never literal column names
- person_id stays NULL when unresolved — never drop rows
- All writes via SparkIcebergWriter.dynamic_overwrite with 
  partition_cols=["_snapshot_date"]
- Add `unmatched_persons_audit` and `match_registry` to
  `TableName.SILVER_TABLES` in naming.py if not already there
- Update CLAUDE.md "Known tables" / Silver list if new entries

After implementing, run:
- poetry run ruff check --fix src/cip/transform/spark/silver/
- poetry run black src/cip/transform/spark/silver/
- poetry run pytest tests/unit/transform/spark/silver/ -v
```
