# 0005 — BigQuery as secondary OLAP target (alongside DuckDB)

- **Status:** Proposed (filled during Sprint 3)
- **Date of decision:** TBD (Sprint 3 of revamp v2)
- **Tags:** olap, cloud, sprint-3

---

## Context (placeholder)

DuckDB is the v1 primary OLAP engine — local, single-writer, zero ops. To prove the OSS↔Enterprise migration story (ADR 0004) and to demonstrate dimensional modeling at cloud-OLAP scale, we add **BigQuery free tier** as a parallel target. Same dbt models, target switch via `--target bq_dev`.

This ADR will be filled in during Sprint 3 of revamp v2 with:
- Why BigQuery over Snowflake / ClickHouse / Athena for the secondary target
- Sync strategy (Python script via PolarsIcebergReader → BQ load jobs vs. BigLake external tables on Iceberg)
- Cost discipline (free-tier monthly cap, sandbox mode, sync volume limits)
- Row-count parity contract between DuckDB and BigQuery

See `docs/planning.md` → Sprint 3 for the implementation plan.

---

## Decision drivers (placeholder — to be filled Sprint 3)

- Harness JD lists BigQuery / ClickHouse / StarRocks → must demonstrate cloud OLAP
- No funded AWS account → free-tier required
- ADR 0004 (open standards first) — sync should not require proprietary connectors
- BigQuery has BigLake support for Iceberg → cleanest cloud-Iceberg story

---

## References

- `docs/planning.md` → Sprint 3
- Plan file (personal scratch): `~/.claude/plans/hi-soft-prism.md`
- Related: [[0002-use-apache-iceberg]], [[0004-open-standards-first]]
