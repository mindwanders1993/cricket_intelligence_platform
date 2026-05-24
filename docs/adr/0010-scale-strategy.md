# 0010 — Scale strategy: 100M-row synthetic + dual-engine perf write-up

- **Status:** Proposed (filled during Sprint 4)
- **Date of decision:** TBD (Sprint 4 of revamp v2)
- **Tags:** performance, scale, sprint-4

---

## Context (placeholder)

The real Cricsheet dataset has ~21k matches and ~11M deliveries — a few-GB-scale dataset. Senior DE roles ask about petabyte-scale work; "I worked on ~10GB" doesn't survive deep probing. We need a defensible scale story without faking the data domain.

**Strategy:** generate a synthetic 100M+ row delivery table with realistic cardinality (10k batters, 5k bowlers, 50 venues, year+month partitions), then document partition pruning + clustering wins on DuckDB and BigQuery with EXPLAIN ANALYZE deltas.

This ADR will be filled during Sprint 4 with:
- Generator design (deterministic seeds, realistic distributions, Iceberg writes via existing `PolarsIcebergWriter`)
- Partition strategy (`year`, `month` partition spec)
- BigQuery clustering choice
- Before/after EXPLAIN ANALYZE methodology
- What we measured + what we learned

See `docs/planning.md` → Sprint 4.

---

## Decision drivers (placeholder)

- Defensible scale story without inventing fake real-world data
- Demonstrates partition pruning + clustering literacy
- Both DuckDB + BigQuery numbers (twin-OLAP narrative from ADR 0005)

---

## References

- `docs/planning.md` → Sprint 4
- `scripts/synth/generate_synthetic_deliveries.py` (planned)
- `docs/perf/scale_test.md` (planned)
- Related: [[0002-use-apache-iceberg]], [[0005-bigquery-as-secondary-target]]
