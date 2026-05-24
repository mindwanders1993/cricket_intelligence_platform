# 0002 — Use Apache Iceberg as the primary table format

- **Status:** Accepted
- **Date of decision:** 2026-02 (Phase 1)
- **Deciders:** Biswajit Brahmma
- **Tags:** storage, lakehouse, sprint-0-docs

---

## Context

The platform is built as a medallion lakehouse (Source → Bronze → Silver → Gold) on object storage. We need a **table format** layered on top of Parquet to provide:

- Atomic snapshot commits across multi-file writes.
- Multi-engine reads (Polars, PySpark, DuckDB, BigQuery, Athena, Spark on EMR).
- Schema evolution without rewriting historical data.
- Partition evolution.
- Time travel for reproducibility + debugging.
- A managed AWS equivalent so cloud migration is config, not rewrite.

There are three serious table-format options in 2026: **Iceberg**, **Delta Lake**, **Apache Hudi**. There's also "no table format — plain Parquet + Hive directory structure", which is what most hobby projects do.

---

## Decision drivers

- **Multi-engine reads** — Polars (ingestion), PySpark (Silver), DuckDB (serving), BigQuery (cloud), Athena (cloud) must all read the same tables.
- **Open spec + governance** — must be Apache-foundation governed, not vendor-controlled.
- **AWS-native path** — must have first-class AWS support (Glue catalog, S3 Tables, Athena, EMR).
- **Snapshot isolation + time travel** — re-runs must not race readers; debugging requires "what did this look like yesterday".
- **Schema + partition evolution** — Cricsheet has added new `key_*` columns over time; partition spec might change.
- **Python tooling** — must have a stable PyIceberg/PyArrow path; we can't use a JVM-only format from Polars.
- **Streaming-ready (future)** — if streaming re-opens (deferred), table format must support concurrent batch + stream writes.

---

## Considered options

### Option A — Apache Iceberg

Apache-governed open spec. PyIceberg client (Python, no JVM). Iceberg REST catalog (open spec). First-class support on AWS Glue, S3 Tables, Snowflake, Databricks, Athena, BigQuery (via external tables). Strong streaming story (`MERGE INTO`, row-level deletes, write-audit-publish).

- **Pros:** Apache-foundation neutral; PyIceberg works without JVM (critical for Polars-based Bronze); REST catalog has an open spec so the catalog is swappable; full AWS-native; Snowflake reads Iceberg directly (key for the "BigQuery + Snowflake portability" story); strong multi-engine ecosystem; explicit snapshot commit semantics; widely deployed at large enterprises (Netflix, Apple, Adobe).
- **Cons:** PyIceberg 0.11.x has known schema-conversion quirks (handled via `_pyarrow_to_schema_without_ids` + `assign_fresh_schema_ids` in our writers); REST catalog adds an extra service to run locally; the spec is newer than Delta, so some tooling lags.

### Option B — Delta Lake

Originally Databricks-controlled; donated to Linux Foundation in 2022. Strong Databricks integration. delta-rs (Rust) gives a JVM-free Python path. Native support on Databricks, decent support on Athena (via Glue connector), via OneLake on Microsoft.

- **Pros:** mature; battle-tested at scale; delta-rs reduces JVM dependency; Databricks-native.
- **Cons:** still effectively Databricks-led despite the foundation move; AWS support is via Glue connectors rather than first-class S3 Tables; BigQuery requires manifest tables (more friction than Iceberg's BigLake support); doesn't fit the "open-standards first" narrative as cleanly as Iceberg.

### Option C — Apache Hudi

Apache-governed, originally Uber. Two table types (Copy-on-Write, Merge-on-Read) with different trade-offs. Strong streaming + CDC story.

- **Pros:** great for high-frequency upserts (CDC, streaming).
- **Cons:** more complex configuration (two table types); smaller multi-engine ecosystem than Iceberg in 2026; AWS support narrower; Python tooling less mature; not a strong fit for batch-shaped append-only Bronze.

### Option D — Plain Parquet + Hive partitioning

Just write Parquet files with `_snapshot_date=…` directory layout. Use Hive metastore (or no metastore — just scan the prefix).

- **Pros:** zero overhead; trivial to read with any engine.
- **Cons:** no atomic commits — partial-write races kill correctness; no schema evolution beyond add-column; no time travel; no snapshot isolation; not interview-defensible for a senior DE portfolio.

---

## Decision

We will use **Option A — Apache Iceberg** as the primary table format for Bronze, Silver, and Gold. Catalog: Iceberg REST locally (`tabulario/iceberg-rest`) → AWS Glue in cloud.

Writers use PyIceberg directly (`PolarsIcebergWriter`) for Polars-based Bronze + Register Silver. Heavy Match Silver uses PySpark via the Iceberg Spark runtime. Gold is materialised by dbt into DuckDB (local) and BigQuery (cloud) reading the Iceberg tables.

---

## Consequences

### Positive

- Multi-engine reads work natively — Polars, Spark, DuckDB (via the Iceberg extension), BigQuery (via BigLake), Athena all read the same tables.
- Snapshot commits are atomic — partial writes never become visible. Re-runs are safe (audit-driven dedup + Iceberg snapshot rollback if needed).
- Schema evolution is in-place — new `key_*` columns flow through Bronze without rewrites.
- Cloud migration is endpoint-only: change `ICEBERG_REST_URI` from `http://iceberg-rest:8181` to a Glue endpoint, and Spark/Polars writes go to S3 unchanged.
- Time travel enables debugging ("what did Silver look like before the bad Bronze load on 2026-05-10?").
- Strong interview talking point — Iceberg is what enterprises are migrating *to*.

### Negative / trade-offs

- PyIceberg version pinning matters; we depend on 0.11.x quirks (documented in `transform/shared/writers.py`).
- The Iceberg REST catalog is an additional always-on service locally (one more container).
- Concurrent writers to the same table need the audit dedup pattern to avoid duplicate `(match_id, revision)` rows. This is by design: append-only Bronze + dedup at read time.
- Spark JAR coordinates (`iceberg-spark-runtime-*`, `iceberg-aws-bundle-*`, `hadoop-aws-*`, `aws-java-sdk-bundle-*`) must match — managed via `_build_spark_iceberg_conf()`.
- DuckDB Iceberg extension currently requires `unsafe_enable_version_guessing` for the read path; we work around by materialising Bronze/Silver as native DuckDB tables (documented in `CLAUDE.md`).

### Neutral

- Naming is 2-segment FQN (`bronze.match_data`) — the catalog name `iceberg` is not in the FQN string. This is the convention everywhere in code (`TableName.bronze()` / `silver()` / `gold()`).

---

## Migration path / future swap

If a hypothetical future drives us off Iceberg:
- **To Delta Lake** — requires a one-time rewrite via cross-format readers (e.g., XTable / OneTable abstraction layers exist). Cost: weeks.
- **To proprietary Snowflake / BigQuery native tables** — defeats the open-format principle from ADR 0004; would only happen if vendor lock-in becomes desirable for some other reason.
- **To Hudi** — only if we re-open streaming and Hudi's CDC story dominates Iceberg's `MERGE INTO` for our workload.

Trigger to revisit: Iceberg loses governance neutrality (e.g., aggressive vendor capture), OR a streaming workload at scale where Hudi's RT table type dominates.

---

## References

- Code: `src/cip/transform/shared/writers.py`, `src/cip/transform/shared/readers.py`
- Catalog: `infra/compose/compose.base.yml` (iceberg-rest service)
- Related: [[0001-use-modular-monolith]] (writers live in one package); [[0004-open-standards-first]] (Iceberg is the canonical open-format example); [[0005-bigquery-as-secondary-target]] (Iceberg → BigLake external tables are the cloud path)
- External: [Apache Iceberg spec](https://iceberg.apache.org/spec/); PyIceberg 0.11.x release notes
