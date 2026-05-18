-- infra/bootstrap/init-metastore.sql
--
-- Bootstrap the PostgreSQL control schema for the Cricket Intelligence Platform.
-- Idempotent — safe to re-run (all DDL uses IF NOT EXISTS).
--
-- Called by:   make bootstrap-db
-- Connects to: cricket_platform DB (see .env → POSTGRES_DB)
--
-- Schema layout:
--   control.*   — pipeline run metadata, DQ results, schema drift tracking
-- ============================================================================

-- ============================================================================
-- SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS control;

-- ============================================================================
-- ENUM TYPES
-- ============================================================================

DO $$ BEGIN
    CREATE TYPE control.pipeline_status AS ENUM (
        'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE control.dq_severity AS ENUM (
        'BLOCK', 'WARN', 'ALERT', 'LOG'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE control.dq_status AS ENUM (
        'PASSED', 'FAILED', 'WARNING', 'SKIPPED'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE control.pipeline_layer AS ENUM (
        'LANDING', 'BRONZE', 'SILVER', 'GOLD'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================================
-- TABLE: control.register_ingestion_log
--
-- One row per file per pipeline run.
-- Tracks every download attempt of people.csv and names.csv from Cricsheet.
-- Used by the idempotency guard in Bronze load to skip already-processed
-- snapshots.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.register_ingestion_log (
    id                  BIGSERIAL PRIMARY KEY,

    -- Run identity
    pipeline_run_id     TEXT        NOT NULL,               -- Airflow run_id or manual UUID
    dag_id              TEXT        NOT NULL DEFAULT 'dag_ingest_cricsheet_register',

    -- Source file metadata
    source_file         TEXT        NOT NULL,               -- 'people.csv' | 'names.csv'
    source_url          TEXT        NOT NULL,               -- Full download URL
    snapshot_date       DATE        NOT NULL,               -- Date of this snapshot (partition key)

    -- File characteristics
    file_size_bytes     BIGINT,
    row_count           INTEGER,
    checksum_sha256     TEXT,

    -- Landing path written to
    landing_path        TEXT,                               -- s3://cricket-source-files/people_and_names/csv/snapshot_date=.../

    -- Bronze load tracking
    bronze_loaded       BOOLEAN     NOT NULL DEFAULT FALSE,
    bronze_loaded_at    TIMESTAMPTZ,
    bronze_table        TEXT,                               -- 'bronze.register_people_raw' | 'bronze.register_names_raw'
    bronze_row_count    INTEGER,

    -- Run status and timing
    status              control.pipeline_status NOT NULL DEFAULT 'RUNNING',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,

    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_register_ingestion_log_snapshot_date
    ON control.register_ingestion_log (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_register_ingestion_log_source_file_snapshot
    ON control.register_ingestion_log (source_file, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_register_ingestion_log_checksum
    ON control.register_ingestion_log (checksum_sha256);

COMMENT ON TABLE control.register_ingestion_log IS
    'Per-file landing ingestion log for the Cricsheet Register pipeline. '
    'Used for idempotency checks before Bronze load.';

-- ============================================================================
-- TABLE: control.register_schema_versions
--
-- Tracks the column fingerprint of people.csv at each snapshot.
-- When new key_* columns are added by Cricsheet, this table records the drift
-- so downstream Silver unpivot logic can handle it gracefully.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.register_schema_versions (
    id                  BIGSERIAL PRIMARY KEY,

    -- Identity
    pipeline_run_id     TEXT        NOT NULL,
    source_file         TEXT        NOT NULL,               -- 'people.csv' | 'names.csv'
    snapshot_date       DATE        NOT NULL,

    -- Schema fingerprint
    column_names        TEXT[]      NOT NULL,               -- Ordered list of column names
    column_count        INTEGER     NOT NULL,
    key_columns         TEXT[]      NOT NULL DEFAULT '{}',  -- Subset of key_* columns only
    schema_hash         TEXT        NOT NULL,               -- SHA256 of sorted column names

    -- Drift detection
    is_schema_changed   BOOLEAN     NOT NULL DEFAULT FALSE,
    new_columns         TEXT[]      NOT NULL DEFAULT '{}',  -- Columns added vs previous snapshot
    removed_columns     TEXT[]      NOT NULL DEFAULT '{}',  -- Columns removed vs previous snapshot
    previous_schema_id  BIGINT      REFERENCES control.register_schema_versions(id),

    -- Audit
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_register_schema_versions_file_snapshot
    ON control.register_schema_versions (source_file, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_register_schema_versions_snapshot_date
    ON control.register_schema_versions (snapshot_date DESC);

COMMENT ON TABLE control.register_schema_versions IS
    'Column fingerprint history for Cricsheet Register CSV files. '
    'Detects schema drift (new key_* columns) between snapshots.';

-- ============================================================================
-- TABLE: control.dq_results
--
-- Central DQ result store. Every check from every layer writes here.
-- Severity drives pipeline branching: BLOCK halts, WARN logs, LOG is audit.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.dq_results (
    id                  BIGSERIAL PRIMARY KEY,

    -- Run context
    pipeline_run_id     TEXT        NOT NULL,
    dag_id              TEXT        NOT NULL,
    task_id             TEXT,

    -- Check identity
    check_id            TEXT        NOT NULL,               -- e.g. 'LND-001', 'BRZ-003'
    check_name          TEXT        NOT NULL,               -- Human-readable name
    layer               control.pipeline_layer NOT NULL,
    source_file         TEXT,                               -- 'people.csv' | 'names.csv' | NULL for table checks
    table_name          TEXT,                               -- Iceberg table checked (if applicable)

    -- Check result
    status              control.dq_status NOT NULL,
    severity            control.dq_severity NOT NULL,
    expected_value      TEXT,                               -- What we expected (as string for flexibility)
    actual_value        TEXT,                               -- What we observed
    row_count_checked   BIGINT,
    failure_row_count   BIGINT,
    failure_pct         NUMERIC(6,4),                       -- 0.0000–100.0000

    -- Detail payload (for GE result JSON etc.)
    detail_json         JSONB,

    -- Timing
    snapshot_date       DATE,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_pipeline_run
    ON control.dq_results (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_dq_results_check_status
    ON control.dq_results (check_id, status);

CREATE INDEX IF NOT EXISTS idx_dq_results_layer_snapshot
    ON control.dq_results (layer, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_dq_results_severity_status
    ON control.dq_results (severity, status)
    WHERE status = 'FAILED';

COMMENT ON TABLE control.dq_results IS
    'Central DQ result store for all pipeline layers. '
    'BLOCK failures halt the pipeline; WARN/LOG are recorded for observability.';

-- ============================================================================
-- TABLE: control.register_change_log
--
-- Tracks delta row counts between consecutive Register snapshots.
-- Flags unexpected spikes or drops in people.csv / names.csv row counts.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.register_change_log (
    id                      BIGSERIAL PRIMARY KEY,

    pipeline_run_id         TEXT        NOT NULL,
    source_file             TEXT        NOT NULL,
    snapshot_date           DATE        NOT NULL,

    -- Row count metrics
    current_row_count       INTEGER     NOT NULL,
    previous_row_count      INTEGER,                        -- NULL on first load
    delta_rows              INTEGER,                        -- current - previous
    delta_pct               NUMERIC(8,4),                   -- (delta / previous) * 100

    -- Identity metrics (people.csv only)
    new_identifiers         INTEGER     DEFAULT 0,          -- identifiers added since last snapshot
    removed_identifiers     INTEGER     DEFAULT 0,          -- identifiers removed since last snapshot
    changed_identifiers     INTEGER     DEFAULT 0,          -- rows with same id but different content

    -- Flag anomalies
    is_anomaly              BOOLEAN     NOT NULL DEFAULT FALSE,
    anomaly_reason          TEXT,

    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_register_change_log_file_snapshot
    ON control.register_change_log (source_file, snapshot_date);

COMMENT ON TABLE control.register_change_log IS
    'Delta tracking between consecutive Register snapshots. '
    'Flags unexpected row count changes for observability.';

-- ============================================================================
-- TRIGGER: auto-update updated_at on register_ingestion_log
-- ============================================================================

CREATE OR REPLACE FUNCTION control.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_register_ingestion_log_updated_at
    ON control.register_ingestion_log;

CREATE TRIGGER trg_register_ingestion_log_updated_at
    BEFORE UPDATE ON control.register_ingestion_log
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- ============================================================================
-- VIEWS: operational convenience queries
-- ============================================================================

CREATE OR REPLACE VIEW control.v_latest_register_snapshot AS
SELECT DISTINCT ON (source_file)
    source_file,
    snapshot_date           AS latest_snapshot,
    completed_at            AS last_completed_at,
    row_count,
    status::TEXT            AS last_status
FROM control.register_ingestion_log
WHERE status = 'SUCCESS'
ORDER BY source_file, snapshot_date DESC, completed_at DESC;

COMMENT ON VIEW control.v_latest_register_snapshot IS
    'Quick view of the most recent successful ingestion per Register file.';

CREATE OR REPLACE VIEW control.v_dq_failures AS
SELECT
    pipeline_run_id,
    dag_id,
    layer::TEXT,
    check_id,
    check_name,
    severity::TEXT,
    status::TEXT,
    actual_value,
    failure_pct,
    checked_at
FROM control.dq_results
WHERE status IN ('FAILED', 'WARNING')
ORDER BY checked_at DESC;

COMMENT ON VIEW control.v_dq_failures IS
    'All DQ failures and warnings, ordered newest first.';

-- ============================================================================
-- TABLE: control.archive_download_log
--
-- One row per archive ZIP file per pipeline run.
-- Tracks every download attempt of Cricsheet match archive ZIPs
-- (e.g. all_matches.zip, odis.zip, tests.zip, t20s.zip).
-- Used by the idempotency guard in extract/Bronze tasks to skip
-- already-processed archive runs.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.archive_download_log (
    id                  BIGSERIAL PRIMARY KEY,

    -- Run identity
    pipeline_run_id     TEXT        NOT NULL,               -- Airflow run_id or manual UUID
    dag_id              TEXT        NOT NULL DEFAULT 'dag_ingest_cricsheet_archives',

    -- Source archive metadata
    archive_file        TEXT        NOT NULL,               -- 'all_matches.zip' | 'odis.zip' | etc.
    source_url          TEXT        NOT NULL,               -- Full download URL
    snapshot_date       DATE        NOT NULL,               -- Date of this snapshot (partition key)

    -- File characteristics
    file_size_bytes     BIGINT,
    checksum_sha256     TEXT,

    -- Landing path written to MinIO
    landing_path        TEXT,                               -- s3://cricket-source-files/match_data/zip/snapshot_date=.../

    -- Extraction tracking
    extracted_path      TEXT,                               -- s3://cricket-source-files/match_data/json/snapshot_date=.../
    file_count          INTEGER,                            -- Number of JSON files extracted from ZIP

    -- Run status and timing
    status              control.pipeline_status NOT NULL DEFAULT 'RUNNING',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,

    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_archive_download_log_snapshot_date
    ON control.archive_download_log (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_archive_download_log_archive_snapshot
    ON control.archive_download_log (archive_file, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_archive_download_log_checksum
    ON control.archive_download_log (checksum_sha256);

COMMENT ON TABLE control.archive_download_log IS
    'Per-archive landing ingestion log for the Cricsheet match archive pipeline. '
    'Used for idempotency checks before Bronze extraction and load.';

DROP TRIGGER IF EXISTS trg_archive_download_log_updated_at
    ON control.archive_download_log;

CREATE TRIGGER trg_archive_download_log_updated_at
    BEFORE UPDATE ON control.archive_download_log
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- ============================================================================
-- TABLE: control.bronze_match_ingestion_log
--
-- One row per pipeline run per archive batch.
-- Tracks Bronze ingestion of match JSON documents into the
-- cricket.bronze.match_documents Iceberg table.
-- Used for idempotency (skip snapshot_date already loaded) and
-- observability (row counts, duration, errors).
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.bronze_match_ingestion_log (
    id                  BIGSERIAL PRIMARY KEY,

    -- Run identity
    pipeline_run_id     TEXT        NOT NULL,
    dag_id              TEXT        NOT NULL DEFAULT 'dag_ingest_cricsheet_archives',
    archive_download_id BIGINT      REFERENCES control.archive_download_log(id),

    -- Source metadata
    archive_file        TEXT        NOT NULL,               -- Source ZIP (e.g. 'all_matches.zip')
    snapshot_date       DATE        NOT NULL,               -- Partition key for Bronze table

    -- Load metrics
    files_attempted     INTEGER     NOT NULL DEFAULT 0,     -- JSON files attempted
    files_succeeded     INTEGER     NOT NULL DEFAULT 0,     -- JSON files successfully parsed
    files_failed        INTEGER     NOT NULL DEFAULT 0,     -- JSON files that raised errors
    rows_written        BIGINT      NOT NULL DEFAULT 0,     -- Rows appended to Iceberg table
    bronze_table        TEXT        NOT NULL DEFAULT 'cricket.bronze.match_documents',

    -- Run status and timing
    status              control.pipeline_status NOT NULL DEFAULT 'RUNNING',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    duration_seconds    NUMERIC(10,3),
    error_message       TEXT,

    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_match_ingestion_log_snapshot_date
    ON control.bronze_match_ingestion_log (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_bronze_match_ingestion_log_pipeline_run
    ON control.bronze_match_ingestion_log (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_bronze_match_ingestion_log_archive_snapshot
    ON control.bronze_match_ingestion_log (archive_file, snapshot_date);

COMMENT ON TABLE control.bronze_match_ingestion_log IS
    'Per-run Bronze ingestion log for the Cricsheet match archive pipeline. '
    'Tracks JSON-to-Iceberg load metrics and idempotency state per snapshot_date.';

DROP TRIGGER IF EXISTS trg_bronze_match_ingestion_log_updated_at
    ON control.bronze_match_ingestion_log;

CREATE TRIGGER trg_bronze_match_ingestion_log_updated_at
    BEFORE UPDATE ON control.bronze_match_ingestion_log
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- ============================================================================
-- VIEW: control.v_latest_archive_snapshot
-- ============================================================================

CREATE OR REPLACE VIEW control.v_latest_archive_snapshot AS
SELECT
    archive_file,
    MAX(snapshot_date)      AS latest_snapshot,
    MAX(completed_at)       AS last_completed_at,
    SUM(file_count)         AS file_count,
    MAX(status::TEXT)       AS last_status
FROM control.archive_download_log
WHERE status = 'SUCCESS'
GROUP BY archive_file;

COMMENT ON VIEW control.v_latest_archive_snapshot IS
    'Quick view of the most recent successful download per Cricsheet archive ZIP.';

-- ============================================================================
-- TABLE: control.match_file_audit
--
-- Per-file (file_name, content_hash) audit log spanning the full
-- match-data pipeline: landing → bronze → archive → silver → gold.
--
-- The single source of truth for whether a given JSON blob has been
-- processed at each layer. Drives:
--   1. Bronze skip-on-duplicate (filename + content_hash lookup) so the
--      daily incremental DAG's 2-day overlap costs zero Bronze rows.
--   2. Silver incremental scoping (pending_silver_match_ids → DELETE+INSERT
--      per match_id; no MERGE INTO).
--   3. Gold incremental scoping (pending_gold_match_ids → dbt incremental
--      models filter via WHERE match_id IN (...)).
--
-- PK is (file_name, content_hash) — *not* archive_file — so the same
-- byte-identical match seen via both archives (full + incremental) collapses
-- to one audit row.
-- ============================================================================

CREATE TABLE IF NOT EXISTS control.match_file_audit (
    file_name              TEXT        NOT NULL,
    content_hash           TEXT        NOT NULL,            -- sha256 hex of JSON bytes
    match_id               TEXT        NOT NULL,            -- file stem (Cricsheet ID)
    file_type              TEXT        NOT NULL DEFAULT 'json',

    -- Provenance
    archive_file           TEXT        NOT NULL,            -- 'all_json.zip' | 'recently_added_2_json.zip' | 'bootstrap'
    archive_download_id    BIGINT      REFERENCES control.archive_download_log(id),
    landing_path           TEXT        NOT NULL,            -- s3://… path of the landing object (per-archive prefix)
    archive_path           TEXT,                            -- s3://… path under match_data/archive/… (NULL until copied)
    loaded_by_pipeline     TEXT        NOT NULL,            -- 'full' | 'incremental' | 'bootstrap'

    -- 5-stage status (TIMESTAMPTZ = when reached that stage; NULL = not yet)
    landing_loaded_at      TIMESTAMPTZ NOT NULL,
    bronze_loaded_at       TIMESTAMPTZ,
    archived_at            TIMESTAMPTZ,
    silver_loaded_at       TIMESTAMPTZ,
    gold_loaded_at         TIMESTAMPTZ,

    -- Bronze revision linkage
    revision               INT,
    pipeline_run_id        TEXT        NOT NULL,

    -- Audit
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (file_name, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_match_file_audit_match_id
    ON control.match_file_audit (match_id);

CREATE INDEX IF NOT EXISTS idx_match_file_audit_silver_pending
    ON control.match_file_audit (match_id)
    WHERE silver_loaded_at IS NULL AND bronze_loaded_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_match_file_audit_gold_pending
    ON control.match_file_audit (match_id)
    WHERE gold_loaded_at IS NULL AND silver_loaded_at IS NOT NULL;

COMMENT ON TABLE control.match_file_audit IS
    'Per-file 5-stage audit log for the match-data pipeline. '
    'Drives Bronze skip-on-duplicate, incremental Silver scoping, and incremental Gold scoping. '
    'PK (file_name, content_hash) collapses identical files seen via multiple archives.';

DROP TRIGGER IF EXISTS trg_match_file_audit_updated_at
    ON control.match_file_audit;

CREATE TRIGGER trg_match_file_audit_updated_at
    BEFORE UPDATE ON control.match_file_audit
    FOR EACH ROW EXECUTE FUNCTION control.set_updated_at();

-- ============================================================================
-- DONE
-- ============================================================================
SELECT 'control schema bootstrap complete' AS result;
