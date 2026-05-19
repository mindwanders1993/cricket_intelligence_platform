# src/cip/ingestion/audit/match_file_audit.py
#
# Thin psycopg2 wrapper over control.match_file_audit — the per-file
# (file_name, content_hash) audit log that drives:
#
#   1. Bronze skip-on-duplicate. Daily incremental DAG queries
#      lookup_seen() before loading; byte-identical re-arrivals
#      (Cricsheet's 2-day overlap) cost zero Bronze rows.
#
#   2. Silver incremental scoping. Silver job calls
#      pending_silver_match_ids() to scope DELETE+INSERT to changed
#      matches only.
#
#   3. Gold incremental scoping. Gold dbt incremental models filter
#      WHERE match_id IN (SELECT match_id FROM control.match_file_audit
#                          WHERE gold_loaded_at IS NULL).
#
# PK is (file_name, content_hash) — the same byte-identical file seen
# via the full archive and the incremental archive collapses to one row.
#
# Connection style mirrors MatchDataDownloader: one psycopg2 connection
# per public method call, autocommit on the with-block exit.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cip.common.logging import get_logger

logger = get_logger(__name__)


# ===========================================================================
# Public dataclass for landing-stage inserts
# ===========================================================================


@dataclass(frozen=True)
class AuditRow:
    """One row to be inserted at the landing stage (extract task)."""

    file_name: str
    content_hash: str
    match_id: str
    archive_file: str
    landing_path: str
    loaded_by_pipeline: str  # 'full' | 'incremental' | 'bootstrap'
    pipeline_run_id: str
    landing_loaded_at: datetime
    archive_download_id: int | None = None
    file_type: str = "json"


# ===========================================================================
# MatchFileAudit
# ===========================================================================


class MatchFileAudit:
    """
    psycopg2 wrapper over control.match_file_audit.

    Usage:
        audit = MatchFileAudit.from_settings()
        seen = audit.lookup_seen({"abc123...", "def456..."})
        # → set of content_hashes already in the audit log

        audit.mark_silver_loaded(["12345", "67890"], ts=datetime.utcnow())
    """

    def __init__(self, pg_dsn: str) -> None:
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "MatchFileAudit":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(pg_dsn=pg_dsn)

    # -------------------------------------------------------------------------
    # Bronze stage — skip lookup + landing insert + bronze mark + archive mark
    # -------------------------------------------------------------------------

    def lookup_bronze_loaded(self, content_hashes: set[str]) -> set[str]:
        """Return the subset of content_hashes whose audit row has bronze_loaded_at set.

        Used by the Bronze loader to drop already-Bronze-loaded files BEFORE
        write. Extract.py stamps landing_loaded_at on every file as it arrives,
        so a content_hash existing in the audit log does NOT imply it's already
        in Bronze — we must filter by bronze_loaded_at IS NOT NULL.
        """
        if not content_hashes:
            return set()

        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT content_hash
                    FROM control.match_file_audit
                    WHERE content_hash = ANY(%s)
                      AND bronze_loaded_at IS NOT NULL
                    """,
                    (list(content_hashes),),
                )
                return {row[0] for row in cur.fetchall()}

    def insert_landing(self, rows: list[AuditRow]) -> int:
        """Insert one audit row per file at the landing stage.

        ON CONFLICT (file_name, content_hash) DO NOTHING — safe to re-run
        an extract that partially populated the audit log.

        Returns the number of rows actually inserted (conflicts skipped).
        """
        if not rows:
            return 0

        import psycopg2
        from psycopg2.extras import execute_values

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                values = [
                    (
                        r.file_name,
                        r.content_hash,
                        r.match_id,
                        r.file_type,
                        r.archive_file,
                        r.archive_download_id,
                        r.landing_path,
                        r.loaded_by_pipeline,
                        r.landing_loaded_at,
                        r.pipeline_run_id,
                    )
                    for r in rows
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO control.match_file_audit (
                        file_name, content_hash, match_id, file_type,
                        archive_file, archive_download_id, landing_path,
                        loaded_by_pipeline, landing_loaded_at, pipeline_run_id
                    ) VALUES %s
                    ON CONFLICT (file_name, content_hash) DO NOTHING
                    """,
                    values,
                )
                inserted = cur.rowcount
            conn.commit()

        logger.info(
            "audit.insert_landing complete",
            extra={"requested": len(rows), "inserted": inserted},
        )
        return inserted

    def mark_bronze_loaded(
        self,
        rows: list[tuple[str, str, int]],
        pipeline_run_id: str,
        archive_file: str,
        ts: datetime,
    ) -> int:
        """Stamp bronze_loaded_at + revision after a successful Bronze append.

        `rows` is a list of (file_name, content_hash, revision) tuples — one
        entry per Bronze row written. Each (file_name, content_hash) pair maps
        to exactly one revision.

        Returns the number of audit rows updated.
        """
        if not rows:
            return 0

        import psycopg2

        file_names = [r[0] for r in rows]
        content_hashes = [r[1] for r in rows]
        revisions = [r[2] for r in rows]

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.match_file_audit AS a
                    SET bronze_loaded_at = %s,
                        revision         = v.revision,
                        archive_file     = %s,
                        pipeline_run_id  = %s
                    FROM (
                        SELECT *
                        FROM UNNEST(%s::text[], %s::text[], %s::int[])
                             AS t(file_name, content_hash, revision)
                    ) AS v
                    WHERE a.file_name    = v.file_name
                      AND a.content_hash = v.content_hash
                    """,
                    (ts, archive_file, pipeline_run_id, file_names, content_hashes, revisions),
                )
                updated = cur.rowcount
            conn.commit()

        logger.info(
            "audit.mark_bronze_loaded complete",
            extra={"requested": len(rows), "updated": updated, "ts": ts.isoformat()},
        )
        return updated

    def mark_archived(
        self,
        file_hash_to_archive_path: dict[tuple[str, str], str],
        ts: datetime,
    ) -> int:
        """Stamp archive_path + archived_at after copying each file to the archive prefix."""
        if not file_hash_to_archive_path:
            return 0

        import psycopg2

        file_names: list[str] = []
        content_hashes: list[str] = []
        archive_paths: list[str] = []
        for (file_name, content_hash), archive_path in file_hash_to_archive_path.items():
            file_names.append(file_name)
            content_hashes.append(content_hash)
            archive_paths.append(archive_path)

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.match_file_audit AS a
                    SET archive_path = v.archive_path,
                        archived_at  = %s
                    FROM (
                        SELECT *
                        FROM UNNEST(%s::text[], %s::text[], %s::text[])
                             AS t(file_name, content_hash, archive_path)
                    ) AS v
                    WHERE a.file_name    = v.file_name
                      AND a.content_hash = v.content_hash
                    """,
                    (ts, file_names, content_hashes, archive_paths),
                )
                updated = cur.rowcount
            conn.commit()

        logger.info(
            "audit.mark_archived complete",
            extra={"requested": len(file_names), "updated": updated, "ts": ts.isoformat()},
        )
        return updated

    # -------------------------------------------------------------------------
    # Silver stage
    # -------------------------------------------------------------------------

    def pending_silver_match_ids(self) -> list[str]:
        """Return match_ids whose latest Bronze content has not yet been written to Silver.

        For matches with multiple revisions, only one match_id is returned (DISTINCT).
        The Silver job is responsible for picking MAX(revision) per match_id when
        reading Bronze.
        """
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT match_id
                    FROM control.match_file_audit
                    WHERE bronze_loaded_at IS NOT NULL
                      AND silver_loaded_at IS NULL
                    ORDER BY match_id
                    """
                )
                return [row[0] for row in cur.fetchall()]

    def mark_silver_loaded(self, match_ids: list[str], ts: datetime) -> int:
        """Stamp silver_loaded_at for every audit row of these match_ids."""
        if not match_ids:
            return 0

        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.match_file_audit
                    SET silver_loaded_at = %s
                    WHERE match_id = ANY(%s)
                      AND bronze_loaded_at IS NOT NULL
                      AND silver_loaded_at IS NULL
                    """,
                    (ts, match_ids),
                )
                updated = cur.rowcount
            conn.commit()

        logger.info(
            "audit.mark_silver_loaded complete",
            extra={"match_ids": len(match_ids), "rows_updated": updated, "ts": ts.isoformat()},
        )
        return updated

    # -------------------------------------------------------------------------
    # Gold stage
    # -------------------------------------------------------------------------

    def pending_gold_match_ids(self) -> list[str]:
        """Return match_ids whose Silver row has not yet been reflected into Gold."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT match_id
                    FROM control.match_file_audit
                    WHERE silver_loaded_at IS NOT NULL
                      AND gold_loaded_at IS NULL
                    ORDER BY match_id
                    """
                )
                return [row[0] for row in cur.fetchall()]

    def mark_gold_loaded_pending(self, match_ids: list[str], ts: datetime) -> int:
        """Incremental Gold DAG: stamp gold_loaded_at for the pending match_ids."""
        if not match_ids:
            return 0

        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.match_file_audit
                    SET gold_loaded_at = %s
                    WHERE match_id = ANY(%s)
                      AND silver_loaded_at IS NOT NULL
                      AND gold_loaded_at IS NULL
                    """,
                    (ts, match_ids),
                )
                updated = cur.rowcount
            conn.commit()

        logger.info(
            "audit.mark_gold_loaded_pending complete",
            extra={"match_ids": len(match_ids), "rows_updated": updated, "ts": ts.isoformat()},
        )
        return updated

    def mark_gold_loaded_all_silver(self, ts: datetime) -> int:
        """Full-refresh Gold DAG: stamp gold_loaded_at for every silver-ready row.

        Use this after `dbt run --full-refresh` which rebuilds every dim/fact
        from scratch — every match_id with silver_loaded_at IS NOT NULL was
        included.
        """
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.match_file_audit
                    SET gold_loaded_at = %s
                    WHERE silver_loaded_at IS NOT NULL
                    """,
                    (ts,),
                )
                updated = cur.rowcount
            conn.commit()

        logger.info(
            "audit.mark_gold_loaded_all_silver complete",
            extra={"rows_updated": updated, "ts": ts.isoformat()},
        )
        return updated


# ===========================================================================
# Helpers
# ===========================================================================


def _match_id_from_filename(file_name: str) -> str:
    """Cricsheet match files are named '{match_id}.json'. Strip the suffix."""
    return file_name.removesuffix(".json")
