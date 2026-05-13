# src/cip/quality/checks/match_bronze_dq.py
#
# Bronze DQ checks for the Cricsheet match archive pipeline.
#
# Checks (run after task_load_bronze):
#   MAT-BRZ-001  files_failed == 0 in bronze_match_ingestion_log             BLOCK
#   MAT-BRZ-002  (match_id, revision) unique per snapshot                     BLOCK
#   MAT-BRZ-003  Bronze row count == manifest file_count                      BLOCK
#   MAT-BRZ-004  metadata coverage — match_type/gender/team_a/team_b null ≤1% WARN
#
# Results are persisted to control.dq_results.
# BLOCK failures raise DQBlockingFailureError after persisting.

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger
from cip.quality.checks.register_dq import (
    DQBlockingFailureError,
    DQCheckResult,
    DQRunSummary,
    _pct,
)

if TYPE_CHECKING:
    from cip.transform.shared.readers import PolarsIcebergReader

logger = get_logger(__name__)

_BRONZE_TABLE = TableName.bronze("match_documents")
_DAG_ID = "dag_ingest_cricsheet_archives"
_TASK_ID = "run_dq"
_ARCHIVE_FILE = "all_json.zip"
_NULL_THRESHOLD_PCT = 1.0  # MAT-BRZ-004: warn if > 1% nulls in metadata cols


class MatchBronzeDQChecker:
    """
    Runs all Bronze DQ checks for the match archive pipeline.

    Usage:
        checker = MatchBronzeDQChecker.from_settings()
        summary = checker.run_all(snapshot_date="2026-05-01", pipeline_run_id="run-xyz")
    """

    def __init__(self, reader: "PolarsIcebergReader", pg_dsn: str) -> None:
        self._reader = reader
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "MatchBronzeDQChecker":
        from cip.common.settings import get_settings
        from cip.transform.shared.readers import PolarsIcebergReader

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(reader=PolarsIcebergReader.from_settings(), pg_dsn=pg_dsn)

    def run_all(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        dag_id: str = _DAG_ID,
    ) -> DQRunSummary:
        """
        Run all match Bronze DQ checks for the given snapshot_date.

        Reads Bronze Iceberg table, control.bronze_match_ingestion_log, and
        the extraction manifest. Persists results to control.dq_results.
        Raises DQBlockingFailureError if any BLOCK check failed.
        """
        from cip.common.exceptions import TableNotFoundError

        logger.info(
            "MatchBronzeDQChecker.run_all started",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        snap_filter = f"{META.SNAPSHOT_DATE} = '{snapshot_date}'"

        try:
            bronze_df = self._reader.read_table(_BRONZE_TABLE, row_filter=snap_filter)
        except TableNotFoundError as exc:
            logger.error(
                "Bronze table not found — cannot run DQ checks",
                extra={"error": str(exc), "snapshot_date": snapshot_date},
            )
            raise

        ingestion_log = self._get_ingestion_log(snapshot_date)
        manifest = self._get_manifest(snapshot_date)

        results: list[DQCheckResult] = [
            self._check_files_failed(ingestion_log),
            self._check_unique_grain(bronze_df),
            self._check_row_count_vs_manifest(bronze_df, manifest),
            self._check_metadata_coverage(bronze_df),
        ]

        self._persist_results(results, snapshot_date, pipeline_run_id, dag_id)

        summary = DQRunSummary(
            checks=results,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
        )

        logger.info(
            "MatchBronzeDQChecker.run_all complete",
            extra={
                "snapshot_date": snapshot_date,
                "total_checks": len(results),
                "passed": summary.passed_count,
                "failed_or_warned": summary.failed_count,
                "blocking_failures": len(summary.blocking_failures),
            },
        )

        if summary.has_blocking_failures:
            raise DQBlockingFailureError(summary.blocking_failures)

        return summary

    # -------------------------------------------------------------------------
    # Individual checks
    # -------------------------------------------------------------------------

    def _check_files_failed(self, ingestion_log: dict | None) -> DQCheckResult:
        """MAT-BRZ-001: No failed JSON files during Bronze load."""
        if ingestion_log is None:
            return DQCheckResult(
                check_id="MAT-BRZ-001",
                check_name="bronze.match_documents — files_failed == 0",
                layer="BRONZE",
                source_file=_ARCHIVE_FILE,
                table_name=_BRONZE_TABLE,
                status="SKIPPED",
                severity="BLOCK",
                expected_value="0 failed files",
                actual_value="No ingestion log found for this snapshot",
            )

        files_failed = ingestion_log.get("files_failed", 0) or 0
        status = "PASSED" if files_failed == 0 else "FAILED"
        return DQCheckResult(
            check_id="MAT-BRZ-001",
            check_name="bronze.match_documents — files_failed == 0",
            layer="BRONZE",
            source_file=_ARCHIVE_FILE,
            table_name=_BRONZE_TABLE,
            status=status,
            severity="BLOCK",
            expected_value="0 failed files",
            actual_value=f"{files_failed} failed files",
            row_count_checked=ingestion_log.get("files_attempted"),
            failure_row_count=files_failed,
        )

    def _check_unique_grain(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-BRZ-002: (match_id, revision) must be unique per snapshot."""
        total = df.height
        unique = df.select(["match_id", "revision"]).unique().height
        dup_count = total - unique
        status = "PASSED" if dup_count == 0 else "FAILED"
        return DQCheckResult(
            check_id="MAT-BRZ-002",
            check_name="bronze.match_documents — (match_id, revision) unique",
            layer="BRONZE",
            source_file=_ARCHIVE_FILE,
            table_name=_BRONZE_TABLE,
            status=status,
            severity="BLOCK",
            expected_value="0 duplicate (match_id, revision) pairs",
            actual_value=f"{dup_count} duplicate (match_id, revision) pairs",
            row_count_checked=total,
            failure_row_count=dup_count,
            failure_pct=_pct(dup_count, total),
        )

    def _check_row_count_vs_manifest(self, df: pl.DataFrame, manifest: dict | None) -> DQCheckResult:
        """MAT-BRZ-003: Bronze row count must equal manifest file_count."""
        bronze_rows = df.height

        if manifest is None:
            return DQCheckResult(
                check_id="MAT-BRZ-003",
                check_name="bronze.match_documents — row count == manifest file_count",
                layer="BRONZE",
                source_file=_ARCHIVE_FILE,
                table_name=_BRONZE_TABLE,
                status="SKIPPED",
                severity="BLOCK",
                expected_value="rows == manifest file_count",
                actual_value="Manifest not found for this snapshot",
                row_count_checked=bronze_rows,
            )

        expected = manifest.get("file_count", 0) or 0
        loss = abs(bronze_rows - expected)
        status = "PASSED" if bronze_rows == expected else "FAILED"
        return DQCheckResult(
            check_id="MAT-BRZ-003",
            check_name="bronze.match_documents — row count == manifest file_count",
            layer="BRONZE",
            source_file=_ARCHIVE_FILE,
            table_name=_BRONZE_TABLE,
            status=status,
            severity="BLOCK",
            expected_value=f"{expected} rows (from manifest)",
            actual_value=f"{bronze_rows} rows in Bronze",
            row_count_checked=expected,
            failure_row_count=loss if status == "FAILED" else 0,
            failure_pct=_pct(loss, expected) if status == "FAILED" and expected > 0 else 0.0,
        )

    def _check_metadata_coverage(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-BRZ-004: match_type / gender / team_a / team_b null rate <= 1%."""
        total = df.height
        check_cols = ["match_type", "gender", "team_a", "team_b"]
        available_cols = [c for c in check_cols if c in df.columns]

        if not available_cols or total == 0:
            return DQCheckResult(
                check_id="MAT-BRZ-004",
                check_name="bronze.match_documents — metadata coverage (match_type/gender/team_a/team_b)",
                layer="BRONZE",
                source_file=_ARCHIVE_FILE,
                table_name=_BRONZE_TABLE,
                status="SKIPPED",
                severity="WARN",
                actual_value="No data or metadata columns absent",
            )

        null_rows = df.filter(
            pl.any_horizontal([(pl.col(c).is_null() | pl.col(c).str.strip_chars().eq("")) for c in available_cols])
        ).height

        pct = _pct(null_rows, total)
        status = "PASSED" if pct <= _NULL_THRESHOLD_PCT else "WARNING"
        return DQCheckResult(
            check_id="MAT-BRZ-004",
            check_name="bronze.match_documents — metadata coverage (match_type/gender/team_a/team_b)",
            layer="BRONZE",
            source_file=_ARCHIVE_FILE,
            table_name=_BRONZE_TABLE,
            status=status,
            severity="WARN",
            expected_value=f"<= {_NULL_THRESHOLD_PCT}% rows with null/empty metadata fields",
            actual_value=f"{null_rows} rows ({pct:.2f}%) with at least one empty metadata field",
            row_count_checked=total,
            failure_row_count=null_rows,
            failure_pct=pct,
        )

    # -------------------------------------------------------------------------
    # Control DB helpers
    # -------------------------------------------------------------------------

    def _get_ingestion_log(self, snapshot_date: str) -> dict | None:
        """Return latest bronze_match_ingestion_log row for this snapshot as dict."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT files_attempted, files_succeeded, files_failed, rows_written
                    FROM control.bronze_match_ingestion_log
                    WHERE archive_file = %s AND snapshot_date = %s AND status = 'SUCCESS'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (_ARCHIVE_FILE, snapshot_date),
                )
                row = cur.fetchone()

        if row is None:
            return None
        return {
            "files_attempted": row[0],
            "files_succeeded": row[1],
            "files_failed": row[2],
            "rows_written": row[3],
        }

    def _get_manifest(self, snapshot_date: str) -> dict | None:
        """Return manifest as dict, or None if not found."""
        try:
            from cip.ingestion.cricsheet.manifest import manifest_object_key
            from cip.ingestion.io.minio import MinIOClient

            from cip.common.settings import get_settings

            cfg = get_settings().storage
            minio = MinIOClient.from_settings()
            key = manifest_object_key(snapshot_date)
            data = minio.read_bytes(cfg.bucket_landing, key)
            import json

            return json.loads(data)
        except Exception as exc:
            logger.warning(
                "Manifest not found — MAT-BRZ-003 will be SKIPPED",
                extra={"snapshot_date": snapshot_date, "error": str(exc)},
            )
            return None

    def _persist_results(
        self,
        results: list[DQCheckResult],
        snapshot_date: str,
        pipeline_run_id: str,
        dag_id: str,
    ) -> None:
        import json

        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                for r in results:
                    cur.execute(
                        """
                        INSERT INTO control.dq_results (
                            pipeline_run_id, dag_id, task_id,
                            check_id, check_name, layer, source_file, table_name,
                            status, severity,
                            expected_value, actual_value,
                            row_count_checked, failure_row_count, failure_pct,
                            detail_json, snapshot_date
                        ) VALUES (
                            %s, %s, %s,
                            %s, %s, %s::control.pipeline_layer, %s, %s,
                            %s::control.dq_status, %s::control.dq_severity,
                            %s, %s,
                            %s, %s, %s,
                            %s::jsonb, %s
                        )
                        """,
                        (
                            pipeline_run_id,
                            dag_id,
                            _TASK_ID,
                            r.check_id,
                            r.check_name,
                            r.layer,
                            r.source_file,
                            r.table_name,
                            r.status,
                            r.severity,
                            r.expected_value,
                            r.actual_value,
                            r.row_count_checked,
                            r.failure_row_count,
                            r.failure_pct,
                            json.dumps(r.detail_json) if r.detail_json else None,
                            snapshot_date,
                        ),
                    )
            conn.commit()

        logger.info(
            "DQ results persisted to control.dq_results",
            extra={"count": len(results), "snapshot_date": snapshot_date},
        )
