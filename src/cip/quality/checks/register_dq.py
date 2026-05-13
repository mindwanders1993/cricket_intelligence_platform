# src/cip/quality/checks/register_dq.py
#
# Register pipeline DQ checks — Silver layer.
#
# Checks (run after load_silver):
#   REG-SLV-001  silver.persons — person_id not null                        BLOCK
#   REG-SLV-002  silver.persons — person_id unique                           BLOCK
#   REG-SLV-003  silver.person_identifiers — key columns not null            BLOCK
#   REG-SLV-004  silver.person_identifiers — unique grain                    WARN
#   REG-SLV-005  bronze.register_people row count vs people.csv landing      BLOCK
#   REG-SLV-006  bronze.register_name_variations row count vs names.csv      BLOCK
#   REG-SLV-007  orphan check — name_variations.identifier in persons        WARN
#
# All results are persisted to control.dq_results.
# BLOCK failures raise DQBlockingFailureError after persisting.

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from cip.transform.shared.readers import PolarsIcebergReader

logger = get_logger(__name__)

_BRONZE_PEOPLE = TableName.bronze("register_people")
_BRONZE_NAME_VARIATIONS = TableName.bronze("register_name_variations")
_SILVER_PERSONS = TableName.silver("persons")
_SILVER_PERSON_IDENTIFIERS = TableName.silver("person_identifiers")
_SILVER_NAME_VARIATIONS = TableName.silver("name_variations")

_DAG_ID = "dag_ingest_cricsheet_register"
_TASK_ID = "run_dq"
_ROW_COUNT_THRESHOLD = 0.99  # Bronze rows must be >= 99% of landing rows


# ===========================================================================
# Result types
# ===========================================================================


@dataclass(frozen=True)
class DQCheckResult:
    check_id: str
    check_name: str
    layer: str  # PostgreSQL control.pipeline_layer ENUM: LANDING/BRONZE/SILVER/GOLD
    source_file: str | None
    table_name: str | None
    status: str  # PASSED | FAILED | WARNING | SKIPPED
    severity: str  # BLOCK | WARN | ALERT | LOG
    expected_value: str | None = None
    actual_value: str | None = None
    row_count_checked: int | None = None
    failure_row_count: int | None = None
    failure_pct: float | None = None
    detail_json: dict | None = None

    @property
    def is_blocking(self) -> bool:
        return self.status == "FAILED" and self.severity == "BLOCK"


@dataclass(frozen=True)
class DQRunSummary:
    checks: list[DQCheckResult]
    snapshot_date: str
    pipeline_run_id: str

    @property
    def has_blocking_failures(self) -> bool:
        return any(r.is_blocking for r in self.checks)

    @property
    def blocking_failures(self) -> list[DQCheckResult]:
        return [r for r in self.checks if r.is_blocking]

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.checks if r.status == "PASSED")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.checks if r.status in ("FAILED", "WARNING"))


class DQBlockingFailureError(RuntimeError):
    """Raised when one or more BLOCK-severity checks fail after persisting results."""

    def __init__(self, failures: list[DQCheckResult]) -> None:
        ids = ", ".join(f.check_id for f in failures)
        super().__init__(
            f"Register DQ: {len(failures)} blocking check(s) failed — {ids}. "
            f"Check control.dq_results for details."
        )
        self.failures = failures


# ===========================================================================
# Checker
# ===========================================================================


class RegisterDQChecker:
    """
    Runs all DQ checks for the Register pipeline Silver layer and persists
    results to control.dq_results.

    Usage:
        checker = RegisterDQChecker.from_settings()
        summary = checker.run_all(snapshot_date="2026-05-11", pipeline_run_id="run-xyz")
    """

    def __init__(self, reader: "PolarsIcebergReader", pg_dsn: str) -> None:
        self._reader = reader
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "RegisterDQChecker":
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
        Run all register DQ checks for the given snapshot_date.

        Reads Silver and Bronze Iceberg tables, queries control.register_ingestion_log
        for landing row counts, runs seven checks, persists all results to
        control.dq_results, then raises DQBlockingFailureError if any BLOCK check failed.
        """
        from cip.common.exceptions import TableNotFoundError

        logger.info(
            "RegisterDQChecker.run_all started",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        snap_filter = f"{META.SNAPSHOT_DATE} = '{snapshot_date}'"

        # Load Silver tables — reused across multiple checks.
        try:
            persons_df = self._reader.read_table(_SILVER_PERSONS, row_filter=snap_filter)
            identifiers_df = self._reader.read_table(_SILVER_PERSON_IDENTIFIERS, row_filter=snap_filter)
            name_var_df = self._reader.read_table(_SILVER_NAME_VARIATIONS, row_filter=snap_filter)
        except TableNotFoundError as exc:
            logger.error(
                "Silver table not found — cannot run DQ checks",
                extra={"error": str(exc), "snapshot_date": snapshot_date},
            )
            raise

        # Bronze row counts — read only the partition column (minimal data transfer).
        bronze_people_count = self._reader.read_table(
            _BRONZE_PEOPLE,
            columns=[META.SNAPSHOT_DATE],
            row_filter=snap_filter,
        ).height

        bronze_name_var_count = self._reader.read_table(
            _BRONZE_NAME_VARIATIONS,
            columns=[META.SNAPSHOT_DATE],
            row_filter=snap_filter,
        ).height

        landing_counts = self._get_landing_row_counts(snapshot_date)

        results: list[DQCheckResult] = [
            self._check_persons_not_null(persons_df),
            self._check_persons_unique(persons_df),
            self._check_identifiers_not_null(identifiers_df),
            self._check_identifiers_unique(identifiers_df),
            self._check_row_count_threshold(
                "people.csv",
                _BRONZE_PEOPLE,
                "REG-SLV-005",
                landing_counts.get("people.csv"),
                bronze_people_count,
            ),
            self._check_row_count_threshold(
                "names.csv",
                _BRONZE_NAME_VARIATIONS,
                "REG-SLV-006",
                landing_counts.get("names.csv"),
                bronze_name_var_count,
            ),
            self._check_orphan_identifiers(persons_df, name_var_df),
        ]

        self._persist_results(results, snapshot_date, pipeline_run_id, dag_id)

        summary = DQRunSummary(
            checks=results,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
        )

        logger.info(
            "RegisterDQChecker.run_all complete",
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

    def _check_persons_not_null(self, df: pl.DataFrame) -> DQCheckResult:
        """REG-SLV-001: silver.persons — person_id must not be null."""
        total = df.height
        null_count = df.filter(pl.col("person_id").is_null()).height
        status = "PASSED" if null_count == 0 else "FAILED"
        return DQCheckResult(
            check_id="REG-SLV-001",
            check_name="silver.persons — person_id not null",
            layer="SILVER",
            source_file="people.csv",
            table_name=_SILVER_PERSONS,
            status=status,
            severity="BLOCK",
            expected_value="0 null person_id values",
            actual_value=f"{null_count} null person_id values",
            row_count_checked=total,
            failure_row_count=null_count,
            failure_pct=_pct(null_count, total),
        )

    def _check_persons_unique(self, df: pl.DataFrame) -> DQCheckResult:
        """REG-SLV-002: silver.persons — person_id must be unique per snapshot."""
        total = df.height
        unique_count = df["person_id"].n_unique()
        dup_count = total - unique_count
        status = "PASSED" if dup_count == 0 else "FAILED"
        return DQCheckResult(
            check_id="REG-SLV-002",
            check_name="silver.persons — person_id unique",
            layer="SILVER",
            source_file="people.csv",
            table_name=_SILVER_PERSONS,
            status=status,
            severity="BLOCK",
            expected_value="0 duplicate person_id values",
            actual_value=f"{dup_count} duplicate person_id values",
            row_count_checked=total,
            failure_row_count=dup_count,
            failure_pct=_pct(dup_count, total),
        )

    def _check_identifiers_not_null(self, df: pl.DataFrame) -> DQCheckResult:
        """REG-SLV-003: silver.person_identifiers — key columns must not be null."""
        total = df.height
        null_rows = df.filter(
            pl.col("identifier").is_null()
            | pl.col("source_system").is_null()
            | pl.col("source_identifier").is_null()
        ).height
        status = "PASSED" if null_rows == 0 else "FAILED"
        return DQCheckResult(
            check_id="REG-SLV-003",
            check_name="silver.person_identifiers — key columns not null",
            layer="SILVER",
            source_file="people.csv",
            table_name=_SILVER_PERSON_IDENTIFIERS,
            status=status,
            severity="BLOCK",
            expected_value="0 rows with null in (identifier, source_system, source_identifier)",
            actual_value=f"{null_rows} rows with at least one null key column",
            row_count_checked=total,
            failure_row_count=null_rows,
            failure_pct=_pct(null_rows, total),
        )

    def _check_identifiers_unique(self, df: pl.DataFrame) -> DQCheckResult:
        """REG-SLV-004: silver.person_identifiers — unique on (identifier, source_system, source_identifier)."""
        total = df.height
        unique_count = df.select(["identifier", "source_system", "source_identifier"]).unique().height
        dup_count = total - unique_count
        status = "PASSED" if dup_count == 0 else "WARNING"
        return DQCheckResult(
            check_id="REG-SLV-004",
            check_name="silver.person_identifiers — unique grain",
            layer="SILVER",
            source_file="people.csv",
            table_name=_SILVER_PERSON_IDENTIFIERS,
            status=status,
            severity="WARN",
            expected_value="0 duplicate (identifier, source_system, source_identifier) tuples",
            actual_value=f"{dup_count} duplicate tuples",
            row_count_checked=total,
            failure_row_count=dup_count,
            failure_pct=_pct(dup_count, total),
        )

    def _check_row_count_threshold(
        self,
        source_file: str,
        bronze_fqn: str,
        check_id: str,
        landing_rows: int | None,
        bronze_rows: int,
    ) -> DQCheckResult:
        """
        REG-SLV-005/006: Bronze row count must be >= 99% of the landing file row count.

        SKIPPED if no landing record is found in control.register_ingestion_log
        (e.g. DQ run before download task completed).
        """
        bronze_label = bronze_fqn.split(".")[-1]
        check_name = f"row count threshold — {source_file} landing vs Bronze {bronze_label}"

        if landing_rows is None:
            return DQCheckResult(
                check_id=check_id,
                check_name=check_name,
                layer="BRONZE",
                source_file=source_file,
                table_name=bronze_fqn,
                status="SKIPPED",
                severity="WARN",
                expected_value=f">= {_ROW_COUNT_THRESHOLD*100:.0f}% of landing row count",
                actual_value="landing row count not found in control.register_ingestion_log",
            )

        expected_min = int(landing_rows * _ROW_COUNT_THRESHOLD)
        loss = max(0, landing_rows - bronze_rows)
        status = "PASSED" if bronze_rows >= expected_min else "FAILED"

        return DQCheckResult(
            check_id=check_id,
            check_name=check_name,
            layer="BRONZE",
            source_file=source_file,
            table_name=bronze_fqn,
            status=status,
            severity="BLOCK",
            expected_value=f">= {expected_min} rows (>={_ROW_COUNT_THRESHOLD*100:.0f}% of {landing_rows} landing rows)",
            actual_value=f"{bronze_rows} rows in Bronze",
            row_count_checked=landing_rows,
            failure_row_count=loss if status == "FAILED" else 0,
            failure_pct=_pct(loss, landing_rows) if status == "FAILED" else 0.0,
        )

    def _check_orphan_identifiers(
        self,
        persons_df: pl.DataFrame,
        name_var_df: pl.DataFrame,
    ) -> DQCheckResult:
        """
        REG-SLV-007: Every identifier in silver.name_variations must exist in
        silver.persons.person_id. Orphans > 5% trigger FAILED; otherwise WARNING.
        """
        total_unique = name_var_df["identifier"].n_unique()

        if total_unique == 0:
            return DQCheckResult(
                check_id="REG-SLV-007",
                check_name="orphan check — name_variations.identifier in persons.person_id",
                layer="SILVER",
                source_file=None,
                table_name=_SILVER_NAME_VARIATIONS,
                status="SKIPPED",
                severity="WARN",
                actual_value="silver.name_variations is empty for this snapshot",
            )

        person_ids = persons_df["person_id"].drop_nulls().to_list()
        orphan_df = (
            name_var_df.select("identifier")
            .unique()
            .filter(~pl.col("identifier").is_in(person_ids))
        )
        orphan_count = orphan_df.height
        pct = _pct(orphan_count, total_unique)

        if orphan_count == 0:
            status = "PASSED"
        elif pct > 5.0:
            status = "FAILED"
        else:
            status = "WARNING"

        return DQCheckResult(
            check_id="REG-SLV-007",
            check_name="orphan check — name_variations.identifier in persons.person_id",
            layer="SILVER",
            source_file=None,
            table_name=_SILVER_NAME_VARIATIONS,
            status=status,
            severity="WARN",
            expected_value="0 name_variations identifiers absent from persons.person_id",
            actual_value=f"{orphan_count} orphan identifiers ({pct:.2f}%)",
            row_count_checked=total_unique,
            failure_row_count=orphan_count,
            failure_pct=pct,
            detail_json=(
                {"orphan_sample": orphan_df.head(10).to_series().to_list()} if orphan_count > 0 else None
            ),
        )

    # -------------------------------------------------------------------------
    # Control DB helpers
    # -------------------------------------------------------------------------

    def _get_landing_row_counts(self, snapshot_date: str) -> dict[str, int]:
        """Query control.register_ingestion_log for the latest landing row count per file."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (source_file)
                        source_file,
                        row_count
                    FROM control.register_ingestion_log
                    WHERE snapshot_date = %s
                      AND status = 'SUCCESS'
                      AND row_count IS NOT NULL
                    ORDER BY source_file, id DESC
                    """,
                    (snapshot_date,),
                )
                rows = cur.fetchall()
        return {row[0]: row[1] for row in rows}

    def _persist_results(
        self,
        results: list[DQCheckResult],
        snapshot_date: str,
        pipeline_run_id: str,
        dag_id: str,
    ) -> None:
        """Persist all DQ check results to control.dq_results in a single transaction."""
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


# ===========================================================================
# Helpers
# ===========================================================================


def _pct(numerator: int, denominator: int) -> float:
    """Return numerator/denominator * 100, capped at 99.9999 for NUMERIC(6,4)."""
    if denominator == 0:
        return 0.0
    return min(round(numerator / denominator * 100, 4), 99.9999)
