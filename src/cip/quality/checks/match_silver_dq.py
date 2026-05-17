# src/cip/quality/checks/match_silver_dq.py
#
# Silver DQ checks for the Match pipeline.
#
# Checks (run after task_build_silver):
#   MAT-SLV-001  silver.matches.match_id NOT NULL                             BLOCK
#   MAT-SLV-002  silver.matches unique on match_id                            BLOCK
#   MAT-SLV-003  silver.innings unique grain (match_id, innings_number)       BLOCK
#   MAT-SLV-004  silver.deliveries unique grain                                BLOCK
#   MAT-SLV-005  silver.unmatched_persons_audit ≤ 5% of players + officials   WARN
#   MAT-SLV-006  silver.wickets — fielders non-empty for catch/run-out etc.   WARN
#   MAT-SLV-007  silver.match_players.person_id refs silver.persons           WARN
#   MAT-SLV-008  silver.match_players unique grain                            BLOCK
#   MAT-SLV-009  silver.match_officials unique grain                          BLOCK
#   MAT-SLV-010  silver.match_registry unique grain                           BLOCK
#   MAT-SLV-011  silver.match_powerplays unique grain                         BLOCK
#   MAT-SLV-012  silver.deliveries null rate on batter/bowler ≤ 1%            WARN
#
# Results land in control.dq_results.
# BLOCK failures raise DQBlockingFailureError after persisting.

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger
from cip.quality.checks.people_and_names_dq import (
    DQBlockingFailureError,
    DQCheckResult,
    DQRunSummary,
    _pct,
)

if TYPE_CHECKING:
    from cip.transform.shared.readers import PolarsIcebergReader

logger = get_logger(__name__)

_S_MATCHES = TableName.silver("matches")
_S_INNINGS = TableName.silver("innings")
_S_DELIVERIES = TableName.silver("deliveries")
_S_WICKETS = TableName.silver("wickets")
_S_MATCH_PLAYERS = TableName.silver("match_players")
_S_MATCH_OFFICIALS = TableName.silver("match_officials")
_S_MATCH_REGISTRY = TableName.silver("match_registry")
_S_MATCH_POWERPLAYS = TableName.silver("match_powerplays")
_S_UNMATCHED_AUDIT = TableName.silver("unmatched_persons_audit")
_S_PERSONS = TableName.silver("persons")

_DAG_ID = "dag_build_silver_match_data"
_TASK_ID = "run_dq"
_ARCHIVE_FILE = "all_json.zip"

_UNMATCHED_THRESHOLD_PCT = 5.0  # MAT-SLV-005
_DELIVERIES_NULL_THRESHOLD_PCT = 1.0  # MAT-SLV-012

# MAT-SLV-006 — dismissal kinds that REQUIRE at least one fielder credited.
# Bowled / lbw / hit wicket / etc. don't need a fielder.
_FIELDER_REQUIRED_KINDS = ("caught", "run out", "stumped", "caught and bowled")


class MatchDataSilverDQChecker:
    """
    Runs all Silver DQ checks for the match pipeline and persists results
    to control.dq_results.

    Usage:
        checker = MatchDataSilverDQChecker.from_settings()
        summary = checker.run_all(snapshot_date="2026-05-17", pipeline_run_id="run-xyz")
    """

    def __init__(self, reader: "PolarsIcebergReader", pg_dsn: str) -> None:
        self._reader = reader
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "MatchDataSilverDQChecker":
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
        logger.info(
            "MatchDataSilverDQChecker.run_all started",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        snap_filter = f"{META.SNAPSHOT_DATE} = '{snapshot_date}'"

        # Read each Silver table with minimal column projection.
        matches = self._read(_S_MATCHES, ["match_id"], snap_filter)
        innings = self._read(_S_INNINGS, ["match_id", "innings_number"], snap_filter)
        deliveries = self._read(
            _S_DELIVERIES,
            ["match_id", "innings_number", "over_number", "delivery_number", "batter", "bowler"],
            snap_filter,
        )
        wickets = self._read(_S_WICKETS, ["match_id", "kind", "fielders"], snap_filter)
        match_players = self._read(_S_MATCH_PLAYERS, ["match_id", "team", "player_name", "person_id"], snap_filter)
        match_officials = self._read(_S_MATCH_OFFICIALS, ["match_id", "role", "official_name"], snap_filter)
        match_registry = self._read(_S_MATCH_REGISTRY, ["match_id", "display_name"], snap_filter)
        match_powerplays = self._read(
            _S_MATCH_POWERPLAYS, ["match_id", "innings_number", "from_over", "type"], snap_filter
        )
        unmatched_audit = self._read(_S_UNMATCHED_AUDIT, ["match_id"], snap_filter)
        persons = self._read(_S_PERSONS, ["person_id"], None)  # Register layer — no snapshot filter

        results: list[DQCheckResult] = [
            self._check_matches_not_null(matches),
            self._check_matches_unique(matches),
            self._check_grain_unique(innings, "MAT-SLV-003", _S_INNINGS, ["match_id", "innings_number"]),
            self._check_grain_unique(
                deliveries,
                "MAT-SLV-004",
                _S_DELIVERIES,
                ["match_id", "innings_number", "over_number", "delivery_number"],
            ),
            self._check_unmatched_rate(unmatched_audit, match_players, match_officials),
            self._check_wickets_fielders(wickets),
            self._check_match_players_person_id_referential(match_players, persons),
            self._check_grain_unique(
                match_players, "MAT-SLV-008", _S_MATCH_PLAYERS, ["match_id", "team", "player_name"]
            ),
            self._check_grain_unique(
                match_officials, "MAT-SLV-009", _S_MATCH_OFFICIALS, ["match_id", "role", "official_name"]
            ),
            self._check_grain_unique(match_registry, "MAT-SLV-010", _S_MATCH_REGISTRY, ["match_id", "display_name"]),
            self._check_grain_unique(
                match_powerplays,
                "MAT-SLV-011",
                _S_MATCH_POWERPLAYS,
                ["match_id", "innings_number", "from_over", "type"],
            ),
            self._check_deliveries_metadata_coverage(deliveries),
        ]

        self._persist_results(results, snapshot_date, pipeline_run_id, dag_id)

        summary = DQRunSummary(
            checks=results,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
        )

        logger.info(
            "MatchDataSilverDQChecker.run_all complete",
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

    def _check_matches_not_null(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-SLV-001: silver.matches.match_id NOT NULL."""
        total = df.height
        null_count = df.filter(pl.col("match_id").is_null()).height
        status = "PASSED" if null_count == 0 else "FAILED"
        return DQCheckResult(
            check_id="MAT-SLV-001",
            check_name=f"{_S_MATCHES} — match_id NOT NULL",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_MATCHES,
            status=status,
            severity="BLOCK",
            expected_value="0 null match_id",
            actual_value=f"{null_count} null match_id",
            row_count_checked=total,
            failure_row_count=null_count,
            failure_pct=_pct(null_count, total),
        )

    def _check_matches_unique(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-SLV-002: silver.matches unique on match_id."""
        total = df.height
        unique = df.select("match_id").unique().height
        dup_count = total - unique
        status = "PASSED" if dup_count == 0 else "FAILED"
        return DQCheckResult(
            check_id="MAT-SLV-002",
            check_name=f"{_S_MATCHES} — match_id unique",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_MATCHES,
            status=status,
            severity="BLOCK",
            expected_value="0 duplicate match_id",
            actual_value=f"{dup_count} duplicate match_id",
            row_count_checked=total,
            failure_row_count=dup_count,
            failure_pct=_pct(dup_count, total),
        )

    def _check_grain_unique(
        self,
        df: pl.DataFrame,
        check_id: str,
        table_name: str,
        grain_cols: list[str],
    ) -> DQCheckResult:
        """Generic uniqueness check for a multi-column grain."""
        total = df.height
        unique = df.select(grain_cols).unique().height
        dup_count = total - unique
        status = "PASSED" if dup_count == 0 else "FAILED"
        grain_str = ", ".join(grain_cols)
        return DQCheckResult(
            check_id=check_id,
            check_name=f"{table_name} — unique grain ({grain_str})",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=table_name,
            status=status,
            severity="BLOCK",
            expected_value=f"0 duplicate ({grain_str})",
            actual_value=f"{dup_count} duplicate ({grain_str})",
            row_count_checked=total,
            failure_row_count=dup_count,
            failure_pct=_pct(dup_count, total),
        )

    def _check_unmatched_rate(
        self,
        audit_df: pl.DataFrame,
        players_df: pl.DataFrame,
        officials_df: pl.DataFrame,
    ) -> DQCheckResult:
        """MAT-SLV-005: unmatched_persons_audit row count ≤ 5% of players + officials."""
        unmatched = audit_df.height
        denominator = players_df.height + officials_df.height

        if denominator == 0:
            return DQCheckResult(
                check_id="MAT-SLV-005",
                check_name=f"{_S_UNMATCHED_AUDIT} — unmatched rate ≤ {_UNMATCHED_THRESHOLD_PCT}%",
                layer="SILVER",
                source_file=_ARCHIVE_FILE,
                table_name=_S_UNMATCHED_AUDIT,
                status="SKIPPED",
                severity="WARN",
                actual_value="No players + officials rows to compare against",
            )

        pct = _pct(unmatched, denominator)
        status = "PASSED" if pct <= _UNMATCHED_THRESHOLD_PCT else "WARNING"
        return DQCheckResult(
            check_id="MAT-SLV-005",
            check_name=f"{_S_UNMATCHED_AUDIT} — unmatched rate ≤ {_UNMATCHED_THRESHOLD_PCT}%",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_UNMATCHED_AUDIT,
            status=status,
            severity="WARN",
            expected_value=f"≤ {_UNMATCHED_THRESHOLD_PCT}% of (match_players + match_officials)",
            actual_value=f"{unmatched} unmatched / {denominator} total = {pct:.2f}%",
            row_count_checked=denominator,
            failure_row_count=unmatched,
            failure_pct=pct,
        )

    def _check_wickets_fielders(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-SLV-006: silver.wickets.fielders must be non-empty for catch/run-out/stumped/c&b."""
        kind_required = df.filter(pl.col("kind").is_in(list(_FIELDER_REQUIRED_KINDS)))
        total = kind_required.height

        if total == 0:
            return DQCheckResult(
                check_id="MAT-SLV-006",
                check_name=f"{_S_WICKETS} — fielders non-empty for catch/run-out/stumped/c&b",
                layer="SILVER",
                source_file=_ARCHIVE_FILE,
                table_name=_S_WICKETS,
                status="SKIPPED",
                severity="WARN",
                actual_value="No fielder-required wickets in snapshot",
            )

        # Polars: list.len() returns null on a null list; coalesce to 0 for the comparison.
        offenders = kind_required.filter(pl.col("fielders").is_null() | (pl.col("fielders").list.len() == 0)).height
        pct = _pct(offenders, total)
        status = "PASSED" if offenders == 0 else "WARNING"
        return DQCheckResult(
            check_id="MAT-SLV-006",
            check_name=f"{_S_WICKETS} — fielders non-empty for catch/run-out/stumped/c&b",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_WICKETS,
            status=status,
            severity="WARN",
            expected_value="0 fielder-required wickets with empty fielders",
            actual_value=f"{offenders} of {total} fielder-required wickets have empty fielders",
            row_count_checked=total,
            failure_row_count=offenders,
            failure_pct=pct,
        )

    def _check_match_players_person_id_referential(
        self,
        match_players_df: pl.DataFrame,
        persons_df: pl.DataFrame,
    ) -> DQCheckResult:
        """MAT-SLV-007: match_players.person_id (if non-null) must exist in silver.persons.person_id."""
        with_pid = match_players_df.filter(pl.col("person_id").is_not_null())
        total = with_pid.height

        if total == 0:
            return DQCheckResult(
                check_id="MAT-SLV-007",
                check_name=f"{_S_MATCH_PLAYERS} — person_id refs {_S_PERSONS}",
                layer="SILVER",
                source_file=_ARCHIVE_FILE,
                table_name=_S_MATCH_PLAYERS,
                status="SKIPPED",
                severity="WARN",
                actual_value="No resolved person_id rows to check",
            )

        valid_ids = persons_df.select("person_id").unique()
        orphans = with_pid.join(valid_ids, on="person_id", how="anti").height
        pct = _pct(orphans, total)
        status = "PASSED" if orphans == 0 else "WARNING"
        return DQCheckResult(
            check_id="MAT-SLV-007",
            check_name=f"{_S_MATCH_PLAYERS} — person_id refs {_S_PERSONS}",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_MATCH_PLAYERS,
            status=status,
            severity="WARN",
            expected_value="0 orphan person_id values",
            actual_value=f"{orphans} of {total} resolved person_ids absent from silver.persons",
            row_count_checked=total,
            failure_row_count=orphans,
            failure_pct=pct,
        )

    def _check_deliveries_metadata_coverage(self, df: pl.DataFrame) -> DQCheckResult:
        """MAT-SLV-012: silver.deliveries — batter/bowler null rate ≤ 1%."""
        total = df.height

        if total == 0:
            return DQCheckResult(
                check_id="MAT-SLV-012",
                check_name=f"{_S_DELIVERIES} — batter/bowler null rate ≤ {_DELIVERIES_NULL_THRESHOLD_PCT}%",
                layer="SILVER",
                source_file=_ARCHIVE_FILE,
                table_name=_S_DELIVERIES,
                status="SKIPPED",
                severity="WARN",
                actual_value="No deliveries in snapshot",
            )

        null_rows = df.filter(pl.col("batter").is_null() | pl.col("bowler").is_null()).height
        pct = _pct(null_rows, total)
        status = "PASSED" if pct <= _DELIVERIES_NULL_THRESHOLD_PCT else "WARNING"
        return DQCheckResult(
            check_id="MAT-SLV-012",
            check_name=f"{_S_DELIVERIES} — batter/bowler null rate ≤ {_DELIVERIES_NULL_THRESHOLD_PCT}%",
            layer="SILVER",
            source_file=_ARCHIVE_FILE,
            table_name=_S_DELIVERIES,
            status=status,
            severity="WARN",
            expected_value=f"≤ {_DELIVERIES_NULL_THRESHOLD_PCT}% rows with null batter or bowler",
            actual_value=f"{null_rows} of {total} rows with null batter/bowler = {pct:.2f}%",
            row_count_checked=total,
            failure_row_count=null_rows,
            failure_pct=pct,
        )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _read(
        self,
        fqn: str,
        columns: list[str],
        row_filter: str | None,
    ) -> pl.DataFrame:
        """Read a Silver table with column projection. Returns empty DF if absent."""
        from cip.common.exceptions import TableNotFoundError

        try:
            return self._reader.read_table(fqn, columns=columns, row_filter=row_filter)
        except TableNotFoundError:
            logger.warning(
                "Silver table not found — DQ checks against it will treat as empty",
                extra={"table": fqn, "row_filter": row_filter},
            )
            return pl.DataFrame(schema={c: pl.Utf8 for c in columns})

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
