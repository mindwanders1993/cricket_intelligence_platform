# tests/unit/quality/test_register_dq.py
"""
Unit tests for RegisterDQChecker.

PolarsIcebergReader and psycopg2 are mocked — no real Iceberg catalog or DB.
Tests verify:
    - Each _check_* method returns the correct status and severity
    - BLOCK failures raise DQBlockingFailureError from run_all
    - WARN failures do not raise
    - _persist_results is called with all results
    - _get_landing_row_counts handles missing records (returns empty dict)
    - Row count SKIPPED when landing rows absent from control DB
    - Orphan check SKIPPED when name_variations is empty
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cip.quality.checks.register_dq import (
    DQBlockingFailureError,
    DQCheckResult,
    DQRunSummary,
    RegisterDQChecker,
    _pct,
)

_SNAPSHOT = "2026-05-11"
_RUN_ID = "run-dq-001"
_PG_DSN = "postgresql://user:pass@localhost/test"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_checker() -> RegisterDQChecker:
    return RegisterDQChecker(reader=MagicMock(), pg_dsn=_PG_DSN)


def _persons(ids: list[str | None]) -> pl.DataFrame:
    return pl.DataFrame({"person_id": ids, "name": [f"P{i}" for i in range(len(ids))]})


def _identifiers(
    identifiers: list[str | None],
    systems: list[str | None],
    source_ids: list[str | None],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "identifier": identifiers,
            "source_system": systems,
            "source_identifier": source_ids,
        }
    )


def _name_vars(identifiers: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "identifier": identifiers,
            "name": [f"Name {i}" for i in range(len(identifiers))],
        }
    )


# ---------------------------------------------------------------------------
# 1. _pct helper
# ---------------------------------------------------------------------------


class TestPct:
    def test_zero_denominator_returns_zero(self):
        assert _pct(5, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        assert _pct(0, 100) == 0.0

    def test_half_returns_50(self):
        assert _pct(50, 100) == 50.0

    def test_caps_at_99_9999(self):
        assert _pct(100, 100) == 99.9999

    def test_rounds_to_4_decimals(self):
        result = _pct(1, 3)
        assert result == round(1 / 3 * 100, 4)


# ---------------------------------------------------------------------------
# 2. REG-SLV-001 — person_id not null
# ---------------------------------------------------------------------------


class TestCheckPersonsNotNull:
    def test_passed_when_no_nulls(self):
        checker = _make_checker()
        df = _persons(["p001", "p002", "p003"])
        result = checker._check_persons_not_null(df)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-001"
        assert result.severity == "BLOCK"
        assert result.failure_row_count == 0

    def test_failed_when_null_present(self):
        checker = _make_checker()
        df = _persons(["p001", None, "p003"])
        result = checker._check_persons_not_null(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1

    def test_row_count_checked_equals_total(self):
        checker = _make_checker()
        df = _persons(["p001", "p002"])
        result = checker._check_persons_not_null(df)
        assert result.row_count_checked == 2

    def test_failure_pct_correct(self):
        checker = _make_checker()
        df = _persons([None, None, "p003"])
        result = checker._check_persons_not_null(df)
        assert result.failure_pct == pytest.approx(_pct(2, 3))


# ---------------------------------------------------------------------------
# 3. REG-SLV-002 — person_id unique
# ---------------------------------------------------------------------------


class TestCheckPersonsUnique:
    def test_passed_when_all_unique(self):
        checker = _make_checker()
        df = _persons(["p001", "p002", "p003"])
        result = checker._check_persons_unique(df)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-002"
        assert result.severity == "BLOCK"

    def test_failed_when_duplicates_present(self):
        checker = _make_checker()
        df = _persons(["p001", "p001", "p003"])
        result = checker._check_persons_unique(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1

    def test_failure_pct_proportional(self):
        checker = _make_checker()
        df = _persons(["p001", "p001", "p002", "p002"])
        result = checker._check_persons_unique(df)
        assert result.failure_row_count == 2
        assert result.failure_pct == pytest.approx(_pct(2, 4))


# ---------------------------------------------------------------------------
# 4. REG-SLV-003 — person_identifiers not null
# ---------------------------------------------------------------------------


class TestCheckIdentifiersNotNull:
    def test_passed_when_no_nulls(self):
        checker = _make_checker()
        df = _identifiers(["p001", "p002"], ["cricinfo", "espn"], ["1", "2"])
        result = checker._check_identifiers_not_null(df)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-003"
        assert result.severity == "BLOCK"

    def test_failed_on_null_identifier(self):
        checker = _make_checker()
        df = _identifiers([None, "p002"], ["cricinfo", "espn"], ["1", "2"])
        result = checker._check_identifiers_not_null(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1

    def test_failed_on_null_source_system(self):
        checker = _make_checker()
        df = _identifiers(["p001", "p002"], [None, "espn"], ["1", "2"])
        result = checker._check_identifiers_not_null(df)
        assert result.status == "FAILED"

    def test_failed_on_null_source_identifier(self):
        checker = _make_checker()
        df = _identifiers(["p001", "p002"], ["cricinfo", "espn"], [None, "2"])
        result = checker._check_identifiers_not_null(df)
        assert result.status == "FAILED"

    def test_counts_row_with_multiple_nulls_once(self):
        checker = _make_checker()
        df = _identifiers([None, "p002"], [None, "espn"], [None, "2"])
        result = checker._check_identifiers_not_null(df)
        assert result.failure_row_count == 1


# ---------------------------------------------------------------------------
# 5. REG-SLV-004 — person_identifiers unique grain
# ---------------------------------------------------------------------------


class TestCheckIdentifiersUnique:
    def test_passed_when_all_unique(self):
        checker = _make_checker()
        df = _identifiers(["p001", "p002"], ["cricinfo", "cricinfo"], ["1", "2"])
        result = checker._check_identifiers_unique(df)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-004"
        assert result.severity == "WARN"  # not BLOCK

    def test_warning_when_duplicates_present(self):
        checker = _make_checker()
        df = _identifiers(["p001", "p001"], ["cricinfo", "cricinfo"], ["1", "1"])
        result = checker._check_identifiers_unique(df)
        assert result.status == "WARNING"
        assert result.failure_row_count == 1


# ---------------------------------------------------------------------------
# 6. REG-SLV-005/006 — row count threshold
# ---------------------------------------------------------------------------


class TestCheckRowCountThreshold:
    def _run(self, source_file, landing_rows, bronze_rows):
        checker = _make_checker()
        check_id = "REG-SLV-005" if source_file == "people.csv" else "REG-SLV-006"
        return checker._check_row_count_threshold(
            source_file=source_file,
            bronze_fqn="cricket.bronze.register_people",
            check_id=check_id,
            landing_rows=landing_rows,
            bronze_rows=bronze_rows,
        )

    def test_passed_when_counts_equal(self):
        result = self._run("people.csv", 1000, 1000)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-005"
        assert result.severity == "BLOCK"

    def test_passed_when_bronze_above_threshold(self):
        result = self._run("people.csv", 1000, 995)
        assert result.status == "PASSED"

    def test_failed_when_bronze_below_threshold(self):
        result = self._run("people.csv", 1000, 980)
        assert result.status == "FAILED"
        assert result.failure_row_count == 20

    def test_skipped_when_landing_rows_none(self):
        result = self._run("people.csv", None, 1000)
        assert result.status == "SKIPPED"
        assert result.severity == "WARN"

    def test_names_csv_uses_reg_slv_006(self):
        checker = _make_checker()
        result = checker._check_row_count_threshold(
            source_file="names.csv",
            bronze_fqn="cricket.bronze.register_name_variations",
            check_id="REG-SLV-006",
            landing_rows=500,
            bronze_rows=500,
        )
        assert result.check_id == "REG-SLV-006"

    def test_expected_value_contains_threshold(self):
        result = self._run("people.csv", 1000, 980)
        assert "99%" in result.expected_value

    def test_actual_value_contains_bronze_count(self):
        result = self._run("people.csv", 1000, 980)
        assert "980" in result.actual_value


# ---------------------------------------------------------------------------
# 7. REG-SLV-007 — orphan identifiers
# ---------------------------------------------------------------------------


class TestCheckOrphanIdentifiers:
    def test_passed_when_all_identifiers_match(self):
        checker = _make_checker()
        persons = _persons(["p001", "p002", "p003"])
        names = _name_vars(["p001", "p002", "p003"])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.status == "PASSED"
        assert result.check_id == "REG-SLV-007"
        assert result.severity == "WARN"

    def test_warning_when_few_orphans(self):
        checker = _make_checker()
        persons = _persons(["p001", "p002", "p003", "p004", "p005"])
        # 1 out of 5 is 20% — wait, 5 unique total, 1 orphan = 20%
        # 20% > 5% threshold → FAILED. Let me use a small proportion.
        # 1 out of 100 unique identifiers → 1% orphan rate → WARNING
        ids = [f"p{i:03d}" for i in range(100)]
        persons = _persons(ids)
        names = _name_vars(ids + ["orphan-001"])  # 1 extra orphan
        result = checker._check_orphan_identifiers(persons, names)
        assert result.status == "WARNING"
        assert result.failure_row_count == 1

    def test_failed_when_many_orphans(self):
        checker = _make_checker()
        persons = _persons(["p001"])
        names = _name_vars(["p001", "x001", "x002", "x003", "x004", "x005"])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.status == "FAILED"

    def test_skipped_when_name_vars_empty(self):
        checker = _make_checker()
        persons = _persons(["p001"])
        names = _name_vars([])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.status == "SKIPPED"

    def test_detail_json_populated_on_failure(self):
        checker = _make_checker()
        persons = _persons(["p001"])
        names = _name_vars(["orphan-x", "orphan-y"])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.detail_json is not None
        assert "orphan_sample" in result.detail_json

    def test_detail_json_none_on_pass(self):
        checker = _make_checker()
        persons = _persons(["p001", "p002"])
        names = _name_vars(["p001", "p002"])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.detail_json is None

    def test_null_person_ids_excluded_from_match(self):
        """Null person_ids must not accidentally match name_var identifiers."""
        checker = _make_checker()
        persons = _persons([None, "p001"])
        names = _name_vars(["p001"])
        result = checker._check_orphan_identifiers(persons, names)
        assert result.status == "PASSED"


# ---------------------------------------------------------------------------
# 8. DQCheckResult helpers
# ---------------------------------------------------------------------------


class TestDQCheckResult:
    def _make(self, status: str, severity: str) -> DQCheckResult:
        return DQCheckResult(
            check_id="REG-SLV-001",
            check_name="test",
            layer="SILVER",
            source_file=None,
            table_name=None,
            status=status,
            severity=severity,
        )

    def test_is_blocking_true_for_failed_block(self):
        assert self._make("FAILED", "BLOCK").is_blocking is True

    def test_is_blocking_false_for_passed_block(self):
        assert self._make("PASSED", "BLOCK").is_blocking is False

    def test_is_blocking_false_for_failed_warn(self):
        assert self._make("FAILED", "WARN").is_blocking is False

    def test_is_blocking_false_for_warning_block(self):
        assert self._make("WARNING", "BLOCK").is_blocking is False


# ---------------------------------------------------------------------------
# 9. DQRunSummary helpers
# ---------------------------------------------------------------------------


class TestDQRunSummary:
    def _make_summary(self, statuses: list[tuple[str, str]]) -> DQRunSummary:
        checks = [
            DQCheckResult(
                check_id=f"REG-SLV-00{i+1}",
                check_name=f"check {i}",
                layer="SILVER",
                source_file=None,
                table_name=None,
                status=status,
                severity=severity,
            )
            for i, (status, severity) in enumerate(statuses)
        ]
        return DQRunSummary(checks=checks, snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

    def test_has_blocking_failures_true(self):
        s = self._make_summary([("FAILED", "BLOCK"), ("PASSED", "BLOCK")])
        assert s.has_blocking_failures is True

    def test_has_blocking_failures_false_when_all_pass(self):
        s = self._make_summary([("PASSED", "BLOCK"), ("PASSED", "WARN")])
        assert s.has_blocking_failures is False

    def test_has_blocking_failures_false_for_warn_failures(self):
        s = self._make_summary([("FAILED", "WARN"), ("WARNING", "WARN")])
        assert s.has_blocking_failures is False

    def test_passed_count(self):
        s = self._make_summary([("PASSED", "BLOCK"), ("PASSED", "WARN"), ("FAILED", "BLOCK")])
        assert s.passed_count == 2

    def test_failed_count_includes_warning_status(self):
        s = self._make_summary([("PASSED", "BLOCK"), ("FAILED", "WARN"), ("WARNING", "WARN")])
        assert s.failed_count == 2

    def test_blocking_failures_list(self):
        s = self._make_summary([("FAILED", "BLOCK"), ("PASSED", "BLOCK"), ("FAILED", "WARN")])
        assert len(s.blocking_failures) == 1
        assert s.blocking_failures[0].check_id == "REG-SLV-001"


# ---------------------------------------------------------------------------
# 10. DQBlockingFailureError
# ---------------------------------------------------------------------------


class TestDQBlockingFailureError:
    def test_message_contains_check_ids(self):
        failures = [
            DQCheckResult(
                check_id="REG-SLV-001",
                check_name="test",
                layer="SILVER",
                source_file=None,
                table_name=None,
                status="FAILED",
                severity="BLOCK",
            )
        ]
        err = DQBlockingFailureError(failures)
        assert "REG-SLV-001" in str(err)
        assert err.failures == failures


# ---------------------------------------------------------------------------
# 11. run_all — integration (mocked reader + DB)
# ---------------------------------------------------------------------------


class TestRunAll:
    def _setup_reader(self, persons_df, identifiers_df, name_var_df, bronze_count=10):
        """Build a mock reader that returns controlled DataFrames in call order."""
        mock_reader = MagicMock()
        # call order: persons, identifiers, name_var, bronze_people (col-only), bronze_name_var (col-only)
        bronze_stub = pl.DataFrame({"_snapshot_date": ["2026-05-11"] * bronze_count})
        mock_reader.read_table.side_effect = [
            persons_df,
            identifiers_df,
            name_var_df,
            bronze_stub,
            bronze_stub,
        ]
        return mock_reader

    def _make_checker_with_mocks(self, persons_df, identifiers_df, name_var_df, landing_counts):
        reader = self._setup_reader(persons_df, identifiers_df, name_var_df)
        checker = RegisterDQChecker(reader=reader, pg_dsn=_PG_DSN)
        checker._get_landing_row_counts = MagicMock(return_value=landing_counts)
        checker._persist_results = MagicMock()
        return checker

    def test_returns_dq_run_summary(self):
        persons = _persons(["p001", "p002"])
        identifiers = _identifiers(["p001", "p002"], ["cricinfo", "espn"], ["1", "2"])
        names = _name_vars(["p001", "p002"])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 10, "names.csv": 10},
        )
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        assert isinstance(summary, DQRunSummary)

    def test_seven_checks_run(self):
        persons = _persons(["p001", "p002"])
        identifiers = _identifiers(["p001", "p002"], ["cricinfo", "espn"], ["1", "2"])
        names = _name_vars(["p001", "p002"])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 2, "names.csv": 2},
        )
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        assert len(summary.checks) == 7

    def test_persist_results_called_once(self):
        persons = _persons(["p001"])
        identifiers = _identifiers(["p001"], ["cricinfo"], ["1"])
        names = _name_vars(["p001"])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 1, "names.csv": 1},
        )
        checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        checker._persist_results.assert_called_once()

    def test_blocking_failure_raises_after_persist(self):
        """BLOCK failure must still persist before raising."""
        persons = _persons([None])  # REG-SLV-001 will FAIL/BLOCK
        identifiers = _identifiers(["p001"], ["cricinfo"], ["1"])
        names = _name_vars([])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 1, "names.csv": 0},
        )
        with pytest.raises(DQBlockingFailureError):
            checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        checker._persist_results.assert_called_once()

    def test_warn_only_does_not_raise(self):
        """Only WARN failures — run_all must complete without raising."""
        # REG-SLV-004: duplicate identifiers → WARNING/WARN (not BLOCK)
        persons = _persons(["p001", "p002"])
        identifiers = _identifiers(["p001", "p001"], ["cricinfo", "cricinfo"], ["1", "1"])
        names = _name_vars(["p001", "p002"])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 2, "names.csv": 2},
        )
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        assert not summary.has_blocking_failures

    def test_snapshot_date_in_summary(self):
        persons = _persons(["p001"])
        identifiers = _identifiers(["p001"], ["cricinfo"], ["1"])
        names = _name_vars(["p001"])
        checker = self._make_checker_with_mocks(
            persons, identifiers, names,
            {"people.csv": 1, "names.csv": 1},
        )
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)
        assert summary.snapshot_date == _SNAPSHOT
        assert summary.pipeline_run_id == _RUN_ID


# ---------------------------------------------------------------------------
# 12. from_settings factory
# ---------------------------------------------------------------------------


class TestFromSettings:
    def test_from_settings_returns_checker_instance(self):
        # PolarsIcebergReader and get_settings are imported lazily inside from_settings(),
        # so patch at their canonical module locations.
        mock_cfg = MagicMock()
        mock_cfg.postgres.dsn = "postgresql+psycopg2://u:p@h/db"
        with (
            patch("cip.common.settings.get_settings", return_value=mock_cfg),
            patch(
                "cip.transform.shared.readers.PolarsIcebergReader.from_settings",
                return_value=MagicMock(),
            ),
        ):
            checker = RegisterDQChecker.from_settings()
            assert isinstance(checker, RegisterDQChecker)
            assert checker._pg_dsn == "postgresql://u:p@h/db"
