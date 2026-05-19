# tests/unit/quality/test_match_bronze_dq.py
#
# Unit tests for MatchBronzeDQChecker (MAT-BRZ-001..004).

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pytest

from cip.quality.checks.people_and_names_dq import DQBlockingFailureError

_SNAPSHOT = "2026-05-01"
_RUN_ID = "dq-test-run"
_ARCHIVE_FILE = "all_json.zip"


def _make_checker(reader=None, pg_dsn="postgresql://u:p@h/db"):
    from cip.quality.checks.match_bronze_dq import MatchBronzeDQChecker

    return MatchBronzeDQChecker(
        reader=reader or MagicMock(),
        pg_dsn=pg_dsn,
    )


def _bronze_df(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "match_id": pl.Utf8,
                "revision": pl.Utf8,
                "match_type": pl.Utf8,
                "gender": pl.Utf8,
                "team_a": pl.Utf8,
                "team_b": pl.Utf8,
                "_snapshot_date": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# MAT-BRZ-001: files_failed == 0
# ---------------------------------------------------------------------------


class TestCheckFilesFailed:
    def _check(self, ingestion_log):
        checker = _make_checker()
        return checker._check_files_failed(ingestion_log)

    def test_passes_when_no_failures(self):
        result = self._check({"files_attempted": 100, "files_succeeded": 100, "files_failed": 0})
        assert result.status == "PASSED"
        assert result.severity == "BLOCK"
        assert result.check_id == "MAT-BRZ-001"

    def test_fails_when_files_failed_nonzero(self):
        result = self._check({"files_attempted": 100, "files_succeeded": 97, "files_failed": 3})
        assert result.status == "FAILED"
        assert result.severity == "BLOCK"
        assert result.failure_row_count == 3

    def test_skipped_when_no_log(self):
        result = self._check(None)
        assert result.status == "SKIPPED"
        assert result.check_id == "MAT-BRZ-001"

    def test_files_failed_none_treated_as_zero(self):
        result = self._check({"files_attempted": 50, "files_succeeded": 50, "files_failed": None})
        assert result.status == "PASSED"


# ---------------------------------------------------------------------------
# MAT-BRZ-002: (match_id, revision) unique
# ---------------------------------------------------------------------------


class TestCheckUniqueGrain:
    def _check(self, df):
        checker = _make_checker()
        return checker._check_unique_grain(df)

    def test_passes_on_unique_grain(self):
        df = _bronze_df(
            [
                {"match_id": "a", "revision": "1"},
                {"match_id": "b", "revision": "1"},
                {"match_id": "a", "revision": "2"},
            ]
        )
        result = self._check(df)
        assert result.status == "PASSED"
        assert result.severity == "BLOCK"
        assert result.check_id == "MAT-BRZ-002"

    def test_fails_on_duplicate_grain(self):
        df = _bronze_df(
            [
                {"match_id": "a", "revision": "1"},
                {"match_id": "a", "revision": "1"},  # duplicate
                {"match_id": "b", "revision": "1"},
            ]
        )
        result = self._check(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1

    def test_empty_df_passes(self):
        df = pl.DataFrame({"match_id": pl.Series([], dtype=pl.Utf8), "revision": pl.Series([], dtype=pl.Utf8)})
        result = self._check(df)
        assert result.status == "PASSED"
        assert result.failure_row_count == 0


# ---------------------------------------------------------------------------
# MAT-BRZ-003: audit coherence (this run)
# ---------------------------------------------------------------------------


class TestCheckAuditCoherence:
    def _check_with_audit_count(self, ingestion_log, audit_count):
        checker = _make_checker()
        checker._count_audit_bronze_loaded_for_run = MagicMock(return_value=audit_count)
        return checker._check_audit_coherence(ingestion_log, pipeline_run_id=_RUN_ID)

    def test_passes_when_audit_count_matches_rows_written(self):
        log = {"files_attempted": 30, "files_succeeded": 30, "files_failed": 0, "rows_written": 5, "pipeline_run_id": _RUN_ID}
        result = self._check_with_audit_count(log, audit_count=5)
        assert result.status == "PASSED"
        assert result.check_id == "MAT-BRZ-003"
        assert result.severity == "BLOCK"

    def test_fails_when_audit_count_lags_rows_written(self):
        log = {"files_attempted": 30, "files_succeeded": 30, "files_failed": 0, "rows_written": 10, "pipeline_run_id": _RUN_ID}
        result = self._check_with_audit_count(log, audit_count=7)
        assert result.status == "FAILED"
        assert result.failure_row_count == 3
        assert result.severity == "BLOCK"

    def test_passes_when_both_zero_audit_skip_only(self):
        # Daily DAG no-op day: every file already in Bronze, nothing written,
        # no audit row newly marked.
        log = {"files_attempted": 30, "files_succeeded": 0, "files_failed": 0, "rows_written": 0, "pipeline_run_id": _RUN_ID}
        result = self._check_with_audit_count(log, audit_count=0)
        assert result.status == "PASSED"

    def test_skipped_when_bronze_skipped_this_run(self):
        # Per-snapshot idempotency guard fired — Bronze wrote nothing for this run_id.
        # The log row belongs to a prior run; MAT-BRZ-003 should SKIP, not FAIL.
        log = {"files_attempted": 31, "files_succeeded": 31, "files_failed": 0, "rows_written": 31, "pipeline_run_id": "prior-run-id"}
        result = self._check_with_audit_count(log, audit_count=0)
        assert result.status == "SKIPPED"

    def test_skipped_when_no_ingestion_log(self):
        result = self._check_with_audit_count(None, audit_count=0)
        assert result.status == "SKIPPED"


# ---------------------------------------------------------------------------
# MAT-BRZ-004: metadata coverage
# ---------------------------------------------------------------------------


class TestCheckMetadataCoverage:
    def _check(self, df):
        checker = _make_checker()
        return checker._check_metadata_coverage(df)

    def _make_df(self, rows):
        schema = {"match_id": pl.Utf8, "match_type": pl.Utf8, "gender": pl.Utf8, "team_a": pl.Utf8, "team_b": pl.Utf8}
        return pl.DataFrame(rows, schema=schema)

    def test_passes_when_all_metadata_present(self):
        df = self._make_df(
            [
                {"match_id": "a", "match_type": "T20", "gender": "male", "team_a": "India", "team_b": "Eng"},
                {"match_id": "b", "match_type": "ODI", "gender": "male", "team_a": "Aus", "team_b": "NZ"},
            ]
        )
        result = self._check(df)
        assert result.status == "PASSED"
        assert result.check_id == "MAT-BRZ-004"
        assert result.severity == "WARN"

    def test_warns_when_null_rate_above_threshold(self):
        _schema = {"match_id": pl.Utf8, "match_type": pl.Utf8, "gender": pl.Utf8, "team_a": pl.Utf8, "team_b": pl.Utf8}
        rows = [
            {"match_id": str(i), "match_type": None, "gender": None, "team_a": None, "team_b": None} for i in range(10)
        ]
        df = pl.DataFrame(rows, schema=_schema)
        result = self._check(df)
        assert result.status == "WARNING"

    def test_skipped_on_empty_df(self):
        _schema = {"match_id": pl.Utf8, "match_type": pl.Utf8, "gender": pl.Utf8, "team_a": pl.Utf8, "team_b": pl.Utf8}
        df = pl.DataFrame(schema=_schema)
        result = self._check(df)
        assert result.status == "SKIPPED"

    def test_empty_string_counts_as_null(self):
        rows = [
            {"match_id": "a", "match_type": "", "gender": "male", "team_a": "India", "team_b": "Eng"},
        ] * 100
        df = self._make_df(rows)
        result = self._check(df)
        assert result.status == "WARNING"

    def test_one_percent_or_below_passes(self):
        rows = [
            {"match_id": str(i), "match_type": "T20", "gender": "male", "team_a": "India", "team_b": "Eng"}
            for i in range(99)
        ]
        rows.append({"match_id": "x", "match_type": None, "gender": "male", "team_a": "India", "team_b": "Eng"})
        df = self._make_df(rows)
        result = self._check(df)
        assert result.status == "PASSED"


# ---------------------------------------------------------------------------
# Tests: run_all (integration-style with mocked Iceberg + control DB)
# ---------------------------------------------------------------------------


class TestRunAll:
    def _make_checker_with_mocks(self, bronze_df, ingestion_log=None, audit_count=0):
        reader = MagicMock()
        reader.read_table.return_value = bronze_df

        checker = _make_checker(reader=reader)

        checker._get_ingestion_log = MagicMock(return_value=ingestion_log)
        checker._count_audit_bronze_loaded_for_run = MagicMock(return_value=audit_count)
        checker._persist_results = MagicMock()
        return checker

    def _good_df(self):
        return pl.DataFrame(
            {
                "match_id": ["a", "b", "c"],
                "revision": ["1", "1", "1"],
                "match_type": ["T20", "ODI", "Test"],
                "gender": ["male", "male", "male"],
                "team_a": ["India", "Aus", "Eng"],
                "team_b": ["Eng", "NZ", "SA"],
                "_snapshot_date": [_SNAPSHOT] * 3,
            }
        )

    def test_all_pass_returns_summary(self):
        df = self._good_df()
        log = {"files_attempted": 3, "files_succeeded": 3, "files_failed": 0, "rows_written": 3, "pipeline_run_id": _RUN_ID}

        checker = self._make_checker_with_mocks(df, ingestion_log=log, audit_count=3)
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        assert summary.passed_count == 4
        assert summary.failed_count == 0
        assert not summary.has_blocking_failures
        checker._persist_results.assert_called_once()

    def test_block_failure_raises(self):
        df = self._good_df()
        log = {"files_attempted": 3, "files_succeeded": 2, "files_failed": 1, "rows_written": 2, "pipeline_run_id": _RUN_ID}  # MAT-BRZ-001 fails

        checker = self._make_checker_with_mocks(df, ingestion_log=log, audit_count=2)

        with pytest.raises(DQBlockingFailureError) as exc_info:
            checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        assert "MAT-BRZ-001" in str(exc_info.value)
        checker._persist_results.assert_called_once()

    def test_persist_called_even_on_failure(self):
        df = pl.DataFrame(
            {
                "match_id": ["a", "a"],
                "revision": ["1", "1"],  # duplicate → MAT-BRZ-002 FAILED
                "match_type": ["T20", "T20"],
                "gender": ["male", "male"],
                "team_a": ["India", "India"],
                "team_b": ["Eng", "Eng"],
                "_snapshot_date": [_SNAPSHOT, _SNAPSHOT],
            }
        )
        log = {"files_attempted": 2, "files_succeeded": 2, "files_failed": 0, "rows_written": 2, "pipeline_run_id": _RUN_ID}

        checker = self._make_checker_with_mocks(df, ingestion_log=log, audit_count=2)

        with pytest.raises(DQBlockingFailureError):
            checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        checker._persist_results.assert_called_once()

    def test_skipped_checks_not_counted_as_failures(self):
        df = self._good_df()
        log = None  # triggers SKIPPED for MAT-BRZ-001 and MAT-BRZ-003

        checker = self._make_checker_with_mocks(df, ingestion_log=log, audit_count=0)
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        statuses = {r.check_id: r.status for r in summary.checks}
        assert statuses["MAT-BRZ-001"] == "SKIPPED"
        assert statuses["MAT-BRZ-003"] == "SKIPPED"
        assert not summary.has_blocking_failures

    def test_run_all_produces_four_results(self):
        df = self._good_df()
        log = {"files_attempted": 3, "files_succeeded": 3, "files_failed": 0, "rows_written": 3, "pipeline_run_id": _RUN_ID}

        checker = self._make_checker_with_mocks(df, ingestion_log=log, audit_count=3)
        summary = checker.run_all(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        assert len(summary.checks) == 4
        check_ids = {r.check_id for r in summary.checks}
        assert check_ids == {"MAT-BRZ-001", "MAT-BRZ-002", "MAT-BRZ-003", "MAT-BRZ-004"}
