"""Unit tests for UnmatchedPersonsAuditSilverTransform.

This transform consumes DataFrames produced by upstream players/officials
transforms — no Bronze read, no F.col calls in the no-input path — so we
can exercise its empty-input path with pure mocks.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cip.transform.spark.silver.unmatched_audit import (
    _SILVER_UNMATCHED_AUDIT,
    REASON_NO_REGISTER_MATCH,
    REASON_NO_REGISTRY_MAPPING,
    ResolutionResult,
    UnmatchedPersonsAuditSilverTransform,
)


class TestConstants:
    def test_target_fqn(self):
        assert _SILVER_UNMATCHED_AUDIT == "silver.unmatched_persons_audit"

    def test_reason_taxonomy_stable(self):
        # Downstream MAT-SLV-005 / dashboards filter on these literals.
        assert REASON_NO_REGISTRY_MAPPING == "NO_REGISTRY_MAPPING"
        assert REASON_NO_REGISTER_MATCH == "NO_REGISTER_MATCH"


class TestResolutionResult:
    def test_named_tuple_fields(self):
        df = MagicMock(name="unmatched_df")
        result = ResolutionResult(row_count=5, unmatched_df=df)
        assert result.row_count == 5
        assert result.unmatched_df is df

    def test_unmatched_df_can_be_none(self):
        result = ResolutionResult(row_count=0, unmatched_df=None)
        assert result.unmatched_df is None


class TestRunEmptyInput:
    def test_no_dfs_skips_write_and_returns_zero(self):
        spark = MagicMock()
        writer = MagicMock()
        transform = UnmatchedPersonsAuditSilverTransform(spark=spark, writer=writer)

        rows = transform.run([], snapshot_date="2026-05-17", pipeline_run_id="run-1")

        assert rows == 0
        writer.dynamic_overwrite.assert_not_called()


class TestInstantiation:
    def test_constructor_accepts_spark_and_writer(self):
        spark = MagicMock()
        writer = MagicMock()
        transform = UnmatchedPersonsAuditSilverTransform(spark=spark, writer=writer)
        assert transform._spark is spark
        assert transform._writer is writer
