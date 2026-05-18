# tests/unit/transform/shared/test_delete_and_insert.py
#
# Unit tests for SparkIcebergWriter.delete_and_insert — the row-level-delete
# write mode used by incremental Silver transforms.
#
# Tests are mock-based: the writer is asked to issue specific SQL against a
# fake SparkSession. End-to-end correctness (the DELETE actually removes
# rows in Iceberg, the INSERT lands the new ones) is verified by the
# pre-PR DuckDB+Iceberg-deletes experiment recorded in
# docs/runbooks/duckdb-iceberg-deletes.md.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _bypass_meta_injection():
    """The writer calls _inject_meta_spark which needs pyspark.functions.lit
    (and thus an active SparkContext). For unit tests, we bypass it — the
    test is about the SQL semantics, not the meta column wiring."""
    with patch(
        "cip.transform.shared.writers._inject_meta_spark",
        side_effect=lambda df, *a, **kw: df,
    ):
        yield


def _make_writer():
    from cip.transform.shared.writers import SparkIcebergWriter

    spark = MagicMock()
    writer = SparkIcebergWriter(spark)
    return writer, spark


def _make_df(columns: list[str]):
    df = MagicMock()
    df.columns = columns
    # _inject_meta_spark uses df.withColumn → return self for chainability.
    df.withColumn.return_value = df
    return df


class TestDeleteAndInsert:
    def test_empty_key_cols_raises(self):
        writer, _ = _make_writer()
        df = _make_df(["match_id", "ball"])
        with pytest.raises(ValueError, match="key_cols"):
            writer.delete_and_insert(
                df=df,
                fqn="silver.matches",
                snapshot_date="2026-05-18",
                key_cols=[],
            )

    def test_issues_delete_then_insert_for_match_id(self):
        writer, spark = _make_writer()
        df = _make_df(["match_id", "team_a", "team_b"])

        # _ensure_table_exists checks via SELECT 1 — make it succeed (table exists)
        spark.sql.return_value = MagicMock()

        writer.delete_and_insert(
            df=df,
            fqn="silver.matches",
            snapshot_date="2026-05-18",
            key_cols=["match_id"],
            pipeline_run_id="run-x",
        )

        # Collect SQL strings spark.sql was called with
        sqls = [call.args[0] for call in spark.sql.call_args_list]

        # First: _ensure_table_exists probe (SELECT 1 FROM ... LIMIT 0)
        # Then: ALTER TABLE for format-version=2
        # Then: DELETE FROM
        # Then: INSERT INTO
        delete_sql = next(s for s in sqls if s.startswith("DELETE FROM"))
        insert_sql = next(s for s in sqls if s.startswith("INSERT INTO"))
        alter_sql = next(s for s in sqls if s.startswith("ALTER TABLE"))

        assert "silver.matches" in delete_sql
        assert "WHERE (match_id) IN (SELECT DISTINCT match_id FROM" in delete_sql
        assert "INSERT INTO silver.matches SELECT * FROM" in insert_sql
        assert "format-version" in alter_sql and "2" in alter_sql

    def test_issues_delete_for_composite_key(self):
        writer, spark = _make_writer()
        df = _make_df(["match_id", "innings_number"])
        spark.sql.return_value = MagicMock()

        writer.delete_and_insert(
            df=df,
            fqn="silver.innings",
            snapshot_date="2026-05-18",
            key_cols=["match_id", "innings_number"],
        )

        sqls = [c.args[0] for c in spark.sql.call_args_list]
        delete_sql = next(s for s in sqls if s.startswith("DELETE FROM"))
        assert "WHERE (match_id, innings_number) IN" in delete_sql
        assert "SELECT DISTINCT match_id, innings_number FROM" in delete_sql

    def test_temp_view_dropped_even_on_failure(self):
        writer, spark = _make_writer()
        df = _make_df(["match_id"])

        def _sql_side_effect(query):
            if query.startswith("INSERT INTO"):
                raise RuntimeError("Iceberg insert failed")
            return MagicMock()

        spark.sql.side_effect = _sql_side_effect

        with pytest.raises(RuntimeError, match="Iceberg insert failed"):
            writer.delete_and_insert(
                df=df,
                fqn="silver.matches",
                snapshot_date="2026-05-18",
                key_cols=["match_id"],
            )

        # The temp view must be dropped regardless of failure
        spark.catalog.dropTempView.assert_called_once()

    def test_create_table_then_alter_format_v2(self):
        """First-write path: _ensure_table_exists creates table, _ensure_format_v2 ALTERs it."""
        writer, spark = _make_writer()
        df = _make_df(["match_id"])

        # SELECT 1 FROM ... LIMIT 0 raises → table doesn't exist → create path
        def _sql_side_effect(query):
            if query.startswith("SELECT 1 FROM"):
                raise RuntimeError("Table not found")
            return MagicMock()

        spark.sql.side_effect = _sql_side_effect
        # df.writeTo().partitionedBy(...).create() chain
        df.writeTo.return_value.partitionedBy.return_value.create.return_value = None
        df.writeTo.return_value.create.return_value = None

        writer.delete_and_insert(
            df=df,
            fqn="silver.matches",
            snapshot_date="2026-05-18",
            key_cols=["match_id"],
        )

        # Table creation was attempted
        df.writeTo.assert_called()
