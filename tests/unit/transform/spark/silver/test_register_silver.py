"""Unit tests for PeopleAndNamesSilverTransform."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cip.transform.spark.silver.persons import (
    _BRONZE_IDENTIFIERS,
    _BRONZE_NAME_VARIATIONS,
    _BRONZE_PEOPLE,
    _SILVER_NAME_VARIATIONS,
    _SILVER_PERSON_IDENTIFIERS,
    _SILVER_PERSONS,
    PeopleAndNamesSilverTransform,
    SilverRegisterResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_spark_df(columns: list[str], rows: int = 3) -> MagicMock:
    """Return a mock Spark DataFrame that supports the chain used by the transform."""
    df = MagicMock()
    df.columns = columns
    df.count.return_value = rows

    # withColumnRenamed and dropDuplicates must return a fresh mock with the
    # same interface so the chain .withColumnRenamed(...).dropDuplicates(...)
    # does not raise AttributeError.
    df.withColumnRenamed.return_value = df
    df.dropDuplicates.return_value = df
    df.filter.return_value = df
    return df


def _make_transform() -> tuple[PeopleAndNamesSilverTransform, MagicMock, MagicMock]:
    """Return (transform, mock_spark, mock_writer)."""
    spark = MagicMock()
    writer = MagicMock()
    transform = PeopleAndNamesSilverTransform(spark=spark, writer=writer)
    return transform, spark, writer


# ---------------------------------------------------------------------------
# SilverRegisterResult
# ---------------------------------------------------------------------------


class TestSilverRegisterResult:
    def test_total_rows_sums_all_tables(self):
        result = SilverRegisterResult(persons_rows=10, person_identifiers_rows=25, name_variations_rows=5)
        assert result.total_rows == 40

    def test_zero_rows(self):
        result = SilverRegisterResult(persons_rows=0, person_identifiers_rows=0, name_variations_rows=0)
        assert result.total_rows == 0


# ---------------------------------------------------------------------------
# Table name constants
# ---------------------------------------------------------------------------


class TestTableNameConstants:
    def test_bronze_source_fqns(self):
        assert _BRONZE_PEOPLE == "bronze.people"
        assert _BRONZE_IDENTIFIERS == "bronze.people_identifiers"
        assert _BRONZE_NAME_VARIATIONS == "bronze.name_variations"

    def test_silver_target_fqns(self):
        assert _SILVER_PERSONS == "silver.persons"
        assert _SILVER_PERSON_IDENTIFIERS == "silver.person_identifiers"
        assert _SILVER_NAME_VARIATIONS == "silver.name_variations"


# ---------------------------------------------------------------------------
# _run_persons
# ---------------------------------------------------------------------------


class TestRunPersons:
    def test_reads_bronze_people_table(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_persons("2026-05-11", "run-001")

        spark.read.format.assert_called_once_with("iceberg")
        spark.read.format.return_value.load.assert_called_once_with(_BRONZE_PEOPLE)

    def test_renames_identifier_to_person_id(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_persons("2026-05-11", "run-001")

        rename_calls = [c.args[0] for c in df.withColumnRenamed.call_args_list]
        assert "identifier" in rename_calls

    def test_writes_to_silver_persons(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_persons("2026-05-11", "run-001")

        writer.dynamic_overwrite.assert_called_once()
        call_kwargs = writer.dynamic_overwrite.call_args.kwargs
        assert call_kwargs["fqn"] == _SILVER_PERSONS
        assert call_kwargs["snapshot_date"] == "2026-05-11"

    def test_returns_row_count(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"], rows=7)
        spark.read.format.return_value.load.return_value = df

        count = transform._run_persons("2026-05-11", "run-001")

        assert count == 7

    def test_deduplicates_on_person_id(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_persons("2026-05-11", "run-001")

        df.dropDuplicates.assert_called_once_with(["person_id"])


# ---------------------------------------------------------------------------
# _run_person_identifiers
# ---------------------------------------------------------------------------


class TestRunPersonIdentifiers:
    def test_reads_bronze_identifiers_table(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "key_source", "key_value", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_person_identifiers("2026-05-11", "run-001")

        spark.read.format.return_value.load.assert_called_once_with(_BRONZE_IDENTIFIERS)

    def test_renames_key_source_to_source_system(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "key_source", "key_value", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_person_identifiers("2026-05-11", "run-001")

        rename_calls = [c.args[0] for c in df.withColumnRenamed.call_args_list]
        assert "key_source" in rename_calls

    def test_renames_key_value_to_source_identifier(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "key_source", "key_value", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_person_identifiers("2026-05-11", "run-001")

        rename_calls = [c.args[0] for c in df.withColumnRenamed.call_args_list]
        assert "key_value" in rename_calls

    def test_writes_to_silver_person_identifiers(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "key_source", "key_value", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_person_identifiers("2026-05-11", "run-001")

        call_kwargs = writer.dynamic_overwrite.call_args.kwargs
        assert call_kwargs["fqn"] == _SILVER_PERSON_IDENTIFIERS


# ---------------------------------------------------------------------------
# _run_name_variations
# ---------------------------------------------------------------------------


class TestRunNameVariations:
    def test_reads_bronze_name_variations_table(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_name_variations("2026-05-11", "run-001")

        spark.read.format.return_value.load.assert_called_once_with(_BRONZE_NAME_VARIATIONS)

    def test_deduplicates_on_identifier_and_name(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_name_variations("2026-05-11", "run-001")

        df.dropDuplicates.assert_called_once_with(["identifier", "name"])

    def test_writes_to_silver_name_variations(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform._run_name_variations("2026-05-11", "run-001")

        call_kwargs = writer.dynamic_overwrite.call_args.kwargs
        assert call_kwargs["fqn"] == _SILVER_NAME_VARIATIONS


# ---------------------------------------------------------------------------
# run_all
# ---------------------------------------------------------------------------


class TestRunAll:
    def test_returns_silver_register_result(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        result = transform.run_all("2026-05-11", "run-001")

        assert isinstance(result, SilverRegisterResult)

    def test_calls_all_three_writes(self):
        transform, spark, writer = _make_transform()
        df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"])
        spark.read.format.return_value.load.return_value = df

        transform.run_all("2026-05-11", "run-001")

        assert writer.dynamic_overwrite.call_count == 3

    def test_total_rows_is_sum(self):
        transform, spark, writer = _make_transform()

        # Give each Bronze table a different row count so we can verify summation
        call_count = 0
        row_counts = [10, 25, 5]

        def side_effect_load(fqn):
            nonlocal call_count
            df = _make_spark_df(["identifier", "name", "_snapshot_date", "_ingested_at"], rows=row_counts[call_count])
            call_count += 1
            return df

        spark.read.format.return_value.load.side_effect = side_effect_load

        result = transform.run_all("2026-05-11", "run-001")

        assert result.persons_rows == 10
        assert result.person_identifiers_rows == 25
        assert result.name_variations_rows == 5
        assert result.total_rows == 40

    def test_from_spark_classmethod(self):
        spark = MagicMock()
        mock_writer = MagicMock()
        # SparkIcebergWriter is imported lazily inside from_spark; patch at source
        with patch("cip.transform.shared.writers.SparkIcebergWriter.from_spark", return_value=mock_writer):
            transform = PeopleAndNamesSilverTransform.from_spark(spark)
        assert transform._spark is spark
        assert transform._writer is mock_writer
