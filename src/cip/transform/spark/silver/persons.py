from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

# Bronze source tables
_BRONZE_PEOPLE = TableName.bronze("register_people")
_BRONZE_IDENTIFIERS = TableName.bronze("register_identifiers")
_BRONZE_NAME_VARIATIONS = TableName.bronze("register_name_variations")

# Silver target tables
_SILVER_PERSONS = TableName.silver("persons")
_SILVER_PERSON_IDENTIFIERS = TableName.silver("person_identifiers")
_SILVER_NAME_VARIATIONS = TableName.silver("name_variations")


@dataclass(frozen=True)
class SilverRegisterResult:
    persons_rows: int
    person_identifiers_rows: int
    name_variations_rows: int

    @property
    def total_rows(self) -> int:
        return self.persons_rows + self.person_identifiers_rows + self.name_variations_rows


class RegisterSilverTransform:
    """
    Promotes the three Bronze Register tables to Silver.

    Bronze → Silver contract per table:

    register_people → silver.persons
        - Rename: identifier → person_id
        - Rename: _ingested_at → _bronze_loaded_at  (Silver _ingested_at is fresh)
        - Deduplicate on person_id (MAX _snapshot_date wins — latest snapshot)

    register_identifiers → silver.person_identifiers
        - Rename: key_source → source_system
        - Rename: key_value  → source_identifier
        - Rename: _ingested_at → _bronze_loaded_at
        - No dedup (partition-level overwrite handles idempotency)

    register_name_variations → silver.name_variations
        - Rename: _ingested_at → _bronze_loaded_at
        - Deduplicate on (identifier, name) within snapshot

    All writes use SparkIcebergWriter.dynamic_overwrite() — the standard
    Silver write mode that replaces only the _snapshot_date partition present
    in the incoming DataFrame.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    @classmethod
    def from_spark(cls, spark: "SparkSession") -> "RegisterSilverTransform":
        from cip.transform.shared.writers import SparkIcebergWriter

        return cls(spark=spark, writer=SparkIcebergWriter.from_spark(spark))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(self, snapshot_date: str, pipeline_run_id: str) -> SilverRegisterResult:
        """
        Run all three Silver register transforms for the given snapshot.

        Args:
            snapshot_date:   ISO date string (YYYY-MM-DD) — must match the
                             Bronze partition being promoted.
            pipeline_run_id: Airflow run_id or manual UUID.

        Returns:
            SilverRegisterResult with row counts per table.
        """
        logger.info(
            "RegisterSilverTransform.run_all started",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        persons_rows = self._run_persons(snapshot_date, pipeline_run_id)
        identifiers_rows = self._run_person_identifiers(snapshot_date, pipeline_run_id)
        name_var_rows = self._run_name_variations(snapshot_date, pipeline_run_id)

        result = SilverRegisterResult(
            persons_rows=persons_rows,
            person_identifiers_rows=identifiers_rows,
            name_variations_rows=name_var_rows,
        )
        logger.info(
            "RegisterSilverTransform.run_all complete",
            extra={
                "snapshot_date": snapshot_date,
                "persons_rows": persons_rows,
                "person_identifiers_rows": identifiers_rows,
                "name_variations_rows": name_var_rows,
                "total_rows": result.total_rows,
            },
        )
        return result

    # ------------------------------------------------------------------
    # Internal transforms
    # ------------------------------------------------------------------

    def _run_persons(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = self._spark.read.format("iceberg").load(_BRONZE_PEOPLE).filter(f"{META.SNAPSHOT_DATE} = '{snapshot_date}'")

        # Rename identifier → person_id; _ingested_at → _bronze_loaded_at
        df = (
            df.withColumnRenamed("identifier", "person_id").withColumnRenamed(META.INGESTED_AT, META.BRONZE_LOADED_AT)
            # Deduplicate: keep one row per person_id (latest _snapshot_date wins)
            .dropDuplicates(["person_id"])
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_PERSONS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="people.csv",
        )
        logger.info("silver.persons written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count

    def _run_person_identifiers(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = (
            self._spark.read.format("iceberg")
            .load(_BRONZE_IDENTIFIERS)
            .filter(f"{META.SNAPSHOT_DATE} = '{snapshot_date}'")
        )

        # Rename key_source → source_system, key_value → source_identifier
        df = (
            df.withColumnRenamed("key_source", "source_system")
            .withColumnRenamed("key_value", "source_identifier")
            .withColumnRenamed(META.INGESTED_AT, META.BRONZE_LOADED_AT)
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_PERSON_IDENTIFIERS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="people.csv",
        )
        logger.info("silver.person_identifiers written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count

    def _run_name_variations(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = (
            self._spark.read.format("iceberg")
            .load(_BRONZE_NAME_VARIATIONS)
            .filter(f"{META.SNAPSHOT_DATE} = '{snapshot_date}'")
        )

        df = (
            df.withColumnRenamed(META.INGESTED_AT, META.BRONZE_LOADED_AT)
            # Dedup on (identifier, name) — names.csv may have duplicates within a snapshot
            .dropDuplicates(["identifier", "name"])
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_NAME_VARIATIONS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="names.csv",
        )
        logger.info("silver.name_variations written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
