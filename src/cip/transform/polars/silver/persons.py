from __future__ import annotations

from dataclasses import dataclass

from cip.common.contracts.enums import Layer
from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger

logger = get_logger(__name__)

_BRONZE_PEOPLE = TableName.bronze("people")
_BRONZE_IDENTIFIERS = TableName.bronze("people_identifiers")
_BRONZE_NAME_VARIATIONS = TableName.bronze("name_variations")

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


class PolarsPeopleAndNamesSilverTransform:
    """
    Promotes the three Bronze Register tables to Silver using Polars + PyIceberg.

    Bronze → Silver contract per table:

    register_people → silver.persons
        - Filter to snapshot_date partition
        - Rename: identifier → person_id
        - Rename: _ingested_at → _bronze_loaded_at  (fresh _ingested_at added by writer)
        - Deduplicate on person_id

    register_identifiers → silver.person_identifiers
        - Rename: key_source → source_system
        - Rename: key_value  → source_identifier
        - Rename: _ingested_at → _bronze_loaded_at

    register_name_variations → silver.name_variations
        - Rename: _ingested_at → _bronze_loaded_at
        - Deduplicate on (identifier, name)

    All writes use PolarsIcebergWriter.overwrite_partition() which replaces
    only the _snapshot_date partition — idempotent for re-runs.
    """

    def __init__(self, reader, writer) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    def from_settings(cls) -> "PolarsPeopleAndNamesSilverTransform":
        from cip.transform.shared.readers import PolarsIcebergReader
        from cip.transform.shared.writers import PolarsIcebergWriter

        return cls(reader=PolarsIcebergReader.from_settings(), writer=PolarsIcebergWriter.from_settings())

    def run_all(self, snapshot_date: str, pipeline_run_id: str) -> SilverRegisterResult:
        logger.info(
            "PolarsPeopleAndNamesSilverTransform.run_all started",
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
            "PolarsPeopleAndNamesSilverTransform.run_all complete",
            extra={
                "snapshot_date": snapshot_date,
                "persons_rows": persons_rows,
                "person_identifiers_rows": identifiers_rows,
                "name_variations_rows": name_var_rows,
                "total_rows": result.total_rows,
            },
        )
        return result

    def _run_persons(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = self._reader.read_table(
            _BRONZE_PEOPLE,
            row_filter=f"{META.SNAPSHOT_DATE} = '{snapshot_date}'",
        )
        df = df.rename({"identifier": "person_id", META.INGESTED_AT: META.BRONZE_LOADED_AT}).unique(
            subset=["person_id"]
        )
        row_count = self._writer.overwrite_partition(
            df=df,
            fqn=_SILVER_PERSONS,
            snapshot_date=snapshot_date,
            layer=Layer.SILVER,
            partition_cols=["_snapshot_date"],
            pipeline_run_id=pipeline_run_id,
            source_file="people.csv",
            source_url="https://cricsheet.org/register/",
        )
        logger.info("silver.persons written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count

    def _run_person_identifiers(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = self._reader.read_table(
            _BRONZE_IDENTIFIERS,
            row_filter=f"{META.SNAPSHOT_DATE} = '{snapshot_date}'",
        )
        df = df.rename(
            {
                "key_source": "source_system",
                "key_value": "source_identifier",
                META.INGESTED_AT: META.BRONZE_LOADED_AT,
            }
        ).unique(subset=["identifier", "source_system", "source_identifier"])
        row_count = self._writer.overwrite_partition(
            df=df,
            fqn=_SILVER_PERSON_IDENTIFIERS,
            snapshot_date=snapshot_date,
            layer=Layer.SILVER,
            partition_cols=["_snapshot_date"],
            pipeline_run_id=pipeline_run_id,
            source_file="people.csv",
            source_url="https://cricsheet.org/register/",
        )
        logger.info("silver.person_identifiers written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count

    def _run_name_variations(self, snapshot_date: str, pipeline_run_id: str) -> int:
        df = self._reader.read_table(
            _BRONZE_NAME_VARIATIONS,
            row_filter=f"{META.SNAPSHOT_DATE} = '{snapshot_date}'",
        )
        df = df.rename({META.INGESTED_AT: META.BRONZE_LOADED_AT}).unique(subset=["identifier", "name"])
        row_count = self._writer.overwrite_partition(
            df=df,
            fqn=_SILVER_NAME_VARIATIONS,
            snapshot_date=snapshot_date,
            layer=Layer.SILVER,
            partition_cols=["_snapshot_date"],
            pipeline_run_id=pipeline_run_id,
            source_file="names.csv",
            source_url="https://cricsheet.org/register/",
        )
        logger.info("silver.name_variations written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
