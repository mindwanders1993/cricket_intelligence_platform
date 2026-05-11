# src/cip/transform/polars/bronze/register_loader.py
#
# Bronze loader for the Cricsheet Register pipeline.
#
# Wires ParsedRegister (three Polars LazyFrames from parse.py) to
# PolarsIcebergWriter, which handles catalog connection, table creation,
# metadata injection, and PyArrow write.
#
# Idempotency:
#   We use create_and_append() which creates the table on first run.
#   Partition-level idempotency (overwrite, not duplicate) is achieved by
#   having the Airflow DAG delete the _snapshot_date partition before
#   calling this loader — standard "delete-then-insert" pattern for
#   Bronze loads where data volume is small (Register << 50k rows/day).
#
#   For a pure-loader-level overwrite (no DAG dependency), call
#   loader.overwrite_snapshot() instead of loader.load().
#
# Table targets (catalog: cricket, namespace: bronze):
#   cricket.bronze.register_people          ← ParsedRegister.persons
#   cricket.bronze.register_identifiers     ← ParsedRegister.person_identifiers
#   cricket.bronze.register_name_variations ← ParsedRegister.name_variations
#
# Called by:
#   orchestration/airflow/dags/dag_ingest_cricsheet_register.py
#
# Usage:
#   from cip.transform.polars.bronze.register_loader import RegisterLoader
#   result = RegisterLoader.from_settings().load(parsed)

from __future__ import annotations

import time
from dataclasses import dataclass, field

from cip.common.contracts.enums import Layer
from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger
from cip.ingestion.register.parse import ParsedRegister
from cip.transform.shared.writers import PolarsIcebergWriter

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Canonical Bronze table FQNs for the Register pipeline
# These use the BRONZE_TABLES registry in TableName — any typo raises ValueError
# at import time, not at runtime.
# ---------------------------------------------------------------------------
TABLE_PERSONS = TableName.bronze("register_people")
TABLE_PERSON_IDENTIFIERS = TableName.bronze("register_identifiers")
TABLE_NAME_VARIATIONS = TableName.bronze("register_name_variations")

# Partition column — must match IcebergProperties and the DAG's delete filter
PARTITION_COL = META.SNAPSHOT_DATE  # "_snapshot_date"


# ===========================================================================
# Result dataclass
# ===========================================================================


@dataclass
class LoadResult:
    """
    Returned by RegisterLoader.load() or overwrite_snapshot().

    Attributes:
        persons_rows:        Rows written to cricket.bronze.register_people
        identifiers_rows:    Rows written to cricket.bronze.register_identifiers
        name_variations_rows: Rows written to cricket.bronze.register_name_variations
        snapshot_date:       The _snapshot_date partition that was written
        pipeline_run_id:     The Airflow / test run ID passed in
        duration_seconds:    Wall-clock time for all three writes
    """

    persons_rows: int
    identifiers_rows: int
    name_variations_rows: int
    snapshot_date: str
    pipeline_run_id: str
    duration_seconds: float = 0.0
    tables: list[str] = field(
        default_factory=lambda: [
            TABLE_PERSONS,
            TABLE_PERSON_IDENTIFIERS,
            TABLE_NAME_VARIATIONS,
        ]
    )

    @property
    def total_rows(self) -> int:
        return self.persons_rows + self.identifiers_rows + self.name_variations_rows


# ===========================================================================
# RegisterLoader
# ===========================================================================


class RegisterLoader:
    """
    Writes a ParsedRegister to Bronze Iceberg tables.

    Delegates all catalog/PyArrow/schema work to PolarsIcebergWriter
    from transform.shared.writers — this class only handles:
        1. Collecting LazyFrames to DataFrames
        2. Calling the writer for each of the 3 tables
        3. Logging + returning a LoadResult

    Two public methods:
        load()               → create_and_append (safe for first run)
        overwrite_snapshot() → delete partition first, then append
                               (idempotent re-run without DAG coordination)
    """

    def __init__(self, writer: PolarsIcebergWriter | None = None) -> None:
        self._writer = writer or PolarsIcebergWriter.from_settings()

    @classmethod
    def from_settings(cls) -> "RegisterLoader":
        """Build loader using platform settings (MinIO + Iceberg REST catalog)."""
        return cls(writer=PolarsIcebergWriter.from_settings())

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def load(self, parsed: ParsedRegister) -> LoadResult:
        """
        Write all three Bronze Register tables.

        Uses create_and_append() — safe for first run and incremental loads.
        If you need idempotent re-runs without DAG-level partition deletion,
        use overwrite_snapshot() instead.

        Args:
            parsed: Output of RegisterParser.parse()

        Returns:
            LoadResult with row counts and timing
        """
        logger.info(
            "RegisterLoader.load() starting",
            extra={
                "snapshot_date": parsed.snapshot_date,
                "pipeline_run_id": parsed.pipeline_run_id,
            },
        )

        started = time.monotonic()

        persons_rows = self._write(
            lf=parsed.persons,
            table=TABLE_PERSONS,
            parsed=parsed,
            source_file="people.csv",
        )

        identifiers_rows = self._write(
            lf=parsed.person_identifiers,
            table=TABLE_PERSON_IDENTIFIERS,
            parsed=parsed,
            source_file="people.csv",
        )

        name_vars_rows = self._write(
            lf=parsed.name_variations,
            table=TABLE_NAME_VARIATIONS,
            parsed=parsed,
            source_file="names.csv",
        )

        duration = round(time.monotonic() - started, 3)

        result = LoadResult(
            persons_rows=persons_rows,
            identifiers_rows=identifiers_rows,
            name_variations_rows=name_vars_rows,
            snapshot_date=parsed.snapshot_date,
            pipeline_run_id=parsed.pipeline_run_id,
            duration_seconds=duration,
        )

        logger.info(
            "RegisterLoader.load() complete",
            extra={
                "total_rows": result.total_rows,
                "persons": persons_rows,
                "identifiers": identifiers_rows,
                "name_variations": name_vars_rows,
                "duration_seconds": duration,
            },
        )

        return result

    def overwrite_snapshot(self, parsed: ParsedRegister) -> LoadResult:
        """
        Idempotent re-run: delete the _snapshot_date partition then append.

        Use this when re-processing a snapshot that was already loaded
        (e.g. Cricsheet published a corrected people.csv for the same date).

        The delete+append is NOT atomic at the loader level — Iceberg's
        snapshot isolation ensures readers see either the old or new snapshot,
        never a partial state.

        Args:
            parsed: Output of RegisterParser.parse()

        Returns:
            LoadResult with row counts and timing
        """
        logger.info(
            "RegisterLoader.overwrite_snapshot() — deleting partition before write",
            extra={
                "snapshot_date": parsed.snapshot_date,
                "pipeline_run_id": parsed.pipeline_run_id,
                "partition_col": PARTITION_COL,
            },
        )

        self._delete_snapshot_partition(parsed.snapshot_date)
        return self.load(parsed)

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _write(
        self,
        lf,
        table: str,
        parsed: ParsedRegister,
        source_file: str,
    ) -> int:
        """
        Collect a LazyFrame and write it via PolarsIcebergWriter.

        Returns row count written (0 if frame is empty — no write attempted).
        """
        df = lf.collect()

        if df.is_empty():
            logger.warning(
                "Empty frame — skipping write",
                extra={"table": table, "snapshot_date": parsed.snapshot_date},
            )
            return 0

        return self._writer.create_and_append(
            df=df,
            fqn=table,
            snapshot_date=parsed.snapshot_date,
            layer=Layer.BRONZE,
            partition_cols=[PARTITION_COL],
            pipeline_run_id=parsed.pipeline_run_id,
            source_file=source_file,
            source_url="https://cricsheet.org/register/",
        )

    def _delete_snapshot_partition(self, snapshot_date: str) -> None:
        """
        Delete the _snapshot_date partition from all three Bronze tables.

        Uses PyIceberg's delete_files() with an equality predicate.
        No-ops gracefully if the partition does not exist yet (first run).
        """
        from pyiceberg.expressions import EqualTo

        catalog = self._writer._catalog

        for fqn in [TABLE_PERSONS, TABLE_PERSON_IDENTIFIERS, TABLE_NAME_VARIATIONS]:
            try:
                table = catalog.load_table(fqn)
                table.delete(EqualTo(PARTITION_COL, snapshot_date))
                logger.info(
                    "Deleted partition",
                    extra={"table": fqn, "snapshot_date": snapshot_date},
                )
            except Exception as exc:
                # Table may not exist yet on a true first run — that's fine
                logger.warning(
                    "Partition delete skipped (table may not exist yet)",
                    extra={"table": fqn, "error": str(exc)},
                )
