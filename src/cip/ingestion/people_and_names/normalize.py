# src/cip/ingestion/people_and_names/normalize.py
#
# Normalizer for Cricsheet Register CSV files read from the MinIO landing zone.
#
# Responsibilities:
#   - Read people.csv + names.csv from MinIO as raw bytes → Polars LazyFrame
#   - Enforce all-string schema (infer_schema=False) for source fidelity
#   - Attach 4 system metadata columns to every row:
#       _snapshot_date    — partition key, from the landing path
#       _ingested_at      — UTC timestamp of this Bronze load run
#       _pipeline_run_id  — Airflow run_id for lineage
#       _row_hash         — SHA-256 of all value columns (row-level dedup key)
#   - Return NormalizedPeopleAndNames(people=LazyFrame, names=LazyFrame)
#   - No writes — caller (bronze writer) decides what to do with the frames
#
# Called by:
#   src/cip/transform/polars/bronze/people_and_names_loader.py
#
# Usage:
#   from cip.ingestion.people_and_names.normalize import PeopleAndNamesNormalizer
#   normalizer = PeopleAndNamesNormalizer.from_settings()
#   result = normalizer.run(snapshot_date="2026-05-11", pipeline_run_id="run-001")
#   result.people.collect()   # polars.DataFrame

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import datetime, timezone

import polars as pl

from cip.common.logging import get_logger
from cip.ingestion.io.minio import MinIOClient

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Register file names — must match download.py REGISTER_SOURCES keys
# ---------------------------------------------------------------------------
_PEOPLE_FILE = "people.csv"
_NAMES_FILE = "names.csv"

# Source-file prefix — must match StorageSettings.prefix_people_and_names_csv
_SOURCE_FILE_PREFIX = "people_and_names/csv"


# ===========================================================================
# Result dataclass
# ===========================================================================


@dataclass
class NormalizedPeopleAndNames:
    """
    Output of PeopleAndNamesNormalizer.run().
    Both frames are LazyFrames — call .collect() only when writing.
    """

    people: pl.LazyFrame
    names: pl.LazyFrame
    snapshot_date: str
    pipeline_run_id: str
    ingested_at: datetime

    @property
    def people_columns(self) -> list[str]:
        return self.people.columns

    @property
    def names_columns(self) -> list[str]:
        return self.names.columns


# ===========================================================================
# PeopleAndNamesNormalizer
# ===========================================================================


class PeopleAndNamesNormalizer:
    """
    Reads Register CSVs from MinIO landing zone and normalizes them
    into all-string Polars LazyFrames with system metadata columns.

    Instantiation:
        normalizer = PeopleAndNamesNormalizer.from_settings()

    Usage:
        result = normalizer.run(snapshot_date="2026-05-11", pipeline_run_id="run-001")
        people_df = result.people.collect()
    """

    def __init__(self, minio_client: MinIOClient) -> None:
        self._minio = minio_client

    @classmethod
    def from_settings(cls) -> "PeopleAndNamesNormalizer":
        return cls(minio_client=MinIOClient.from_settings())

    # -----------------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------------

    def run(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
    ) -> NormalizedPeopleAndNames:
        """
        Read both Register CSVs for a given snapshot_date from landing,
        attach metadata columns, return NormalizedPeopleAndNames.

        Args:
            snapshot_date:   ISO date string (YYYY-MM-DD) — matches landing partition.
            pipeline_run_id: Airflow run_id or manual identifier for lineage.

        Returns:
            NormalizedPeopleAndNames with .people and .names as LazyFrames.

        Raises:
            FileNotFoundError: If either CSV is absent in the landing zone.
            ValueError:        If either frame has zero rows after parsing.
        """
        ingested_at = datetime.now(timezone.utc)

        logger.info(
            "Starting Register normalization",
            extra={
                "snapshot_date": snapshot_date,
                "pipeline_run_id": pipeline_run_id,
            },
        )

        people_lf = self._read_and_normalize(
            source_file=_PEOPLE_FILE,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            ingested_at=ingested_at,
        )

        names_lf = self._read_and_normalize(
            source_file=_NAMES_FILE,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            ingested_at=ingested_at,
        )

        logger.info(
            "Register normalization complete",
            extra={
                "snapshot_date": snapshot_date,
                "people_columns": people_lf.columns,
                "names_columns": names_lf.columns,
            },
        )

        return NormalizedPeopleAndNames(
            people=people_lf,
            names=names_lf,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            ingested_at=ingested_at,
        )

    # -----------------------------------------------------------------------
    # Internal: read one file from MinIO landing → normalized LazyFrame
    # -----------------------------------------------------------------------

    def _read_and_normalize(
        self,
        source_file: str,
        snapshot_date: str,
        pipeline_run_id: str,
        ingested_at: datetime,
    ) -> pl.LazyFrame:
        """
        Download one CSV from MinIO landing, parse all-string, add metadata columns.
        Returns a LazyFrame — no data is materialized until .collect() is called.
        """
        object_key = f"{_SOURCE_FILE_PREFIX}/snapshot_date={snapshot_date}/{source_file}"

        logger.info(
            "Reading from landing",
            extra={"object_key": object_key, "source_file": source_file},
        )

        # --- Read raw bytes from MinIO ---
        raw_bytes = self._minio.read_object(object_key)
        if not raw_bytes:
            raise FileNotFoundError(
                f"Landing object not found: {object_key}. "
                f"Run the download task for snapshot_date={snapshot_date} first."
            )

        # --- Parse CSV with all-string schema (source fidelity) ---
        # infer_schema_length=0 forces every column to Utf8 (string).
        # null_values=[""] treats empty fields as null — Cricsheet CSVs use
        # empty string for optional identity keys.
        df = pl.read_csv(
            io.BytesIO(raw_bytes),
            infer_schema_length=0,  # all columns → Utf8, no type inference
            null_values=[""],  # empty string → null
            truncate_ragged_lines=True,  # Cricsheet CSVs occasionally have trailing commas
            encoding="utf-8",
        )

        if df.is_empty():
            raise ValueError(
                f"{source_file} parsed to an empty DataFrame for "
                f"snapshot_date={snapshot_date}. Possible corrupt landing file."
            )

        logger.info(
            "CSV parsed",
            extra={
                "source_file": source_file,
                "rows": len(df),
                "columns": df.columns,
            },
        )

        # --- Attach system metadata columns ---
        df = self._attach_metadata(
            df=df,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            ingested_at=ingested_at,
        )

        return df.lazy()

    # -----------------------------------------------------------------------
    # Internal: metadata column attachment
    # -----------------------------------------------------------------------

    @staticmethod
    def _attach_metadata(
        df: pl.DataFrame,
        snapshot_date: str,
        pipeline_run_id: str,
        ingested_at: datetime,
    ) -> pl.DataFrame:
        """
        Add 4 system metadata columns to a DataFrame.

        Columns added:
            _snapshot_date    (Utf8)      — ISO date string, partition key
            _ingested_at      (Datetime)  — UTC timestamp of this load run
            _pipeline_run_id  (Utf8)      — Airflow run_id for lineage
            _row_hash         (Utf8)      — SHA-256 of all value columns (row-level dedup)

        Metadata columns are prefixed with _ and placed AFTER all source columns.
        This ensures source schema is untouched and metadata is visually distinct.
        """
        # _row_hash: concatenate all source column values per row, hash with SHA-256.
        # Null values are represented as the literal string "__NULL__" so that
        # (null, "a") and ("a", null) produce different hashes.
        value_cols = df.columns  # before adding metadata

        df = df.with_columns(
            pl.concat_str(
                [pl.col(c).fill_null("__NULL__") for c in value_cols],
                separator="|",
            )
            .map_elements(
                lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest(),
                return_dtype=pl.Utf8,
            )
            .alias("_row_hash")
        )

        # Remaining metadata — scalar values broadcast to all rows
        df = df.with_columns(
            [
                pl.lit(snapshot_date).alias("_snapshot_date"),
                pl.lit(ingested_at).alias("_ingested_at"),
                pl.lit(pipeline_run_id).alias("_pipeline_run_id"),
            ]
        )

        return df

    # -----------------------------------------------------------------------
    # Utility: expose landing object key for downstream use
    # -----------------------------------------------------------------------

    @staticmethod
    def landing_object_key(source_file: str, snapshot_date: str) -> str:
        """Return the MinIO object key for a given file and snapshot date."""
        return f"{_SOURCE_FILE_PREFIX}/snapshot_date={snapshot_date}/{source_file}"
