# src/cip/ingestion/people_and_names/parse.py
#
# Parser for Cricsheet Register normalized frames.
#
# Responsibilities:
#   - Derive silver_persons from people.csv (core identity columns)
#   - Derive silver_person_identifiers by unpivoting all key_* columns
#   - Derive silver_name_variations from names.csv
#   - Handle schema drift: new key_* columns are auto-detected, no code change needed
#   - All inputs and outputs are Polars LazyFrames — no I/O in this module
#
# Called by:
#   src/cip/transform/polars/bronze/people_and_names_loader.py
#
# Usage:
#   from cip.ingestion.people_and_names.parse import PeopleAndNamesParser
#   from cip.ingestion.people_and_names.normalize import PeopleAndNamesNormalizer
#
#   normalized = PeopleAndNamesNormalizer.from_settings().run("2026-05-11", "run-001")
#   parsed = PeopleAndNamesParser.parse(normalized)
#   parsed.persons.collect()
#   parsed.person_identifiers.collect()
#   parsed.name_variations.collect()

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from cip.common.logging import get_logger
from cip.ingestion.people_and_names.normalize import NormalizedPeopleAndNames

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Column constants — people.csv core identity columns
# These are stable columns that will NEVER be unpivoted.
# Everything else starting with key_ is an external identifier.
# ---------------------------------------------------------------------------

# Core columns present in every Cricsheet people.csv snapshot.
# Verified against the live register: identifier, name, unique_name are the
# only non-key_ columns. gender/dob existed in older formats but are absent
# from the current Cricsheet register — do not add them without confirming
# they have returned to the source file.
_PEOPLE_CORE_COLS = [
    "identifier",  # Cricsheet's own UUID — primary key across all match data
    "name",  # canonical display name
    "unique_name",  # slug-style unique name used in filenames
]

# Metadata columns added by normalize.py — must be preserved on all output frames
_META_COLS = [
    "_snapshot_date",
    "_ingested_at",
    "_pipeline_run_id",
    "_row_hash",
]

# names.csv column names
_NAMES_IDENTIFIER_COL = "identifier"
_NAMES_NAME_COL = "name"


# ===========================================================================
# Result dataclass
# ===========================================================================


@dataclass
class ParsedPeopleAndNames:
    """
    Output of PeopleAndNamesParser.parse().

    Three LazyFrames shaped for Silver layer loading:

    persons:
        One row per person. Core identity columns + metadata.
        Schema: identifier, name, unique_name + _meta cols

    person_identifiers:
        One row per (identifier, key_source) pair. Unpivoted from key_* columns.
        Schema: identifier, key_source, key_value + _meta cols
        key_source: the column name stripped of "key_" prefix (e.g. "cricinfo")
        key_value:  the external ID value (null rows are dropped)

    name_variations:
        One row per (identifier, name) pair from names.csv.
        Schema: identifier, name + _meta cols
    """

    persons: pl.LazyFrame
    person_identifiers: pl.LazyFrame
    name_variations: pl.LazyFrame
    snapshot_date: str
    pipeline_run_id: str


# ===========================================================================
# PeopleAndNamesParser
# ===========================================================================


class PeopleAndNamesParser:
    """
    Stateless transformer: NormalizedPeopleAndNames → ParsedPeopleAndNames.

    All methods are classmethods — no instantiation needed.
    No I/O, no side effects.

    Usage:
        parsed = PeopleAndNamesParser.parse(normalized)
    """

    @classmethod
    def parse(cls, normalized: NormalizedPeopleAndNames) -> ParsedPeopleAndNames:
        """
        Parse a NormalizedPeopleAndNames into three Silver-shaped LazyFrames.

        Args:
            normalized: Output of PeopleAndNamesNormalizer.run()

        Returns:
            ParsedPeopleAndNames with .persons, .person_identifiers, .name_variations
        """
        logger.info(
            "Parsing Register frames",
            extra={
                "snapshot_date": normalized.snapshot_date,
                "pipeline_run_id": normalized.pipeline_run_id,
            },
        )

        persons_lf = cls._parse_persons(normalized.people)
        identifiers_lf = cls._parse_person_identifiers(normalized.people)
        name_variations_lf = cls._parse_name_variations(normalized.names)

        logger.info(
            "Register parse complete",
            extra={
                "snapshot_date": normalized.snapshot_date,
                "persons_columns": persons_lf.columns,
                "identifier_columns": identifiers_lf.columns,
                "name_variation_columns": name_variations_lf.columns,
            },
        )

        return ParsedPeopleAndNames(
            persons=persons_lf,
            person_identifiers=identifiers_lf,
            name_variations=name_variations_lf,
            snapshot_date=normalized.snapshot_date,
            pipeline_run_id=normalized.pipeline_run_id,
        )

    # -----------------------------------------------------------------------
    # silver_persons
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_persons(cls, people_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Select core identity columns from people.csv frame.

        Only columns in _PEOPLE_CORE_COLS are kept; all key_* columns are
        dropped here (they go to person_identifiers instead).

        Metadata columns (_snapshot_date etc.) are preserved.

        Schema drift handling:
            - If a new NON-key_ column appears in people.csv, it is silently
              dropped here. Add it to _PEOPLE_CORE_COLS explicitly to include it.
            - If a core column is missing, a null column of type Utf8 is added
              so the downstream Bronze schema never breaks.
        """
        available_cols = people_lf.columns

        # Build selection: core cols that exist + null placeholders for missing ones
        select_exprs = []
        for col in _PEOPLE_CORE_COLS:
            if col in available_cols:
                select_exprs.append(pl.col(col))
            else:
                logger.warning(
                    "Core column missing from people.csv — inserting null placeholder",
                    extra={"missing_column": col},
                )
                select_exprs.append(pl.lit(None).cast(pl.Utf8).alias(col))

        # Always keep metadata columns
        for col in _META_COLS:
            if col in available_cols:
                select_exprs.append(pl.col(col))

        return people_lf.select(select_exprs)

    # -----------------------------------------------------------------------
    # silver_person_identifiers
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_person_identifiers(cls, people_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Unpivot all key_* columns from people.csv into long format.

        Input (wide):
            identifier | key_cricinfo | key_espn | key_wikidata | ...
            p001       | 253802       | 4321     | Q123456      | ...

        Output (long):
            identifier | key_source | key_value | _meta cols
            p001       | cricinfo   | 253802    |
            p001       | espn       | 4321      |
            p001       | wikidata   | Q123456   |

        - key_source = column name with "key_" prefix stripped
        - key_value  = the external ID string
        - Rows where key_value IS NULL are dropped (person has no ID for that source)
        - Schema drift: new key_* columns are auto-detected — no code change needed

        This design means adding a new external registry to Cricsheet
        (e.g. key_icc) automatically flows into silver_person_identifiers
        without any pipeline changes.
        """
        available_cols = people_lf.columns
        key_cols = [c for c in available_cols if c.startswith("key_")]

        if not key_cols:
            logger.warning(
                "No key_* columns found in people.csv — returning empty identifiers frame",
                extra={"available_columns": available_cols},
            )
            # Return an empty frame with the correct schema
            return pl.LazyFrame(
                schema={
                    "identifier": pl.Utf8,
                    "key_source": pl.Utf8,
                    "key_value": pl.Utf8,
                    **{col: pl.Utf8 for col in _META_COLS},
                }
            )

        logger.info(
            "Unpivoting key columns",
            extra={"key_columns": key_cols, "count": len(key_cols)},
        )

        # Columns to carry through the unpivot as index
        index_cols = ["identifier"] + _META_COLS

        # Polars melt (unpivot): wide → long on key_* columns
        # id_vars       = columns to keep as-is on every row
        # value_vars    = columns to unpivot (key_* cols become rows)
        # variable_name = name of the new "column name" column
        # value_name    = name of the new "column value" column
        identifiers_lf = (
            people_lf.select(index_cols + key_cols).melt(
                id_vars=index_cols,
                value_vars=key_cols,
                variable_name="key_source",
                value_name="key_value",
            )
            # Strip the "key_" prefix from key_source so "key_cricinfo" → "cricinfo"
            .with_columns(pl.col("key_source").str.strip_prefix("key_").alias("key_source"))
            # Drop rows where there is no external ID for this source
            .filter(pl.col("key_value").is_not_null())
        )

        return identifiers_lf

    # -----------------------------------------------------------------------
    # silver_name_variations
    # -----------------------------------------------------------------------

    @classmethod
    def _parse_name_variations(cls, names_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Parse names.csv into silver_name_variations.

        names.csv has two columns: identifier, name
        One person can have multiple name rows (aliases, transliterations, etc.)

        Output schema:
            identifier | name | _meta cols

        - Rows with null identifier or null name are dropped (orphan / corrupt rows)
        - Deduplication within the same snapshot is applied on (identifier, name)
          to guard against any duplicate rows in the source file
        """
        available_cols = names_lf.columns

        select_exprs = [pl.col(_NAMES_IDENTIFIER_COL), pl.col(_NAMES_NAME_COL)]
        for col in _META_COLS:
            if col in available_cols:
                select_exprs.append(pl.col(col))

        return (
            names_lf.select(select_exprs)
            .filter(pl.col(_NAMES_IDENTIFIER_COL).is_not_null() & pl.col(_NAMES_NAME_COL).is_not_null())
            .unique(
                subset=[_NAMES_IDENTIFIER_COL, _NAMES_NAME_COL],
                keep="first",
            )
        )

    @classmethod
    def parse_from_dfs(
        cls,
        people_df: "pl.DataFrame",
        names_df: "pl.DataFrame",
        snapshot_date: str,
        pipeline_run_id: str,
    ) -> "ParsedPeopleAndNames":
        """
        Parse directly from two raw Polars DataFrames.

        Used by the Airflow task_parse callable when frames are reconstructed
        from MinIO-staged Parquet. Wraps them in a NormalizedPeopleAndNames and
        delegates to the canonical parse() classmethod.

        Args:
            people_df:        Collected people DataFrame (from staged Parquet)
            names_df:         Collected names DataFrame (from staged Parquet)
            snapshot_date:    ISO date string — e.g. "2026-05-11"
            pipeline_run_id:  Airflow run_id or test UUID
        """
        from datetime import datetime, timezone

        from cip.ingestion.people_and_names.normalize import NormalizedPeopleAndNames

        normalized = NormalizedPeopleAndNames(
            people=people_df.lazy(),  # .people not .people_df
            names=names_df.lazy(),  # .names not .names_df
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            ingested_at=datetime.now(tz=timezone.utc),  # required field
        )
        return cls.parse(normalized)
