# platform/transform/shared/partitioning.py
#
# Partition strategy definitions for the Cricket Intelligence Platform.
#
# Design rules:
#   1. Bronze  — partitioned by _snapshot_date (daily ingestion cadence)
#   2. Silver  — partitioned by match_type + _snapshot_date (query pattern)
#   3. Gold    — partitioned by season (analytical query pattern)
#   4. Every strategy is documented with the query pattern it optimises for.
#
# Usage:
#   from cip.transform.shared.partitioning import PartitionStrategy
#   spec = PartitionStrategy.for_table(Layer.SILVER, "deliveries")
#   # → PartitionSpec with (match_type IDENTITY, _snapshot_date DAY)

from __future__ import annotations

from dataclasses import dataclass

from cip.common.contracts.enums import Layer
from cip.common.contracts.naming import META
from cip.common.logging import get_logger

logger = get_logger(__name__)


# ===========================================================================
# Partition field descriptors
# ===========================================================================


@dataclass(frozen=True)
class PartitionField:
    """
    Describes one field in a partition spec.

    Attributes:
        column:      Source column name
        transform:   Iceberg transform: "identity" | "day" | "month" | "year" |
                     "bucket[N]" | "truncate[N]"
        name:        Human-readable partition name (shown in MinIO path)
    """

    column: str
    transform: str
    name: str

    def to_dict(self) -> dict[str, str]:
        return {"column": self.column, "transform": self.transform, "name": self.name}


@dataclass(frozen=True)
class TablePartitionSpec:
    """
    Complete partition specification for one Iceberg table.

    Attributes:
        table_fqn:      Iceberg FQN pattern (e.g. "cricket.bronze.*")
        fields:         Ordered list of PartitionField
        query_pattern:  Description of the query this partitioning optimises
        write_order:    Suggested sort columns for write distribution
    """

    table_fqn: str
    fields: list[PartitionField]
    query_pattern: str
    write_order: list[str]

    @property
    def column_names(self) -> list[str]:
        return [f.column for f in self.fields]

    def to_pyiceberg_spec(self):
        """
        Convert to a PyIceberg PartitionSpec object.
        Requires PyIceberg to be installed.
        """
        from pyiceberg.partitioning import PartitionField as PyIcebergField
        from pyiceberg.partitioning import PartitionSpec as PyIcebergSpec
        from pyiceberg.transforms import (
            BucketTransform,
            DayTransform,
            IdentityTransform,
            MonthTransform,
            TruncateTransform,
            YearTransform,
        )

        def _resolve_transform(t: str):
            t = t.lower().strip()
            if t == "identity":
                return IdentityTransform()
            if t == "day":
                return DayTransform()
            if t == "month":
                return MonthTransform()
            if t == "year":
                return YearTransform()
            if t.startswith("bucket["):
                n = int(t[7:-1])
                return BucketTransform(n)
            if t.startswith("truncate["):
                n = int(t[9:-1])
                return TruncateTransform(n)
            raise ValueError(f"Unknown Iceberg transform: {t}")

        py_fields = [
            PyIcebergField(
                source_id=i + 1,  # field_id — resolved at table creation
                field_id=1000 + i,
                name=f.name,
                transform=_resolve_transform(f.transform),
            )
            for i, f in enumerate(self.fields)
        ]
        return PyIcebergSpec(*py_fields)

    def to_spark_partition_expr(self) -> list[str]:
        """
        Convert to a list of Spark partitionedBy() expressions.
        Used by SparkIcebergWriter.create_table_if_not_exists().

        Returns strings like:
            ["days(_snapshot_date)", "identity(match_type)"]
        """
        expr_map = {
            "identity": lambda col, _: col,
            "day": lambda col, _: f"days({col})",
            "month": lambda col, _: f"months({col})",
            "year": lambda col, _: f"years({col})",
            "bucket": lambda col, n: f"bucket({n}, {col})",
            "truncate": lambda col, n: f"truncate({col}, {n})",
        }
        result = []
        for f in self.fields:
            t = f.transform.lower()
            if t.startswith("bucket["):
                n = int(t[7:-1])
                result.append(expr_map["bucket"](f.column, n))
            elif t.startswith("truncate["):
                n = int(t[9:-1])
                result.append(expr_map["truncate"](f.column, n))
            else:
                fn = expr_map.get(t)
                if fn:
                    result.append(fn(f.column, None))
        return result


# ===========================================================================
# Partition strategy registry
# ===========================================================================


class PartitionStrategy:
    """
    Central registry of partition strategies for all platform tables.

    Layer conventions:
        BRONZE  → DAY(_snapshot_date)
                  Optimises: "give me all files ingested on date X for replay"
                  One partition per ingestion run day.

        SILVER  → IDENTITY(match_type) + DAY(_snapshot_date)
                  Optimises: "all T20 deliveries processed on date X"
                  Enables format-specific Silver scans without full table reads.

        GOLD    → IDENTITY(season) [facts]
                  IDENTITY(match_type) + IDENTITY(season) [delivery fact]
                  Optimises: "IPL 2024 season analysis" — most dashboard queries
                  are season-scoped and format-scoped.
    """

    # -----------------------------------------------------------------------
    # Bronze partition specs
    # -----------------------------------------------------------------------
    # All Bronze tables use the same strategy:
    # daily ingestion → one partition per snapshot_date
    # Keeps replay simple: reprocess date X → overwrite date X partition

    _BRONZE_DEFAULT = TablePartitionSpec(
        table_fqn="cricket.bronze.*",
        fields=[
            PartitionField(
                column=META.SNAPSHOT_DATE,
                transform="day",
                name="snapshot_day",
            ),
        ],
        query_pattern="Replay and incremental Bronze reads by ingestion date",
        write_order=[META.SNAPSHOT_DATE],
    )

    # -----------------------------------------------------------------------
    # Silver partition specs — per table
    # -----------------------------------------------------------------------

    _SILVER_SPECS: dict[str, TablePartitionSpec] = {
        "matches": TablePartitionSpec(
            table_fqn="cricket.silver.matches",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="day", name="snapshot_day"),
            ],
            query_pattern="Match lookup by format (T20, ODI, Test) for Silver joins",
            write_order=["match_type", "season", META.SNAPSHOT_DATE],
        ),
        "deliveries": TablePartitionSpec(
            table_fqn="cricket.silver.deliveries",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="day", name="snapshot_day"),
            ],
            query_pattern=(
                "Delivery scans by format — largest Silver table. "
                "match_type pruning cuts scan by ~80% for T20-only queries."
            ),
            write_order=["match_type", "match_id", "innings_number", "over", "ball"],
        ),
        "innings": TablePartitionSpec(
            table_fqn="cricket.silver.innings",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="day", name="snapshot_day"),
            ],
            query_pattern="Innings totals by format",
            write_order=["match_type", "match_id", "innings_number"],
        ),
        "wickets": TablePartitionSpec(
            table_fqn="cricket.silver.wickets",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="day", name="snapshot_day"),
            ],
            query_pattern="Wicket event analysis by format",
            write_order=["match_type", "match_id"],
        ),
        "persons": TablePartitionSpec(
            table_fqn="cricket.silver.persons",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern=(
                "Register-derived dimension — small table, monthly partitioning "
                "is sufficient. Identity resolution updates arrive monthly."
            ),
            write_order=["person_id"],
        ),
        "person_identifiers": TablePartitionSpec(
            table_fqn="cricket.silver.person_identifiers",
            fields=[
                PartitionField(column="source_system", transform="identity", name="source_system"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern=(
                "Cross-source identifier lookups — partitioned by source_system "
                "so CricInfo lookups don't scan ESPNcricinfo rows."
            ),
            write_order=["source_system", "source_identifier"],
        ),
        "name_variations": TablePartitionSpec(
            table_fqn="cricket.silver.name_variations",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern="Alias lookups — small table, monthly partition sufficient",
            write_order=["identifier", "name"],
        ),
        "match_players": TablePartitionSpec(
            table_fqn="cricket.silver.match_players",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column=META.SNAPSHOT_DATE, transform="day", name="snapshot_day"),
            ],
            query_pattern="Player participation by format",
            write_order=["match_type", "match_id", "person_id"],
        ),
        "match_officials": TablePartitionSpec(
            table_fqn="cricket.silver.match_officials",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern="Official assignment lookup — low cardinality",
            write_order=["match_id", "official_role"],
        ),
        "teams": TablePartitionSpec(
            table_fqn="cricket.silver.teams",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern="Team dimension — near-static, monthly partition sufficient",
            write_order=["team_name"],
        ),
        "venues": TablePartitionSpec(
            table_fqn="cricket.silver.venues",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern="Venue dimension — near-static",
            write_order=["venue_name"],
        ),
        "competitions": TablePartitionSpec(
            table_fqn="cricket.silver.competitions",
            fields=[
                PartitionField(column=META.SNAPSHOT_DATE, transform="month", name="snapshot_month"),
            ],
            query_pattern="Competition dimension",
            write_order=["competition_name"],
        ),
    }

    # -----------------------------------------------------------------------
    # Gold partition specs — per table
    # -----------------------------------------------------------------------

    _GOLD_SPECS: dict[str, TablePartitionSpec] = {
        "fact_delivery": TablePartitionSpec(
            table_fqn="cricket.gold.fact_delivery",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column="season", transform="identity", name="season"),
            ],
            query_pattern=(
                "Primary fact table — most dashboard queries filter by "
                "match_type='T20' AND season='2024'. "
                "Double partition cuts scan dramatically on IPL/T20 dashboards."
            ),
            write_order=["match_type", "season", "match_id", "innings_number", "over", "ball"],
        ),
        "fact_innings": TablePartitionSpec(
            table_fqn="cricket.gold.fact_innings",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column="season", transform="identity", name="season"),
            ],
            query_pattern="Innings totals by format and season",
            write_order=["match_type", "season", "match_id"],
        ),
        "fact_match_result": TablePartitionSpec(
            table_fqn="cricket.gold.fact_match_result",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column="season", transform="identity", name="season"),
            ],
            query_pattern="Match outcome analysis — toss, result, margin by season",
            write_order=["match_type", "season", "match_date"],
        ),
        "fact_player_match": TablePartitionSpec(
            table_fqn="cricket.gold.fact_player_match",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
                PartitionField(column="season", transform="identity", name="season"),
            ],
            query_pattern="Player per-match aggregates — feature store for ML",
            write_order=["match_type", "season", "player_id"],
        ),
        # Dimensions — unpartitioned (small, fits in single file per table)
        "dim_player": TablePartitionSpec(
            table_fqn="cricket.gold.dim_player",
            fields=[],
            query_pattern="Small dimension — no partitioning needed",
            write_order=["player_id"],
        ),
        "dim_match": TablePartitionSpec(
            table_fqn="cricket.gold.dim_match",
            fields=[
                PartitionField(column="match_type", transform="identity", name="match_type"),
            ],
            query_pattern="Match dimension partitioned by format for format-specific joins",
            write_order=["match_type", "season", "match_date"],
        ),
        "dim_team": TablePartitionSpec(
            table_fqn="cricket.gold.dim_team",
            fields=[],
            query_pattern="Small dimension",
            write_order=["team_id"],
        ),
        "dim_venue": TablePartitionSpec(
            table_fqn="cricket.gold.dim_venue",
            fields=[],
            query_pattern="Small dimension",
            write_order=["venue_id"],
        ),
        "dim_competition": TablePartitionSpec(
            table_fqn="cricket.gold.dim_competition",
            fields=[],
            query_pattern="Small dimension",
            write_order=["competition_id"],
        ),
        "dim_date": TablePartitionSpec(
            table_fqn="cricket.gold.dim_date",
            fields=[
                PartitionField(column="year", transform="identity", name="year"),
            ],
            query_pattern="Date spine — partitioned by year",
            write_order=["date_day"],
        ),
    }

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    @classmethod
    def for_table(cls, layer: Layer, table_name: str) -> TablePartitionSpec:
        """
        Return the partition spec for a specific table.
        Falls back to layer default if no table-specific spec exists.

        Args:
            layer:       Layer enum
            table_name:  Short table name (not FQN)

        Returns:
            TablePartitionSpec

        Example:
            spec = PartitionStrategy.for_table(Layer.SILVER, "deliveries")
            spark_exprs = spec.to_spark_partition_expr()
            # → ["match_type", "days(_snapshot_date)"]
        """
        if layer == Layer.BRONZE:
            return cls._BRONZE_DEFAULT

        spec_map = {
            Layer.SILVER: cls._SILVER_SPECS,
            Layer.GOLD: cls._GOLD_SPECS,
        }.get(layer, {})

        spec = spec_map.get(table_name)
        if spec is None:
            logger.warning(
                "No partition spec found — using unpartitioned",
                extra={"layer": layer, "table": table_name},
            )
            return TablePartitionSpec(
                table_fqn=f"cricket.{layer}.{table_name}",
                fields=[],
                query_pattern="No partitioning — table not in registry",
                write_order=[],
            )
        return spec

    @classmethod
    def partition_columns(cls, layer: Layer, table_name: str) -> list[str]:
        """
        Return just the partition column names for a table.
        Used as a quick check without constructing the full spec.

        Example:
            cols = PartitionStrategy.partition_columns(Layer.GOLD, "fact_delivery")
            # → ["match_type", "season"]
        """
        return cls.for_table(layer, table_name).column_names

    @classmethod
    def write_order(cls, layer: Layer, table_name: str) -> list[str]:
        """
        Return the recommended write sort order for a table.
        Passed to Iceberg SortOrder at table creation for co-location.
        """
        return cls.for_table(layer, table_name).write_order

    @classmethod
    def all_silver_specs(cls) -> dict[str, TablePartitionSpec]:
        return dict(cls._SILVER_SPECS)

    @classmethod
    def all_gold_specs(cls) -> dict[str, TablePartitionSpec]:
        return dict(cls._GOLD_SPECS)

    @classmethod
    def describe(cls, layer: Layer, table_name: str) -> None:
        """Print a human-readable description of a table's partition strategy."""
        spec = cls.for_table(layer, table_name)
        print(f"\nTable:         {spec.table_fqn}")
        print(f"Query pattern: {spec.query_pattern}")
        print(f"Partitions:    {[f'{f.column} ({f.transform})' for f in spec.fields]}")
        print(f"Write order:   {spec.write_order}\n")
